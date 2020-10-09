from databroker.assets.handlers_base import HandlerBase


class APBBinFileHandler(HandlerBase):
    "Read electrometer *.bin files"

    def __init__(self, fpath):
        # It's a text config file, which we don't store in the resources yet, parsing for now
        fpath_txt = f"{os.path.splitext(fpath)[0]}.txt"

        with open(fpath_txt, "r") as fp:
            content = fp.readlines()
            content = [x.strip() for x in content]

        _ = int(content[0].split(":")[1])
        Gains = [int(x) for x in content[1].split(":")[1].split(",")]
        Offsets = [int(x) for x in content[2].split(":")[1].split(",")]
        FAdiv = float(content[3].split(":")[1])
        FArate = float(content[4].split(":")[1])
        trigger_timestamp = float(content[5].split(":")[1].strip().replace(",", "."))

        raw_data = np.fromfile(fpath, dtype=np.int32)

        columns = ["timestamp", "i0", "it", "ir", "iff", "aux1", "aux2", "aux3", "aux4"]
        num_columns = len(columns) + 1  # TODO: figure out why 1
        raw_data = raw_data.reshape((raw_data.size // num_columns, num_columns))

        derived_data = np.zeros((raw_data.shape[0], raw_data.shape[1] - 1))
        derived_data[:, 0] = (
            raw_data[:, -2] + raw_data[:, -1] * 8.0051232 * 1e-9
        )  # Unix timestamp with nanoseconds
        for i in range(num_columns - 2):
            derived_data[:, i + 1] = raw_data[:, i]  # ((raw_data[:, i] ) - Offsets[i]) / Gains[i]

        self.df = pd.DataFrame(data=derived_data, columns=columns)
        self.raw_data = raw_data

    def __call__(self):
        return self.df
