import time as ttime

from ophyd import set_and_wait
from ophyd.status import SubscriptionStatus


class FlyerAPB:
    def __init__(self, det, pbs, motor):
        self.name = f'{det.name}-{"-".join([pb.name for pb in pbs])}-flyer'
        self.parent = None
        self.det = det
        self.pbs = pbs  # a list of passed pizza-boxes
        self.motor = motor
        self._motor_status = None

    def kickoff(self, *args, **kwargs):
        set_and_wait(self.det.trig_source, 1)
        # TODO: handle it on the plan level
        # set_and_wait(self.motor, 'prepare')

        def callback(value, old_value, **kwargs):
            print(f"kickoff: {ttime.ctime()} {old_value} ---> {value}")
            if int(round(old_value)) == 0 and int(round(value)) == 1:
                # Now start mono move
                self._motor_status = self.motor.set("start")
                return True
            else:
                return False

        streaming_st = SubscriptionStatus(self.det.streaming, callback)
        self.det.stream.set(1)
        self.det.stage()
        for pb in self.pbs:
            pb.stage()
            pb.kickoff()
        return streaming_st

    def complete(self):
        def callback_det(value, old_value, **kwargs):
            print(f"complete: {ttime.time()} {old_value} ---> {value}")
            if int(round(old_value)) == 1 and int(round(value)) == 0:
                return True
            else:
                return False

        streaming_st = SubscriptionStatus(self.det.streaming, callback_det)

        def callback_motor():
            # print(f'callback_motor {ttime.time()}')
            self.det.stream.set(0)
            self.det.complete()
            for pb in self.pbs:
                pb.complete()

        self._motor_status.add_callback(callback_motor)
        print(f"complete operation is happening ({ttime.ctime(ttime.time())})")
        return streaming_st & self._motor_status

    def describe_collect(self):
        return_dict = {
            self.det.name: {
                f"{self.det.name}": {
                    "source": "APB",
                    "dtype": "array",
                    "shape": [-1, -1],
                    "filename_bin": self.det.filename_bin,
                    "filename_txt": self.det.filename_txt,
                    "external": "FILESTORE:",
                }
            }
        }
        # Also do it for all pizza-boxes
        for pb in self.pbs:
            return_dict[pb.name] = pb.describe_collect()[pb.name]

        return return_dict

    def collect_asset_docs(self):
        yield from self.det.collect_asset_docs()
        for pb in self.pbs:
            yield from pb.collect_asset_docs()

    def collect(self):
        self.det.unstage()
        for pb in self.pbs:
            pb.unstage()

        def collect_all():
            for pb in self.pbs:
                yield from pb.collect()
            yield from self.det.collect()

        print(f"collect is being returned ({ttime.ctime(ttime.time())})")
        return collect_all()


# Example:
# flyer_apb = FlyerAPB(det=apb_stream, pbs=[pb9.enc1], motor=hhm)
