import pkg_resources
import pytest
import pandas

from pizza_box.handlers import APBBinFileHandler


basename = "ff2df6b8-eff5-4bb5-9718-69698ab6fa3c"


def _get_file(basename, extension):
    file = pkg_resources.resource_filename("pizza_box", f"tests/example_data/{basename}.{extension}")
    return file


@pytest.fixture
def bin_file():
    bin_file = _get_file(basename, "bin")
    return bin_file


@pytest.fixture
def txt_file():
    txt_file = _get_file(basename, "txt")
    return txt_file


def test_handler_init(bin_file):
    h = APBBinFileHandler(bin_file)

    assert hasattr(h, "raw_data")
    assert hasattr(h, "df")


def test_handler_call(bin_file):
    h = APBBinFileHandler(bin_file)
    data = h()
    assert data is not None
    assert type(data) is pandas.DataFrame
    assert list(data.columns) == ["timestamp", "i0", "it", "ir", "iff", "aux1", "aux2", "aux3", "aux4"]
    assert data["i0"][0] == -2512861.0
    assert data.shape == (30500, 9)
