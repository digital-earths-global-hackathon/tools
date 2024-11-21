import intake
import easygems.healpix as egh
import matplotlib.pyplot as plt
from matplotlib.image import imread
import numpy as np
from numpy.typing import ArrayLike
import tempfile


def test_healpix_plot() -> None:
    date = "2020-10-28"
    cat = intake.open_catalog("https://data.nextgems-h2020.eu/online.yaml")
    ds = cat["ICON.ngc3028"].to_dask()
    tas = ds.tas.sel(time=date)
    egh.healpix_show(tas)
    with tempfile.NamedTemporaryFile(suffix='png') as tmpfile:
        plt.savefig(tmpfile)
        image = imread(tmpfile)
        assert_equal(np.round(image.mean(axis=(0, 1)) * 10), np.array((8, 8, 8, 10)))
        assert_equal(np.round(image.std(axis=(0, 1)) * 10), np.array((3, 3, 3, 0)))


def assert_equal(a1: ArrayLike, a2: ArrayLike) -> None:
    assert np.array_equal(a1, a2)

