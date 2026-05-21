import pathlib

import numpy as np
import xarray as xr


refdata = pathlib.Path(__file__).parent / "data"


def test_read_netcdf_uncompressed():
    """Test reading of a simple uncompressed variable from NetCDF."""
    ds = xr.open_dataset(refdata / "test_float32.nc")
    assert np.array_equal(ds["value"].values, np.arange(128))


def test_read_netcdf_zstd():
    """Test reading of a Zstd compressed (CDO) variable from NetCDF."""
    ds = xr.open_dataset(refdata / "test_float32_zstd_cdo.nc")
    assert np.array_equal(ds["value"].values, np.arange(128))
