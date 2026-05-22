import healpix_tools

import xarray as xr
import pytest
import numpy as np
import easygems.healpix as egh

@pytest.fixture
def raw_ds(zoom: int = 0) -> xr.Dataset:
    data = xr.DataArray(np.arange(12 * 4**zoom).astype("float32"), dims="cell")
    return xr.Dataset(
        dict(data=data),
    )


def test_attach_crs(raw_ds: xr.Dataset):
    zoom = 0
    ds = healpix_tools.attach_crs(raw_ds, zoom=zoom)
    assert egh.get_nside(ds['data']) == 2**zoom
    assert egh.get_nest(ds['data'])
