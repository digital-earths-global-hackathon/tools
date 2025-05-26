import xarray as xr


def attach_crs(dataset:xr.Dataset, zoom:int)->xr.Dataset:
    """You will need to re-assign the return value to the dataset!"""
    crs = xr.DataArray(
        name="crs",
        attrs={
            "grid_mapping_name": "healpix",
            "healpix_nside": 2**zoom,
            "healpix_order": "nest",
        },
    )
    return dataset.assign_coords(crs=crs)
