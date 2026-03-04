#!/usr/bin/env python

# %%
import os
import logging
import intake
import zarr_tools
from pathlib import Path

logger = logging.getLogger("coarsen_ifs")
logger.setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO)
# %%

catalog_file = "https://digital-earths-global-hackathon.github.io/catalog/catalog.yaml"
catalog_location = "EU"
catalog_source = "ifs_tco3999_rcbmf"
catalog_params = {
    "zoom": 11,
    "time": "PT1H",
}

sim = intake.open_catalog(catalog_file)[catalog_location][catalog_source]

ds = sim(**catalog_params, chunks=dict(time=4, level=5)).to_dask()
logger.info("original dataset:\n%s", ds)
# %%

ds_coarsened = ds.isel(time=slice(0, 48)).coarsen(value=64).mean()

logger.info("coarsened dataset:\n%s", ds_coarsened)
# %%

rename_dict = {
    "time": "time",
    "lat": "lat",
    "lon": "lon",
    "crs": "crs",
    "level": "lev",
    "value": "cell",
    "u": "ua",  # zonal wind
    "v": "va",  # meridional wind
    "w": "omega",  # vertical velocity (pressure velocity)
    "z": "z",  # geopotential
    "t": "ta",  # temperature
    "q": "hus",  # specific humidity
    "r": "hur",  # relative humidity
    "tp": "pr",  # total precipitation
    "tcwv": "prw",  # precipitable water vapor
    "sp": "ps",  # surface pressure
    "msl": "psl",  # mean sea level pressure
    "10u": "uas",  # 10m u wind
    "10v": "vas",  # 10m v wind
    "2t": "tas",  # 2m temperature
    "ttr": "rlut",  # TOA outgoing longwave radiation
    "slhf": "hflsd",  # surface latent heat flux
    "sshf": "hflsu",  # surface sensible heat flux
    # 'lsp': 'prs',             # large scale precipitation (not consistent with other models)
    # 'orog': 'ELEV',           # NOT available
    # 'sfcWind': 'sfcWind',     # NOT available
    # 'zg': 'zg',               # NOT available
}
# %%
ds_out = ds_coarsened.rename(rename_dict)[
    [
        "ua",
        "va",
        "omega",
        "z",
        "ta",
        "hus",
        "hur",
        "pr",
        "prw",
        "ps",
        "psl",
        "uas",
        "vas",
        "tas",
        "rlut",
        "hflsd",
        "hflsu",
    ]
]
logger.info("final dataset:\n%s", ds_out)

# %%
if not os.path.isdir("/scratch/k/k202134/ifs_coarsened.zarr"):
    zarr_tools.create_zarr_structure(
        "/scratch/k/k202134/ifs_coarsened.zarr", ds_out, timechunk=4, order=8
    )
zarr_tools.write_parts(
    ds_out, Path("/scratch/k/k202134/ifs_coarsened.zarr"), time_chunk=4
)
# %%
