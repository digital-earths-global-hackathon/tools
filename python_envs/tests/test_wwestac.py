#!/usr/bin/env python3

import pystac_client
import xarray as xr
import easygems.healpix as egh
import matplotlib.pyplot as plt
import xpystac  # noqa: F401 – registers the "stac" engine with xarray, just needs to be there, not actually imported
import intake

catalog = pystac_client.Client.open("https://wwestac.cloud.dkrz.de/stac-fastapi-es/")
item = catalog.get_collection("ngc4008").get_item("ngc4008_P1D_7")
asset = item.assets["disk"]
ds = xr.open_dataset(asset)

cat = intake.open_catalog(
    "https://digital-earths-global-hackathon.github.io/catalog/catalog.yaml"
)
ds2 = cat["online.icon_ngc4008"](zoom=7).to_dask()

assert (ds["tas"].isel(time=-1).values == ds2["tas"].isel(time=-1).values).all()
