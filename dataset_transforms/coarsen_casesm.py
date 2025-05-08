#!/usr/bin/env python

# %%

import intake
import xarray as xr
import chunk_tools
import zarr_tools
from pathlib import Path
import logging

logging.basicConfig()
logger = logging.getLogger("coarsen_casesm")
logger.setLevel(logging.DEBUG)
cat = intake.open_catalog("/public/home/florain/catalog/CN/main.yaml")

# %%


def rechunk_dataset(name:str, params, zoom_in:int, outfile:Path):
    logger.info (f"starting to process {name} with parameters {params} and {zoom_in=}")
    logger.info(f"Output is going to '{outfile}'")
    outfile = Path (outfile)
    read_chunk_size = chunk_tools.compute_chunksize(zoom_in - 1) * 4
    time_chunk = 16*4**10//read_chunk_size
    logger.info (f" {time_chunk=}")
    in_ds = cat[name](**params, zoom=zoom_in, chunks = dict (cell=read_chunk_size, time=time_chunk, lev=5)).to_dask()
    out_ds = in_ds.coarsen(cell=4).mean()
    logger.info (f" {out_ds=}")
    if not outfile.exists():
        zarr_tools.create_zarr_structure(outfile, out_ds, time_chunk, order=zoom_in-1)
    zarr_tools.write_parts(out_ds, outfile, time_chunk*8)
    return out_ds


# %%
zoom_in=9
time_map = {'PT3H': '3h', 'PT1H': '1h', 'PT6H': '6h'}
dims = {'PT3H': '2d', 'PT1H': '2d', 'PT6H': '3d'}
for ct, ft in time_map.items():
    # outfile=Path(f"/scratch/k/k207030/CAS_ESM_coarsened_{zoom_in-1}.zarr")
    outfile=Path(f"/data2/share/florain/CAS-ESM2_10km_cumulus_{dims[ct]}{ft}_z{zoom_in-1}.zarr")
    try:
        out_ds = rechunk_dataset(name="CASESM2_cumulus", params=dict(time=ct), zoom_in=zoom_in, outfile=outfile)
    except Exception as e:
        logger.error(f"Failed to process {name} with parameters {params}: {e}")

# %%
chunk_tools.get_chunksizes(out_ds, 'time', order=9, timechunk=16)
# %%
