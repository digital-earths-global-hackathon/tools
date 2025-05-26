#!/usr/bin/env python

# %%

import intake
import chunk_tools
import zarr_tools
from pathlib import Path
import logging
import healpix_tools

logging.basicConfig()
logger = logging.getLogger("coarsen_casesm")
logger.setLevel(logging.DEBUG)
cat = intake.open_catalog("/public/home/florain/catalog/CN/main.yaml")

# %%


def rechunk_dataset(name: str, params, zoom_in: int, outfile: Path):
    logger.info(f"starting to process {name} with parameters {params} and {zoom_in=}")
    logger.info(f"Output is going to '{outfile}'")
    outfile = Path(outfile)
    zoom_out = zoom_in - 1
    chunks = compute_read_chunks(zoom_out)
    read_params = dict(**params, zoom=zoom_in, chunks=chunks)
    in_ds = cat[name](**read_params).to_dask()
    out_ds = process_ds(in_ds, zoom_out)
    write_ds(out_ds, outfile, zoom_out, chunks["time"])
    return out_ds


def write_ds(out_ds, outfile, zoom_out, time_chunk):
    if not outfile.exists():
        zarr_tools.create_zarr_structure(outfile, out_ds, time_chunk, order=zoom_out)
    zarr_tools.write_parts(out_ds, outfile, time_chunk * 8)


def process_ds(in_ds, zoom_out):
    out_ds = in_ds.coarsen(cell=4).mean()
    out_ds = healpix_tools.attach_crs(out_ds, zoom_out)
    return out_ds


def compute_read_chunks(zoom_out):
    read_chunk_size = chunk_tools.compute_chunksize(zoom_out) * 4
    time_chunk = 18 * 4**10 // read_chunk_size
    chunks = dict(cell=read_chunk_size, time=time_chunk, lev=5)
    logger.debug(f" {time_chunk=}")
    return chunks


# %%
zoom_in = 9
time_map = {"PT3H": "3h", "PT1H": "1h", "PT6H": "6h"}
dims = {"PT3H": "2d", "PT1H": "2d", "PT6H": "3d"}
for zoom_in in range (8,0,-1):
    for variant in [ "cumulus", "nocumulus"]:
        name = f"casesm2_{variant}"
        for ct, ft in time_map.items():
            params = dict(time=ct)
            # outfile=Path(f"/scratch/k/k207030/CAS_ESM_coarsened_{zoom_in-1}.zarr")
            outfile = Path(
                f"/data2/share/florain/CAS-ESM2_10km_{variant}_{dims[ct]}{ft}_z{zoom_in - 1}.zarr"
            )
            try:
                out_ds = rechunk_dataset(
                    name=name, params=params, zoom_in=zoom_in, outfile=outfile
                )
            except Exception as e:
                logger.error(f"Failed to process {name} with parameters {params}: {e}")
                raise e

