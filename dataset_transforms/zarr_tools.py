import logging
import xarray as xr
from pathlib import Path
import chunk_tools
import zarr

logging.basicConfig()
logger = logging.getLogger("zarr_tools")
logger.setLevel(logging.INFO)


def create_zarr_structure(path, outds, timechunk, order):
    store = create_store(path)
    outds.to_zarr(
        store,
        encoding=chunk_tools.get_encodings(
            outds=outds, timechunk=timechunk, order=order
        ),
        compute=False,
    )
    store.close()


def create_store(path):
    store = zarr.storage.DirectoryStore(
        path, normalize_keys=False, dimension_separator="/"
    )
    return store


def write_parts(outds: xr.Dataset, path: Path, time_chunk: int):
    status_filename = path / Path(".write_status")
    try:
        with open(status_filename) as status:
            start = int(status.read())
            logger.info("Found status file. Starting from {start}.")
    except FileNotFoundError:
        logger.warning(
            f"Could not read start from {status_filename}. Starting from zero."
        )
        start = 0
    for i in range(start, len(outds.time), time_chunk):
        tslice = slice(i, i + time_chunk)
        for x in outds:
            wds = xr.Dataset({x: outds[x]})
            (wds.isel(time=tslice).to_zarr(path, region=dict(time=tslice)))
        with open(status_filename, mode="w") as status:
            status.write(str(i + time_chunk))
        logger.info(f"Processed time steps starting at {i}")
    if status_filename.exists:
        status_filename.unlink()
