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
    status_filename, start = check_for_status(path)
    outds = handle_timeless_variables(outds, path, start)
    handle_time_dependent_variables(outds, path, time_chunk, status_filename, start)


def check_for_status(path):
    status_filename = path / Path(".write_status")
    try:
        with open(status_filename) as status:
            start = int(status.read())
            logger.info(f"Found status file. Starting from {start}.")
    except FileNotFoundError:
        logger.warning(
            f"Could not read start from {status_filename}. Starting from zero."
        )
        start = 0
    return status_filename, start


def handle_timeless_variables(outds, path, start):
    timeless = {x: outds[x] for x in outds.variables if "time" not in outds[x].dims}
    logger.debug(f"{list(timeless)=}")
    if start == 0:
        wds = xr.Dataset(timeless)
        wds.to_zarr(path, mode="r+")

    drop = [k for k in outds.variables if k in timeless]
    logger.debug(f" Dropping {drop}")
    outds = outds.drop_vars(drop)
    return outds


def handle_time_dependent_variables(outds, path, time_chunk, status_filename, start):
    for i in range(start, len(outds.time), time_chunk):
        tslice = slice(i, i + time_chunk)
        for var_name in outds:
            write_variable_chunk(outds, var_name, path, tslice)
        with open(status_filename, mode="w") as status:
            status.write(str(i + time_chunk))
        logger.info(f"Processed time steps starting at {i}")


def write_variable_chunk(outds, var_name, path, tslice):
    logger.debug(f"Writing {var_name}, dims: {outds[var_name].dims}")
    wds = xr.Dataset({var_name: outds[var_name]})
    (wds.isel(time=tslice).to_zarr(path, region=dict(time=tslice)))
