# %%
import xarray as xr
import yaml
import numpy as np
import remap_tools  # local package to outsource the remap functions
import zarr_tools
import logging
from pathlib import Path
import easygems.healpix as egh


# %%
logging.basicConfig()
logger = logging.getLogger("icon2zarr")
logger.setLevel(logging.INFO)

# %%
infiles = {
    "3d": [
        "/work/mh0287/m211032/Icon/Git_lev/icon.XPP.20240717/build.intel-hdint/experiments/slo1774/slo1774_atm_3d_ml_14900101T000000Z.nc",
        "/work/mh0287/m211032/Icon/Git_lev/icon.XPP.20240717/build.intel-hdint/experiments/slo1774/slo1774_atm_3d_ml_16100101T000000Z.nc",
    ],
    "2d": [
        "/work/mh0287/m211032/Icon/Git_lev/icon.XPP.20240717/build.intel-hdint/experiments/slo1774/slo1774_atm_2d_ml_14900101T000000Z.nc",
        "/work/mh0287/m211032/Icon/Git_lev/icon.XPP.20240717/build.intel-hdint/experiments/slo1774/slo1774_atm_2d_ml_16100101T000000Z.nc",
    ],
}


# %%
def convert_files(zoom, subset, output_dir, config):
    files = infiles[subset]
    outfile = Path(f"{output_dir}/ICON_{subset}_z{zoom}.zarr")
    curr_conf = config[subset]
    timechunk = curr_conf["chunks"]["time"]

    ds = xr.open_mfdataset(files, chunks=curr_conf["chunks"], use_cftime=True)
    if subset == "2d":
        out_ds = process_2d(curr_conf, ds)
    elif subset == "3d":
        out_ds = process_3d(curr_conf, ds)
    else:
        raise RuntimeError("Unknown subset type")
    out_ds = remap_tools.remap_delaunay(out_ds, zoom)
    logger.info(f"Trying to write to {outfile}")
    if not outfile.exists():
        zarr_tools.create_zarr_structure(
            path=outfile, outds=out_ds, timechunk=timechunk, order=zoom
        )
    zarr_tools.write_parts(outds=out_ds, path=outfile, time_chunk=24)
    return out_ds, outfile


def process_2d(curr_conf, ds):
    ds = ds.rename(curr_conf["renames"])
    ds = ds.isel(curr_conf["isel"])
    ds = ds.drop_vars(list(curr_conf["isel"]) + curr_conf["drop"])
    ds["lon"] = np.rad2deg(ds["lon"])
    ds["lat"] = np.rad2deg(ds["lat"])
    return ds


def process_3d(curr_conf, ds):
    ds = ds.rename(curr_conf["renames"])
    ds = ds.drop_vars(curr_conf["drop"])
    ds["lon"] = np.rad2deg(ds["lon"])
    ds["lat"] = np.rad2deg(ds["lat"])
    return ds


# %%
output_dir = "/scratch/k/k202134/icon_remapped"
config = yaml.safe_load(open("icon_to_zarr.yaml"))
#! rm -rf /scratch/k/k202134/icon_remapped/ICON_3d_z5.zarr
#! rm -rf /scratch/k/k202134/icon_remapped/ICON_2d_z5.zarr

for subset in ("3d", "2d"):
    for zoom in (5,):
        out_ds, outfile = convert_files(
            zoom=zoom, subset=subset, output_dir=output_dir, config=config
        )

# %%
egh.healpix_show(
    xr.open_dataset(
        "/scratch/k/k202134/icon_remapped/ICON_3d_z5.zarr",
        use_cftime=True,
    )["u"].isel(time=2, height=50)
)

# %%
egh.healpix_show(
    xr.open_dataset(
        "/scratch/k/k202134/icon_remapped/ICON_2d_z5.zarr",
        use_cftime=True,
    )["t_s"].isel(time=2),
    cmap="inferno",
)

# %%
