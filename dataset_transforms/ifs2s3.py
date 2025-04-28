import sys
import zarr
from ifs_to_zarr import rechunk_dataset
import numpy as np

storage_options = {
    "endpoint_url": "https://s3.eu-dkrz-1.dkrz.cloud",
    "key": "ifs-fesom",
    "secret": "***"
}
s3prefix = "s3://wrcp-hackathon/data/IFS-FESOM"


variables2d = {
   "time": "time",
   "rlut": lambda ttr: ttr / -3600,  # 0 - net
   "rsut": lambda tisr, tsr: (tisr - tsr) / 3600,  # incoming - net, for W/m2
   "pr": lambda tp: tp * (1000 / 3600),  # pr: kg m-2 s-1
   "psl": "msl",
   "ts": "skt",  # surface
   "uas": "10u",
   "vas": "10v",
   "tas": "2t",
   "ts": "skt",
   "clwvi": "tclw",
   "clivi": "tciw",
   "hflsd": lambda slhf: slhf / 3600,  # we checked sign
   "hfssd": lambda sshf: sshf / 3600,
   "rlutcs": lambda ttrc: ttrc / -3600,
   "rlus": lambda strd, str: (strd - str) / 3600,  # we checked sign
   "rluscs": lambda strc: strc / -3600,
   "rlds": lambda strd: strd / 3600,
   "rsdt": lambda tisr: tisr / 3600,
   "rsus": lambda ssrd, ssr: (ssrd - ssr) / 3600,
   "rsuscs": lambda ssrdc, ssrc: (ssrdc - ssrc) / 3600,
   "rsds": lambda ssrd: ssrd / 3600,
   "rsdscs": lambda ssrdc: ssrdc / 3600,
   "prs": lambda sf: sf * (1000 / 3600),
   "prw": "tcwv",
   "ps": "sp",
   "tauu": lambda ewss: ewss / 3600,
   "tauv": lambda nsss: nsss / 3600,
   "clt": "tcc",
   "swe": lambda sd: sd * 1000,
   "mrso": lambda swvl1, swvl2, swvl3, swvl4: swvl1 + swvl2 + swvl3 + swvl4,  # change unit still
   "siconc": "ci"
}

# what do we need to do here?
variables3d = {
    "ta": "t"
}


def add_crs(z, nside):
    crs = z.require_array(name="crs", dtype=np.float32, shape=(1,))
    crs.attrs["_ARRAY_DIMENSIONS"] = ("crs",)
    crs.attrs["grid_mapping_name"] = "healpix"
    crs.attrs["healpix_nside"] = nside
    crs.attrs["healpix_order"] = "nest"


if sys.argv[1] == "2D_hourly_healpix128":
    chunks_per_dim = {
        2: (7*24, 4**6),
    }

    zarr_out = zarr.open(s3prefix + "/2D_hourly_healpix128.zarr", mode="a",
                         storage_options=storage_options,
                         zarr_version=2)

    zarr_in = zarr.open(
        "reference::/work/bm1235/u233156/gribscan_cycle4_3999_RCBMF/gribscan_1h1d_2D_healpix128/jsons/sfc.dir/atm2d.json",
        mode="r")
    # do we need something from here?
    #reference::/work/bm1235/u233156/gribscan_cycle4_3999_RCBMF/gribscan_1h1d_2D_healpix128/jsons/hl.dir/atm2d.json

    rechunk_dataset(zarr_in, zarr_out, chunks_per_dim, variables2d)

    add_crs(zarr_out, 128)


elif sys.argv[1] == "3D_hourly_healpix128":

    chunks_per_dim = {
        3: (7*24, 5, 4**6),
    }

    zarr_in = zarr.open(
        "reference::/work/bm1235/u233156/gribscan_cycle4_3999_RCBMF/gribscan_1h1d_2D_healpix128/jsons/pl.dir/atm3d.json"
        , mode="r")

    zarr_out = zarr.open(s3prefix+"/3D_hourly_healpix128.zarr", mode="a",
                         storage_options=storage_options,
                         zarr_version=2)

    rechunk_dataset(zarr_in, zarr_out, chunks_per_dim, variables3d)

    add_crs(zarr_out, 128)


# finally consolidate the dataset
zarr.consolidate_metadata(zarr_out.store)
