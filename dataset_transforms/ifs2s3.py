import sys
import zarr
from ifs_to_zarr import rechunk_dataset
import numpy as np
from argparse import ArgumentParser


def add_crs(z, nside):
    crs = z.require_array(name="crs", dtype=np.float32, shape=(1,))
    crs.attrs["_ARRAY_DIMENSIONS"] = ("crs",)
    crs.attrs["grid_mapping_name"] = "healpix"
    crs.attrs["healpix_nside"] = nside
    crs.attrs["healpix_order"] = "nest"


parser = ArgumentParser()
parser.add_argument("nside", type=int, choices=[128, 512, 2048])
parser.add_argument("freq", type=str)
parser.add_argument("--only", type=str, nargs="*", default=None)
parser.add_argument("--nprocs", type=int, default=64)

args = parser.parse_args()


def filter_vardict(vardict):
    if args.only is None:
        return vardict
    else:
        return {k: v
                for k, v in vardict.items()
                if k in args.only}


storage_options = {
    "endpoint_url": "https://s3.eu-dkrz-1.dkrz.cloud",
    "key": "ifs-fesom",
    "secret": "***"
}
s3prefix = "s3://wrcp-hackathon/data/IFS-FESOM"

zarr_out = zarr.open(s3prefix + f"/{args.freq}_healpix{args.nside}.zarr", mode="a",
                     storage_options=storage_options,
                     zarr_version=2)

add_crs(zarr_out, args.nside)

if args.freq == "hourly":
    assert args.nside in [128, 2048]

    variables2d = filter_vardict({
        "time": "time",
        "lon": "lon",
        "lat": "lat",
        "rlut": lambda ttr: ttr / -3600,  # 0 - net
        "rsut": lambda tisr, tsr: (tisr - tsr) / 3600,  # incoming - net, for W/m2
        "pr": lambda tp: tp * (1000 / 3600),  # pr: kg m-2 s-1
        "psl": "msl",
        "ts": "skt",  # surface
        "uas": "10u",
        "vas": "10v",
        "tas": "2t",
        "clivi": "tciw",
        "clwvi": "tclw",
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
        "mrso": lambda swvl1, swvl2, swvl3, swvl4: (7 * swvl1 + 21 * swvl2 + 72 * swvl3 + 189 * swvl4) / (7+21+72+189),
        "siconc": "ci",
        "10si": "10si",
        "2d": "2d",
        "blh": "blh",
        "chnk": "chnk",
        "e": "e",
        "fdir": "fdir",
        "hcc": "hcc",
        "i10fg": "i10fg",
        "lcc": "lcc",
        "lgws": "lgws",
        "litota1": "litota1",
        "litoti": "litoti",
        "lsm": "lsm",
        "lsp": "lsp",
        "mcc": "mcc",
        "mgws": "mgws",
        "mtpr": "mtpr",
        "mucape": "mucape",
        "rsn": "rsn",
        "sro": "sro",
        "ssro": "ssro",
        "sst": "sst",
        "stl1": "stl1",
        "stl2": "stl2",
        "stl3": "stl3",
        "stl4": "stl4",
        "tcrw": "tcrw",
        "tcsw": "tcsw",
        "tprate": "tprate",
        "tsrc": "tsrc",
        "z": "z",
    })

    variables2d_hl = filter_vardict({
        v: v for v in (
            "100si",
            "100u",
            "100v",
        )
    })

    variables3d = filter_vardict({
        "level": "level",
        "zg": "z",
        "ua": "u",
        "va": "v",
        "wa" : "w",  # , and for wap it is var120
        "ta" : "t",
        "hur": "r",
        "hus": "q",
        "qall": lambda crwc, cswc, ciwc, clwc: crwc + cswc + ciwc + clwc,
        "clwc": "clwc",
        "ciwc": "ciwc",
        "crwc": "crwc",
        "cswc": "cswc",
        "cc": "cc",
        "pv": "pv",
    })

    variables3d_snow = filter_vardict({
        "level_snow": "level",
        **{v: v for v in
           ('sd',
            'lwcs',
            'avg_sd24')
           }
    })

    if args.nside <= 128:
        chunks_per_dim = {
            2: (7*24, 4**6),
            3: (7*24, 5, 4**6),
        }
    else:
        chunks_per_dim = {
            2: (24, 4**8),
            3: (24, 5, 4**8),
        }

    zarr_in = zarr.open(
        f"reference::/work/bm1235/u233156/gribscan_cycle4_3999_RCBMF/gribscan_1h1d_2D_healpix{args.nside}/jsons/sfc.dir/atm2d.json",
        mode="r")
    rechunk_dataset(zarr_in, zarr_out, chunks_per_dim, variables2d, args.nprocs)

    zarr_in = zarr.open(
        f"reference::/work/bm1235/u233156/gribscan_cycle4_3999_RCBMF/gribscan_1h1d_2D_healpix{args.nside}/jsons/hl.dir/atm2d.json",
        mode="r")
    rechunk_dataset(zarr_in, zarr_out, chunks_per_dim, variables2d_hl, args.nprocs)

    zarr_in = zarr.open(
        f"reference::/work/bm1235/u233156/gribscan_cycle4_3999_RCBMF/gribscan_1h1d_2D_healpix{args.nside}/jsons/pl.dir/atm3d.json"
        , mode="r")
    rechunk_dataset(zarr_in, zarr_out, chunks_per_dim, variables3d, args.nprocs)

    zarr_in = zarr.open(
        f"reference::/work/bm1235/u233156/gribscan_cycle4_3999_RCBMF/gribscan_1h1d_2D_healpix{args.nside}/jsons/sol.dir/atm2d.json"
        , mode="r")
    rechunk_dataset(zarr_in, zarr_out, chunks_per_dim, variables3d_snow, args.nprocs)

    for vname in variables3d_snow:
        ad = zarr_out[vname].attrs["_ARRAY_DIMENSIONS"]
        zarr_out[vname].attrs["_ARRAY_DIMENSIONS"] = [d if d != "level" else "level_snow"
                                                      for d in ad]

if args.freq == "daily":
    assert args.nside in [128, 512]

    if args.nside <= 128:
        chunks_per_dim = {
            2: (4*30, 4**6),
            3: (4*30, 10, 4**6),
        }
    else:
        chunks_per_dim = {
            2: (30, 4**7),
            3: (30, 10, 4**7),
        }

    variables2d = filter_vardict({
        v: v for v in
        ['avg_sisnthick',
         'avg_sithick',
         'avg_sivn',
         'avg_tos',
         'avg_zos',
         'avg_mlotst125',
         'avg_siconc',
         'avg_siue',
         'avg_sos',
         'time',
         'lon',
         'lat']
    })

    variables3d = filter_vardict({
        v: v for v in
        ['avg_von',
         'avg_thetao',
         'avg_uoe',
         'avg_wo',
         'avg_so',
         'level']
    })

    nside_in_filename_wtf = 2048 if args.nside == 512 else args.nside

    zarr_in = zarr.open(
        f"reference::/work/bm1235/u233156/gribscan_cycle4_3999_RCBMF/gribscan_1h1d_2D_healpix{nside_in_filename_wtf}/jsons/o2d.dir/atm2d.json"
        , mode="r")
    rechunk_dataset(zarr_in, zarr_out, chunks_per_dim, variables2d, args.nprocs)

    zarr_in = zarr.open(
        f"reference::/work/bm1235/u233156/gribscan_cycle4_3999_RCBMF/gribscan_1h1d_2D_healpix{nside_in_filename_wtf}/jsons/o3d.dir/atm2d.json"
        , mode="r")
    rechunk_dataset(zarr_in, zarr_out, chunks_per_dim, variables3d, args.nprocs)

if args.freq == "monthly":
    assert args.nside in [128, 2048]

    if args.nside <= 128:
        chunks_per_dim = {
            2: (4, 4**6),
            3: (4, 10, 4**6),
        }
    else:
        chunks_per_dim = {
            2: (4, 4**8),
            3: (4, 10, 4**8),
        }

    variables = {'time': 'time',
                 'lon': 'lon',
                 'lat': 'lat',
                 "rlut": lambda ttr: ttr / -3600,  # 0 - net
                 "rsut": lambda tisr, tsr: (tisr - tsr) / 3600,  # incoming - net, for W/m2
                 "pr": lambda tp: tp * (1000 / 3600),  # pr: kg m-2 s-1
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
                 "tauu": lambda ewss: ewss / 3600,
                 "tauv": lambda nsss: nsss / 3600,
                 **{v: v for v in
                    ('fdir',
                     'lsp',
                     'ssro',
                     'e',
                     'sro',
                     'lgws',
                     'mgws',
                     'tsrc',
                     )}
                 }

    zarr_in = zarr.open(
        f"reference::/work/bm1235/u233156/gribscan_cycle4_3999_RCBMF/gribscan_monthly_healpix{args.nside}/jsons/sfc.dir/atm2d.json"
        , mode="r")

    rechunk_dataset(zarr_in, zarr_out, chunks_per_dim, variables, args.nprocs)


# rename dimension `value` -> `cell`
for vname, var in zarr_out.arrays():
    ad = var.attrs["_ARRAY_DIMENSIONS"]
    var.attrs["_ARRAY_DIMENSIONS"] = [d if d != "value" else "cell"
                                      for d in ad]

# finally consolidate the dataset
zarr.consolidate_metadata(zarr_out.store)
