import xarray as xr
import numpy as np
import zarr
import numcodecs
import glob

source_path = "/home/k/k202186/wrcp-work/ARP-GEM"
dataset_name = "ARPGEM2_2p6km"


def get_input_files(source_path, dataset_name, time, method, zoom=8, variables=()):
    time_map = {"PT1H": "1hr", "PT6H": "6hr"}

    if method == "mean" and time != "PT6H":
        print(
            f"{source_path}/{dataset_name}/*/{dataset_name}_arpgem_{time_map[time]}_hpz{zoom}_averaged_*.nc"
        )
        files = glob.glob(
            f"{source_path}/{dataset_name}/*/{dataset_name}_arpgem_{time_map[time]}_hpz{zoom}_*_averaged_*.nc"
        )
    elif method == "inst":  # inst
        files = glob.glob(
            f"{source_path}/{dataset_name}/*/{dataset_name}_arpgem_{time_map[time]}_hpz{zoom}_*.nc"
        )
        files = [
            *filter(
                lambda x: "averaged" not in x,
                glob.glob(
                    f"{source_path}/{dataset_name}/*/{dataset_name}_arpgem_{time_map[time]}_hpz{zoom}_*.nc"
                ),
            )
        ]
    else:
        raise Exception('method not supported. use "mean" or "inst"')

    if len(variables):
        files = [
            *filter(lambda f: any(map(lambda var: f"_{var}_" in f, variables)), files)
        ]

    return files


def clean_dataset(ds: xr.Dataset) -> xr.Dataset:
    ds = ds.drop_dims("bnds")
    return ds


def rename_dataset(ds: xr.Dataset) -> xr.Dataset:
    # Clean variable names
    var_rename = {
        var: var.replace("_averaged", "").replace("_snapshot", "")
        for var in ds.data_vars
        if "_averaged" in var or "_snapshot" in var
    }

    dim_rename = {}
    if "plev" in ds.dims:
        dim_rename["plev"] = "level"
    if "cells" in ds.dims:
        dim_rename["cells"] = "cell"

    return ds.rename({**var_rename, **dim_rename})


def add_crs(ds: xr.Dataset, zoom=8) -> xr.Dataset:
    attrs = {
        "_ARRAY_DIMENSIONS": ("crs",),
        "grid_mapping_name": "healpix",
        "healpix_nside": 2**zoom,
        "healpix_order": "nest",
    }
    ds["crs"] = xr.Variable(dims=(), data=0, attrs=attrs)
    return ds


def get_dtype(da):
    if np.issubdtype(da.dtype, np.floating):
        return "float32"
    else:
        return da.dtype


def get_encoding(dataset):
    return {
        var: {
            "compressor": get_compressor(),
            "dtype": get_dtype(dataset[var]),
            #            "chunks": get_chunks(dataset[var].dims),
        }
        for var in dataset.variables
        if var not in dataset.dims
    }


def get_chunks(dimensions):
    if "level" in dimensions:
        chunks = {
            "time": 6,
            "cell": 4**6,
            "level": 4,
        }
    else:
        chunks = {
            "time": 6,
            "cell": 4**7,
        }

    return tuple((chunks[d] for d in dimensions))


def get_compressor():
    return numcodecs.Blosc("zstd", clevel=6)


def rechunk_dataset(ds: xr.Dataset, chunks_per_dims: dict) -> xr.Dataset:
    for var in filter(
        lambda x: x.dims in chunks_per_dims.keys(), ds.data_vars.values()
    ):
        _ = dict(zip(var.dims, chunks_per_dims[var.dims]))
        print("Rechunking", var.name, _)
        ds[var.name] = ds[var.name].chunk(_)
    return ds


def run(time, method, name=dataset_name, dry=False):
    output_name = f"/scratch/k/k202186/{name}_{time}_{method}_z8.zarr"
    print("Will create", output_name)
    files = get_input_files(source_path, name, time, method)
    print("Opening files", len(files), "files")
    print(*files, sep="\n")
    d = xr.open_mfdataset(files)
    assert d is not None
    print("Done:", d)

    new = clean_dataset(d)
    new = rename_dataset(new)
    new = add_crs(new)

    if time == "PT6H":
        chunks_per_dim = {
            ("time"): -1,
            ("cell"): -1,
            ("level"): -1,
            ("time", "cell"): (4 * 7, 4**7),
            ("time", "level", "cell"): (4 * 7, 10, 4**7),
        }
    elif time == "PT1H":
        chunks_per_dim = {
            ("time"): -1,
            ("cell"): -1,
            ("level"): -1,
            ("time", "cell"): (24, 4**7),
            ("time", "level", "cell"): (24, 10, 4**7),
        }

    rechunk_dataset(new, chunks_per_dim)
    print(new, flush=True)
    print("Writing to,", output_name)
    out_store = zarr.DirectoryStore(output_name, dimension_separator="/")
    new.to_zarr(out_store, encoding=get_encoding(new))
    print("Done")


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument("method", type=str, choices=["inst", "mean"])
    parser.add_argument("time", type=str, choices=["PT1H", "PT6H"])
    parser.add_argument("name", type=str, choices=["1p3", "2p6"], default="2p6")
    parser.add_argument("variables", type=str, nargs="*", default=())
    args = parser.parse_args()

    if args.time == "PT6H" and args.method != "inst":
        raise ("PT6H only has inst data")

    dataset_name = f"ARPGEM2_{args.name}km"
    run(args.time, args.method, dataset_name, dry=False)
