import cloudpickle
import zarr
import numpy as np
import multiprocessing
import warnings
from typing import Tuple
from tqdm.contrib.concurrent import process_map
import tempfile

warnings.filterwarnings(
    "ignore", "Found an empty list of filters in the array metadata document."
)
# %%
temp_path = "/fastdata/k20200/k202160/tmp_ifs_remap"


async def get_client(**kwargs):
    import aiohttp
    import aiohttp_retry

    retry_options = aiohttp_retry.ExponentialRetry(
        attempts=3, exceptions={OSError, aiohttp.ServerDisconnectedError}
    )
    retry_client = aiohttp_retry.RetryClient(
        raise_for_status=False, retry_options=retry_options
    )
    return retry_client


def gen_array(zarr_out, template, chunks, name=None, **kwargs):
    name = name or template.basename
    return zarr_out.require_array(
        name=name,
        chunk_key_encoding=zarr.core.chunk_key_encodings.DefaultChunkKeyEncoding(
            "v2", "/"
        ),
        shape=template.shape,
        chunks=chunks,
        dtype="float32",
        fill_value=np.nan,
        attributes=dict(template.attrs),
        **kwargs
    )


def simple_remap(slice_to_process, in_var, out_var):
    #print(f"single remap: {slice_to_process} {in_var.basename} -> {out_var.basename}")
    in_var = cloudpickle.loads(in_var)
    out_var[*slice_to_process] = in_var(slice_to_process)


def simple_remap_(t):
    simple_remap(*t)


def double_remap(slice_to_process, in_var, out_var):
    with tempfile.TemporaryDirectory(dir=temp_path) as tempdir:
        temp_zarr = zarr.open(tempdir, mode="w")
        temp_chunks = in_var.chunks[:-1]+(out_var.chunks[-1],)
        temp_var = gen_array(temp_zarr, in_var, temp_chunks)
        for sl in iter_slices(slice_to_process, in_var.chunks):
            simple_remap(sl, in_var, temp_var)

        simple_remap(slice_to_process, temp_var, out_var)


def iter_slices(to_go, chunks, current=()):
    pos = len(current)
    my_chunk = chunks[pos]
    start, stop, _ = to_go[0].indices(to_go[0].stop)
    for i in range(start, stop, my_chunk):
        my_slice = slice(i, min(i + my_chunk, stop))
        if len(to_go) == 1:
            yield (*current, my_slice)
        else:
            yield from iter_slices(to_go[1:], chunks, current=(*current, my_slice))


def shape_union(shape_a: Tuple[int, ...], shape_b: Tuple[int, ...]) -> Tuple[int, ...]:
    if len(shape_a) != len(shape_b):
        raise ValueError(
            f"Cannot compute union of shapes with different rank: "
            f"{len(shape_a)} vs {len(shape_b)}"
        )
    return tuple(max(a_dim, b_dim) for a_dim, b_dim in zip(shape_a, shape_b))


def rechunk_dataset(zarr_in, zarr_out, chunks_per_dim, variables):
    for varname_out, op in variables.items():
        if type(op) is str:
            var_in = zarr_in[op]
            var_in_op = lambda sl: var_in[*sl]
        else:
            varnames_in = op.__code__.co_varnames
            var_in = zarr_in[varnames_in[0]]
            var_in_op = lambda sl: op(*(zarr_in[v][*sl] for v in varnames_in))

        dim = len(var_in.shape)
        chunks = chunks_per_dim.get(dim, var_in.chunks)
        var_out = gen_array(zarr_out, var_in, chunks, varname_out)
        print(f"processing {varname_out} {var_out.shape=}...")
        whole_slice = tuple(slice(s) for s in var_out.shape)
        todo_list = [
            (slic, cloudpickle.dumps(var_in_op), var_out)
            for slic in iter_slices(whole_slice, shape_union(chunks, var_in.chunks))
        ]
        process_map(simple_remap_,
                    todo_list,
                    max_workers=16)
