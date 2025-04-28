import cloudpickle
import zarr
import numpy as np
import multiprocessing
import warnings
from tqdm.contrib.concurrent import process_map
from numcodecs import Blosc

warnings.filterwarnings(
    "ignore", "Found an empty list of filters in the array metadata document."
)
# %%
temp_path = "/fastdata/k20200/k202160/tmp_ifs_remap"


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


def simple_remap(slice_to_process, in_vars, out_var, op=None):
    if op is None:
        op = lambda x: x
    out_var[*slice_to_process] = op(*(v[*slice_to_process] for v in in_vars))


def double_remap(slice_to_process, in_vars, out_var, op):
    zippath = temp_path+f"/tmp_{multiprocessing.current_process().pid}.zarr.zip"
    temp_zarr = zarr.open(zarr.storage.ZipStore(zippath, mode="w", compression=0), mode="w", zarr_version=2)
    # only rechunk last dimension first
    temp_chunks = (*in_vars[0].chunks[:-1], out_var.chunks[-1]*4**2)
    temp_var = gen_array(temp_zarr, in_vars[0], temp_chunks,
                         compressor=Blosc(cname='lz4', clevel=1, shuffle=0))
    for sl in iter_slices(slice_to_process, tuple_max(*(v.chunks for v in in_vars), temp_chunks)):
        simple_remap(sl, in_vars, temp_var, op)
    for sl in iter_slices(slice_to_process, tuple_max(temp_chunks, out_var.chunks)):
        simple_remap(sl, [temp_var], out_var)


def remap(t):
    slice_to_process, in_vars, out_var, op = t
    op = cloudpickle.loads(op)
    if len(slice_to_process) >= 3:
        _remap = double_remap
    else:
        _remap = simple_remap
    _remap(slice_to_process, in_vars, out_var, op)


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


def tuple_max(*tuples):
    assert all(len(tuples[0]) == len(t) for t in tuples), \
        (f"Cannot compute union of shapes with different rank: "
         f"{[len(t) for t in tuples]}")
    return tuple(max(dims) for dims in zip(*tuples))


def rechunk_dataset(zarr_in, zarr_out, chunks_per_dim, variables):
    for varname_out, op in variables.items():
        if type(op) is str:
            vars_in = [zarr_in[op]]
            op = lambda x: x
        else:
            vars_in = [zarr_in[v] for v in op.__code__.co_varnames]

        dim = len(vars_in[0].shape)
        chunks = chunks_per_dim.get(dim, vars_in[0].chunks)
        var_out = gen_array(zarr_out, vars_in[0], chunks, varname_out,
                            compressor=Blosc(cname='lz4', clevel=5, shuffle=1))
        print(f"processing {varname_out} {var_out.shape=}...")
        whole_slice = tuple(slice(s) for s in var_out.shape)
        op_pickle = cloudpickle.dumps(op)
        todo_list = [
            (slic, vars_in, var_out, op_pickle)
            for slic in iter_slices(whole_slice, tuple_max(chunks, *(v.chunks for v in vars_in)))
        ]
        process_map(remap,
                    todo_list,
                    max_workers=32)
