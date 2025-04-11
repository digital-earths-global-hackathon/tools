from numcodecs import Blosc
import numpy as np
import logging

logging.basicConfig()
logger = logging.getLogger("chunk_tools")
# logger.setLevel(logging.DEBUG)


def get_encodings(outds, order, timechunk):
    encodings = {
        var: dict(
            dtype=get_dtype(outds[var]),
            chunks=get_chunksizes(
                outds=outds, var=var, order=order, timechunk=timechunk
            ),
            compressor=Blosc(cname="zstd", clevel=5, shuffle=Blosc.BITSHUFFLE),
        )
        for var in outds
    }
    return encodings


def get_dtype(da):
    if np.issubdtype(da.dtype, np.floating):
        return "float32"
    else:
        return da.dtype


def get_chunksizes(outds, var, order, timechunk):
    logger.debug(f"{outds=}, {var=}")
    var_shape = outds[var].shape
    if len(var_shape) == 0:
        return tuple([])
    logger.debug(f"{var_shape=}")
    timechunk = min(timechunk, var_shape[0])
    spacechunk = compute_chunksize(order=order)
    if len(var_shape) == 1:
        return min(var_shape[0], 1024**2)
    elif len(var_shape) == 2:
        chunksizes = (timechunk, spacechunk)
        return chunksizes
    elif len(var_shape) == 3:
        chunksizes = (timechunk, 5, spacechunk)
        return chunksizes
    else:
        raise Exception(
            "can't compute chunking for variables that have more than 3 dimensions!"
        )


def compute_chunksize(order):
    start_split = 8
    if order < start_split:
        return 12 * 4**order
    else:
        return 4 ** (start_split + 1)


def isPrime(number):
    limit = int(number / 2)  # limit indicates how many times we need to run the loop
    flag = 0  # to keep track whether the number is prime or not
    if number == 0 or number == 1:
        print(f"The Given Number {number} is Not Prime")
        return
    for i in range(2, limit + 1):
        if number % i == 0:
            flag = 1
            break
    if flag == 0:
        return True
    else:
        return False
