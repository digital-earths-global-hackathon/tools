import chunk_tools


def test_chunk_size():
    assert chunk_tools.compute_chunksize(0) == 12
    assert chunk_tools.compute_chunksize(1) == 48
    assert chunk_tools.compute_chunksize(6) == 12 * 4**6
    assert chunk_tools.compute_chunksize(7) == 12 * 4**7
    assert chunk_tools.compute_chunksize(8) == 4**9
    assert chunk_tools.compute_chunksize(9) == 4**9
