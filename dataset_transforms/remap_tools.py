import numpy as np
import xarray as xr
import healpix as hp
import easygems.remap as egr


def gen_weights(ds, order):
    nside = hp.order2nside(order)
    npix = hp.nside2npix(nside)

    hp_lon, hp_lat = hp.pix2ang(
        nside=nside, ipix=np.arange(npix), lonlat=True, nest=True
    )
    lon_periodic = np.hstack((ds.lon - 360, ds.lon, ds.lon + 360))
    lat_periodic = np.hstack((ds.lat, ds.lat, ds.lat))
    weights = egr.compute_weights_delaunay((ds.lon, ds.lat), (hp_lon, hp_lat))

    # Compute weights
    weights = egr.compute_weights_delaunay(
        points=(lon_periodic, lat_periodic), xi=(hp_lon, hp_lat)
    )

    # Remap the source indices back to their valid range
    weights = weights.assign(src_idx=weights.src_idx % ds.lat.size)
    return weights


def remap_delaunay(ds: xr.Dataset, order: int) -> xr.Dataset:
    """Expects the cell dimension of the dataset to be named ncells, and lat/lon to be named lat and lon, and to be in degree."""
    weights = gen_weights(ds, order)
    npix = len(weights.tgt_idx)
    ds_remap = xr.apply_ufunc(
        egr.apply_weights,
        ds,
        kwargs=weights,
        keep_attrs=True,
        input_core_dims=[["ncells"]],
        output_core_dims=[["cell"]],
        output_dtypes=["f4"],
        vectorize=True,
        dask="parallelized",
        dask_gufunc_kwargs={
            "output_sizes": {"cell": npix},
        },
    )
    return ds_remap
