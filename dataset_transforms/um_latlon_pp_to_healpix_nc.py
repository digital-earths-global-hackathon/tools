"""
Contains a UMHealpixRegridder class that lets you convert from UM lat/lon .pp to .nc
Can be run as a command line script with args easy to see in main()
"""
import sys
from itertools import product
from pathlib import Path

import easygems.remap as egr
import healpix as hp
import iris
import numpy as np
import xarray as xr
from cartopy.util import add_cyclic_point

WEIGHTS_PATH = '/gws/nopw/j04/hrcm/mmuetz/weights/regrid_weights_N2560_hpz10.nc'
TMPDIR = '/work/scratch-nopw2/mmuetz/wcrp_hackathon/'


def _xr_add_cyclic_point(da, lonname):
    """Add a cyclic column to the longitude dim."""

    # Use add_cyclic_point to interpolate input data
    lon_idx = da.dims.index(lonname)
    wrap_data, wrap_lon = add_cyclic_point(da.values, coord=da[lonname], axis=lon_idx)
    coords = {n: c for n, c in da.coords.items() if n != lonname}
    coords[lonname] = wrap_lon

    # Generate output DataArray with new data but same structure as input
    daout = xr.DataArray(data=wrap_data,
                         coords=coords,
                         dims=da.dims,
                         attrs=da.attrs)
    return daout


def gen_weights(da, zoom=10, lonname='longitude', latname='latitude', add_cyclic=True, weights_path=WEIGHTS_PATH):
    """Generate delaunay weights for regridding.

    Can use quite a lot of RAM: 30-40G for a UM N2560 conversion.

    Assumption is that da has lon: 0 to 360, lat: -90 to 90.
    It is important to make sure that the input domain contains the output domain, i.e. its convex hull is bigger.
    Input domain is defined by the lat/lon coords in da, output domain is defined by healpix zoom level and is
    roughly 0 to 360.
    This is to ensure that the interpolation can proceed for all points - if you end up with NaNs in your output
    it could be because of this, and it might be necessary to add a cyclic point to the input domain.

    Parameters:
        da (xr.DataArray): input data array with lat/lon coords to use.
        zoom (int): desired zoom level.
        lonname (str): name of longitude coord.
        latname (str): name of latitude coord.
        add_cyclic (bool): whether to add cyclic points.
        weights_path (str): path to weights file.
    """
    weights_path = Path(weights_path)
    assert not weights_path.exists(), f'Weights file {weights_path} already exists'
    weights_path.parent.mkdir(parents=True, exist_ok=True)

    nside = hp.order2nside(zoom)
    npix = hp.nside2npix(nside)

    # Expand input domain by one in the lon dim.
    if add_cyclic:
        print(da[lonname])
        da = _xr_add_cyclic_point(da, lonname)
        print(da[lonname])

    hp_lon, hp_lat = hp.pix2ang(nside=nside, ipix=np.arange(npix), lonlat=True, nest=True)

    # TODO: is this necessary for the UM?
    # This was in code that I copied the function from but I think I can leave it out.
    # hp_lon += 360 / (4 * nside) / 4  # shift quarter-width
    hp_lon = hp_lon % 360  # [0, 360)
    # Apply a 360 degree offset. This ensures that all hp_lon are within da[lonname].
    hp_lon[hp_lon == 0] = 360

    da_flat = da.stack(cell=(lonname, latname))

    print('computing weights')
    weights = egr.compute_weights_delaunay((da_flat[lonname].values, da_flat[latname].values), (hp_lon, hp_lat))
    weights.to_netcdf(weights_path)
    print(f'saved weights to {weights_path}')


def hp_coarsen(data):
    """Coarsen healpix data by one zoom level

    Parameters:
        data (np.ndarray): healpix data
    """
    assert data.size % 12 == 0 and (data.size // 12) % 4 == 0, 'Does not look like healpix data'
    assert data.size != 12, 'Cannot coarsen healpix zool level 0'
    # TODO: Need to check how to do regridding when there are nans.
    return np.nanmean(data.reshape(-1, 4), axis=1)


# TODO: this fixes the problem with the above, but I need to figure out best way to save the weights only when nec.
def hp_coarsen_with_weights(data, nan_weights=None):
    """Coarsen healpix data by one zoom level handling nans

    Parameters:
        data (np.ndarray): healpix data
        nan_weights (np.ndarray): weights for zoom level (not needed for first call at highest zoom level)
    """
    if nan_weights is None:
        coarse_field = np.nanmean(data.reshape(-1, 4), axis=-1)
        new_weights = 1 - np.isnan(data.reshape(-1, 4)).sum(axis=-1) / 4
    else:
        coarse_field = (data * nan_weights).reshape(-1, 4).sum(axis=-1) / nan_weights.reshape(-1, 4).sum(axis=-1)
        new_weights = nan_weights.reshape(-1, 4).mean(axis=-1)
    return coarse_field, new_weights


class UMLatLon2HealpixRegridder:
    """Regrid UM lat/lon .pp files to healpix .nc"""

    def __init__(self, method='easygems_delaunay', zoom_level=10, add_cyclic=True, weights_path=WEIGHTS_PATH):
        """Initate a UM regridder for a particular method/zoom levels.

        Parameters:
            method (str) : regridding method [easygems_delaunay, earth2grid].
            zoom_level (int) : required healpix zoom level.
            weights_path (pathlib.Path | str | None) : path to pre-computed weights (see gen_weights above).
        """
        if method not in ['easygems_delaunay', 'earth2grid']:
            raise ValueError('method must be either easygems_delaunay or earth2grid')
        self.method = method
        self.zoom_level = zoom_level
        self.add_cyclic = add_cyclic
        self.weights_path = weights_path
        if method == 'easygems_delaunay':
            self.weights = xr.load_dataset(self.weights_path)

    def regrid(self, da, lonname, latname):
        """Do the regridding - set up common data to allow looping over all dims that are not lat/lon

        Parameters:
            da (xr.DataArray) : DataArray to be regridded
            lonname (str): name of longitude coord.
            latname (str): name of latitude coord.

        Returns:
            xr.DataArray : regridded data
        """
        if self.add_cyclic:
            da = _xr_add_cyclic_point(da, lonname)
        dsout_tpl = xr.Dataset(coords=da.copy().drop_vars([lonname, latname]).coords, attrs=da.attrs)

        # This is the shape of the dataset without lat/lon.
        dim_shape = [v for v in dsout_tpl.sizes.values()]
        # These are the ranges - can be used to iter over an idx that selects out each individual lat/lon field for
        # any number of dims by passing to product as product(*dim_ranges).
        dim_ranges = [range(s) for s in dim_shape]
        ncell = 12 * 4 ** self.zoom_level
        regridded_data = np.zeros(dim_shape + [ncell])
        print(f'  - {dict(dsout_tpl.sizes)}')
        if self.method == 'easygems_delaunay':
            self._regrid_easygems_delaunay(da, dim_ranges, regridded_data, lonname, latname)
        elif self.method == 'earth2grid':
            self._regrid_earth2grid(da, dim_ranges, regridded_data, lonname, latname)
        reduced_dims = [d for d in da.dims if d not in [lonname, latname]]
        coords = {**dsout_tpl.coords, 'cell': np.arange(regridded_data.shape[-1])}
        daout = xr.DataArray(
            regridded_data,
            dims=reduced_dims + ['cell'],
            coords=coords,
            attrs=da.attrs,
        )
        daout.attrs['grid_mapping'] = 'healpix_nested'
        daout.attrs['healpix_zoom'] = self.zoom_level
        daout.attrs['coarsened'] = False
        daout.attrs['regrid_method'] = self.method
        return daout

    def _regrid_easygems_delaunay(self, da, dim_ranges, regridded_data, lonname, latname):
        """Use precomputed weights file to do Delaunay regridding."""
        da_flat = da.stack(cell=(lonname, latname))
        for idx in product(*dim_ranges):
            print(f'    - {idx}')
            # TODO: speed up with xr.apply_ufunc...
            regridded_data[idx] = egr.apply_weights(da_flat[idx].values, **self.weights)

    def _regrid_earth2grid(self, da, dim_ranges, regridded_data, lonname, latname):
        """Use earth2grid (which uses torch) to do regridding."""
        # I'm not assuming these will be installed.
        import earth2grid
        import torch

        lat_lon_shape = (len(da[latname]), len(da[lonname]))
        src = earth2grid.latlon.equiangular_lat_lon_grid(*lat_lon_shape)

        # The y-dir is indexed in reverse for some reason.
        # Build a slice to invert latitude (for passing to regridding).
        data_slice = [slice(None) if d != latname else slice(None, None, -1) for d in da.dims]
        target_data = da.values[*data_slice].copy().astype(np.double)

        # Note, you pass in PixelOrder.NEST here. .XY() (as in example) is equivalent to .RING.
        hpx = earth2grid.healpix.Grid(level=self.zoom_level, pixel_order=earth2grid.healpix.PixelOrder.NEST)
        regrid = earth2grid.get_regridder(src, hpx)
        for idx in product(*dim_ranges):
            print(f'    - {idx}')
            z_torch = torch.as_tensor(target_data[idx])
            z_hpx = regrid(z_torch)
            # if idx == () this still works (i.e. does nothing to regridded_data).
            regridded_data[idx] = z_hpx.numpy()

    @staticmethod
    def coarsen(da, zooms=range(11)[::-1]):
        """Produce xr.DataArrays at all zoom levels by successively coarsening.
        Parameters:
            da (xr.DataArray) : DataArray to be coarsened, assumed to have been created with .regrid.
            zooms (list | range) : zooms to coarsen to - decreasing from max to min.

        Returns:
            Dict[xr.DataArray] : mapping of zoom to coarsened xr.DataArray.
        """
        zooms = list(zooms)
        assert len(da['cell']) == 12 * 4**zooms[0], f'cell has wrong number of points for {zooms[0]}'

        dsout_tpl = xr.Dataset(coords=da.copy().drop_vars(['cell']).coords, attrs=da.attrs)
        dim_shape = [v for v in dsout_tpl.sizes.values()]
        # These are the ranges - can be used to iter over an idx that selects out each individual lat/lon field for
        # any number of dims by passing to product as product(*dim_ranges).
        dim_ranges = [range(s) for s in dim_shape]
        reduced_dims = [d for d in da.dims if d not in ['cell']]

        num_nans = np.isnan(da.values).sum()
        print(f'  - {num_nans} NaNs in {da.name}')
        if num_nans:
            coarsen = 'with_weights'
        else:
            coarsen = 'without_weights'

        timename = [c for c in da.coords if c.startswith('time')][0]
        das = {}
        for zoom in zooms:
            if zoom == zooms[0]:
                regridded_data = da.values
                daout = da
            else:
                print(f'  - coarsen to {zoom}')
                coarse_regridded_data = np.zeros(dim_shape + [12 * 4**zoom])
                for idx in product(*dim_ranges):
                    coarse_regridded_data[idx] = hp_coarsen(regridded_data[idx])
                regridded_data = coarse_regridded_data

                coords = {timename: da[timename], 'cell': np.arange(regridded_data.shape[1])}
                daout = xr.DataArray(regridded_data, dims=reduced_dims + ['cell'], coords=coords, attrs=da.attrs)
                daout.attrs['grid_mapping'] = 'healpix_nested'
                daout.attrs['healpix_zoom'] = zoom
                daout.attrs['coarsened'] = True
            das[zoom] = daout
        return das


def main():
    if len(sys.argv) == 2 and sys.argv[1] == 'gen_weights':
        inpath = Path('/gws/nopw/j04/hrcm/cache/torau/Lorenzo_u-cu087/OLR/20200101T0000Z_pa000.pp')
        cube = iris.load_cube(inpath)
        da = xr.DataArray.from_iris(cube)

        gen_weights(da, zoom=10)
        return


if __name__ == '__main__':
    main()
