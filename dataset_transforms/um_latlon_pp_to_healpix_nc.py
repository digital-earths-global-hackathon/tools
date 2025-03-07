"""
Contains a UMRegridder class that lets you convert from UM lat/lon .pp to .nc
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

WEIGHTS_PATH = '/gws/nopw/j04/hrcm/mmuetz/weights/regrid_weights_N2560_hpz10.nc'
TMPDIR = '/work/scratch-nopw2/mmuetz/wcrp_hackathon/'


def gen_weights(da, zoom=10, weights_path=WEIGHTS_PATH):
    """Generate delaunay weights for regridding.

    Parameters:
        da (xr.DataArray): input data array with lat/lon coords to use.
        zoom (int): desired zoom level.
        weights_path (str): path to weights file.
    """
    weights_path = Path(weights_path)
    assert not weights_path.exists(), f'Weights file {weights_path} already exists'
    weights_path.parent.mkdir(parents=True, exist_ok=True)

    nside = hp.order2nside(zoom)
    npix = hp.nside2npix(nside)

    hp_lon, hp_lat = hp.pix2ang(nside=nside, ipix=np.arange(npix), lonlat=True, nest=True)
    hp_lon = hp_lon % 360  # [0, 360)
    hp_lon += 360 / (4 * nside) / 4  # shift quarter-width

    da_flat = da.stack(cell=('longitude', 'latitude'))

    print('computing weights')
    weights = egr.compute_weights_delaunay((da_flat.longitude.values, da_flat.latitude.values), (hp_lon, hp_lat))
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
def hp_coarsen_with_weights(data, weights=None):
    """Coarsen healpix data by one zoom level handling nans

    Parameters:
        data (np.ndarray): healpix data
        weights (np.ndarray): weights for zoom level (not needed for first call at highest zoom level)
    """
    if weights is None:
        coarse_field = np.nanmean(data.reshape(-1, 4), axis=-1)
        new_weights = 1 - np.isnan(data.reshape(-1, 4)).sum(axis=-1) / 4
    else:
        coarse_field = (data * weights).reshape(-1, 4).sum(axis=-1) / weights.reshape(-1, 4).sum(axis=-1)
        new_weights = weights.reshape(-1, 4).mean(axis=-1)
    return coarse_field, new_weights


class UMRegridder:
    """Regrid UM lat/lon .pp files to healpix .nc"""

    def __init__(self, method='easygems_delaunay', zooms=range(11)[::-1], weights_path=WEIGHTS_PATH, tmpdir=TMPDIR):
        """Initate a UM regridder for a particular method/zoom levels.

        Parameters:
            method (str) : regridding method [easygems_delaunay, earth2grid]
            zooms (list) : list of zoom levels (highest first)
        """
        if method not in ['easygems_delaunay', 'earth2grid']:
            raise ValueError('method must be either easygems_delaunay or earth2grid')
        self.method = method
        self.zooms = list(zooms)
        self.max_zoom_level = zooms[0]
        self.weights_path = weights_path
        if method == 'easygems_delaunay':
            self.weights = xr.open_dataset(self.weights_path)
        self.tmpdir = Path(tmpdir)

    def run(self, inpath, outpath_tpl, varname):
        """Do end-to-end regridding, including converting to tmp .nc file on scratch
        There will be one healpix .nc for every lat/lon .pp.
        NOTE: This assumes one variable per .pp file.

        Parameters:
            inpath (str, Path) : path to input file
            outpath_tpl (str, Path) : path template (with {zoom}) to output file
            varname (str) : name of variable to regrid
        """
        print(f'Run regridding: {inpath}:{varname}')
        print(f'-> {outpath_tpl} zooms={self.zooms}')
        inpath = Path(inpath)
        print('- convert .pp to .nc')
        tmppath = self._pp_to_nc(inpath)
        try:
            print('- load .nc')
            da = self._load_da(tmppath, varname)
            print(f'- do regrid using {self.method}')
            regridded_data, dim_shape, dim_ranges = self.regrid(da)
            print(f'- coarsen and save')
            self._coarsen_and_save(da, regridded_data, outpath_tpl, dim_shape, dim_ranges, varname)
        finally:
            tmppath.unlink()

    def _pp_to_nc(self, inpath):
        """Convert inpath (.pp) to .nc"""
        tmppath = self.tmpdir.joinpath(*inpath.parts[1:]).with_suffix('.nc')
        tmppath.parent.mkdir(exist_ok=True, parents=True)

        # Stop annoying error message.
        iris.FUTURE.save_split_attrs = True
        # For some reason, loading .pp then saving as .nc using iris, then reloading with xarray
        # is way faster.
        cube = iris.load_cube(inpath)
        iris.save(cube, tmppath)
        return tmppath

    @staticmethod
    def _load_da(path, varname):
        """Load the DataArray in varname from path"""
        da = xr.open_dataset(path)[varname]
        da.load()
        return da

    def regrid(self, da):
        """Do the regridding - set up common data to allow looping over all dims that are not lat/lon

        Parameters:
            da (xr.DataArray) : DataArray to be regridded
        """
        dsout_tpl = xr.Dataset(coords=da.copy().drop_vars(['latitude', 'longitude']).coords, attrs=da.attrs)

        # This is the shape of the dataset without lat/lon.
        dim_shape = [v for v in dsout_tpl.sizes.values()]
        # These are the ranges - can be used to iter over an idx that selects out each individual lat/lon field for
        # any number of dims by passing to product as product(*dim_ranges).
        dim_ranges = [range(s) for s in dim_shape]
        ncell = 12 * 4 ** self.max_zoom_level
        regridded_data = np.zeros(dim_shape + [ncell])
        if self.method == 'easygems_delaunay':
            self._regrid_easygems_delaunay(da, dim_ranges, regridded_data)
        elif self.method == 'earth2grid':
            self._regrid_earth2grid(da, dim_ranges, regridded_data)
        return regridded_data, dim_shape, dim_ranges

    def _regrid_easygems_delaunay(self, da, dim_ranges, regridded_data):
        """Use precomputed weights file to do Delaunay regridding."""
        da_flat = da.stack(cell=('longitude', 'latitude'))
        for idx in product(*dim_ranges):
            print(f'  - {idx}')
            regridded_data[idx] = egr.apply_weights(da_flat[idx].values, **self.weights)

    def _regrid_earth2grid(self, da, dim_ranges, regridded_data):
        """Use earth2grid (which uses torch) to do regridding."""
        # I'm not assuming these will be installed.
        import earth2grid
        import torch


        lat_lon_shape = (len(da.latitude), len(da.longitude))
        src = earth2grid.latlon.equiangular_lat_lon_grid(*lat_lon_shape)

        # The y-dir is indexed in reverse for some reason.
        # Build a slice to invert latitude (for passing to regridding).
        data_slice = [slice(None) if d != 'latitude' else slice(None, None, -1) for d in da.dims]
        target_data = da.values[*data_slice].copy().astype(np.double)

        # Note, you pass in PixelOrder.NEST here. .XY() (as in example) is equivalent to .RING.
        hpx = earth2grid.healpix.Grid(level=self.max_zoom_level, pixel_order=earth2grid.healpix.PixelOrder.NEST)
        regrid = earth2grid.get_regridder(src, hpx)
        for idx in product(*dim_ranges):
            print(f'  - {idx}')
            z_torch = torch.as_tensor(target_data[idx])
            z_hpx = regrid(z_torch)
            # if idx == () this still works (i.e. does nothing to regridded_data).
            regridded_data[idx] = z_hpx.numpy()

    def _coarsen_and_save(self, da, regridded_data, outpath_tpl, dim_shape, dim_ranges, varname):
        """Produce the all zoom level data by successively coarsening and save."""
        dsout_tpl = xr.Dataset(coords=da.copy().drop_vars(['latitude', 'longitude']).coords, attrs=da.attrs)
        reduced_dims = [d for d in da.dims if d not in ['latitude', 'longitude']]
        for zoom in self.zooms:
            outpath = Path(str(outpath_tpl).format(zoom=zoom))
            dsout = dsout_tpl.copy()
            if zoom != self.max_zoom_level:
                coarse_regridded_data = np.zeros(dim_shape + [12 * 4 ** zoom])
                for idx in product(*dim_ranges):
                    coarse_regridded_data[idx] = hp_coarsen(regridded_data[idx])
                regridded_data = coarse_regridded_data

            dsout[f'{varname}'] = xr.DataArray(regridded_data, dims=reduced_dims + ['cell'],
                                               coords={'cell': np.arange(regridded_data.shape[-1])})
            dsout.attrs['grid_mapping'] = 'healpix_nested'
            dsout.attrs['healpix_zoom'] = zoom
            # dsout.attrs['input_file'] = str(inpath)
            dsout.attrs['suite'] = 'u-cu087'

            outpath.parent.mkdir(exist_ok=True, parents=True)
            print(f'  - {outpath} {zoom}')
            dsout.to_netcdf(outpath)


def main():
    """Entry point plus some examples. Super simple argument 'parsing' of command line args.

    e.g. python convert_latlon_pp_to_hp_nc.py toa_outgoing_longwave_flux easygems_delaunay
    """
    if len(sys.argv) == 2 and sys.argv[1] == 'gen_weights':
        inpath = Path('/gws/nopw/j04/hrcm/cache/torau/Lorenzo_u-cu087/OLR/20200101T0000Z_pa000.pp')
        cube = iris.load_cube(inpath)
        da = xr.DataArray.from_iris(cube)

        gen_weights(da, zoom=10)
        return

    if not Path(WEIGHTS_PATH).exists():
        # Might not be a problem depending on args.
        print(f'WARNING: no weights have been generated: {WEIGHTS_PATH}')

    if len(sys.argv) == 3 and sys.argv[1] == 'toa_outgoing_longwave_flux':
        method = sys.argv[2]
        inpath = Path('/gws/nopw/j04/hrcm/cache/torau/Lorenzo_u-cu087/OLR/20200101T0000Z_pa000.pp')
        outpath_tpl = f'/work/scratch-nopw2/mmuetz/wcrp_hackathon/OLR/20200101T0000Z_pa000.hpz{{zoom}}.{method}.nc'
        varname = 'toa_outgoing_longwave_flux'
    elif len(sys.argv) == 3 and sys.argv[1] == 'air_temperature':
        method = sys.argv[2]
        inpath = '/gws/nopw/j04/hrcm/hackathon/3D/pe_T/20200101T0000Z_pe000.pp'
        outpath_tpl = f'/work/scratch-nopw2/mmuetz/wcrp_hackathon/pe_T/20200101T0000Z_pe000.hpz{{zoom}}.{method}.nc'
        varname = 'air_temperature'
    else:
        inpath = sys.argv[1]
        outpath_tpl = sys.argv[2]
        varname = sys.argv[3]
        method = sys.argv[4]

    um_regridder = UMRegridder(method)
    um_regridder.run(inpath, outpath_tpl, varname)
    return um_regridder


if __name__ == '__main__':
    main()
