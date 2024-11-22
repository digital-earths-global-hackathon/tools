import healpix as hp
import numpy as np
import xarray as xr

import easygems.healpix as egh
import easygems.remap as egr

import cartopy.crs as ccrs
import cartopy.feature as cf
import matplotlib.pyplot as plt

# converts X-SHiELD output that was interpolated to lat-lon to healpix
# Tim Merlis, closely following on Lucas Kluft's https://easy.gems.dkrz.de/Processing/datasets/remapping.html
# uses easy environment https://github.com/digital-earths-global-hackathon/tools/tree/main/python_envs

# this is output that was interpolated from native cubed sphere grid ~3.25km to lat-lon
fn = '/scratch/cimes/GLOBALFV3/stellar_run/processed/20191020.00Z.C3072.L79x2_pire/pp/2020010800/uas_C3072_11520x5760.fre.nc' 
ds = xr.open_dataset(fn)
ds = ds.rename({'grid_yt': 'lat', 'grid_xt': 'lon'})                                                           
ds = ds.stack(xy=("lon", "lat"))

order = zoom = 9
nside = hp.order2nside(order)
npix = hp.nside2npix(nside)

hp_lon, hp_lat = hp.pix2ang(nside=nside, ipix=np.arange(npix), lonlat=True, nest=True)
hp_lon = ((hp_lon + 180) % 360 )
# Lucas had a 180 deg shift that should not be used for X-SHiELD
#hp_lon = (hp_lon + 180) % 360 - 180  # [-180, 180)
hp_lon += 360 / (4 * nside) / 4  # shift quarter-width                                                                    

# compute weights or load precomputed ones
use_precomputed_weights = True
if use_precomputed_weights:
    weight_fn = '/scratch/cimes/tmerlis/healpix_weights_11520x5760_to_zoom' + str(zoom) + '.nc'
    weights = xr.open_dataset(weight_fn)
else:
    weights = egr.compute_weights_delaunay((ds.lon, ds.lat), (hp_lon, hp_lat))
    # You can also save the calculated weights for future use
    #weights.to_netcdf("healpix_weights.nc")\n')

def worldmap(var, **kwargs):
    projection = ccrs.Robinson(central_longitude=-135.5808361)
    fig, ax = plt.subplots(
        figsize=(8, 4), subplot_kw={"projection": projection}, constrained_layout=True
    )
    ax.set_global()

    egh.healpix_show(var, ax=ax, **kwargs)

    # adding coasts fails at Princeton when cartopy tries to download a file from the web
    #ax.add_feature(cf.COASTLINE, linewidth=0.8)
    #ax.add_feature(cf.BORDERS, linewidth=0.4)
    fig.savefig('test_uas.pdf')

worldmap(egr.apply_weights(ds.uas.isel(time=0), **weights))




