import xarray as xr
import pandas as pd
import numpy as np # Often useful, though not strictly for time here
import zarr
from numcodecs import Blosc

compressor = Blosc(cname='zstd', clevel=3, shuffle=2)

# 1. Define the path to your Zarr store
# zarr_path = '/glade/campaign/cgd/cas/brianpm/hack25/DYAMOND2_diag_15min_to_hp1.zarr'
# zarr_path = '/glade/derecho/scratch/digital-earths-hackathon/mpas_DYAMOND2/15min/DYAMOND2_diag_15min_to_hp4_RECHUNK.zarr'
# zarr_path = '/glade/derecho/scratch/digital-earths-hackathon/mpas_DYAMOND2/15min/DYAMOND2_diag_PT15M_to_hp5.zarr'
# zarr_path = '/glade/derecho/scratch/digital-earths-hackathon/mpas_DYAMOND2/15min/DYAMOND2_diag_PT15M_to_hp3.zarr'

zarr_path = '/glade/derecho/scratch/digital-earths-hackathon/mpas_DYAMOND1/15min/DYAMOND1_diag_15min_to_hp1.zarr'


# 2. Generate the correct time coordinates
# Start date: 2016-08-01_00.00.00
# End date: 2016-09-10_00.00.00
# Frequency: 15 minutes

# DYAMOND1
start_time_str = '2016-08-01 00:00:00'
end_time_str = '2016-09-10 00:00:00'
frequency = '15min' # pandas frequency string

# DYAMOND2
# start_time_str = '2020-01-20 00:00:00'
# end_time_str = '2020-03-01 00:00:00'
# frequency = '15min' # pandas frequency string


try:
    correct_times = pd.date_range(start=start_time_str, end=end_time_str, freq=frequency)
    print(f"Generated {len(correct_times)} time points.")
    print(f"First generated time: {correct_times[0]}")
    print(f"Last generated time: {correct_times[-1]}")

    # 3. Open the Zarr dataset using xarray
    # Use consolidated=True if you have .zmetadata, otherwise False or omit.
    # For writing, it's often good to set chunks=None or let xarray handle it,
    # or specify chunks if you know the optimal chunking for 'time'.
    print(f"\nOpening Zarr dataset: {zarr_path}")
    ds = xr.open_zarr(zarr_path) # Adjust consolidated as needed

    print("\nOriginal dataset structure:")
    print(ds)

    # Create the time coordinate with proper encoding
    reference_date = pd.Timestamp('2000-01-01 00:00:00')
    time_values = ((correct_times - reference_date).total_seconds() / 60.0).values
    
    # Create new time coordinate with CF attributes
    new_time = xr.DataArray(
        data=time_values,
        dims=['time'],
        name='time',
        attrs={
            'standard_name': 'time',
            'long_name': 'time',
            'axis': 'T',
            'units': 'minutes since 2000-01-01 00:00:00',
            'calendar': 'proleptic_gregorian'
        }
    )
    
    # Create zarr-specific encoding
    zarr_encoding = {
        'time': {
            'chunks': [384],  # Match your desired time chunk size
            'compressor': compressor,
            'dtype': '<f8',
            '_FillValue': None,
            'filters': None
        }
    }

    
    # Create a new dataset with the updated time coordinate
    ds_new = ds.assign_coords(time=new_time)
    
    # Delete existing time coordinate from zarr store
    print("\nRemoving existing time coordinate...")
    store = zarr.open(zarr_path, mode='a')
    if 'time' in store:
        del store['time']
    if '.zattrs' in store:
        attrs = store['.zattrs']
        if '_ARRAY_DIMENSIONS' in attrs and 'time' in attrs['_ARRAY_DIMENSIONS']:
            attrs['_ARRAY_DIMENSIONS'].remove('time')
    
    # Write to zarr with explicit encoding
    print("\nWriting updated dataset to Zarr...")
    ds_new.to_zarr(
        zarr_path,
        mode='a',
        consolidated=True,
        encoding=zarr_encoding
    )
    
    # Force metadata consolidation
    print("Consolidating metadata...")
    zarr.consolidate_metadata(zarr_path)
    
    # Verify the changes
    print("\nVerifying changes...")
    ds_verify = xr.open_zarr(zarr_path, decode_times=True)
    print("\nTime coordinate after update:")
    print(f"Time values: {ds_verify.time.values[:3]}...")
    print(f"Time encoding: {ds_verify.time.encoding}")
    print(f"Time attributes: {ds_verify.time.attrs}")
    ds_verify.close()

except FileNotFoundError:
    print(f"ERROR: Zarr store not found at {zarr_path}")
except PermissionError:
    print(f"ERROR: No write permissions for {zarr_path}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
    import traceback
    traceback.print_exc()
finally:
    if 'ds' in locals() and ds is not None:
        ds.close()
    if 'ds_updated' in locals() and ds_updated is not None:
        ds_updated.close()
    print("\nScript finished.")