# process_single_file_region.py
import argparse
import time as py_time # To avoid conflict with 'time' dimension name
import logging

import xarray as xr
import zarr
import numpy as np
import pandas as pd
import easygems.remap as egr # Assuming easygems is installed
from dask.diagnostics import ProgressBar # Optional, if dask is used within xarray/egr

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Consistent with your initialize_healpix_zarr.py
KNOWN_VERTICAL_DIM_NAMES = ["nVertLevels", "nVertLevelsP1", "nSoilLevels"]
# List of possible spatial dims and their associated weights files
SPATIAL_DIM_TO_WEIGHTS = {
    "nCells": "/glade/derecho/scratch/digital-earths-hackathon/mpas_DYAMOND2/mpas_to_healpix_weights_order10_wrap4ez4.nc",
    "nVertices": "/glade/derecho/scratch/digital-earths-hackathon/mpas_DYAMOND2/mpas_to_healpix_vertex_weights_order10_wrap4ez4.nc",
    # Add more if needed
}

def find_actual_vertical_dim(data_array, known_vertical_dims):
    """Identifies the actual vertical dimension name in a DataArray from a list of known names."""
    for dim_name in known_vertical_dims:
        if dim_name in data_array.dims:
            return dim_name
    return None

def process_file(nc_file_path, zarr_dir, zarr_prefix, time_offset, # num_time_steps will be 1
                 nside, healpix_dim_name_zarr):
    """
    Regrids a single NetCDF file (assumed to contain 1 time step) and writes its data
    to a specific time_offset in a pre-allocated Zarr store using egr.apply_weights.
    """
    logger.info(f"--- Processing File: {nc_file_path} ---")
    logger.info(f"Target Zarr location: {zarr_dir}, Prefix: {zarr_prefix}, Time Offset for this file: {time_offset}")
    logger.info(f"Nside: {nside}, Target HEALPix Dim: {healpix_dim_name_zarr}")

    num_time_steps_in_file = 1 # Explicitly set as per problem description
    # output spatial dim size should be: 
    npix_hp = 12 * nside**2

    orders = list(range(10, 0, -1))
    zarr_paths_by_order = {
        order: f"{zarr_dir}/{zarr_prefix}_to_hp{order}.zarr"
        for order in orders
    }


    try:
        # 1. Open the NetCDF file
        # Chunks for reading: -1 means load whole dimension. Time dim has length 1.
        ds_mpas = pre_proc_mpas_file(nc_file_path)

        # Open the Zarr store to get variables
        # Use the highest order as the reference
        reference_order = orders[0]
        reference_zarr_path = zarr_paths_by_order[reference_order]
        store = zarr.storage.LocalStore(reference_zarr_path)
        root_group = zarr.group(store=store, overwrite=False)

        if 'time' in ds_mpas.coords:
            time_coord = ds_mpas.time
            for order in orders:
                zarr_path = zarr_paths_by_order[order]
                store = zarr.storage.LocalStore(zarr_path)
                order_group = zarr.group(store=store, overwrite=False)
                if 'time' in order_group:
                    time_array = order_group['time']
                    time_slice = slice(time_offset, time_offset + num_time_steps_in_file)
                    time_array[time_slice] = time_coord.values
                    logger.debug(f"Wrote time coordinate values at offset {time_offset} to order {order}")

        variables_in_zarr = [k for k in root_group.array_keys()]
        number_of_variables = len(variables_in_zarr)

        for ivar, var_name_zarr in enumerate(variables_in_zarr):
            # Zarr variables might have different names if renaming happened during init,
            # but here we assume var_name_zarr is the name in MPAS too.
            # If not, a mapping would be needed.
            # For simplicity, assume var_name_zarr is the name to look for in ds_mpas.
            if var_name_zarr not in ds_mpas:
                logger.debug(f"Variable '{var_name_zarr}' (from Zarr store) not found in current NetCDF file {nc_file_path}. Skipping this variable for this file.")
                continue
            
            zarr_array_for_var = root_group[var_name_zarr]
            zarr_dims = zarr_array_for_var.attrs.get('_ARRAY_DIMENSIONS', [])

            is_time_varying = 'time' in zarr_dims
            logger.info(f"Variable '{var_name_zarr}' is {'time-varying' if is_time_varying else 'static'}")

            if is_time_varying:
                # Original time-varying processing
                logger.info(f"  Processing time-varying variable: {var_name_zarr} ({ivar+1}/{number_of_variables})")
            else:
                # Static variable processing
                logger.info(f"  Processing static variable: {var_name_zarr} ({ivar+1}/{number_of_variables})")

            processing_start_time = py_time.time()
            
            da_mpas_var = ds_mpas[var_name_zarr]
            # Detect spatial dimension for this variable
            spatial_dim = None
            for candidate in SPATIAL_DIM_TO_WEIGHTS.keys():
                if candidate in da_mpas_var.dims:
                    spatial_dim = candidate
                    break
            if spatial_dim is None:
                logger.info(f"Variable '{var_name_zarr}' does not have a recognized spatial dimension. Skipping.")
                continue
            # Use the correct weights file for this spatial dimension
            weights_path = SPATIAL_DIM_TO_WEIGHTS[spatial_dim]
            weights = xr.open_dataset(weights_path)

            logger.debug(f"da_mpas_var has dims: {da_mpas_var.dims}, and shape: {da_mpas_var.shape}")

            # Determine if variable is 3D by checking for known vertical dimensions
            actual_vert_dim_name = find_actual_vertical_dim(da_mpas_var, KNOWN_VERTICAL_DIM_NAMES)
            is_3d = actual_vert_dim_name is not None

            # Transpose for egr.apply_weights: egr expects spatial dim last among (time, vert, space)
            transpose_dims_for_egr = []
            if 'time' in da_mpas_var.dims: # Should be 'time' now if renamed
                transpose_dims_for_egr.append('time')
            actual_vert_dim_name = find_actual_vertical_dim(da_mpas_var, KNOWN_VERTICAL_DIM_NAMES)
            is_3d = actual_vert_dim_name is not None
            if is_3d:
                transpose_dims_for_egr.append(actual_vert_dim_name)
            transpose_dims_for_egr.append(spatial_dim)
            
            # Ensure all original dims are accounted for to avoid dropping them.
            # This simple construction assumes other dims are not interleaved.
            # For more complex layouts, a more robust transpose would be needed.
            # Example: if var has (time, member, nCells), egr might need (member, time, nCells)
            # For now, assume standard (time, [nVertLevels], nCells) + other trailing non-core dims.
            remaining_dims = [d for d in da_mpas_var.dims if d not in transpose_dims_for_egr]
            final_transpose_order_for_egr = remaining_dims + transpose_dims_for_egr

            logger.debug(f"final_transpose_order_for_egr is {final_transpose_order_for_egr}")
            
            try:
                da_mpas_transposed = da_mpas_var.transpose(*final_transpose_order_for_egr)
                logger.debug(f"TRANSPOSED DATA: {da_mpas_transposed.dims = }, {da_mpas_transposed.shape = }")
            except ValueError as e:
                logger.error(f"Failed to transpose variable {var_name_zarr} with order {final_transpose_order_for_egr}. Original dims: {list(da_mpas_var.dims)}. Error: {e}")
                continue # Skip this variable

            logger.debug(f"    Transposed dims for egr: {da_mpas_transposed.dims}, shape: {da_mpas_transposed.shape}")

            # Perform regridding

            da_regridded_egr = xr.apply_ufunc(
                egr.apply_weights,
                da_mpas_transposed,
                kwargs=weights,
                keep_attrs=True,
                input_core_dims=[[da_mpas_transposed.dims[-1]]],
                output_core_dims=[[healpix_dim_name_zarr]],
                on_missing_core_dim='copy',
                output_dtypes=[da_mpas_transposed.dtype],
                vectorize=True,
                dask='parallelized',
                dask_gufunc_kwargs={
                    "output_sizes": {healpix_dim_name_zarr: npix_hp},
                    "allow_rechunk": True
                }
            )
                
            # Identify the spatial dimension in the regridded DataArray
            non_spatial_dims = ['time'] + KNOWN_VERTICAL_DIM_NAMES
            spatial_dim_candidates = [d for d in da_regridded_egr.dims if d not in non_spatial_dims]
            if len(spatial_dim_candidates) != 1:
                logger.error(f"Could not uniquely identify spatial dimension in regridded variable '{var_name_zarr}'. Candidates: {spatial_dim_candidates}")
                continue  # or raise
            spatial_dim_name = spatial_dim_candidates[0]
            rename_dict_after_regrid = {spatial_dim_name: healpix_dim_name_zarr}
            logger.debug(f"    Shape after egr.apply_weights: {da_regridded_egr.shape}, Dims: {da_regridded_egr.dims}")

            
            da_regridded_renamed = da_regridded_egr.rename(rename_dict_after_regrid)
            logger.debug(f"    Renamed dims after regrid: {da_regridded_renamed.dims}")

            # Transpose to final Zarr order, handling both time-varying and static variables
            if is_time_varying:
                # Time-varying variables: (time, healpix_dim_name_zarr, [actual_vert_dim_name_if_3d], ...)
                final_zarr_dim_order = ['time', healpix_dim_name_zarr]
                if is_3d and actual_vert_dim_name in da_regridded_renamed.dims:
                    final_zarr_dim_order.append(actual_vert_dim_name)
                # Add remaining dimensions
                for d_rem in da_regridded_renamed.dims:
                    if d_rem not in final_zarr_dim_order:
                        final_zarr_dim_order.append(d_rem)
            else:
                # Static variables: (healpix_dim_name_zarr, [actual_vert_dim_name_if_3d], ...)
                final_zarr_dim_order = [healpix_dim_name_zarr]
                if is_3d and actual_vert_dim_name in da_regridded_renamed.dims:
                    final_zarr_dim_order.append(actual_vert_dim_name)
                # Add remaining dimensions
                for d_rem in da_regridded_renamed.dims:
                    if d_rem not in final_zarr_dim_order:
                        final_zarr_dim_order.append(d_rem)
            
            try:
                da_regridded_final_order = da_regridded_renamed.transpose(*final_zarr_dim_order)
            except ValueError as e:
                logger.error(f"Failed to transpose regridded variable {var_name_zarr} to Zarr order {final_zarr_dim_order}. Current dims: {list(da_regridded_renamed.dims)}. Error: {e}")
                continue # Skip this variable

            logger.debug(f"    Final Zarr order transpose: {da_regridded_final_order.dims}, shape: {da_regridded_final_order.shape}")            

            ##### COARSEN AND WRITE TO LOWER RESOLUTION ZARR
            # start at highest resolution, and then coarsen in loop
            current_data = da_regridded_final_order
            
            # time_offset is the absolute index for this file's single time step
            # num_time_steps_in_file is 1
            time_slice_in_zarr = slice(time_offset, time_offset + num_time_steps_in_file)
            
            for order in orders:
                zarr_path = zarr_paths_by_order[order]
                # Open the Zarr store for this order
                store = zarr.storage.LocalStore(zarr_path)
                root_group = zarr.group(store=store, overwrite=False)
                zarr_array_for_var = root_group[var_name_zarr]
                
                if is_time_varying:
                    # Original time-varying writing logic
                    target_idx_for_zarr_write = [slice(None)] * zarr_array_for_var.ndim
                    time_dim_zarr_idx = zarr_dims.index('time')
                    target_idx_for_zarr_write[time_dim_zarr_idx] = time_slice_in_zarr
                else:
                    # Static variable writing logic - write to all time steps
                    target_idx_for_zarr_write = [slice(None)] * zarr_array_for_var.ndim

                # zarr_dims = zarr_array_for_var.attrs.get('_ARRAY_DIMENSIONS', [])
                # # Prepare the slice for time as before
                # target_idx_for_zarr_write = [slice(None)] * zarr_array_for_var.ndim                                
                # try:
                #     time_dim_zarr_idx = zarr_dims.index('time')
                #     target_idx_for_zarr_write[time_dim_zarr_idx] = time_slice_in_zarr
                # except ValueError:
                #     logger.error(f"FATAL: 'time' dimension not found in Zarr array for variable '{var_name_zarr}'. Zarr dims: {zarr_array_for_var.dims}")
                #     raise # This is a critical error in setup

                # Write the data
                data_to_write = current_data.data
                if hasattr(data_to_write, "compute"):
                    data_to_write = data_to_write.compute()
                logger.debug(f"    Writing {var_name_zarr} (shape {data_to_write.shape}) to Zarr region: {target_idx_for_zarr_write}")
                zarr_array_for_var[tuple(target_idx_for_zarr_write)] = data_to_write
                logger.debug(f"Wrote {var_name_zarr} to {zarr_path} (order {order})")
                # Coarsen for next lower order
                if order > 1:
                    current_data = current_data.coarsen({healpix_dim_name_zarr: 4}).mean()

            processing_end_time = py_time.time()
            logger.info(f"    Finished writing {var_name_zarr}. Time: {processing_end_time - processing_start_time:.2f}s")

        logger.info(f"--- Successfully processed file: {nc_file_path} ---")

    except Exception as e:
        logger.error(f"ERROR processing file {nc_file_path}: {e}", exc_info=True)
        # import traceback # For more detailed trace in logs if exc_info=True isn't enough
        # traceback.print_exc()
        raise # Re-raise to ensure PBS job fails if one file fails

def pre_proc_mpas_file(datafil):
    """modified pre-processor for mpas files
       Main function is to fix the time coordinate.
    """
    ds_mpas = xr.open_dataset(datafil, engine='netcdf4', mask_and_scale=True) # , chunks={'Time': 1, 'nCells':1000000, 'nVertLevels':-1, 'nVertLevelsP1':-1,'nSoilLevels':-1})

    if "Time" in ds_mpas.dims:
        # Use a fixed reference date for all files
        ref_date = '2000-01-01 00:00:00'  # Or any other suitable fixed date

        # Clean and convert xtime strings
        time_str = ds_mpas.xtime.astype(str).values.astype('U').ravel()
        # Remove extra whitespace and handle empty strings
        time_str = [x.strip() for x in time_str]
        time_str = [x.replace("_", " ") for x in time_str]
        if isinstance(time_str, np.ndarray) or isinstance(time_str, list):
            time_str = "".join(time_str)

        # Convert to datetime
        # change coordinate (and index) from "Time" to "time"
        time_coord = pd.to_datetime(time_str)

        # Calculate hours since reference date for the coordinate values
        hours_since = (time_coord - pd.Timestamp(ref_date)) / pd.Timedelta('1h')
        if isinstance(hours_since, xr.DataArray):
            hours_since = hours_since.values
        elif isinstance(hours_since, float):
            hours_since = np.array([hours_since,])

        # Create time coordinate with specific encoding
        time_var = xr.DataArray(
            hours_since,
            dims='Time',
            name='time',
            attrs={'long_name': 'time', 
                'axis': 'T',
                'reference_date': ref_date},
                )
        time_calendar = 'standard'

        time_var.encoding = {
            'dtype': 'float64',
            'units': f'hours since {ref_date}',
            'calendar': time_calendar,
            '_FillValue': None
        }    
        ds_mpas_new = ds_mpas.assign_coords(time=time_var)
        ds_mpas_new = ds_mpas_new.swap_dims({"Time":"time"})
    elif ("time" in ds_mpas.dims) and ("time" in ds_mpas.coords):
        time_var = ds_mpas['time']
        time_calendar = getattr(time_var, "calendar", "standard")
        # might need to figure out units for encoding (?)
        time_var.encoding = {
            'dtype': 'float64',
            'calendar': time_calendar,
            '_FillValue': None
        }    
        ds_mpas_new = ds_mpas.assign_coords(time=time_var)
    else:
        raise ValueError("Need more information about MPAS time dimension.")


    # Find variables with dtype 'S64'
    s64_vars = [var for var in ds_mpas_new.variables if ds_mpas_new[var].dtype == 'S64']
    logger.debug(f"Variables with S64 dtype: {s64_vars}")

    # Drop these variables from the dataset
    ds_mpas_clean = ds_mpas_new.drop_vars(s64_vars)

    # Explicitly drop xtime and xtime_old if they are present:
    ds_mpas_clean = ds_mpas_clean.drop_vars(['xtime', 'xtime_old'], errors='ignore')

    return ds_mpas_clean



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Regrid a single MPAS file (1 time step) and write to a Zarr region.")
    parser.add_argument("--nc_file_path", required=True, help="Path to input NetCDF file.")
    parser.add_argument("--zarr_dir", required=True, help="Path to the directory for Zarr store.")
    parser.add_argument("--zarr_prefix", required=True, help="Zarr name prefix")
    parser.add_argument("--time_offset", type=int, required=True, help="Starting time index in Zarr for this file's data.")
    # num_time_steps is implicitly 1 for this script's design
    parser.add_argument("--nside", type=int, required=True, help="HEALPix nside.")
    parser.add_argument("--healpix_dim_name_zarr", default="cell", help="HEALPix dimension name used in Zarr store (e.g., 'cell', 'ncells_healpix').")
    # variables_to_process will be inferred from the Zarr store
    # mpas_vert_dim_name will use KNOWN_VERTICAL_DIM_NAMES

    # NOTE: weights files are defined as module-level data at the top.

    args = parser.parse_args()

    # Wrap the main processing in a ProgressBar context if any Dask operations are anticipated
    # (e.g., if xarray.open_dataset uses dask implicitly or egr.apply_weights does)

    main_timer_start = py_time.perf_counter()
    with ProgressBar():
        process_file(
            args.nc_file_path, args.zarr_dir, args.zarr_prefix, args.time_offset,
            args.nside, args.healpix_dim_name_zarr)
    main_timer_end = py_time.perf_counter()

    logger.info(f"    MAIN TIMER: {main_timer_end - main_timer_start:.2f}s")
    logger.info(f"Processing script finished for {args.nc_file_path}")