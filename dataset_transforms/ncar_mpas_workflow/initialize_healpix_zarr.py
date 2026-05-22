# initialize_healpix_zarr.py
import argparse
import logging
import os

import xarray as xr
import zarr
from numcodecs import Blosc
import numpy as np
import pandas as pd
from dask.diagnostics import ProgressBar
import healpix as hp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Acceptable vertical dimensions
vertical_dim_names = ["nVertLevels", "nVertLevelsP1", "nSoilLevels"]


def get_var_attributes_and_dtype(ds, var_name):
    """Helper to extract attributes and dtype, cleaning up problematic ones for Zarr."""
    if var_name not in ds:
        raise KeyError(f"Variable '{var_name}' not found in the sample dataset.")
    
    attrs = ds[var_name].attrs.copy()
    dtype = ds[var_name].dtype

    # Remove attributes that can cause issues or are not needed in Zarr
    for k_pop in ['_FillValue', 'missing_value', 'coordinates', 'grid_mapping', 'cell_methods']:
        attrs.pop(k_pop, None)
    return attrs, dtype


def gather_metadata_and_coords(manifest_path):
    """Get a dictionary of important information based on manifest file."""
    if not os.path.exists(manifest_path):
        raise FileNotFoundError(f"Manifest file not found: {manifest_path}")

    manifest_df = pd.read_csv(manifest_path)

    if manifest_df.empty:
        raise ValueError("Manifest file is empty.")

    total_time_steps = manifest_df['num_timesteps'].sum()
    first_file_path = manifest_df['filepath'].iloc[0]
        
    logger.info(f"Total time steps from manifest: {total_time_steps}")
    logger.info(f"Using first file for metadata: {first_file_path}")

    sample_ds = pre_proc_mpas_file(first_file_path)
    variables_to_process = [
        v for v in sample_ds.data_vars
        if sample_ds[v].dtype.kind in {'f', 'i'}
    ]

    # --- Efficient time coordinate construction ---
    # Assume each file has one time step, equally spaced
    # Get the first time value and the interval (assume hours since reference)
    time_data = sample_ds['time'].data
    if len(time_data) == 1:
        start_time = float(time_data[0])
        # Try to get interval from the second file if it exists
        if len(manifest_df) > 1:
            second_file_path = manifest_df['filepath'].iloc[1]
            logger.info(f"Loading second file for time interval: {second_file_path}")
            second_ds = pre_proc_mpas_file(second_file_path)
            second_time_data = second_ds['time'].data
            interval = float(second_time_data[0]) - start_time
        else:
            interval = 1.0  # fallback if only one file
            logger.warning("Only one file in manifest; assuming interval=1.0 hour.")
    else:
        # If more than one time in the file, get interval from first two
        start_time = float(time_data[0])
        interval = float(time_data[1] - time_data[0])

    concatenated_time_coords = start_time + np.arange(total_time_steps) * interval

    # # Collect all time coordinate values
    # # SLOW WAY THAT READS EACH FILE.
    # all_time_coords_list = []
    # for idx, row in manifest_df.iterrows():
    #     current_ds = pre_proc_mpas_file(row['filepath'])
    #     time_data = current_ds['time'].copy(deep=True).data
    #     all_time_coords_list.append(time_data)
    # concatenated_time_coords = np.concatenate(all_time_coords_list)
    # if len(concatenated_time_coords) != total_time_steps:
    #     raise ValueError(f"Total time steps mismatch: manifest sum {total_time_steps}, concatenated coords {len(concatenated_time_coords)}")

    return {
        "manifest_df": manifest_df,
        "total_time_steps": total_time_steps,
        "sample_ds": sample_ds,
        "variables_to_process": variables_to_process,
        "concatenated_time_coords": concatenated_time_coords,
    }


def create_zarr_for_nside(
    zarr_path,
    nside,
    healpix_dim_name,
    time_chunk_size,
    spatial_chunk_size,
    meta
):
    """
    Create an empty Zarr store for a given nside.
    """

    # meta comes from gather_metadata_and_coords
    manifest_df = meta["manifest_df"]
    total_time_steps = meta["total_time_steps"]
    sample_ds = meta["sample_ds"]
    variables_to_process = meta["variables_to_process"]
    concatenated_time_coords = meta["concatenated_time_coords"]

    npix_hp = 12 * nside**2

    logger.info(f"Initializing Zarr store: {zarr_path}")
    logger.info(f"  nside={nside}, npix_hp={npix_hp}, healpix_dim_name={healpix_dim_name}")

    if os.path.exists(zarr_path):
        logger.warning(f"Zarr store {zarr_path} already exists. Skipping creation.")
        return 1

    store = zarr.storage.LocalStore(zarr_path)
    root_group = zarr.group(store=store, overwrite=True, zarr_format=2)

    # --- Coordinates ---
    # Time coordinate
    time_attrs = sample_ds['time'].attrs.copy() if 'time' in sample_ds else {}
    time_dtype = concatenated_time_coords.dtype
    time_encoding = sample_ds['time'].encoding.copy() if 'time' in sample_ds else {}
    for k_enc in ['_FillValue', 'missing_value', 'dtype', 'chunks', 'compressor', 'filters', 'zarr_format', 'preferred_chunks']:
        time_encoding.pop(k_enc, None)

    za_time = root_group.create_array(
        name='time',
        shape=(int(total_time_steps),),
        chunks=(int(time_chunk_size),),
        dtype=time_dtype,
        overwrite=True
    )
    za_time[:] = concatenated_time_coords
    za_time.attrs.update(time_attrs)
    za_time.attrs.update(time_encoding)
    # Ensure units attribute is present for datetime decoding
    if 'units' not in za_time.attrs:
        za_time.attrs['units'] = time_encoding.get('units', 'hours since 2000-01-01 00:00:00')
    za_time.attrs['_ARRAY_DIMENSIONS'] = ['time']
    logger.info(f"Created 'time' coordinate: shape=({total_time_steps}), chunks=({time_chunk_size}), dtype={time_dtype}")

    # HEALPix coordinate
    hp_indices_attrs = {'long_name': 'HEALPix pixel index', 'units': '1', 'nside': nside}
    za_hp_indices = root_group.create_array(
        name=healpix_dim_name,
        shape=(int(npix_hp),),
        chunks=(int(spatial_chunk_size),),
        dtype='i4',
        overwrite=True
    )
    za_hp_indices[:] = np.arange(npix_hp)
    za_hp_indices.attrs.update(hp_indices_attrs)
    za_hp_indices.attrs['_ARRAY_DIMENSIONS'] = [healpix_dim_name]
    logger.info(f"Created '{healpix_dim_name}' coordinate: shape=({npix_hp}), chunks=({spatial_chunk_size}), dtype=i4")

    # Vertical coordinates
    # vertical_dim_names = ["nVertLevels", "nVertLevelsP1", "nSoilLevels"]
    # ^-- vertical_dim_names defined as module data, so should not need it here.
    for vdim in vertical_dim_names:
        if vdim in sample_ds.coords or vdim in sample_ds.dims:
            if vdim in sample_ds.coords:
                vert_coord_data = sample_ds[vdim].data
                vert_attrs = sample_ds[vdim].attrs.copy()
                vert_encoding = sample_ds[vdim].encoding.copy()
            else:
                vert_coord_data = np.arange(sample_ds.dims[vdim])
                vert_attrs = {'long_name': vdim}
                vert_encoding = {}
            for k_enc in ['_FillValue', 'missing_value', 'dtype', 'chunks', 'compressor', 'filters', 'zarr_format', 'preferred_chunks']:
                vert_encoding.pop(k_enc, None)
            vert_chunks = (int(len(vert_coord_data)),)
            za_vert = root_group.create_array(
                name=vdim,
                shape=(int(len(vert_coord_data)),),
                chunks=vert_chunks,
                dtype=vert_coord_data.dtype,
                overwrite=True
            )
            za_vert[:] = vert_coord_data
            za_vert.attrs.update(vert_attrs)
            za_vert.attrs.update(vert_encoding)
            za_vert.attrs['_ARRAY_DIMENSIONS'] = [vdim]
            logger.info(f"Created '{vdim}' coordinate: shape=({len(vert_coord_data)}), chunks=({vert_chunks[0]}), dtype={vert_coord_data.dtype}")

    # --- Variables ---
    compressor = Blosc(cname='zstd', clevel=3, shuffle=2)
    for var_name in variables_to_process:
        if var_name not in sample_ds:
            logger.warning(f"Variable '{var_name}' not found in sample file. Skipping.")
            continue
        sample_var = sample_ds[var_name]
        attrs = sample_var.attrs.copy()
        dtype = sample_var.dtype
        for k_pop in ['_FillValue', 'missing_value', 'coordinates', 'grid_mapping', 'cell_methods']:
            attrs.pop(k_pop, None)
        fill_value = sample_var.encoding.get('_FillValue', None)
        if fill_value is None and np.issubdtype(dtype, np.floating):
            fill_value = np.nan

        # Check if variable is time-varying
        is_time_varying = 'time' in sample_var.dims or 'Time' in sample_var.dims
           
        # Set up dimensions based on whether variable is time-varying
        if is_time_varying:
            var_dims_in_zarr = ['time', healpix_dim_name]
            var_shape_in_zarr = [int(total_time_steps), int(npix_hp)]
            var_chunks_in_zarr = [int(time_chunk_size), int(spatial_chunk_size)]
        else:
            var_dims_in_zarr = [healpix_dim_name]
            var_shape_in_zarr = [int(npix_hp)]
            var_chunks_in_zarr = [int(spatial_chunk_size)]

        # Check for vertical dimension
        var_vertical_dim = None
        for vdim in vertical_dim_names:
            if vdim in sample_var.dims:
                var_vertical_dim = vdim
                break
        is_3d = var_vertical_dim is not None
        if is_3d:
            if var_vertical_dim not in root_group:
                logger.error(f"ERROR: Vertical coordinate {var_vertical_dim} not found in root_group. Skipping variable {var_name}.")
                continue
            vert_dim_size = int(root_group[var_vertical_dim].shape[0])
            var_dims_in_zarr.append(var_vertical_dim)
            var_shape_in_zarr.append(vert_dim_size)
            var_chunks_in_zarr.append(vert_dim_size)

        logger.info(f"Initializing variable: {var_name}")
        logger.info(f"  Is time-varying: {is_time_varying}")
        logger.info(f"  Target Zarr Dims: {var_dims_in_zarr}")
        logger.info(f"  Shape: {var_shape_in_zarr}")
        logger.info(f"  Chunks: {var_chunks_in_zarr}")
        logger.info(f"  Dtype: {dtype}, Fill: {fill_value}")

        # # Check on the original spatial dimension
        # spatial_dim = None
        # for candidate in ["nCells", "nVertices"]:
        #     if candidate in sample_var.dims:
        #         spatial_dim = candidate
        #         break
        # if spatial_dim is None:
        #     logger.warning(f"Variable '{var_name}' does not have a recognized spatial dimension. Skipping.")
        #     continue
        logger.info(f"Initializing variable: {var_name}")
        logger.info(f"  Target Zarr Dims: {var_dims_in_zarr}")
        logger.info(f"  Shape: {var_shape_in_zarr}")
        logger.info(f"  Chunks: {var_chunks_in_zarr}")
        logger.info(f"  Dtype: {dtype}, Fill: {fill_value}")

        za_var = root_group.create_array(
            name=var_name,
            shape=tuple(var_shape_in_zarr),
            chunks=tuple(var_chunks_in_zarr),
            dtype=dtype,
            compressor=compressor,
            fill_value=fill_value,
            overwrite=True
        )
        za_var.attrs.update(attrs)
        za_var.attrs['healpix_nside'] = nside
        za_var.attrs['healpix_dim_name'] = healpix_dim_name
        za_var.attrs['_ARRAY_DIMENSIONS'] = var_dims_in_zarr

    # Optionally consolidate metadata
    zarr.consolidate_metadata(store)
    logger.info(f"Zarr store {zarr_path} initialized and metadata consolidated.")
    return 0



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

        # ds_mpas_new = ds_mpas.assign_coords(time=('Time', hours_since))
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
        ds_mpas_new =  ds_mpas.assign_coords(time=time_var)
    else:
        raise ValueError("Need more information about MPAS time dimension.")

    # Find variables with dtype 'S64'
    s64_vars = [var for var in ds_mpas_new.variables if ds_mpas_new[var].dtype == 'S64']
    logger.info(f"Variables with S64 dtype: {s64_vars}")

    # Drop these variables from the dataset
    ds_mpas_clean = ds_mpas_new.drop_vars(s64_vars)

    # Explicitly drop xtime and xtime_old if they are present:
    ds_mpas_clean = ds_mpas_clean.drop_vars(['xtime', 'xtime_old'], errors='ignore')

    return ds_mpas_clean


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Initialize a Zarr store for HEALPix regridded MPAS data using a manifest.")
    parser.add_argument("--manifest_path", required=True, help="Path to the input manifest CSV file.")
    parser.add_argument("--zarr_dir", required=True, help="Path to the directory for Zarr store to be created.")
    parser.add_argument("--zarr_prefix", required=True, help="Prefix for zarr file name.")
    parser.add_argument("--time_chunk_size", type=int, required=True, help="Chunk size for the time dimension.")
    parser.add_argument("--spatial_chunk_size", type=int, required=True, help="Chunk size for the HEALPix spatial dimension.")

    args = parser.parse_args()

    meta = gather_metadata_and_coords(args.manifest_path)
    healpix_dim_name = 'cell'

    for order in  [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]:        
        nside = hp.order2nside(order)
        npix = hp.nside2npix(nside)
        zarr_path = f"{args.zarr_dir}/{args.zarr_prefix}_to_hp{order}.zarr"
        create_zarr_for_nside(
            zarr_path=zarr_path,
            nside=nside,
            healpix_dim_name='cell',
            time_chunk_size=args.time_chunk_size,
            spatial_chunk_size=args.spatial_chunk_size,
            meta=meta
        )

    # mpas_spatial_dim_name = 'nCells'

    # with ProgressBar():
    #     status = initialize_zarr_store(
    #         manifest_path=args.manifest_path,
    #         zarr_path=args.zarr_path,
    #         nside=nside,
    #         healpix_dim_name=healpix_dim_name,
    #         mpas_spatial_dim_name=mpas_spatial_dim_name,
    #         time_chunk_size=args.time_chunk_size,
    #         spatial_chunk_size=args.spatial_chunk_size, # use 196608 b/c it evenly divides the healpix grids
    #         )
    # exit(status)




    ########### Archived initial version of this code ###############
    ########### Ok to delete.

    # def initialize_zarr_store(manifest_path, zarr_path, nside, healpix_dim_name, mpas_spatial_dim_name, time_chunk_size, spatial_chunk_size):
#     """
#     Initializes an empty Zarr store with the correct dimensions and chunking
#     based on a manifest file and a sample NetCDF file.
#     """
#     logger.info(f"Initializing Zarr store: {zarr_path}")
#     logger.info("Using manifest: {manifest_path}")

    # ALL MOVED TO:   gather_metadata_and_coords
    # manifest_df = pd.read_csv(manifest_path)
    # total_time_steps = manifest_df['num_timesteps'].sum()
    # first_file_path = manifest_df['filepath'].iloc[0]

    # assumes "Time" in file, and converts to time (decodable)
    # sample_ds = pre_proc_mpas_file(first_file_path)

    # variables_to_process = [
    #     v for v in sample_ds.data_vars
    #     if sample_ds[v].dtype.kind in {'f', 'i'}  # Only float or int types
    # ]
    
    # # Collect all time coordinate values
    # all_time_coords_list = []
    # for idx, row in manifest_df.iterrows():
    #     logger.info(f"Reading time coordinates from: {row['filepath']} ({idx+1}/{len(manifest_df)})")
    #     current_ds = pre_proc_mpas_file(row['filepath'])
    #     time_data = current_ds['time'].copy(deep=True).data
    #     all_time_coords_list.append(time_data)
        
    # concatenated_time_coords = np.concatenate(all_time_coords_list)

    # MOVED TO create_zarr_for_nside
    # npix_hp = 12 * nside**2
    # logger.info(f"Target HEALPix nside: {nside}, npix: {npix_hp}, dimension name: {healpix_dim_name}")

    # # Check if Zarr store already exists
    # if os.path.exists(zarr_path):
    #     logger.info(f"Zarr store {zarr_path} already exists. Overwriting not enabled (remove manually if intended).")
    #     # For safety, don't overwrite unless explicitly told. Add an --overwrite flag if needed.
    #     return 1 # Indicate an issue

    # store = zarr.storage.LocalStore(zarr_path)
    # root_group = zarr.group(store=store, overwrite=True, zarr_format=2) # Start fresh

    # # --- Create Coordinates ---
    # # Time coordinate
    # time_attrs = sample_ds['time'].attrs.copy() if 'time' in sample_ds else {}
    # time_dtype = concatenated_time_coords.dtype
    # time_encoding = sample_ds['time'].encoding.copy() if 'time' in sample_ds else {}
    # for k_enc in ['_FillValue', 'missing_value', 'dtype', 'chunks', 'compressor', 'filters', 'zarr_format', 'preferred_chunks']:
    #     time_encoding.pop(k_enc, None)
    
    # za_time = root_group.create_array(name='time',
    #                                   shape=(int(total_time_steps),),
    #                                   chunks=(int(time_chunk_size),),
    #                                   dtype=time_dtype,
    #                                   overwrite=True)
    # za_time[:] = concatenated_time_coords
    # za_time.attrs.update(time_attrs)
    # za_time.attrs.update(time_encoding)
    # za_time.attrs['_ARRAY_DIMENSIONS'] = ['time']
    # logger.info(f"Created 'time' coordinate: shape=({total_time_steps}), chunks=({time_chunk_size}), dtype={time_dtype}")

    # # HEALPix dimension coordinate (pixel indices)
    # hp_indices_attrs = {'long_name': 'HEALPix pixel index',
    #                     'units': '1',
    #                     'nside': nside}
    # za_hp_indices = root_group.create_array(name=healpix_dim_name,
    #                                             shape=(int(npix_hp),),
    #                                             chunks=(int(spatial_chunk_size),), # Or npix_hp if not chunking this coord
    #                                             dtype='i4',
    #                                             overwrite=True)
    # za_hp_indices[:] = np.arange(npix_hp)
    # za_hp_indices.attrs.update(hp_indices_attrs)
    # za_hp_indices.attrs['_ARRAY_DIMENSIONS'] = [healpix_dim_name]
    # logger.info(f"Created '{healpix_dim_name}' coordinate: shape=({npix_hp}), chunks=({spatial_chunk_size}), dtype=i4")

    # # Create all vertical coordinate arrays present in the sample dataset
    # for vdim in vertical_dim_names:
    #     if vdim in sample_ds.coords or vdim in sample_ds.dims:
    #         if vdim in sample_ds.coords:
    #             vert_coord_data = sample_ds[vdim].data
    #             vert_attrs = sample_ds[vdim].attrs.copy()
    #             vert_encoding = sample_ds[vdim].encoding.copy()
    #         else:
    #             vert_coord_data = np.arange(sample_ds.dims[vdim])
    #             vert_attrs = {'long_name': vdim}
    #             vert_encoding = {}
    #         for k_enc in ['_FillValue', 'missing_value', 'dtype', 'chunks', 'compressor', 'filters', 'zarr_format', 'preferred_chunks']:
    #             vert_encoding.pop(k_enc, None)
    #         vert_chunks = (int(len(vert_coord_data)),)
    #         za_vert = root_group.create_array(
    #             name=vdim,
    #             shape=(int(len(vert_coord_data)),),
    #             chunks=vert_chunks,
    #             dtype=vert_coord_data.dtype,
    #             overwrite=True
    #         )
    #         za_vert[:] = vert_coord_data
    #         za_vert.attrs.update(vert_attrs)
    #         za_vert.attrs.update(vert_encoding)
    #         za_vert.attrs['_ARRAY_DIMENSIONS'] = [vdim]
    #         logger.info(f"Created '{vdim}' coordinate: shape=({len(vert_coord_data)}), chunks=({vert_chunks[0]}), dtype={vert_coord_data.dtype}")


    # # --- Initialize Datasets for each variable ---
    # compressor = Blosc(cname='zstd', clevel=3, shuffle=2)

    # for var_name in variables_to_process:
    #     if var_name not in sample_ds:
    #         logger.warning(f"WARNING: Variable '{var_name}' not found in sample file {first_file_path}. Skipping.")
    #         continue
        
    #     sample_var = sample_ds[var_name]
    #     attrs, dtype = get_var_attributes_and_dtype(sample_ds, var_name)
    #     fill_value = sample_var.encoding.get('_FillValue', None)
    #     if fill_value is None and np.issubdtype(dtype, np.floating):
    #         fill_value = np.nan

    #     var_dims_in_zarr = ['time', healpix_dim_name]
    #     var_shape_in_zarr = [int(total_time_steps), int(npix_hp)]
    #     var_chunks_in_zarr = [int(time_chunk_size), int(spatial_chunk_size)]

    #     # Check for vertical dimension
    #     var_vertical_dim = None
    #     for vdim in vertical_dim_names:
    #         if vdim in sample_var.dims:
    #             var_vertical_dim = vdim
    #             break
    #     is_3d = var_vertical_dim is not None
    #     if is_3d:
    #         if var_vertical_dim not in root_group:
    #             logger.error(f"ERROR: Vertical coordinate {var_vertical_dim} not found in root_group. Make sure it's in sample_ds.coords.")
    #             continue
    #         vert_dim_size = int(root_group[var_vertical_dim].shape[0])
    #         var_dims_in_zarr.append(var_vertical_dim)
    #         var_shape_in_zarr.append(vert_dim_size)
    #         var_chunks_in_zarr.append(vert_dim_size)  # or set a chunk size if you prefer

    #     logger.info(f"Initializing variable: {var_name}")
    #     logger.info(f"  Target Zarr Dims: {var_dims_in_zarr}")
    #     logger.info(f"  Shape: {var_shape_in_zarr}")
    #     logger.info(f"  Chunks: {var_chunks_in_zarr}")
    #     logger.info(f"  Dtype: {dtype}, Fill: {fill_value}")

    #     za_var = root_group.create_array(name=var_name,
    #                                         shape=tuple(var_shape_in_zarr),
    #                                         chunks=tuple(var_chunks_in_zarr),
    #                                         dtype=dtype,
    #                                         compressor=compressor,
    #                                         fill_value=fill_value,
    #                                         overwrite=True) # Overwrite since group is new
    #     za_var.attrs.update(attrs)
    #     # Add HEALPix specific attributes
    #     za_var.attrs['healpix_nside'] = nside
    #     za_var.attrs['healpix_dim_name'] = healpix_dim_name
    #     za_var.attrs['original_mpas_spatial_dim'] = mpas_spatial_dim_name
    #     za_var.attrs['_ARRAY_DIMENSIONS'] = var_dims_in_zarr

    # zarr.consolidate_metadata(store)
    # logger.info("Zarr store initialization complete and metadata consolidated.")
    # logger.info(f"Store path: {zarr_path}")
    # return 0
