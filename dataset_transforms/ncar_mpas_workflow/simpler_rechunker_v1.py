import argparse
import sys
import os
import shutil # For potentially cleaning up an existing output directory


import xarray as xr
import pandas as pd
import dask.array as da
import zarr
import dask
from numcodecs import Blosc

from dask.diagnostics import ProgressBar

import signal
import gc
import time
import psutil  # Add to imports at top
from dask.distributed import get_client, Client, LocalCluster


#
# trying to use dask to speed things up
#
def optimize_dask_config(available_memory):
    """Configure dask based on available system memory"""
    chunk_size = min(256, int(available_memory * 0.1 / 1e6))  # 10% of memory, max 128MB
    dask.config.set({
        'array.chunk-size': f'{chunk_size}MiB',
        'distributed.worker.memory.target': 0.6,
        'distributed.worker.memory.spill': 0.8,
        'distributed.worker.memory.pause': 0.85
    })

def get_available_memory():
    """Get available system memory in bytes"""
    return psutil.virtual_memory().available

def estimate_batch_memory(ds_rechunked, batch_vars):
    """Estimate memory needed for a batch of variables"""
    return sum(ds_rechunked[var].nbytes for var in batch_vars)

# currentnly not used
def check_chunk_size(chunks, dtype):
    """Check if chunk size exceeds Blosc 2GB limit"""
    element_size = np.dtype(dtype).itemsize
    chunk_bytes = np.prod(chunks) * element_size
    if chunk_bytes > 2e9:
        print(f"WARNING: Chunk size {chunk_bytes/1e9:.2f}GB exceeds Blosc 2GB limit!")
        return False
    return True


def process_batch(ds_rechunked, batch_vars, final_chunks_for_rechunk):
    """Process a batch of variables"""
    while batch_vars:
        try:
            # Check if batch would use more than X% of available memory
            batch_size_bytes = estimate_batch_memory(ds_rechunked, batch_vars)
            available_memory = get_available_memory()
            memory_usage_gb = batch_size_bytes/1e9
            memory_available_gb = available_memory/1e9            
            print(f"Batch memory estimate: {memory_usage_gb:.1f}GB")
            print(f"Available memory: {memory_available_gb:.1f}GB")

            if batch_size_bytes > 0.9 * available_memory:
                # Reduce batch size by half
                # mid = len(batch_vars) // 2
                # print(f"Batch too large ({batch_size_bytes/1e9:.1f}GB), splitting in half")
                # return process_batch(ds_rechunked, batch_vars[:mid], final_chunks_for_rechunk)
                print("ESTIMATING THAT BATCH IS TOO LARGE. SEE WHAT HAPPENS...")
            
            print(f"Processing {len(batch_vars)} variables...")
            ds_batch = ds_rechunked[batch_vars]
            # with ProgressBar():
            #     ds_batch = ds_batch.compute()
            return ds_batch
            
        except MemoryError as me:
            if len(batch_vars) == 1:
                raise RuntimeError(f"Cannot process even a single variable: {batch_vars[0]}")
            # Reduce batch size and retry
            mid = len(batch_vars) // 2
            print(f"Memory error, reducing batch from {len(batch_vars)} to {mid} variables")
            return process_batch(ds_rechunked, batch_vars[:mid], final_chunks_for_rechunk)

def get_completed_variables(output_path):
    """Get list of variables already present in output zarr.
       This is used when trying to resume.
    """
    if not os.path.exists(output_path):
        return set()
    try:
        store = zarr.open(output_path, mode='r')
        return set(store.array_keys())
    except Exception as e:
        print(f"Error reading existing zarr store: {e}")
        return set()


def rechunk_zarr_flexible(input_path: str,
                          output_path: str,
                          target_chunks_map: dict,
                          overwrite_output: bool = False,
                          resume: bool = False,
                          fix_time: bool = False,
                          time_config: dict = None):
    """
    Rechunks a Zarr archive to new chunk sizes using Xarray and Dask.
    """
    try:
        if os.path.exists(output_path):
            if resume:
                print(f"Resuming processing with existing output at: {output_path}")
            elif overwrite_output:
                print(f"Warning: Output path '{output_path}' already exists and will be overwritten.")
                try:
                    shutil.rmtree(output_path)
                    print(f"Successfully removed existing directory: {output_path}")
                except OSError as e:
                    print(f"Error removing directory {output_path}: {e}")
                    return
            else:
                print(f"Error: Output path '{output_path}' already exists. "
                    f"Use --overwrite to replace or --resume to continue processing.")
                return

        # Configure dask based on system memory
        available_memory = get_available_memory()
        optimize_dask_config(available_memory)
        
        # Create local dask cluster
        cluster = LocalCluster(
            n_workers=16,  # Adjust based on your needs
            threads_per_worker=1,
            memory_limit='15GB'  # Adjust based on your system
        )
        client = Client(cluster)
        print(f"Dask dashboard available at: {client.dashboard_link}")

        print(f"Starting rechunking process...")
        print(f"Input Zarr: {input_path}")
        print(f"Output Zarr: {output_path}")
        print(f"Target Chunks Configuration: {target_chunks_map}")

        if not os.path.exists(input_path):
            print(f"Error: Input path '{input_path}' does not exist.")
            return

        ds_original = None  # Initialize to ensure it's defined for the finally block

        try:
            # If fixing time, generate correct time values
            if fix_time and time_config:
                print("\nGenerating correct time coordinates...")
                correct_times = pd.date_range(
                    start=time_config['start_time'],
                    end=time_config['end_time'],
                    freq=time_config['frequency']
                )
                print(f"Generated {len(correct_times)} time points.")
                print(f"First time: {correct_times[0]}")
                print(f"Last time: {correct_times[-1]}")
                
                # Create new time coordinate
                reference_date = pd.Timestamp('2000-01-01 00:00:00')
                time_values = ((correct_times - reference_date).total_seconds() / 60.0).values
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

            # 1. Open the source Zarr dataset
            # consolidated=True is recommended for reading Zarr stores.
            print(f"\nOpening source dataset: {input_path}")

            # Open the source zarr store
            source = zarr.open(input_path, mode='r')
            # Create dictionary to store dask arrays
            data_vars = {}

            # Load each variable as a dask array with optimal chunks
            arrays = list(source.arrays())  # Get list of arrays first
            if not arrays:
                raise ValueError("No arrays found in the Zarr store")            
                
            # Load each variable as a dask array with optimal chunks
            for var_name, zarray in arrays:
                print(f"Variable: {var_name}, Shape: {zarray.shape}, Attributes: {dict(zarray.attrs)}")
                
                # Get all attributes first
                attrs = dict(zarray.attrs)
                
                # Get dimensions - either from _ARRAY_DIMENSIONS or infer from shape
                if '_ARRAY_DIMENSIONS' in attrs:
                    dims = attrs.pop('_ARRAY_DIMENSIONS')  # Remove from attrs after getting dims
                else:
                    # Infer dimensions based on array shape
                    dims = [f"dim_{i}" for i in range(len(zarray.shape))]
                    print(f"Warning: No _ARRAY_DIMENSIONS found for {var_name}, using inferred dims: {dims}")
                
                # Create the dask array
                data = da.from_zarr(source[var_name])
                
                # Store variable with dimensions and attributes
                data_vars[var_name] = (dims, data, attrs)
            
            # Create xarray dataset from dask arrays
            # Create xarray dataset with explicit data_vars format

            # Remove CF time attributes if fixing time to prevent decoding attempts
            if fix_time and 'time' in data_vars:
                # Remove time attributes that trigger decoding
                for attr in ['units', 'calendar']:
                    data_vars['time'][2].pop(attr, None)
                print("Removed CF time attributes to prevent decoding")

            # Create xarray dataset from dask arrays
            ds_original = xr.Dataset(
                {
                    var_name: xr.DataArray(
                        data=data_vars[var_name][1],  # the dask array
                        dims=data_vars[var_name][0],  # the dimensions
                        attrs=data_vars[var_name][2]  # the attributes
                    )
                    for var_name in data_vars
                }
            )

            # After creating ds_original and before rechunking
            if fix_time and time_config:
                print("Updating time coordinate with corrected values...")
                ds_original = ds_original.assign_coords(time=new_time)
                print("Time coordinate updated successfully")
                print(f"    New time range: {ds_original.time.values[0]} to {ds_original.time.values[-1]}")

            print("Source dataset opened successfully.")
            print("\nOriginal chunking (for a sample variable if present):")
            for var_name, data_array in ds_original.data_vars.items():
                print(f"  Variable: {var_name}, Orig Chunks: {data_array.chunks}")
                break

            # 2. Prepare the final chunking dictionary for ds.chunk()
            # This dictionary will be passed to the .chunk() method.
            # For each dimension in target_chunks_map, use the specified chunk size
            # or the dimension's actual size if it's smaller.
            # Dimensions in the dataset NOT listed in target_chunks_map will typically
            # be consolidated into a single chunk along that axis by ds.chunk().
            final_chunks_for_rechunk = {}
            print("\nDetermining effective chunk sizes for rechunking:")
            for dim_name, desired_chunk_size in target_chunks_map.items():
                if dim_name in ds_original.dims:
                    actual_dim_size = ds_original.dims[dim_name]
                    chunk_size = min(actual_dim_size, desired_chunk_size)
                    final_chunks_for_rechunk[dim_name] = chunk_size
                    print(f"  Dimension '{dim_name}': Target chunk {desired_chunk_size}, "
                        f"Actual dim size {actual_dim_size}. Effective chunk set to {chunk_size}.")
                    if desired_chunk_size > actual_dim_size:
                        print(f"    (Note: Target chunk for dim '{dim_name}' was larger than the dimension itself.)")
                else:
                    print(f"  Warning: Dimension '{dim_name}' from target_chunks_map "
                        f"not found in the dataset. It will be ignored in the .chunk() call.")

            if not final_chunks_for_rechunk:
                raise ValueError("The 'target_chunks_map' did not contain any dimensions "
                                "present in the dataset, or was empty. No rechunking can be performed.")
            
            print(f"\nEffective chunking to be applied by ds.chunk(): {final_chunks_for_rechunk}")
            print("Dimensions present in the dataset but not specified in this map will "
                "typically be consolidated into single chunks by Xarray/Dask.")

            # 3. Rechunk the dataset "virtually" (sets up the Dask graph)
            ds_rechunked = ds_original.chunk(chunks=final_chunks_for_rechunk)

            print("\nDataset virtually rechunked. New Dask graph chunking:")
            for var_name, data_array in ds_rechunked.data_vars.items():
                # data_array.chunks is a tuple of tuples, showing chunk sizes for each dimension
                print(f"  Variable: {var_name}, New Dask Chunks: {data_array.chunks}")
            for coord_name, coord_array in ds_rechunked.coords.items():
                if coord_array.chunks: # Only print for chunked coordinates
                    print(f"  Coordinate: {coord_name}, New Dask Chunks: {coord_array.chunks}")

            # 4. Write the rechunked data to a new Zarr store
            print(f"\nWriting rechunked dataset to: {output_path}")
            # Process variables in smaller batches to reduce graph size

            # Adjust batch size based on variable sizes
            total_size = sum(ds_rechunked[var].nbytes for var in ds_rechunked.data_vars)
            avg_var_size = total_size / len(ds_rechunked.data_vars)
            batch_size = max(1, int(1e9 / avg_var_size))  # Target ~1GB per batch
            print(f"Calculated optimal batch size: {batch_size} variables")


            # Before processing batches, get list of completed variables
            completed_vars = get_completed_variables(output_path)
            if completed_vars:
                print(f"\nFound {len(completed_vars)} already processed variables")
                print("Already processed:", completed_vars)
                
            # Filter out already processed variables
            all_vars = [var for var in list(ds_rechunked.data_vars) 
                       if var not in completed_vars]
            
            if not all_vars:
                print("All variables have been processed. Nothing to do.")
                return
            
            # Create the output store if it doesn't exist
            if not os.path.exists(output_path):
                store = zarr.open(output_path, mode='w')
            elif resume:
                store = zarr.open(output_path, mode='a')

            # Process variables in batches
            total_batches = (len(all_vars) + batch_size - 1) // batch_size
            start_time = time.time()
            for i in range(0, len(all_vars), batch_size):
                batch_num = i // batch_size + 1
                print(f"\nProcessing batch {batch_num}/{total_batches} "
                    f"(variables {i+1}-{min(i+batch_size, len(all_vars))} of {len(all_vars)})")
            
                batch_vars = all_vars[i:i + batch_size]
                ds_batch = process_batch(ds_rechunked, batch_vars, final_chunks_for_rechunk)

                # print(f"\nProcessing batch of variables {i+1}-{min(i+batch_size, len(all_vars))} of {len(all_vars)}")
                
                # # Create a subset of the dataset with just this batch of variables
                # ds_batch = ds_rechunked[batch_vars]

                # # Load the batch into memory with retries
                # print("Loading batch into memory...")
                # for attempt in range(3):  # Try up to 3 times
                #     try:
                #         with ProgressBar():
                #             ds_batch = ds_batch.compute()
                #         break
                #     except Exception as e:
                #         print(f"Attempt {attempt + 1} failed: {e}")
                #         if attempt == 2:  # Last attempt
                #             raise
                #         time.sleep(5)  # Wait before retry
                
                # Prepare encoding for this batch
                encoding = {
                    var: {'chunks': tuple(
                        final_chunks_for_rechunk[dim] if dim in final_chunks_for_rechunk 
                        else ds_rechunked[var].sizes[dim]
                        for dim in ds_rechunked[var].dims
                    )}
                    for var in batch_vars
                }
                # Special handling for time variable
                if 'time' in batch_vars:
                    if fix_time:
                        # Apply new time encoding if fixing time
                        encoding['time'].update({
                            'compressor': Blosc(cname='zstd', clevel=3, shuffle=2),
                            'dtype': '<f8',
                            '_FillValue': None,
                            'filters': None
                        })
                    else:
                        # Preserve original time values without decoding
                        encoding['time'].update({
                            'dtype': '<f8',
                            '_FillValue': None
                        })     
                # Write this batch - note we're writing directly to the path
                print(f"Writing batch {i//batch_size + 1} of {(len(all_vars) + batch_size - 1)//batch_size}")
                if i == 0 and not resume:
                    mode = 'w'  # First batch - create new store
                else:
                    mode = 'a'  # Subsequent batches - append
                
                # Write directly from memory (no delayed operation needed)
                ds_batch.to_zarr(
                    output_path,
                    mode=mode,
                    encoding=encoding,
                    consolidated=True,
                    zarr_format=2
                )
                elapsed = time.time() - start_time
                eta = (elapsed / batch_num) * (total_batches - batch_num)
                print(f"Elapsed: {elapsed:.1f}s, ETA: {eta:.1f}s")
                
                # Clear memory
                ds_batch = None
                gc.collect()  # Force garbage collection
                time.sleep(1)  # Give system time to free memory

            # Write consolidated metadata at the end
            print("\nConsolidating metadata...")
            zarr.consolidate_metadata(output_path)

            print("\nRechunking process completed successfully!")
            print(f"New Zarr archive saved at: {output_path}")
        except MemoryError as me:
            print(f"Out of memory error: {me}")
            print("Try reducing batch size or chunk sizes")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            # traceback.print_exc()
        finally:
            if 'ds_batch' in locals() and ds_batch is not None:
                del ds_batch
            if 'ds_original' in locals() and ds_original is not None:
                ds_original.close()
            if 'ds_rechunked' in locals() and ds_rechunked is not None:
                ds_rechunked.close()
    finally:
        if 'client' in locals():
            client.close()
        if 'cluster' in locals():
            cluster.close()

# --- Example Usage ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Rechunk a Zarr archive with specified chunk sizes.')
    parser.add_argument('input_path', type=str, help='Path to input Zarr archive')
    parser.add_argument('output_path', type=str, help='Path for output Zarr archive')
    parser.add_argument('--time-chunk', type=int, default=384,
                      help='Chunk size for time dimension (default: 384)')
    parser.add_argument('--cell-chunk', type=int, default=48,
                      help='Chunk size for cell dimension (default: 48)')
    parser.add_argument('--fix-time', action='store_true',
                      help='Fix time coordinates during rechunking')
    parser.add_argument('--time-start', type=str,
                      help='Start time (e.g., "2020-01-20 00:00:00")')
    parser.add_argument('--time-end', type=str,
                      help='End time (e.g., "2020-03-01 00:00:00")')
    parser.add_argument('--time-freq', type=str, default='15min',
                      help='Time frequency (default: "15min")')
    parser.add_argument('--overwrite', action='store_true',
                      help='Overwrite output if it exists')
    parser.add_argument('--resume', action='store_true',
                      help='Resume processing from last completed variable')

    args = parser.parse_args()

    # Modify the overwrite check in rechunk_zarr_flexible
    if os.path.exists(args.output_path) and not (args.overwrite or args.resume):
        print(f"Error: Output path '{args.output_path}' already exists. "
              f"Use --overwrite to replace or --resume to continue processing.")
        sys.exit(1)

    time_config = None
    if args.fix_time:
        if not (args.time_start and args.time_end):
            parser.error("--fix-time requires --time-start and --time-end")
        time_config = {
            'start_time': args.time_start,
            'end_time': args.time_end,
            'frequency': args.time_freq
        }


    # Define chunk sizes based on command line arguments
    desired_chunks = {
        'time': args.time_chunk,
        'cell': args.cell_chunk
    }

    # Simple signal handler without client
    def signal_handler(signum, frame):
        print("\nReceived shutdown signal. Cleaning up...")
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Direct call without client
    rechunk_zarr_flexible(
        input_path=args.input_path,
        output_path=args.output_path,
        target_chunks_map=desired_chunks,
        overwrite_output=args.overwrite,
        fix_time=args.fix_time,
        time_config=time_config,
        resume=args.resume
    )