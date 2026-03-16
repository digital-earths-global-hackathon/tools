# create_file_manifest.py
import argparse
import glob
import os
import xarray as xr
import pandas as pd
from dask.diagnostics import ProgressBar

def create_manifest(file_pattern, manifest_path, mpas_time_dim, check_files=False):
    """
    Scans input NetCDF files, determines time steps and offsets,
    and writes a manifest CSV file.
    """
    print(f"Scanning files with pattern: {file_pattern}")
    input_files = sorted(glob.glob(file_pattern))

    if not input_files:
        print(f"No files found for pattern: {file_pattern}")
        return

    print(f"Found {len(input_files)} files.")

    manifest_data = []
    current_time_offset = 0

    # Careful with this loop.
    # It is going to go through all identified files.
    # Use `check_files` flag to switch between:
    # - (True): open the file and get the number of time steps
    # - (False): ASSUME there is 1 timestep in the file, and just count.
    # When there are many large files, checking each one is slow.

    for i, nc_file in enumerate(input_files):
        print(f"Processing file {i+1}/{len(input_files)}: {nc_file}")
        try:
            if check_files:
                with xr.open_dataset(nc_file, chunks={mpas_time_dim: 1}) as ds:
                    num_times_in_file = len(ds[mpas_time_dim])
                    manifest_data.append({
                        'filepath': os.path.abspath(nc_file), # Store absolute path
                        'num_timesteps': num_times_in_file,
                        'time_offset_start': current_time_offset
                    })
                    current_time_offset += num_times_in_file
                # note: could also have elif here to only look at 1st file.
            else:
                num_times_in_file = 1 ## HARD_CODED_VALUE!!!!! (Could be parameter)
                manifest_data.append({                        
                        'filepath': os.path.abspath(nc_file), # Store absolute path
                        'num_timesteps': num_times_in_file,
                        'time_offset_start': current_time_offset
                        })
                current_time_offset += num_times_in_file
        except Exception as e:
            print(f"Could not process file {nc_file}: {e}. Skipping.")
            continue # or raise error

    if not manifest_data:
        print("WARNING: No data for the manifest.")
        return

    df = pd.DataFrame(manifest_data)
    # Ensure parent directory for manifest exists
    os.makedirs(os.path.dirname(manifest_path), exist_ok=True)
    df.to_csv(manifest_path, index=False)
    print(f"Manifest file created at: {manifest_path} with {len(df)} entries.")
    print(f"Total time steps to be processed: {current_time_offset}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create a manifest file for parallel Zarr processing.")
    parser.add_argument("--file_pattern", required=True, help="Glob pattern for input NetCDF files.")
    parser.add_argument("--manifest_path", required=True, help="Path to save the output manifest CSV file.")
    parser.add_argument("--mpas_time_dim", default="Time", help="Name of the time dimension in MPAS files (e.g., 'Time', 'time').")

    args = parser.parse_args()

    with ProgressBar(): # Optional: for Dask-based xarray reads
        create_manifest(args.file_pattern, args.manifest_path, args.mpas_time_dim)