# Downloading NICAM data from the nowake (Tokyo) http store using rclone

Author: Takashi Arakawa; arakawa@climtech.jp

The Global Hackathon Tokyo node (nowake) offers data access via [http://nowake.nicam.jp/files/](https://nowake.nicam.jp/files/)

*[Rclone](https://rclone.org/) is a command-line program to manage files on cloud storage.*

## Step 1: Configure rclone (one-time setup)

First, create a new configuration for this external S3-compatible storage:

```bash
rclone --config ./rclone.conf.nicam config
```

Use the following options:

- **Choose**: `n` to create a new remote.
- **Enter a name, e.g.**: `nicam`
- **Choose storage type**: `http`
- **Endpoint URL**: `https://nowake.nicam.jp/files/`
- **Edit advanced config?**: `no`

Note: *leave blank* means press the 'return/enter' key without adding anything.

## Step 2: Verify your configuration

Verify your Zarr store path with:

```bash
rclone --config ./rclone.conf.nicam lsf nicam:healpix/NICAM_2d3h_z0.zarr/
```

## Step 3: Download (sync) the Zarr store locally:

Use sync as it only downloads updated or missing files. So if your download crashes running it again will only transfer files you don't have.

```bash
rclone --config ./rclone.conf.nicam sync -P nicam:healpix/NICAM_2d3h_z0.zarr ./NICAM_2d3h_z0.zarr --transfers=40 --multi-thread-streams=10 --checkers=100
```

- `--progress`: Shows real-time progress.
- `--transfers`: files downloading in parallel.
- `--checkers`: Number of parallel checks when looking for differences between source and destination.
- `--multi-thread-streams`: For single large files, this downloads chunks in parallel.

## Extra steps:

You can create a bash script to loop through all available transfers:

```bash
#!/bin/bash
set -e  # Exit on any error
set -u  # Treat unset variables as an error

RCLONE_CONFIG="./rclone.conf.nicam"

# Check if rclone config file exists
if [ ! -f "$RCLONE_CONFIG" ]; then
    echo "Error: Rclone config file $RCLONE_CONFIG not found!" >&2
    exit 1
fi

REMOTE=nicam
REMOTE_PATH=healpix/
LOCAL_DIR=./NICAM_data  # YOU WILL NEED TO ADJUST THIS!

dims=(2d 3d)
freqs=(3h 6h)
zooms=({0..9})

# Create local output directory
mkdir -p "${LOCAL_DIR}"

# Generate filenames using combinations of modes and zoom levels
for dim in "${dims[@]}"; do
    for freq in "${freqs[@]}"; do
        for zoom in "${zooms[@]}"; do
            filename="NICAM_${dim}${freq}_z${zoom}.zarr"
            remote_path="${REMOTE}:${REMOTE_PATH}${filename}/"
            local_path="${LOCAL_DIR}/${filename}"

            echo "Checking $remote_path..."

            # Check if the remote directory exists (requires accessible structure in HTTP listing)
            if rclone --config "$RCLONE_CONFIG" lsf "$remote_path" >/dev/null 2>&1; then
                echo "Syncing $filename ..."
                rclone --config "$RCLONE_CONFIG" sync "$remote_path" "$local_path" --progress --transfers=40 --multi-thread-streams=10 --checkers=100
            else
                echo "Skipping $filename (not found)"
            fi
        done
    done
done
```
