# Downloading data from the DKRZ (Hamburg) S3 store using rclone

Author: Manuel Reis; reis@dkrz.de

The Global Hackathon Hamburg node (DKRZ) offers data access via [their Minio S3 store](https://min.io/):[http://s3.eu-dkrz-1.dkrz.cloud/](http://s3.eu-dkrz-1.dkrz.cloud/)

*[Rclone](https://rclone.org/) is a command-line program to manage files on cloud storage.*

## Step 1: Configure rclone (one-time setup)

First, create a new configuration for this external S3-compatible storage:

```bash
rclone config
```

Use the following options:

- **Choose**: `n` to create a new remote.
- **Enter a name, e.g.**: `dkrz`
- **Choose storage type**: `s3` (option 4)
- **Enter provider**: choose `Minio` (option 7 or 19 depending on `rclone` version)
- **Env_auth**: `false`
- **Access_key_id**: leave blank since it's public
- **Secret_access_key**: leave blank since it's public
- **Region**: `eu-dkrz-1`
- **Endpoint URL**: `https://s3.eu-dkrz-1.dkrz.cloud`
- **Location constraint**: leave blank (default)
- **ACL, server-side encryption**: leave blank (default).
- **Edit advanced config?**: `no`

Note: *leave blank* means press the 'return/enter' key without adding anything.

## Step 2: Verify your configuration

Verify your Zarr store path with:

```bash
rclone lsf dkrz:wrcp-hackathon/data/ICON/d3hp003.zarr/
```

## Step 3: Download (sync) the Zarr store locally:

Use sync as it only downloads updated or missing files. So if your download crashes running it again will only transfer files you don't have.

```bash
rclone sync dkrz:wrcp-hackathon/data/ICON/d3hp003.zarr/P1D_inst_z0_atm ./d3hp003.zarr/P1D_inst_z0_atm  --progress --transfers=40 --multi-thread-streams=10 --checkers=100 
```

- `--progress`: Shows real-time progress.
- `--transfers`: files downloading in parallel.
- `--checkers`: Number of parallel checks when looking for differences between source and destination.
- `--multi-thread-streams`: For single large files, this downloads chunks in parallel.

## Extra steps:

You can create a bash script to loop through all available transfers:

```bash
#!/bin/bash

set -evxu

target_dir=/path/to/directory # YOU WILL NEED TO ADJUST THIS!

sim=(d3hp003)
freqs=(P1D PT6H PT3H PT1H)
methods=(inst mean)
zooms=({0..9})

mkdir -p ${target_dir}/ICON/${sim}.zarr

groups=( $(rclone cat dkrz:wrcp-hackathon/data/ICON/${sim}.zarr/.zmetadata | jq -r '.metadata|keys[]|select(endswith(".zgroup"))|.[:-8]') )
for freq in "${freqs[@]}"; do
    for method in ${methods[@]}; do
        for zoom in "${zooms[@]}"; do
            if [[ ! ${groups[@]} =~ "${freq}_${method}_z${zoom}_atm" ]]; then 
                echo skipping  ${freq}.${method}.${zoom}
                continue
            fi
            echo rclone sync dkrz:wrcp-hackathon/data/ICON/${sim}.zarr/${freq}_${method}_z${zoom}_atm ${target_dir}/${sim}.zarr/${freq}_${method}_z${zoom}_atm --progress --transfers=40 --multi-thread-streams=10 --checkers=100
       done
    done
done


