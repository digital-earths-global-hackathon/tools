# Downloading data from the UK node S3 store using rclone

Authors: Sam Green, sam.green@unsw.edu.au; Mark Muetzelfeldt; mark.muetzelfeldt@reading.ac.uk

Set up for version v5 of the data - as used during the hackathon.

**Note, this is the current version (5 June, 2025) and the one that was used in the hackathon, but there is a new version in development
This is expected to be ready by 18 July, 2025 - contact mark.muetzelfeldt@reading.ac.uk for more info.**

The Global Hackathon UK node offers data access via [their S3 store](https://hackathon-o.s3-ext.jc.rl.ac.uk).

*[Rclone](https://rclone.org/) is a command-line program to manage files on cloud storage.*

## Step 1: Configure rclone (one-time setup)

First, create a new configuration for this external S3-compatible storage:

```bash
rclone config
```

Use the following options:

- **Choose**: `n` to create a new remote.
- **Enter a name, e.g.**: `hackathon`
- **Choose storage type**: `s3` (option 4)
- **Enter provider**: choose `Other` (option 34)
- **Env_auth**: `false`
- **Access_key_id**: leave blank since it's public
- **Secret_access_key**: leave blank since it's public
- **Region**: leave blank (default)
- **Endpoint URL**: `https://hackathon-o.s3-ext.jc.rl.ac.uk`
- **Location constraint**: leave blank (default)
- **ACL, server-side encryption**: leave blank (default).
- **Edit advanced config?**: `no`

Note: *leave blank* means press the 'return/enter' key without adding anything.

## Step 2: Verify your configuration

Verify your Zarr store path with:

```bash
rclone lsf hackathon:sim-data/dev/v5/glm.n2560_RAL3p3/um.PT1H.hp_z1.zarr
```

## Step 3: Download (sync) the Zarr store locally:

Use sync as it only downloads updated or missing files. So if your download crashes running it again will only transfer files you don't have.

```bash
rclone sync -P hackathon:sim-data/dev/v5/glm.n2560_RAL3p3/um.PT1H.hp_z1.zarr ./data.healpix.PT1H.z1.zarr --transfers=40 --multi-thread-streams=10 --checkers=100
```

- `-P`: Shows real-time progress.
- `--transfers`: files downloading in parallel.
- `--checkers`: Number of parallel checks when looking for differences between source and destination.
- `--multi-thread-streams`: For single large files, this downloads chunks in parallel.

## Extra steps:

You can create a bash script to loop through all available transfers:

```bash
#!/bin/bash

set -evxu

target_dir=/path/to/directory # YOU WILL NEED TO ADJUST THIS!

version=v5
sims=(
    # glm.n1280_CoMA9  # currently has no zarr store.
    glm.n2560_RAL3p3
    #glm.n1280_GAL9_nest
    #SAmer_km4p4_RAL3P3.n1280_GAL9_nest
    #Africa_km4p4_RAL3P3.n1280_GAL9_nest
    #SEA_km4p4_RAL3P3.n1280_GAL9_nest
    #SAmer_km4p4_CoMA9_TBv1.n1280_GAL9_nest
    #Africa_km4p4_CoMA9_TBv1.n1280_GAL9_nest
    #SEA_km4p4_CoMA9_TBv1.n1280_GAL9_nest
    #CTC_km4p4_RAL3P3.n1280_GAL9_nest
    #CTC_km4p4_CoMA9_TBv1.n1280_GAL9_nest
)
freqs=(PT1H PT3H)
zooms=({1..10})

mkdir -p ${target_dir}
cd ${target_dir}

for sim in "${sims[@]}"; do
    for freq in "${freqs[@]}"; do
        for zoom in "${zooms[@]}"; do
            echo "$sim $freq $zoom"

            mkdir -p $sim
            cd $sim

            rclone sync -P hackathon:sim-data/dev/$version/$sim/um.$freq.hp_z$zoom.zarr ./data.healpix.$freq.z$zoom.zarr --transfers=40 --multi-thread-streams=10 --checkers=100
        done
    done
done

```

From here you can parallelise those loops to match what data you want and want your system can handle at once. ```--transfers=40 --multi-thread-streams=10 --checkers=100``` seemed to be the right settings for my HPC but your might be different.
