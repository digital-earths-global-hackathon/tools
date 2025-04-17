# Downloading data from https://hackathon-o.s3-ext.jc.rl.ac.uk using rclone

Author: Sam Green; sam.green@unsw.edu.au

## Step 1: Configure rclone (one-time setup)

First, create a new configuration for this external S3-compatible storage:

```bash
rclone config
```

Use the following options:

- **Choose**: n to create a new remote.
- **Enter a name, e.g.**: hackathon
- **Choose storage type**: s3 (option 4)
- **Enter provider**: choose Other (option 14)
- **Env_auth**: false
- **Access_key_id**: blank since it's public
- **Secret_access_key**: blank since it's public
- **Region**: blank (default)
- **Endpoint URL**: https://hackathon-o.s3-ext.jc.rl.ac.uk
- **Location constraint**: leave blank (default)
- **ACL, server-side encryption**: Blank (default).
- **Edit advanced config?**: no

Note: blank means press the 'return/enter' key without adding anything.

## Step 2: Verify your configuration

Verify your Zarr store path with:

```bash
rclone lsf hackathon:sim-data/dev/glm.n2560_RAL3p3/v4/data.healpix.PT1H.z1.zarr
```

## Step 3: Download (sync) the Zarr store locally:

Use sync as it only downloads updated or missing files. So if your download crashes running it again will only transfer files you don't have.

```bash
rclone sync -P hackathon:sim-data/dev/glm.n2560_RAL3p3/v4/data.healpix.PT1H.z1.zarr ./data.healpix.PT1H.z1.zarr --transfers=40 --multi-thread-streams=10 --checkers=100
```

- -P: Shows real-time progress.
- --transfers: files downloading in parallel.
- --multi-thread-streams: For single large files, this downloads chunks in parallel.
- --checkers: Number of parallel checks when looking for differences between source and destination.

## Extra steps:

You can create a bash script to loop through all available trasnfers:

```bash
#!/bin/bash

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

cd /path/to/directory/

for sim in "${sims[@]}"; do
    for freq in "${freqs[@]}"; do
        for zoom in "${zooms[@]}"; do
            echo "$sim $freq $zoom"
            mkdir -p $sim
            cd $sim
            rclone sync -P hackathon:sim-data/dev/$sim/v4/data.healpix.$freq.z$zoom.zarr ./data.healpix.$freq.z$zoom.zarr --transfers=40 --multi-thread-streams=10 --checkers=100
        done
    done
done
```

From here you can parallelise those loops to match what data you want and want your system can handle at once. ```--transfers=40 --multi-thread-streams=10 --checkers=100``` seemed to be the right settings for my HPC but your might be different.