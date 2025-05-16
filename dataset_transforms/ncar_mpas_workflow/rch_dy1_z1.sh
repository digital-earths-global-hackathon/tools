#!/bin/bash
#PBS -N rechunk1
#PBS -A P93300042
#PBS -l walltime=04:00:00
#PBS -l select=1:ncpus=16:mem=128GB
#PBS -q casper
#PBS -j oe
#PBS -k eod

set -e  # Exit on any error

# Load required modules
module load conda
conda activate p12

# Change to working directory
cd /glade/u/home/brianpm/Code/hacknostics/healpix

# Run the python script
# Replace with your actual input/output paths and any other arguments
python simpler_rechunker_v1.py \
    /glade/derecho/scratch/digital-earths-hackathon/mpas_DYAMOND1/15min/DYAMOND1_diag_15min_to_hp1.zarr \
    /glade/derecho/scratch/digital-earths-hackathon/mpas_DYAMOND1/15min/DYAMOND1_diag_PT15M_to_hp1.zarr \
    --time-chunk 384 \
    --cell-chunk 48 \
    --fix-time \
    --time-start "2016-08-01 00:00:00" \
    --time-end "2016-09-10 00:00:00" \
    --time-freq 15min
