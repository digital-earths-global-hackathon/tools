#!/bin/bash
# Main script to submit and coordinate PBS jobs for MPAS to HEALPix Zarr conversion.

echo "--- MPAS to HEALPix Zarr Parallel Conversion Orchestrator ---"

module load conda
conda activate rechunk


START_TIME_ORCHESTRATOR=$(date +%s)

# --- Configuration (MUST BE CAREFULLY SET BY USER) ---
PROJECT_CODE="P93300313"
HPC_QUEUE="casper" # Or "main", "regular", "premium", your specific HPC queue
PYTHON_ENV_PATH="/glade/u/home/brianpm/miniconda3/envs/rechunk" # Path to your conda/virtual env
PYTHON_SCRIPTS_DIR="/glade/u/home/brianpm/Code/hacknostics/healpix/workflow_parallel" # Where create_file_manifest.py etc. are

INPUT_DIR="/glade/campaign/mmm/wmr/fjudt/projects/dyamond_2/3.75km"

# DYAMOND2 - "History"
# FILE_PATTERN="history.2020-01-2[01]_*.nc" # TEST WITH A VERY SMALL SUBSET FIRST (1-2 files)
# FILE_PATTERN="history.2020*.nc" # For a full run
# DYAMOND2 - "Diag" (15min)
FILE_PATTERN="diag.2020-*.nc"

# Zarr store and manifest paths
# ZARR_OUTPUT_PARENT_DIR="/glade/campaign/cgd/cas/brianpm/hack25" # Use a new dir for new runs
ZARR_OUTPUT_PARENT_DIR="/glade/derecho/scratch/digital-earths-hackathon/mpas_DYAMOND2/15min"
ZARR_STORE_NAME="DYAMOND2_diag_15min_to_hp10.zarr"
ZARR_OUTPUT_PREFIX="DYAMOND2_diag_15min"
ZARR_OUTPUT_PATH="${ZARR_OUTPUT_PARENT_DIR}/${ZARR_STORE_NAME}"
# MANIFEST_FILE="${ZARR_OUTPUT_PARENT_DIR}/dy2_hist_3h_manifest.csv"
MANIFEST_FILE="${ZARR_OUTPUT_PARENT_DIR}/dy2_diag_15min_manifest.csv"

# Regridding and dimension names (align with your initialize_healpix_zarr.py)
WEIGHTS_DIR="/glade/derecho/scratch/digital-earths-hackathon/mpas_DYAMOND2"
WEIGHTS_FILE="${WEIGHTS_DIR}/mpas_to_healpix_weights_order10_wrap4ez4.nc"
HEALPIX_ORDER=10 # As per your initialize_healpix_zarr.py
HEALPIX_NSIDE=$((2**HEALPIX_ORDER)) # nside = 1024 for order 10
HEALPIX_DIM_NAME_ZARR="cell"        # As per your initialize_healpix_zarr.py ('cell')
MPAS_SPATIAL_DIM_NC="nCells"    # As per your initialize_healpix_zarr.py

# "Time" for DYAMOND2 history
# "time" for DYAMOND2 diag
# MPAS_TIME_DIM_NC="Time"         # Default for create_manifest and process_single_file
MPAS_TIME_DIM_NC="time"

# Zarr Chunking (align with your initialize_healpix_zarr.py arguments)
# Your initialize_healpix_zarr.py takes these as direct args now.
TIME_CHUNK_ZARR=1    # Has to be 1 if files only have 1 time entry (for parallelization)
NPIX_HP=$((12 * HEALPIX_NSIDE * HEALPIX_NSIDE))
SPATIAL_CHUNK_ZARR=$((NPIX_HP / 4)) # Example: For nside=1024, npix=12582912.  (/64 => Chunk ~196k cells. Tune this.)

# Variables to process (process_single_file_region.py infers from Zarr store, which infers from first MPAS file)
# So, no need to pass VARIABLES_TO_PROCESS_STR to process_single_file_region.py
# but initialize_healpix_zarr.py (your version) also infers them.

# --- PBS Job Resource Requests (Customize these!) ---
INIT_NCPUS=1
INIT_MEM="16GB" # Reading many time coords from manifest files for init
INIT_WALLTIME="02:30:00" # Increased slightly for potentially many files

PROCESS_NCPUS=4    # Cores per task. egr.apply_weights might use multiple.
PROCESS_MEM="90GB" # VERY IMPORTANT: Monitor memory for regridding. Adjust!
PROCESS_WALLTIME="01:15:00" # Per input file. Adjust based on file size and complexity.
PROCESS_ARRAY_CONCURRENCY=200 # Max number of array tasks (files) processed at once.

CONSOLIDATE_NCPUS=1
CONSOLIDATE_MEM="16GB" # Consolidating metadata for large Zarr can take memory
CONSOLIDATE_WALLTIME="01:00:00"

# --- Sanity Checks & Setup ---
# (Include checks for PROJECT_CODE, PYTHON_ENV_PATH, PYTHON_SCRIPTS_DIR, WEIGHTS_DIR as before)
mkdir -p "${ZARR_OUTPUT_PARENT_DIR}"
# Optional: Clean up for fresh test runs
# echo "WARNING: Removing old Zarr store and manifest for testing: ${ZARR_OUTPUT_PATH} ${MANIFEST_FILE}"
# rm -rf "${ZARR_OUTPUT_PATH}" "${MANIFEST_FILE}"


# --- Stage 0: Create File Manifest (Runs locally in this script) ---
module load conda
conda activate rechunk
if [ -f "${MANIFEST_FILE}" ]; then
    echo "Manifest file ${MANIFEST_FILE} already exists. Skipping manifest creation."
else
    echo "--- Stage 0: Creating File Manifest ---"
    python "${PYTHON_SCRIPTS_DIR}/create_file_manifest.py" \
        --file_pattern "${INPUT_DIR}/${FILE_PATTERN}" \
        --manifest_path "${MANIFEST_FILE}" \
        --mpas_time_dim "${MPAS_TIME_DIM_NC}"
    if [ $? -ne 0 ]; then echo "ERROR: File manifest creation failed."; exit 1; fi
fi
NUM_FILES_TO_PROCESS=$( (tail -n +2 "${MANIFEST_FILE}" | wc -l) || echo 0) # Count lines excluding header
if [ "${NUM_FILES_TO_PROCESS}" -lt 1 ]; then echo "No files found for processing in manifest (or manifest error). Exiting."; exit 1; fi
echo "Manifest created with ${NUM_FILES_TO_PROCESS} files to process."


# --- Stage 1: Submit Zarr Initialization Job ---
ZARR_INIT_CHECK="${ZARR_OUTPUT_PARENT_DIR}/${ZARR_OUTPUT_PREFIX}_to_hp10.zarr"
if [ -d "${ZARR_INIT_CHECK}" ]; then
    echo "Zarr store ${ZARR_INIT_CHECK} already exists. Skipping Zarr initialization."
    INIT_JOB_ID="SKIPPED"
else
    echo "--- Stage 1: Submitting Zarr Initialization Job ---"
    INIT_PBS_SCRIPT="${ZARR_OUTPUT_PARENT_DIR}/pbs_init_zarr_job.sh"
    cat <<EOF > "${INIT_PBS_SCRIPT}"
    #!/bin/bash
    #PBS -N ZarrInit_HP${HEALPIX_NSIDE}
    #PBS -A ${PROJECT_CODE} -q ${HPC_QUEUE} -j oe -m n
    #PBS -l select=1:ncpus=${INIT_NCPUS}:mem=${INIT_MEM}
    #PBS -l walltime=${INIT_WALLTIME}

    echo "--- Zarr Init Job Started: \$(date) on \$(hostname) ---"
    module load conda
    conda activate rechunk
    set -e # Exit on error

    # Match arguments to your initialize_healpix_zarr.py
    python "${PYTHON_SCRIPTS_DIR}/initialize_healpix_zarr.py" \\
        --manifest_path "${MANIFEST_FILE}" \\
        --zarr_dir "${ZARR_OUTPUT_PARENT_DIR}" \\
        --zarr_prefix "${ZARR_OUTPUT_PREFIX}" \\
        --time_chunk_size "${TIME_CHUNK_ZARR}" \\
        --spatial_chunk_size "${SPATIAL_CHUNK_ZARR}"

    if [ \$? -ne 0 ]; then echo "ERROR: Zarr Initialization script failed."; exit 1; fi
    echo "--- Zarr Init Job Finished: \$(date) ---"
EOF
    chmod +x "${INIT_PBS_SCRIPT}"
    INIT_JOB_ID=$(qsub "${INIT_PBS_SCRIPT}")
    if [ -z "${INIT_JOB_ID}" ]; then echo "ERROR: qsub failed for Initialization Job."; exit 1; fi
    echo "Initialization Job ID: ${INIT_JOB_ID}"
fi

# --- Stage 2: Submit Parallel Processing Array Job ---
echo "--- Stage 2: Submitting Parallel Processing Array Job ---"
ARRAY_UPPER_BOUND=$((${NUM_FILES_TO_PROCESS} - 1))
PROCESS_PBS_SCRIPT="${ZARR_OUTPUT_PARENT_DIR}/pbs_process_array_job.sh"
cat <<EOF > "${PROCESS_PBS_SCRIPT}"
#!/bin/bash
#PBS -N ZarrProcess_HP${HEALPIX_NSIDE}
#PBS -A ${PROJECT_CODE} -q ${HPC_QUEUE} -j oe -m n
#PBS -l select=1:ncpus=${PROCESS_NCPUS}:mem=${PROCESS_MEM}
#PBS -l walltime=${PROCESS_WALLTIME}

echo "--- Process Array Task Started: \$(date) on \$(hostname) ---"
echo "Job ID: \${PBS_JOBID}, Array Index: \${PBS_ARRAY_INDEX}"
module load conda
conda activate rechunk
set -e

# Get file info from manifest (skip header row using tail -n +2 then head/tail or sed)
# PBS_ARRAY_INDEX is 0-based. Manifest lines (after header) are 1-based for sed.
MANIFEST_DATA_LINE_NUM=\$((\${PBS_ARRAY_INDEX} + 2)) # +1 for 0-index, +1 for header
MANIFEST_LINE=\$(sed -n "\${MANIFEST_DATA_LINE_NUM}p" "${MANIFEST_FILE}")

if [ -z "\${MANIFEST_LINE}" ]; then echo "ERROR: Could not read line \${MANIFEST_DATA_LINE_NUM} from ${MANIFEST_FILE}"; exit 1; fi

NC_FILE_PATH=\$(echo "\${MANIFEST_LINE}" | cut -d',' -f1)
# NUM_TIME_STEPS_IN_FILE=\$(echo "\${MANIFEST_LINE}" | cut -d',' -f2) # This will be 1
TIME_OFFSET=\$(echo "\${MANIFEST_LINE}" | cut -d',' -f3)

echo "Processing File: \${NC_FILE_PATH}"
echo "Time Offset: \${TIME_OFFSET}"

python "${PYTHON_SCRIPTS_DIR}/process_single_file_region.py" \\
    --nc_file_path "\${NC_FILE_PATH}" \\
    --zarr_dir "${ZARR_OUTPUT_PARENT_DIR}" \\
    --zarr_prefix "${ZARR_OUTPUT_PREFIX}" \\
    --time_offset "\${TIME_OFFSET}" \\
    --nside "${HEALPIX_NSIDE}" \\
    --healpix_dim_name_zarr "${HEALPIX_DIM_NAME_ZARR}"
    # process_single_file_region.py infers variables and dims and weights internally

if [ \$? -ne 0 ]; then echo "ERROR: File Processing script failed for \${NC_FILE_PATH}."; exit 1; fi
echo "--- Process Array Task Finished for \${NC_FILE_PATH}: \$(date) ---"
EOF
chmod +x "${PROCESS_PBS_SCRIPT}"

# IF RUNNING INITIALIZATION, CAN MAKE IT DEPENDENT:
ARRAY_JOB_ID=$(qsub -J 0-${ARRAY_UPPER_BOUND}%${PROCESS_ARRAY_CONCURRENCY} -W depend=afterok:${INIT_JOB_ID} "${PROCESS_PBS_SCRIPT}")
# IF NOT RUNNING INITIALIZATION:
# ARRAY_JOB_ID=$(qsub -J 0-${ARRAY_UPPER_BOUND}%${PROCESS_ARRAY_CONCURRENCY} "${PROCESS_PBS_SCRIPT}")

if [ -z "${ARRAY_JOB_ID}" ]; then echo "ERROR: qsub failed for Array Processing Job."; qdel "${INIT_JOB_ID}"; exit 1; fi
echo "Array Processing Job ID: ${ARRAY_JOB_ID}"
# Extract numeric job ID for dependency
ARRAY_JOB_NUM=$(echo "${ARRAY_JOB_ID}" | sed 's/\[.*//; s/\..*//')
echo "-->> ARRAY_JOB_NUM = ${ARRAY_JOB_NUM}"

# # --- Stage 3: Submit Zarr Metadata Consolidation Job ---
# echo "--- Stage 3: Submitting Zarr Consolidation Job ---"
# CONSOLIDATE_PBS_SCRIPT="${ZARR_OUTPUT_PARENT_DIR}/pbs_consolidate_job.sh"
# cat <<EOF > "${CONSOLIDATE_PBS_SCRIPT}"
# #!/bin/bash
# #PBS -N ZarrConsolidate_HP${HEALPIX_NSIDE}
# #PBS -A ${PROJECT_CODE} -q ${HPC_QUEUE} -j oe -m n
# #PBS -l select=1:ncpus=${CONSOLIDATE_NCPUS}:mem=${CONSOLIDATE_MEM}
# #PBS -l walltime=${CONSOLIDATE_WALLTIME}
# #PBS -o ${ZARR_OUTPUT_PARENT_DIR}/ZarrConsolidate_HP${HEALPIX_NSIDE}.out
# #PBS -e ${ZARR_OUTPUT_PARENT_DIR}/ZarrConsolidate_HP${HEALPIX_NSIDE}.err

# echo "--- Consolidate Job Started: \$(date) on \$(hostname) ---"
# module load conda
# conda activate rechunk
# set -e
# python -c "import zarr; import os; store_path = '${ZARR_OUTPUT_PATH}'; print(f'Consolidating metadata for {store_path}...'); store = zarr.storage.LocalStore(store_path); zarr.consolidate_metadata(store); print('Zarr metadata consolidated.');"
# if [ \$? -ne 0 ]; then echo "ERROR: Zarr metadata consolidation failed."; exit 1; fi
# echo "--- Consolidate Job Finished: \$(date) ---"
# EOF
# chmod +x "${CONSOLIDATE_PBS_SCRIPT}"
# CONSOLIDATE_JOB_ID=$(qsub -W depend=afterokarray:${ARRAY_JOB_NUM} "${CONSOLIDATE_PBS_SCRIPT}")
# if [ -z "${CONSOLIDATE_JOB_ID}" ]; then echo "ERROR: qsub failed for Consolidation Job."; qdel "${ARRAY_JOB_ID}"; qdel "${INIT_JOB_ID}"; exit 1; fi
# echo "Consolidation Job ID: ${CONSOLIDATE_JOB_ID}"

END_TIME_ORCHESTRATOR=$(date +%s)
ELAPSED_TIME_ORCHESTRATOR=$((${END_TIME_ORCHESTRATOR} - ${START_TIME_ORCHESTRATOR}))
echo "--- Workflow Submitted ---"
echo "Total submission script time: ${ELAPSED_TIME_ORCHESTRATOR} seconds."
echo "Monitor jobs: Init (${INIT_JOB_ID}), Array (${ARRAY_JOB_ID}), Consolidate (${CONSOLIDATE_JOB_ID})"