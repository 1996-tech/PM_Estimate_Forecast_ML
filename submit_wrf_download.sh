#!/bin/bash
#SBATCH --job-name=priom_wrf_2024
#SBATCH --output=/data/lab/meng/priom_zarrah/uw_rainier/logs/wrf_download_%j.out
#SBATCH --error=/data/lab/meng/priom_zarrah/uw_rainier/logs/wrf_download_%j.err
#SBATCH --time=48:00:00
#SBATCH --partition=kamiak
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=priom.zarrah@wsu.edu

# Load modules
module purge
module load Python/3.9.6-GCCcore-11.2.0
module load SciPy-bundle/2021.10-foss-2021b
module load netCDF/4.8.1-gompi-2021b

# Set working directory
cd /data/lab/meng/priom_zarrah/uw_rainier

# Log job info
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURMD_NODENAME"
echo "Start time: $(date)"
echo "Working directory: $(pwd)"

# Run the download based on argument
SCENARIO=${1:-"test"}

case $SCENARIO in
    "test")
        echo "Running test download (January 2024)..."
        python3 wrf_kamiak_downloader.py \
            --hours "0,6,12,18" \
            --months "1" \
            --workers 4 \
            --base-dir "/data/lab/meng/priom_zarrah/uw_rainier/wrf_data_2024"
        ;;

    "spring")
        echo "Running spring download (Mar-May 2024)..."
        python3 wrf_kamiak_downloader.py \
            --hours "0,6,12,18" \
            --months "3,4,5" \
            --workers 6 \
            --base-dir "/data/lab/meng/priom_zarrah/uw_rainier/wrf_data_2024"
        ;;

    "summer")
        echo "Running summer download (Jun-Aug 2024)..."
        python3 wrf_kamiak_downloader.py \
            --hours "0,6,12,18" \
            --months "6,7,8" \
            --workers 6 \
            --base-dir "/data/lab/meng/priom_zarrah/uw_rainier/wrf_data_2024"
        ;;

    "fall")
        echo "Running fall download (Sep-Nov 2024)..."
        python3 wrf_kamiak_downloader.py \
            --hours "0,6,12,18" \
            --months "9,10,11" \
            --workers 6 \
            --base-dir "/data/lab/meng/priom_zarrah/uw_rainier/wrf_data_2024"
        ;;

    "winter")
        echo "Running winter download (Dec 2023, Jan-Feb 2024)..."
        python3 wrf_kamiak_downloader.py \
            --hours "0,6,12,18" \
            --months "12,1,2" \
            --workers 6 \
            --base-dir "/data/lab/meng/priom_zarrah/uw_rainier/wrf_data_2024"
        ;;

    "full")
        echo "Running full year download..."
        python3 wrf_kamiak_downloader.py \
            --hours "0,6,12,18" \
            --months "1,2,3,4,5,6,7,8,9,10,11,12" \
            --workers 8 \
            --base-dir "/data/lab/meng/priom_zarrah/uw_rainier/wrf_data_2024"
        ;;

    *)
        echo "Usage: sbatch submit_wrf_download.sh [test|spring|summer|fall|winter|full]"
        exit 1
        ;;
esac

echo "Download job completed at: $(date)"
