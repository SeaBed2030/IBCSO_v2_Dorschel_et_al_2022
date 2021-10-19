#!/usr/bin/bash
#SBATCH --ntasks=1
#SBATCH --nodes=1
#SBATCH --cpus-per-task=12
#SBATCH --mem=32G # MEM usage peaks around 22 GB
#Xsrun I know what I am doing

module purge 2>/dev/null
module load ${CONDA} 2>/dev/null
source activate ${CONDA_ENV} #${CONDA_ENV_DEV}
which python

TYPE=$1	# get type (e.g. median)

export SSD_DIR=$(getSSD)  # get new ssd dir from tmp/tmp_$SLURM_JOBID
cd ${SSD_DIR}

# files on isibhv
COMPOSITE_FILE="${RASTERPRODUCT}/${timestamp}_${TYPE}_composite.tif"
MASK_FILE="${RASTERPRODUCT}/${timestamp}_tid_low_mask.tif"

# files on SSD
SSD_COMPOSITE="${SSD_DIR}/${timestamp}_${TYPE}_composite.tif"
SSD_MASK="${SSD_DIR}/${timestamp}_tid_low_mask.tif"
SSD_SRTM="${SSD_DIR}/$(basename ${STATIC_SRTM_GRID})"
SSD_SRTM_MASK="${SSD_DIR}/$(basename ${STATIC_SRTM_MASK})"

START_TIME=$(date -u +"%Y-%m-%d %T")
START_UNIX=$(date -u +%s%3N)
echo "SLURM_JOBID:	$SLURM_JOBID"
echo "EVOKER:	$EVOKER"
echo "START TIME:	$START_TIME"

cp ${COMPOSITE_FILE} ${SSD_COMPOSITE}
cp ${MASK_FILE} ${SSD_MASK}
cp ${STATIC_SRTM_GRID} ${SSD_SRTM}
cp ${STATIC_SRTM_MASK} ${SSD_SRTM_MASK}

# run gap filling algorithm
python ${PYDIR}/D6_gap_fill.py ${SSD_COMPOSITE} ${SSD_SRTM} ${SSD_MASK} --mask_srtm ${SSD_SRTM_MASK} ${GAP_FILL_SETTINGS} --type ${TYPE}

# HOUSEKEEPING
rm ${SSD_COMPOSITE} ${SSD_MASK} ${SSD_SRTM} ${SSD_SRTM_MASK}
mv ${SSD_DIR}/*.tif ${RASTERPRODUCT} # move all GeoTIFF raster files
mv ${SSD_DIR}/*.nc ${GRIDPRODUCT} # move factor netCDF grids from pygmt
mv ${SSD_DIR}/*.html ${LOGDIR} # dask performance report
mv ${SSD_DIR}/*.csv ${LOGDIR} 2>/dev/null # dask memory usage report (per task)
rm ${SSD_DIR}/*.tif 2>/dev/null
rm ${SSD_DIR}/*.nc 2>/dev/null

END_TIME=$(date -u +"%Y-%m-%d %T")
END_UNIX=$(date -u +%s%3N) #unix time in ms

echo "END TIME:	$END_TIME"
echo "CSVLINE	$SLURM_JOB_NAME	$SLURM_JOB_ID	$SLURM_ARRAY_TASK_ID	$SLURM_ARRAY_TASK_COUNT	$EVOKER	$SLURM_JOB_PARTITION	$FILESIZE_TRUE	$START_UNIX	$END_UNIX"
