#!/usr/bin/bash
source "SEABED2030.config"
RUN_NAME="D4"

scancel -n${RUN_NAME}

CHAIN=$1

MSG_INFO "HOUSEKEEPING FOR STAGE D4..."
export PDIR=$(ls -d ${PRODUCTDIR}/* | tail -1) #get latest product
export timestamp=${PDIR##*/}

REPDIR=${PDIR}/${REPORTPRODUCT}
LOGDIR=${REPDIR}/LOGS/${RUN_NAME}
mkdir ${LOGDIR}
mkdir ${PDIR}/${RASTERPRODUCT}
mkdir empty_dir
rsync empty_dir/ ${LOGDIR}/
rsync empty_dir/ ${PDIR}/${RASTERPRODUCT}/
rm -r empty_dir
MSG_SUCCESS "HOUSEKEEPING DONE."

MSG_INFO "Converting netCDF grids to a more neat and GIS-friendly GeoTIFF format..."
NUMBER_OF_FILES=$(GET_NUMBER_OF_FILES "${PDIR}/${GRIDPRODUCT}/${timestamp}_*.grd")
sbatch -J${RUN_NAME} --array=1-${NUMBER_OF_FILES} --partition=mini,fat,xfat --time=00:10:00 --qos='short' --mail-user=${MAIL_PROJECT_DEV} --mail-type=${MAIL_ERROR_EXIT} --output=${LOGDIR}/${RUN_NAME}_%a.slurmlog --error=${LOGDIR}/${RUN_NAME}_%a.slurmerr ${SRUNDIR}/D4_product_to_GTIFF.srun

MSG_BATCH ${RUN_NAME}

CHAIN_SCRIPTS $CHAIN "${SCRIPTDIR}/D5_product_bending.sh" $RUN_NAME # call next script in processing queue (if evoked as part of processing chain)

