#!/usr/bin/bash
source "SEABED2030.config"
RUN_NAME="D6"

scancel -n${RUN_NAME}

CHAIN=$1

MSG_INFO "HOUSEKEEPING: Setting up directories involved in gap filling grid using SRTM15 v2..."
export PDIR=$(ls -d $PRODUCTDIR/* | tail -1) #get latest product folder
export timestamp=${PDIR##*/}
REPDIR=${PDIR}/${REPORTPRODUCT}
export LOGDIR=${REPDIR}/LOGS/${RUN_NAME}
export RASTERPRODUCT=${PDIR}/${RASTERPRODUCT}
export GRIDPRODUCT=${PDIR}/${GRIDPRODUCT}
mkdir ${LOGDIR}
mkdir ${RASTERPRODUCT}
mkdir empty_dir
rsync empty_dir/ ${LOGDIR}/
rm -r empty_dir
MSG_SUCCESS "HOUSEKEEPING: Done."

MSG_INFO "Start gap filling process..."
sbatch -J${RUN_NAME} --mail-user=${MAIL_PROJECT_DEV} --mail-type=${MAIL_ERROR_EXIT} --partition=mini,fat,xfat --time=00:29:00 --qos='short' --output=${LOGDIR}/${RUN_NAME}_med.slurmlog --error=${LOGDIR}/${RUN_NAME}_med.slurmerr ${SRUNDIR}/D6_gap_fill_with_SRTM.srun median

CHAIN_SCRIPTS $CHAIN "${SCRIPTDIR}/D7_logical_checks_and_topography.sh" $RUN_NAME # call next script in processing queue (if evoked as part of processing chain)
