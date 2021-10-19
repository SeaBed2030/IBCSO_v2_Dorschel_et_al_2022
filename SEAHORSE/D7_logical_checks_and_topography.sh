#!/usr/bin/bash
source "SEABED2030.config"
RUN_NAME="D7"

scancel -n${RUN_NAME}

CHAIN=$1

MSG_INFO "HOUSEKEEPING: Setting up directories..."
export PDIR=$(ls -d $PRODUCTDIR/* | tail -1) #get latest product
export timestamp=${PDIR##*/}
REPDIR=${PDIR}/${REPORTPRODUCT}
export LOGDIR=${REPDIR}/LOGS/${RUN_NAME}
export RASTERPRODUCT=${PDIR}/${RASTERPRODUCT}
export GRIDPRODUCT=${PDIR}/${GRIDPRODUCT}
mkdir ${LOGDIR}
mkdir empty_dir
rsync empty_dir/ ${LOGDIR}/
rm -r empty_dir
MSG_SUCCESS "HOUSEKEEPING: Done."

MSG_INFO "Perform logical checks and integration of ice-surface data..."
sbatch -J${RUN_NAME} --mail-user=${MAIL_PROJECT_DEV} --mail-type=${MAIL_ERROR_EXIT} --partition=mini,fat,xfat --time=00:15:00 --qos='short' --output=${LOGDIR}/${RUN_NAME}.slurmlog --error=${LOGDIR}/${RUN_NAME}.slurmerr ${SRUNDIR}/D7_logical_checks_and_topography.srun

CHAIN_SCRIPTS $CHAIN "${SCRIPTDIR}/D9_cleanup.sh" $RUN_NAME # call next script in processing queue (if evoked as part of processing chain)
