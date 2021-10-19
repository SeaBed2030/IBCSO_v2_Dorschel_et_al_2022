#!/usr/bin/bash
source "SEABED2030.config"
RUN_NAME="D5"

scancel -n${RUN_NAME}_med
scancel -n${RUN_NAME}_q25
scancel -n${RUN_NAME}_q75
scancel -n${RUN_NAME}_min
scancel -n${RUN_NAME}_max

CHAIN=$1

MSG_INFO "HOUSEKEEPING: Setting up directories involved in bending of < background surface > and < high-res nearneighbor > ..."
export PDIR=$(ls -d $PRODUCTDIR/* | tail -1) #get latest product
export timestamp=${PDIR##*/}

REPDIR=${PDIR}/${REPORTPRODUCT}
export LOGDIR=${REPDIR}/LOGS/${RUN_NAME}
export RASTERPRODUCT=${PDIR}/${RASTERPRODUCT}
mkdir ${LOGDIR}
mkdir ${RASTERPRODUCT}
mkdir empty_dir
rsync empty_dir/ ${LOGDIR}/
rm -r empty_dir
MSG_SUCCESS "HOUSEKEEPING: Done."

MSG_INFO "Start bending process..."
sbatch -J${RUN_NAME}_med --mail-user=${MAIL_PROJECT_DEV} --mail-type=${MAIL_ERROR_EXIT} --partition=mini,fat,xfat --time=00:10:00 --qos='short' --output=${LOGDIR}/${RUN_NAME}_med.slurmlog --error=${LOGDIR}/${RUN_NAME}_med.slurmerr ${SRUNDIR}/D5_product_bending.srun median
sbatch -J${RUN_NAME}_q25 --mail-user=${MAIL_PROJECT_DEV} --mail-type=${MAIL_ERROR_EXIT} --partition=mini,fat,xfat --time=00:10:00 --qos='short' --output=${LOGDIR}/${RUN_NAME}_q25.slurmlog --error=${LOGDIR}/${RUN_NAME}_q25.slurmerr ${SRUNDIR}/D5_product_bending.srun q25
sbatch -J${RUN_NAME}_q75 --mail-user=${MAIL_PROJECT_DEV} --mail-type=${MAIL_ERROR_EXIT} --partition=mini,fat,xfat --time=00:10:00 --qos='short' --output=${LOGDIR}/${RUN_NAME}_q75.slurmlog --error=${LOGDIR}/${RUN_NAME}_q75.slurmerr ${SRUNDIR}/D5_product_bending.srun q75
sbatch -J${RUN_NAME}_min --mail-user=${MAIL_PROJECT_DEV} --mail-type=${MAIL_ERROR_EXIT} --partition=mini,fat,xfat --time=00:10:00 --qos='short' --output=${LOGDIR}/${RUN_NAME}_min.slurmlog --error=${LOGDIR}/${RUN_NAME}_min.slurmerr ${SRUNDIR}/D5_product_bending.srun min
sbatch -J${RUN_NAME}_max --mail-user=${MAIL_PROJECT_DEV} --mail-type=${MAIL_ERROR_EXIT} --partition=mini,fat,xfat --time=00:10:00 --qos='short' --output=${LOGDIR}/${RUN_NAME}_max.slurmlog --error=${LOGDIR}/${RUN_NAME}_max.slurmerr ${SRUNDIR}/D5_product_bending.srun max

MSG_BATCH $RUN_NAME

CHAIN_SCRIPTS $CHAIN "${SCRIPTDIR}/D6_gap_fill_with_SRTM.sh" $RUN_NAME # call next script in processing queue (if evoked as part of processing chain)
