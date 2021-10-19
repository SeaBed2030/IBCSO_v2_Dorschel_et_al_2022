#!/usr/bin/bash
source '/isibhv/projects/seabed2030/SEAHORSE/SCRIPTS/SEABED2030.config' #load global config file
SHOW_LOGO
RUN_NAME="C1"
scancel -n${RUN_NAME}_large
scancel -n${RUN_NAME}_small
scancel -n${RUN_NAME}_tiny

CHAIN=$1

MSG_INFO "HOUSEKEEPING: Setting up directories involved in blockmedian process..."
LOGDIR=${LOGBASEDIR}/${RUN_NAME}
mkdir ${LOGDIR}
CREATE_FOLDERS
# mkdir ${BLOCKDIR}
# mkdir ${LARGEBLOCKDIR}
# mkdir ${AUGDIR}
mkdir empty_dir
rsync empty_dir/ ${LOGDIR}/
rsync empty_dir/ ${WORK_BLOCKDIR}/
rsync empty_dir/ ${WORK_LARGEBLOCKDIR}/
rsync empty_dir/ ${AUGDIR}/
rm -r empty_dir
MSG_SUCCESS "HOUSEKEEPING: Directories created."

MSG_INFO "Using tile size limit for tiny tiles of <= ${BM_TINY_SIZE}."
MSG_INFO "Using tile size limit for tiny tiles of ${BM_TINY_SIZE} < tilesize <= ${BM_SMALL_SIZE}."
MSG_INFO "Using tile size limit for large tiles of > ${BM_SMALL_SIZE}.\n"

# TINY FILES
NUMBER_OF_TINY_FILES=$(find ${WORK_TILEDIR}/*.tile -type f -not -size +${BM_TINY_SIZE} | wc -l)
if [ ${NUMBER_OF_TINY_FILES} -gt "0" ]
then
	MSG_INFO "Running low & high resolution blockmedian job on < ${NUMBER_OF_TINY_FILES} > tiny files."
	NEW_NAME=${RUN_NAME}_tiny
	# --cpus-per-task=12 --mem=12G
	sbatch -J${NEW_NAME} --partition=smp,fat,xfat,mini --cpus-per-task=12 --mem=12G --chdir=${WORKDIR} --time=02:00:00 --mail-user=${MAIL_EVOKER} --mail-type=${MAIL_ERROR_EXIT},ARRAY_TASKS --output=${LOGDIR}/${NEW_NAME}_%a.slurmlog --error=${LOGDIR}/${NEW_NAME}_%a.slurmerr ${SRUNDIR}/C1_blockmedian_tiny.srun
	MSG_BATCH ${NEW_NAME}
else
	MSG_WARNING "No files smaller than ${BM_TINY_SIZE}."
fi

# SMALL FILES
NUMBER_OF_SMALL_FILES=$(find ${WORK_TILEDIR}/*.tile -type f -size +${BM_TINY_SIZE} -not -size +${BM_SMALL_SIZE} | wc -l)
if [ ${NUMBER_OF_SMALL_FILES} -gt "0" ]
then
	MSG_INFO "Running low & high resolution blockmedian job on < ${NUMBER_OF_SMALL_FILES} > small files."
	NEW_NAME=${RUN_NAME}_small
	# --mem-per-cpu=5000M
	sbatch -J${NEW_NAME} --array=1-${NUMBER_OF_SMALL_FILES}%50 --partition=fat,xfat,mini --chdir=${WORKDIR} --time=01:00:00 --mail-user=${MAIL_EVOKER} --mail-type=${MAIL_ERROR_EXIT},ARRAY_TASKS --output=${LOGDIR}/${NEW_NAME}_%a.slurmlog --error=${LOGDIR}/${NEW_NAME}_%a.slurmerr ${SRUNDIR}/C1_blockmedian.srun small 
	MSG_BATCH ${NEW_NAME}
else
	MSG_WARNING "No files smaller than ${BM_SMALL_SIZE}."
fi

# LARGE FILES
NUMBER_OF_LARGE_FILES=$(find ${WORK_TILEDIR}/*.tile -type f -size +${BM_SMALL_SIZE} | wc -l) # ${BM_SMALL_SIZE} < filesize
if [ ${NUMBER_OF_LARGE_FILES} -gt 0 ]
then
	MSG_INFO "Pausing for < 10 > seconds before submitting next SLURM job array. Otherwise scheduler would be overloaded!"
	sleep 10 # wait for some seconds until SLURM scheduler processed submitted jobs...
	MSG_INFO "Running low & high resolution blockmedian job on < ${NUMBER_OF_LARGE_FILES} > large files."
	NEW_NAME=${RUN_NAME}_large
	# --cpus-per-task=4 --mem-per-cpu=5000M
    sbatch -J${NEW_NAME} --array=1-${NUMBER_OF_LARGE_FILES}%50 --partition=xfat,fat --chdir=${WORKDIR} --mem=42G --time=02:00:00 --mail-user=${MAIL_EVOKER} --mail-type=${MAIL_ERROR_EXIT},ARRAY_TASKS --output=${LOGDIR}/${NEW_NAME}_%a.slurmlog --error=${LOGDIR}/${NEW_NAME}_%a.slurmerr ${SRUNDIR}/C1_blockmedian.srun large
	MSG_BATCH ${NEW_NAME}
else
	MSG_WARNING "No files larger than ${BM_SMALL_SIZE}."
fi

SUM=$(($NUMBER_OF_TINY_FILES+$NUMBER_OF_SMALL_FILES+$NUMBER_OF_LARGE_FILES))

if [ $SUM -gt "0" ]
then
	MSG_INFO "Pausing for < 5 > seconds before submitting next SLURM job array. Otherwise scheduler would be overloaded!"
	sleep 5 # wait for some seconds until SLURM scheduler processed submitted jobs...
	CHAIN_SCRIPTS $CHAIN "${SCRIPTDIR}/C2_merge_bm_tiles.sh" $RUN_NAME
	MSG_MAIL_INFO "STAGE C (blockmedian)"  "Stage >> C << was initiated by\n\n${EVOKER}\n\nat $(getDATETIME)\n\nPlease stand by for updates!"
fi
