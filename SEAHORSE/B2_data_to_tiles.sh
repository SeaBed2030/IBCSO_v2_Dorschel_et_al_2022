#!/usr/bin/bash
source "SEABED2030.config" #load global config file
RUN_NAME="B2"

scancel -n${RUN_NAME}

CHAIN=$1

MSG_INFO "HOUSEKEEPING FOR STAGE B2..."
LOGDIR=${LOGBASEDIR}/${RUN_NAME}
mkdir ${LOGDIR}
CREATE_FOLDERS
mkdir empty_dir
rsync empty_dir/ ${TILEDIR}/
rsync empty_dir/ ${WORK_TILEDIR}/
rsync empty_dir/ ${WORK_MINMAX}/
rsync empty_dir/ ${WORK_EXTENT}/
rsync empty_dir/ ${WORK_LISTUNIQUETILES}/
rsync empty_dir/ ${LOGDIR}/
rsync empty_dir/ ${QA_TILE_INVALID_DIR}/
rm -r empty_dir
find ${LOGBASEDIR} -name "B*.slurmerr" -empty -type f -delete 2>/dev/null
MSG_SUCCESS "HOUSEKEEPING DONE."

NUMBER_OF_SPLITS=$(GET_NUMBER_OF_FILES "${WORK_SPLITXYZDIR}/*.xyzsplit") #get number of splits from work
NUMBER_OF_XYZ=$(GET_NUMBER_OF_FILES "${XYZDIR}/*.xyz") #get total number of cruises in DATA/XYZ

if [ ${NUMBER_OF_SPLITS} -gt "0" ]
then
	MSG_INFO "Total of < ${NUMBER_OF_SPLITS} > split files based on < ${NUMBER_OF_XYZ} > original tiles."
	sbatch -J${RUN_NAME} --array=1-${NUMBER_OF_SPLITS}%100 --partition=smp,fat,xfat --time=02:00:00 --chdir=${WORKDIR} --mem-per-cpu=16G --mail-user=${MAIL_EVOKER} --mail-type=${MAIL_ERROR_EXIT} --output=${LOGDIR}/${RUN_NAME}_%a.slurmlog --error=${LOGDIR}/${RUN_NAME}_%a.slurmerr ${SRUNDIR}/B2_data_to_tiles.srun
	MSG_BATCH ${RUN_NAME}
	
	sleep 10 # wait for some seconds until SLURM scheduler processed submitted jobs...
	CHAIN_SCRIPTS ${CHAIN} "${SCRIPTDIR}/B3_merge_tiles.sh" ${RUN_NAME} # call next script in processing queue (if evoked in as part of processing chain)
else
	MSG_WARNING "No XYZ splits!"
fi
