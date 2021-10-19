#!/usr/bin/bash
source '/isibhv/projects/seabed2030/SEAHORSE/SCRIPTS/SEABED2030.config' #load global config file
SHOW_LOGO
RUN_NAME="C2"
scancel -n${RUN_NAME}

CHAIN=$1

MSG_INFO "HOUSEKEEPING: Setting up directories involved in blockmedian process..."
LOGDIR=${LOGBASEDIR}/${RUN_NAME}
mkdir ${LOGDIR}
CREATE_FOLDERS
mkdir empty_dir
rsync empty_dir/ ${LOGDIR}/
rsync empty_dir/ ${WORK_LARGEBLOCKDIR}/
rm -r empty_dir
rm ${WORK_BLOCKDIR}/*.extent 2>/dev/null
find ${LOGBASEDIR} -name "C*.slurmerr" -empty -type f -delete 2>/dev/null
MSG_SUCCESS "HOUSEKEEPING: Directories created."

MSG_INFO "Syncing dynamic data..."
SYNC_DATA
MSG_SUCCESS "Done."

{ read -a LIST_OF_BM_TILES; } < ${BASIC_TILE_TABLE} #read the list of tiles from text file SCRIPTS/DATA/TILES.txt
NUMBER_OF_BM_TILES=${#LIST_OF_BM_TILES[@]}

NUMBER_OF_FILES=$(GET_NUMBER_OF_FILES "${WORK_BLOCKDIR}/*.bm") #how many files are there in total to process?
if [ ${NUMBER_OF_FILES} -gt "0" ]
then
	MSG_INFO "Merging < ${NUMBER_OF_FILES} > block median files into < ${NUMBER_OF_BM_TILES} > tiles."
	sbatch -J${RUN_NAME} --array=1-${NUMBER_OF_BM_TILES}%50 --partition=smp,fat,xfat,mini --time=00:10:00 --mem-per-cpu=16G --qos='short' --chdir=${WORKDIR} --mail-user=${MAIL_EVOKER} --mail-type=${MAIL_ERROR_EXIT},ARRAY_TASKS --output=$LOGDIR/${RUN_NAME}_%a.slurmlog --error=$LOGDIR/${RUN_NAME}_%a.slurmerr ${SRUNDIR}/C2_merge_bm_tiles.srun
	MSG_BATCH ${RUN_NAME}
	CHAIN_SCRIPTS $CHAIN "${SCRIPTDIR}/C3_augment_bm.sh" $RUN_NAME # call next script in processing queue (if evoked as part of processing chain)
else
	MSG_WARNING "No blockmedian files! Did you run C1?"
fi


