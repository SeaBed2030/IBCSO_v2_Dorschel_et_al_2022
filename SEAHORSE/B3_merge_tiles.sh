#!/usr/bin/bash
source "SEABED2030.config" #load global config file
RUN_NAME="B3"

scancel -n${RUN_NAME}

CHAIN=$1

MSG_INFO "HOUSEKEEPING FOR STAGE B3..."
LOGDIR=${LOGBASEDIR}/${RUN_NAME}
mkdir ${LOGDIR}
CREATE_FOLDERS
mkdir empty_dir
find ${WORK_TILEDIR} -name "*.tile" -type f -delete 2>/dev/null
rsync empty_dir/ ${LOGDIR}/
rm -r empty_dir
MSG_SUCCESS "HOUSEKEEPING DONE."

MSG_INFO "Retrieving number of unique tiles... (this takes long)"
TILE_LIST=${WORK_LISTUNIQUETILES}/tile_list.txt
TILE_LIST_UNIQ_SORT=${WORK_LISTUNIQUETILES}/tile_list_unique_sorted.txt
find ${WORK_LISTUNIQUETILES} -maxdepth 1 -type f -iname "*.uniquetiles" -exec cat {} + > ${TILE_LIST} # merge all "*.uniquetiles" created during B2
sort -u ${TILE_LIST} > ${TILE_LIST_UNIQ_SORT} # sort and remove duplicates
tr $'\n' ' ' < ${TILE_LIST_UNIQ_SORT} > ${TILE_TABLE} && echo "" >> ${TILE_TABLE} # transpose column-style list to row-style list
{ read -a LIST_OF_TILES; } < ${TILE_TABLE} # read the list of tiles from text file

NUMBER_OF_TILES=${#LIST_OF_TILES[@]}
cp ${TILE_TABLE} ${WORK_TILE_TABLE}

if [ ${NUMBER_OF_TILES} -gt "0" ]
then
	MSG_INFO "Merging < ${NUMBER_OF_TILES} > tiles..."
	sbatch -J${RUN_NAME} --array=1-${NUMBER_OF_TILES}%100 --partition=smp,fat,xfat --time=02:00:00 --chdir=${WORKDIR} --mem-per-cpu=16G --mail-user=${MAIL_EVOKER} --mail-type=${MAIL_ERROR_EXIT} --output=${LOGDIR}/${RUN_NAME}_%a.slurmlog --error=${LOGDIR}/${RUN_NAME}_%a.slurmerr ${SRUNDIR}/B3_merge_tiles.srun
	
	sleep 10 # wait for some seconds until SLURM scheduler processed submitted jobs...
	CHAIN_SCRIPTS ${CHAIN} "${SCRIPTDIR}/B4_cleanup.sh" ${RUN_NAME}
else
	MSG_WARNING "No tiles found!"
fi
