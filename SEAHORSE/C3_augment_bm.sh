#!/usr/bin/bash
source '/isibhv/projects/seabed2030/SEAHORSE/SCRIPTS/SEABED2030.config' #load global config file
SHOW_LOGO
RUN_NAME="C3"
scancel -n${RUN_NAME}

CHAIN=$1

MSG_INFO "HOUSEKEEPING: Setting up directories involved in augmentation process..."
LOGDIR=${LOGBASEDIR}/${RUN_NAME}
mkdir ${LOGDIR}
CREATE_FOLDERS
#mkdir ${AUGDIR}
mkdir empty_dir
rsync empty_dir/ ${LOGDIR}/
rsync empty_dir/ ${AUGDIR}/
rm -r empty_dir
find ${LOGBASEDIR} -name "C*.slurmerr" -empty -type f -delete 2>/dev/null
MSG_SUCCESS "HOUSEKEEPING: Directories created."

MSG_INFO "Syncing Blockmedian windows..."
module load ${CONDA} 2>/dev/null
source activate ${CONDA_ENV}
python ${PYDIR}/C3_sync_bm_stats.py --infolder ${WORK_BLOCKDIR} --suffix 'extent'
conda deactivate
module pruge ${CONDA}
MSG_SUCCESS "Done"

MSG_IMPORTANT "Syncing dynamic data..."
SYNC_DATA

MSG_INFO "Updating tiles database with blockmedian windows"
echo -e "#!/usr/bin/bash\n#Xsrun  I know what I am doing\nmodule purge\nmodule load ${CONDA} 2>/dev/null\nsource activate ${CONDA_ENV}\npython ${PYDIR}/C3_sync_bm_stats.py --infolder ${WORK_BLOCKDIR} --suffix extent\nconda deactivate\nmodule purge" | sbatch --job-name=${RUN_NAME}_PY --partition=smp,fat,xfat,mini --time=00:10:00 --qos='short' --mail-user=${MAIL_PROJECT_DEV} --mail-type=${MAIL_ERROR_EXIT} --output=${LOGDIR}/C3_sync_bm_stats.pylog #/dev/null
MSG_SUCCESS "Done."

{ read -a LIST_OF_BM_TILES; } < ${BASIC_TILE_TABLE} #read the list of tiles from text file SCRIPTS/DATA/TILES.txt

NUMBER_OF_BM_TILES=${#LIST_OF_BM_TILES[@]}
if [ ${NUMBER_OF_BM_TILES} -gt "0" ]
then
	sbatch -J${RUN_NAME} --array=1-${NUMBER_OF_BM_TILES}%50 --partition=mini,fat,xfat --time=00:10:00 --qos='short' --mem-per-cpu=16G --mail-user=${MAIL_EVOKER} --mail-type=${MAIL_ERROR_EXIT},ARRAY_TASKS --output=$LOGDIR/${RUN_NAME}_%a.slurmlog --error=$LOGDIR/${RUN_NAME}_%a.slurmerr $SRUNDIR/C3_augment_bm.srun
	MSG_BATCH ${RUN_NAME}
	CHAIN_SCRIPTS $CHAIN "${SCRIPTDIR}/C4_cleanup.sh" $RUN_NAME # call next script in processing queue (if evoked as part of processing chain)
else
	MSG_WARNING "No blockmedian files! Did you run C1?"
fi
