#!/usr/bin/bash
source "SEABED2030.config" #load global config file
RUN_NAME="B4"

CHAIN=$1

LOGDIR=${LOGBASEDIR}/${RUN_NAME} # create log directory
mkdir ${LOGDIR}
CREATE_FOLDERS
mkdir empty_dir
rsync empty_dir/ ${LOGDIR}/
rsync empty_dir/ ${TILEDIR}/
rsync empty_dir/ ${WORK_SPLITXYZDIR}/
rsync empty_dir/ ${WORK_LISTUNIQUETILES}/
rsync empty_dir/ ${QA_TILE_INVALID_DIR}/
rm -r empty_dir
LOGFILE=${LOGDIR}/${RUN_NAME}_from_bash.slurmlog
N_THREADS=12

MSG_INFO "Updating 'metadata' table with min/max values for X, Y and Z..." |& tee ${LOGFILE}
MSG_INFO "Updating 'info_tiles' table with featured cruise RID, tile sizes (MB) and creation date..." |& tee -a ${LOGFILE}
module load ${CONDA} 2>/dev/null
source activate ${CONDA_ENV}
python ${PYDIR}/B4_update_metadata.py --minmax ${WORK_MINMAX} --tiledir ${WORK_TILEDIR} |& tee -a ${LOGFILE}
conda deactivate
module purge 2>/dev/null
MSG_SUCCESS "Done." |& tee -a ${LOGFILE}

MSG_INFO "HOUSEKEEPING FOR STAGE B4..." |& tee -a ${LOGFILE}
rm ${WORK_MINMAX}/*.minmax 2>/dev/null
find ${WORK_TILEDIR} -maxdepth 1 -type f -name "tile_*.til" -delete 2>/dev/null #otherwise the argument list would be too long
find ${LOGBASEDIR} -name "B*.slurmerr" -empty -type f -delete 2>/dev/null # remove empty STDERR logfiles (*.slurmerr)
CREATE_STAGE_REPORT "B"    # create report CSV file from all logs of stage
MSG_SUCCESS "HOUSEKEEPING DONE." |& tee -a ${LOGFILE}

MSG_INFO "Submit copy job of tiles ('*.tile') from > ${WORK_TILEDIR} < to > ${TILEDIR} <..." |& tee -a ${LOGFILE}
echo -e "#!/usr/bin/bash\n
#Xsrun  I know what I am doing\n
find ${WORK_TILEDIR} -iname '*.tile' -type f | xargs -n1 -P${N_THREADS} -I{} rsync -ptg {} ${TILEDIR}\n
cd ${TILEDIR}\n
TILE_CURATION\n" | sbatch --job-name=${RUN_NAME}_COPY_TILES --partition=mini,fat,xfat --ntasks=1 --cpus-per-task=${N_THREADS} --time=02:00:00 --mail-user=${MAIL_PROJECT_DEV} --mail-type=${MAIL_ERROR_EXIT} --output=${LOGDIR}/${RUN_NAME}_COPY_TILES.slurmlog --error=${LOGDIR}/${RUN_NAME}_COPY_TILES.slurmerr

MSG_INFO "Moving '*.removed' files from > ${WORK_REMOVED} < to > ${QA_TILE_INVALID_DIR} <..." |& tee -a ${LOGFILE}
find ${WORK_REMOVED} -iname '*.removed' -type f | xargs -n1 -P${N_THREADS} -I{} mv {} ${QA_TILE_INVALID_DIR} 2>/dev/null

sed -i 's/\x1b\[[0-9;]*m//g' ${LOGFILE}		# remove color escape characters in logfile

MSG_MAIL_INFO "Finished STAGE B (tiling)" "Hola,\nStage >> B << just finished.\nPlease continue with the next stage (C) after tiles have been copied to\n>${TILEDIR}\nthat is still ongoing."
