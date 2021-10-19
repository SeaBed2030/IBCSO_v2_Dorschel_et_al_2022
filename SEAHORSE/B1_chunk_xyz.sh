#!/usr/bin/bash
source "SEABED2030.config" #load global config file
RUN_NAME="B1"

scancel -n${RUN_NAME}

CHAIN=$1
GEBCO_RUN=$2

if [ -z ${CHAIN} ]
then
	CHAIN="0"
fi

if ! [[ "${CHAIN}" =~ ^(0|1)$ ]] #if chain is not in 0 or 1
then
	MSG_WARNING "FIRST PAR MUST BE THE CHAIN (0 OR 1). IF YOU WANT GEBCO; YOU NEED TO PROVIDE CHAIN EXPLICITLY."
	exit
fi

if [ "${GEBCO_RUN}" = "GEBCO" ]
then
	export GEBCO_DELIVERY=1
	WRITE_TO_CONFIG GEBCO_DELIVERY 1
else
	export GEBCO_DELIVERY=0
	WRITE_TO_CONFIG GEBCO_DELIVERY 0
fi

MSG_INFO "HOUSEKEEPING FOR STAGE B1..."
LOGDIR=${LOGBASEDIR}/${RUN_NAME}
mkdir ${LOGDIR} #directory for log output files
CREATE_FOLDERS
mkdir empty_dir
rsync empty_dir/ ${TILEDIR}/
rsync empty_dir/ ${WORK_TILEDIR}/
rsync empty_dir/ ${WORK_SPLITXYZDIR}/
rsync empty_dir/ ${WORK_MINMAX}/
rsync empty_dir/ ${WORK_EXTENT}/
rsync empty_dir/ ${BLOCKDIR}/
rsync empty_dir/ ${AUGDIR}/
rsync empty_dir/ ${LOGDIR}/
rm -r empty_dir

MSG_INFO "HOUSEKEEPING: Syncing dynamic data..."
SYNC_DATA
MSG_INFO "HOUSEKEEPING: Data curation..."
DATA_CURATION
MSG_SUCCESS "HOUSEKEEPING DONE."

if [ "${GEBCO_DELIVERY}" -eq 1 ]
then
	MSG_IMPORTANT "This is a GEBCO run for data up to <${CURRENT_YEAR}>."
fi

NUMBER_OF_FILES=$(GET_NUMBER_OF_FILES "${XYZDIR}/*.xyz") #how many files are there in total to process?
if [ ${NUMBER_OF_FILES} -gt 0 ]
then
	MSG_INFO "Splitting < ${NUMBER_OF_FILES} > XYZ files into ${CHUNK_XYZ_SIZE} splits..."
	sbatch -J${RUN_NAME} --array=1-${NUMBER_OF_FILES}%100 --partition=mini,fat,xfat --time=01:00:00 --mail-user=${MAIL_EVOKER} --mail-type=${MAIL_ERROR_EXIT} --output=${LOGDIR}/${RUN_NAME}_%a.slurmlog --error=${LOGDIR}/${RUN_NAME}_%a.slurmerr ${SRUNDIR}/B1_chunk_xyz.srun
	MSG_BATCH ${RUN_NAME}
	
	sleep 5 # wait for some seconds until SLURM scheduler processed submitted jobs...
	bash ${COVERAGE_SCRIPT} > ${COVERAGE_DIR}/LOGS/COV1_bash.log # start COVERAGE calculation
	CHAIN_SCRIPTS ${CHAIN} "${SCRIPTDIR}/B2_data_to_tiles.sh" ${RUN_NAME} # call next script in processing queue (if evoked in as part of processing chain)
	MSG_MAIL_INFO "STAGE B (tiling)"  "Stage >> B << was initiated by\n\n${EVOKER}\n\nat $(getDATETIME)\n\nPlease stand by for updates!"
else
	MSG_WARNING "No XYZ Files!"
fi

