#!/usr/bin/bash
source 'SEABED2030.config' #load global config file
SHOW_LOGO
RUN_NAME="A2"
scancel -n${RUN_NAME}

CHAIN=$1

MSG_INFO "Setting up directories involved in harmonisation process..."
LOGDIR=${LOGBASEDIR}/${RUN_NAME}
mkdir ${LOGDIR}
CREATE_FOLDERS
mkdir empty_dir
rsync empty_dir/ ${LOGDIR}/
rm ${INCOMINGSPLITDATADIR}/*.hxyz 2>/dev/null
rm ${INCOMINGSPLITDATADIR}/*.stats 2>/dev/null
rm -r empty_dir
MSG_INFO "Done."

NUMBER_OF_FILES=$(GET_NUMBER_OF_FILES "${INCOMINGSPLITDATADIR}/*.insplit") #how many files are there in total to process?
if [ ${NUMBER_OF_FILES} -gt "0" ]
then
	sbatch -J${RUN_NAME} --array=1-${NUMBER_OF_FILES} --partition=mini,fat,xfat --time=01:00:00 --mail-user=${MAIL_PROJECT_DEV} --mail-type=${MAIL_ERROR_EXIT} --output=${LOGDIR}/${RUN_NAME}_%a.incominglog --error=${LOGDIR}/${RUN_NAME}_%a.incomingerr ${SRUNDIR}/A2_harmonise_data.srun
	MSG_BATCH ${RUN_NAME}
	
	CHAIN_SCRIPTS $CHAIN "${SCRIPTDIR}/A3_merge_incoming_splits.sh" $RUN_NAME # call next script in processing queue (if evoked in as part of processing chain)
	MSG_WARNING "A2_harmonise.py needs rework it's badly coded"
else
    MSG_WARNING "No Incoming Files - skip Phase A!"
fi

MSG_WARNING "ADD FILENAME PARAMETER FOR PYTHON SCRIPT"
MSG_WARNING "REMOVE STATS"

