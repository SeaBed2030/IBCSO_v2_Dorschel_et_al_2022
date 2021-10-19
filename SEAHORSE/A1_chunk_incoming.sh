#!/usr/bin/bash
source 'SEABED2030.config' #load global config file
SHOW_LOGO
RUN_NAME="A1"
scancel -n${RUN_NAME}

CHAIN=$1

MSG_INFO "Setting up directories involved in harmonisation process..."
LOGDIR=${LOGBASEDIR}/${RUN_NAME}
CREATE_FOLDERS
mkdir ${LOGDIR}
mkdir empty_dir
rsync empty_dir/ ${LOGDIR}/ 2>/dev/null
rsync empty_dir/ ${INCOMINGSPLITDATADIR}/ 2>/dev/null
rm ${INCOMINGDIR}/*.mxyz 2>/dev/null
rm ${INCOMINGDIR}/*.mstats 2>/dev/null 
rm ${INCOMINGDIR}/*.hxyz 2>/dev/null
rm -r empty_dir
MSG_INFO "...done."

NUMBER_OF_DIRS=$(find ${QA_INC_COMPLETED}/* -maxdepth 0 -type d | wc -l) #how many dirs are in qa_incoming_COMPLETED?
if [ ${NUMBER_OF_DIRS} -gt "0" ]
then
	MSG_MAIL "HARMONISATION" "There are some old cruises in ${QA_INC_COMPLETED} that have already been harmonised.\nPlease consider removing / archiving them ASAP."
	MSG_WARNING "There are some old cruises in ${QA_INC_COMPLETED} that have already been harmonised.\nPlease consider removing / archiving them ASAP."
fi

SYNC_DATA #get dynamic data from mysql db
INCOMING_CURATION #check if all files in INCOMING are in metadata, otherwise move to QA/INCOMING/NOT_IN_DB; make sure all files in XYZ are in metadata and update weights if neccessary

NUMBER_OF_FILES=$(GET_NUMBER_OF_FILES "${INCOMINGDIR}/*.xyz") #how many files are there in total to process?
if [ ${NUMBER_OF_FILES} -gt "0" ]
then
    sbatch -J${RUN_NAME} --partition=mini,fat,xfat --time=00:29:00 --qos='short' --array=1-${NUMBER_OF_FILES} --mail-user=${MAIL_PROJECT_DEV} --mail-type=${MAIL_ERROR_EXIT} --output=${LOGDIR}/${RUN_NAME}_%a.incominglog --error=${LOGDIR}/${RUN_NAME}_%a.incomingerr ${SRUNDIR}/A1_chunk_incoming.srun
    MSG_BATCH ${RUN_NAME}
	CHAIN_SCRIPTS $CHAIN "${SCRIPTDIR}/A2_harmonise_data.sh" $RUN_NAME # parameters: script to run after current job(s) have finished, stage name (will be expanded by grep "A1*" to get all JOB_IDs)
	MSG_MAIL_INFO "STAGE A (harmonisation)"  "Stage >> A << was initiated by\n\n${EVOKER}\n\nat $(getDATETIME)\n\nPlease stand by for updates!"

else
    MSG_WARNING "No Incoming Files - skip Phase A!"
fi

