#!/usr/bin/bash
source 'SEABED2030.config' #load global config file
SHOW_LOGO
RUN_NAME="A3"
scancel -n${RUN_NAME}

CHAIN=$1

MSG_INFO "Setting up directories involved in harmonisation process."
LOGDIR=${LOGBASEDIR}/${RUN_NAME}
mkdir ${LOGDIR} #directory for log output files
CREATE_FOLDERS
mkdir empty_dir
rsync empty_dir/ ${LOGDIR}/
rm -r empty_dir
rm ${INCOMINGDIR}/*.mxyz 2>/dev/null #make sure we add to a new file!
MSG_INFO "Done."

NUMBER_OF_FILES=$(find ${INCOMINGSPLITDATADIR}/*.hxyz -maxdepth 1 -exec basename "{}" \; | cut -d'#' -f1 | uniq | wc -l) #find unique files
if [ ${NUMBER_OF_FILES} -gt "0" ]
then
	sbatch -J$RUN_NAME --array=1-$NUMBER_OF_FILES --partition=mini,fat,xfat --time=00:29:00 --qos="short" --mail-user=${MAIL_PROJECT_DEV} --mail-type=${MAIL_ERROR_EXIT} --output=$LOGDIR/${RUN_NAME}_%a.incominglog --error=$LOGDIR/${RUN_NAME}_%a.incomingerr ${SRUNDIR}/A3_merge_incoming_splits.srun
	MSG_BATCH $RUN_NAME

	CHAIN_SCRIPTS $CHAIN "${SCRIPTDIR}/A4_remove_duplicates.sh" $RUN_NAME # call next script in processing queue (if evoked in as part of processing chain)
else
    MSG_WARNING "No Incoming Files - skip Phase A!"
fi

