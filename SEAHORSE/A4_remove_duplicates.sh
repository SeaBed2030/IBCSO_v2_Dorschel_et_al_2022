#!/usr/bin/bash
source 'SEABED2030.config' #load global config file
SHOW_LOGO
RUN_NAME="A4"
scancel -n${RUN_NAME}_small
scancel -n${RUN_NAME}_large

CHAIN=$1

MSG_INFO "Setting up directories involved in harmonisation process."
LOGDIR=${LOGBASEDIR}/${RUN_NAME}
mkdir ${LOGDIR}
CREATE_FOLDERS
mkdir empty_dir
rsync empty_dir/ ${LOGDIR}/
rsync empty_dir/ ${INCOMINGSPLITDATADIR}/
rm -r empty_dir
MSG_INFO "Done."

NUMBER_OF_LARGE_FILES=$(find $INCOMINGDIR/*.mxyz -maxdepth 1 -size +${HARM_LARGE_SIZE_LIMIT} | wc -l) #larger or equal than
NUMBER_OF_SMALL_FILES=$(find $INCOMINGDIR/*.mxyz -maxdepth 1 -not -size +${HARM_LARGE_SIZE_LIMIT} | wc -l) #smaller than

if [ ${NUMBER_OF_LARGE_FILES} -gt 0 ]
then
	MSG_INFO "Found ${NUMBER_OF_LARGE_FILES} harmonised data sets >= ${HARM_LARGE_SIZE_LIMIT}."
	NEW_NAME=${RUN_NAME}_large
	sbatch -J${NEW_NAME} --time=02:00:00 --partition=fat,xfat --mem=20G --array=1-${NUMBER_OF_LARGE_FILES} --mail-user=${MAIL_PROJECT_DEV} --mail-type=${MAIL_ERROR_EXIT} --output=$LOGDIR/${NEW_NAME}_%a.incominglog --error=$LOGDIR/${NEW_NAME}_%a.incomingerr ${SRUNDIR}/A4_remove_duplicates.srun large
	MSG_BATCH $NEW_NAME
fi

if [ ${NUMBER_OF_SMALL_FILES} -gt 0 ]
then
	MSG_INFO "Found ${NUMBER_OF_SMALL_FILES} harmonised data sets < ${HARM_LARGE_SIZE_LIMIT}."
	NEW_NAME=${RUN_NAME}_small
	sbatch -J${NEW_NAME} --time=01:00:00 --partition=mini,fat,xfat --array=1-${NUMBER_OF_SMALL_FILES} --mail-user=${MAIL_PROJECT_DEV} --mail-type=${MAIL_ERROR_EXIT} --output=$LOGDIR/${NEW_NAME}_%a.incominglog --error=$LOGDIR/${NEW_NAME}_%a.incomingerr ${SRUNDIR}/A4_remove_duplicates.srun small
	MSG_BATCH $NEW_NAME
fi

SUM=$(($NUMBER_OF_SMALL_FILES+$NUMBER_OF_LARGE_FILES))

if [ $SUM -gt 0 ]
then
	CHAIN_SCRIPTS $CHAIN "${SCRIPTDIR}/A5_cleanup.sh" $RUN_NAME # call next script in processing queue (if evoked in as part of processing chain)
fi
