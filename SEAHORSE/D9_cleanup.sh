#!/usr/bin/bash
source "SEABED2030.config"
RUN_NAME="D9_CLEAN"

CHAIN=$1

MSG_INFO "HOUSEKEEPING FOR STAGE D..."
PDIR=$(ls -d $PRODUCTDIR/* | tail -1) #get latest product
REPDIR=${PDIR}/REPORT
LOGDIR_D=${REPDIR}/LOGS/
find ${LOGDIR_D} -name "D*.slurmerr" -empty -type f -delete 2>/dev/null
CREATE_STAGE_REPORT "D" ${LOGDIR_D} ${REPDIR}   # create report CSV file from all logs of stage using specific directories (in PDIR)
MSG_SUCCESS "HOUSEKEEPING DONE."

MSG_MAIL_INFO "Finished STAGE D" "Hola,\nStage >> D << just finished. Please continue with the next stage (E)."

if [ ${CHAIN} -eq "1" ]
then 
	source ${SCRIPTDIR}/E1_reporting.sh
fi
