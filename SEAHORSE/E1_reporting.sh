#!/usr/bin/bash
source "SEABED2030.config"

RUN_NAME="E1"

MSG_INFO "Find last product directory..."
PDIR=$(ls -d $PRODUCTDIR/* | tail -1) # get latest product
timestamp=${PDIR##*/}
REPDIR=${PDIR}/REPORT
LOGDIR=${REPDIR}/LOGS
PLOTDIR=${REPDIR}/PLOTS
mkdir ${PLOTDIR}

MSG_INFO "REPORTING: Updating SEAHORSE report table in '${REPDIR}' with report from STAGE > D < ..."
REPORT_FILE=${REPDIR}/${timestamp}_SEAHORSE_report.csv	# set filepath for output
find ${LOGDIR} -type f -name "*.csv" -exec cat {} + >> ${REPORT_FILE} # get stage D report file from LOGDIR and append to REPORT_FILE
MSG_SUCCESS "REPORTING: SEAHORSE report updated."

MSG_INFO "REPORTING: Creating SEAHORSE summary and runtime plots..."
SEAHORSE_SUMMARY_PLOTS ${REPORT_FILE} ${PLOTDIR} # call function to create plots
MSG_SUCCESS "REPORTING: Plots created."

if [ ${CHAIN} -eq "1" ]
then 
	source ${SCRIPTDIR}/E2_compress_distribute.sh 1
fi
