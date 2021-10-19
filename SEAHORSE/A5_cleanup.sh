#!/usr/bin/bash
source 'SEABED2030.config' #load global config file
SHOW_LOGO
RUN_NAME="A5"

TIMESTAMP=$(getTIMESTAMP)
LOGDIR=${LOGBASEDIR}/${RUN_NAME} # create log directory
mkdir ${LOGDIR}
mkdir empty_dir
rsync empty_dir/ ${LOGDIR}/
LOGFILE=${LOGDIR}/${RUN_NAME}_from_bash.slurmlog

MSG_INFO "Updating MySQL DB with harmonisation timestamp." |& tee ${LOGFILE}
module load ${CONDA} 2>/dev/null
source activate ${CONDA_ENV}
python ${PYDIR}/A5_update_metadata.py ${INCOMINGDIR} ${TIMESTAMP} |& tee -a ${LOGFILE}
module purge
MSG_INFO "Done." |& tee -a ${LOGFILE}

MSG_INFO "Cleaning directories involved in harmonisation process." |& tee -a ${LOGFILE}
rsync empty_dir/ ${INCOMINGSPLITDATADIR}/
rm -r empty_dir
mkdir ${QA_INC_COMPLETED}/${TIMESTAMP}
mv ${INCOMINGDIR}/*.xyz ${QA_INC_COMPLETED}/${TIMESTAMP}/ 2>/dev/null # move harmonised files to QA/
rm -f ${INCOMINGDIR}/*.mxyz 2>/dev/null
find ${LOGBASEDIR} -name "A*.slurmerr" -empty -type f -delete 2>/dev/null
CREATE_STAGE_REPORT "A"    # create report CSV file from all logs of stage
MSG_INFO "Done." |& tee -a ${LOGFILE}

sed -i 's/\x1b\[[0-9;]*m//g' ${LOGFILE}		# remove color escape characters in logfile

MSG_MAIL_INFO "Finished STAGE A (harmonisation)" "Hola,\nStage >> A << just finished. Please continue with the next stage (step)."
