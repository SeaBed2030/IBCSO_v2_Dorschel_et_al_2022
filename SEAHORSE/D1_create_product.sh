#!/usr/bin/bash
source "SEABED2030.config"
RUN_NAME="D1"

CHAIN=$1

MSG_INFO "HOUSEKEEPING: Setting up directories involved in product process..."

LOGDIR=${LOGBASEDIR}/${RUN_NAME} # create log directory
mkdir ${LOGDIR}
mkdir empty_dir
rsync empty_dir/ ${LOGDIR}/
rm -r empty_dir
LOGFILE="${LOGDIR}/D1_from_bash.slurmlog"

timestamp=$(getTIMESTAMP)			# get timestamp (UTC time!)
PDIR=${PRODUCTDIR}/${timestamp}		# create product directory of current run
REPDIR=${PDIR}/${REPORTPRODUCT}		# create report directory

MSG_INFO "CLEANING OLD XYV FILES" | tee -a $LOGFILE
find ${PRODUCTDIR} -name "*.xyv" -type f -delete 2>/dev/null

MSG_INFO "CREATING DIRECTORY ${PDIR}..." | tee -a $LOGFILE
NUMBER_OF_DIRS=($(ls -d ${PRODUCTDIR}/*/ | wc -l))
if [ "${NUMBER_OF_DIRS}" -gt "${KEEP_RUNS}" ]
then
	MSG_WARNING "Number of product directories too large. Discarding oldest!" | tee -a $LOGFILE
	rm -r $(ls -d ${PRODUCTDIR}/*/ | head -1)
fi

mkdir ${PDIR}
mkdir ${PDIR}/${XYVPRODUCT}
mkdir ${REPDIR}
MSG_SUCCESS "HOUSEKEEPING: Directories created." | tee -a $LOGFILE


MSG_INFO "REPORTING: Creating SEAHORSE report table in '${REPDIR}'." | tee -a $LOGFILE
REPORT_FILE=${REPDIR}/${timestamp}_SEAHORSE_report.csv
echo "STEP	JOB_ID	TASK_ID	TOTAL_TASKS	EVOKER	PARTITION	FILESIZE_BYTE	START_TIME	END_TIME" > ${REPORT_FILE} # write header line for REPORT_FILE
find ${ISIBHV_LOGDIR} -type f -name "*.csv" -exec cat {} + >> ${REPORT_FILE} # extract reports from ISIBHV_LOGDIR (STAGE A,B,C) and append to REPORT_FILE
MSG_SUCCESS "REPORTING: SEAHORSE report created." | tee -a $LOGFILE


# merged blockmedian XYV files
LOWNAME="${PDIR}/${XYVPRODUCT}/${timestamp}_blockmedian_low.xyv"
HIGHNAME="${PDIR}/${XYVPRODUCT}/${timestamp}_blockmedian_high.xyv"

extract (){
	type=$1
	column=$2
	valname=$3	
	
	if [ "${valname}" = "x" ] || [ "${valname}" = "y" ]
	then
		return 0
	fi
	
	if [ "${type}" == "low" ]
	then
		INFILE=${LOWNAME}
	elif [ "${type}" == "high" ]
	then
		INFILE=${HIGHNAME}
	else
		MSG_WARNING "Missing parameter < type >!" | tee -a $LOGFILE
		exit 1
	fi
	
	MSG_INFO "Extracting ${valname} (column ${column}) from ${type}-res blockmedian..." | tee -a $LOGFILE
	cut -d$'\t' -f1-2,${column} ${INFILE} > ${PDIR}/${XYVPRODUCT}/${timestamp}_${valname}_${type}.xyv
}

MSG_INFO "Merging augmented blockmedian files from:    ${AUGDIR}" | tee -a $LOGFILE
cat ${AUGDIR}/tile_*_low.xyv > ${LOWNAME}
cat ${AUGDIR}/tile_*_high.xyv > ${HIGHNAME}
MSG_SUCCESS "Done." | tee -a $LOGFILE

COLUMN_INDEX=0
for COLUMN_NAME in "${AUGMENT_COL_NAMES[@]}"; 
do 
	((COLUMN_INDEX=COLUMN_INDEX+1))
	extract 'low' ${COLUMN_INDEX} ${COLUMN_NAME}
	extract 'high' ${COLUMN_INDEX} ${COLUMN_NAME}
done;
MSG_SUCCESS "Done."

rm ${LOWNAME} 2>/dev/null
rm ${HIGHNAME} 2>/dev/null

sed -i 's/\x1b\[[0-9;]*m//g' ${LOGFILE}		# remove color escape characters in logfile

MSG_MAIL_INFO "STAGE D (product bending)"  "Stage >> D << was initiated by\n\n${EVOKER}\n\nat $(getDATETIME)\n\nPlease stand by for updates!"

if [[ "$CHAIN" == "1" ]]
then
	source ${SCRIPTDIR}/D2_create_gridding_script.sh 1
fi
