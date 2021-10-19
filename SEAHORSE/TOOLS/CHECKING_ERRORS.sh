#!/usr/bin/bash
source "SEABED2030.config"

echo "COUNT PARAMETER: >$#<"

# INPUT PARAMETER
if [ $# -eq 1 ] # only SEAHORSE step name given (e.g. "A1")
then
	STEP_NAME=$1
	INPUT_DIR=${LOGBASEDIR}/${STEP_NAME}
	OUTPUT_DIR=${LOGBASEDIR}/_ERROR_CHECKS
elif [ $# -eq 2 ] # SEAHORSE step name given (e.g. "A1") + JOB_ID (for sudo get_my_jobs.sh)
then
	STEP_NAME=$1
	JOB_ID=$2
	INPUT_DIR=${LOGBASEDIR}/${STEP_NAME}
	OUTPUT_DIR=${LOGBASEDIR}/_ERROR_CHECKS
elif [ $# -eq 3 -a -n "${2}" -a -n "${3}" ] # SEAHORSE step name given (e.g. "A1") + input/output log directories (*NOT* empty strings)
then
	STEP_NAME=$1
	INPUT_DIR=${2%/} # remove trailing "/" from path
	OUTPUT_DIR=${3%/} # remove trailing "/" from path
elif [ $# -eq 4 -a -n "${3}" -a -n "${4}" ] # SEAHORSE step name given (e.g. "A1") + JOB_ID (for sudo get_my_jobs.sh) + input/output log directories (*NOT* empty strings)
then
	STEP_NAME=$1
	JOB_ID=$2
	INPUT_DIR=${3%/} # remove trailing "/" from path
	OUTPUT_DIR=${4%/} # remove trailing "/" from path
else
	echo "[ERROR]   No valid input arguments! Please specify (1) name of input log dir ('A1') and [optionally] (2) different input directory (without specfied name!) and (3) output directory."
	exit 1
fi

# FILES
export OUTPUT_SBATCH_ERR=${OUTPUT_DIR}/${STEP_NAME}_sbatch.errors # output file for errors from *.slurmerr
export OUTPUT_JOB_ERR=${OUTPUT_DIR}/${STEP_NAME}_ollie.errors # output file for fails from get_my_jobs.sh

# HOUSEKEEPING
mkdir ${OUTPUT_DIR} 2>/dev/null # create output directory if not existing
rm ${OUTPUT_SBATCH_ERR} ${OUTPUT_JOB_ERR} 2>/dev/null # remove old summary files
find ${INPUT_DIR} -name "*.*err*" -empty -type f -delete 2>/dev/null # remove empty STDERR logfiles in $INPUT_DIR

# [1] SEARCH LOGFILE DIRECTORY
function SEARCH_ERROR () {
	keyword=$1
	logfile=$2
	# search for keyword in all files of directory
	grep -rio -E --include="*.*err*" "${keyword}" ${INPUT_DIR} | sort -u >> ${logfile}
	ERROR_CNT=$(grep -ic "${keyword}" ${logfile}) # count lines aka erroneous files
	echo "[WARNING]    Found < ${ERROR_CNT} > error(s) for keyword(s) '${keyword}'"
}

##---function calls with search patterns and output logfile
SEARCH_ERROR "error" ${OUTPUT_SBATCH_ERR}
SEARCH_ERROR "Out Of Memory|OOM" ${OUTPUT_SBATCH_ERR}
SEARCH_ERROR "TIME LIMIT" ${OUTPUT_SBATCH_ERR}
SEARCH_ERROR "Expired or invalid job" ${OUTPUT_SBATCH_ERR}

##---Remove output logfile if empty
if [ ! -s "${OUTPUT_SBATCH_ERR}" ]; then
	rm ${OUTPUT_SBATCH_ERR} 2>/dev/null
	rm ${ISIBHV_LOGDIR}/_ERROR_CHECKS/${STEP_NAME}_*.errors 2>/dev/null
else
	cp -a ${OUTPUT_SBATCH_ERR} ${ISIBHV_LOGDIR}/_ERROR_CHECKS/ 2>/dev/null
fi


# [2] SEARCH 'get_my_jobs.sh'
if [ -n "${JOB_ID}" ] # JOB_ID is not NULL
then
	if [ "${ON_COMPUTE_NODE}" -eq 1 ]; then
		echo "[INFO]    ON_COMPUTE_NODE:   ${ON_COMPUTE_NODE}"
		ssh ollie0 'sudo get_my_jobs.sh -d1' | grep "${JOB_ID}" | grep -i "NODE_FAIL" > ${OUTPUT_JOB_ERR}
		ssh ollie0 'sudo get_my_jobs.sh -d1' | grep "${JOB_ID}" | grep -i "FAILURE" >> ${OUTPUT_JOB_ERR}
	else
		sudo get_my_jobs.sh -d1 | grep "${JOB_ID}" | grep -i "NODE_FAIL" > ${OUTPUT_JOB_ERR}
		sudo get_my_jobs.sh -d1 | grep "${JOB_ID}" | grep -i "FAILURE" >> ${OUTPUT_JOB_ERR}
		ERROR_CNT=$(wc -l < ${OUTPUT_JOB_ERR})
		echo "[WARNING]    Found < ${ERROR_CNT} > error(s) for job ID < ${JOB_ID} > in 'get_my_jobs.sh'"
	fi
fi

##---Remove output logfile if empty
if [ ! -s "${OUTPUT_JOB_ERR}" ]; then
	rm ${OUTPUT_JOB_ERR} 2>/dev/null
	rm ${ISIBHV_LOGDIR}/_ERROR_CHECKS/${STEP_NAME}_*.errors 2>/dev/null
else
	cp -a ${OUTPUT_JOB_ERR} ${ISIBHV_LOGDIR}/_ERROR_CHECKS/ 2>/dev/null
fi
