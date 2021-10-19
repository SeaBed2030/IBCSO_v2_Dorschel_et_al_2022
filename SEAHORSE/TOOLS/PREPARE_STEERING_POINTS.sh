#!/usr/bin/bash
source "SEABED2030.config"

MSG_INFO "Looking for static file data..."

RUN_MERGE=0

LIST_OF_FILES=$(GET_LIST_OF_FILES "${STATIC_DATADIR}/*.xyz")
for FILE in ${LIST_OF_FILES[@]}  
do 
	PUREFILE=${FILE##*/}
	PUREFILE=${PUREFILE%.*}
	PARS=($(cat ${STEERING_POINTS_TABLE} | awk -F'\t' -v dataset_name=${PUREFILE} -v full_name=${FILE##*/} '$2 == dataset_name {print full_name, $1, $3}')) #$1 rid, $2 dataset_name, $3 tid

	if [ ${#PARS[@]} -gt "0" ]
	then
		mkdir ${STATIC_DATADIR}/_BACKUP
		RUN_MERGE=1
		MSG_INFO "${PUREFILE} IS NEW! HARMONISING..."
		NEWFILENAME=${STATIC_DATADIR}/${PARS[1]}_${PUREFILE}.steering
		sed -i 's/,/ /g' ${FILE}
		sed -i 's/\t/ /g' ${FILE}
		awk -F" " -v rid=${PARS[1]} -v tid=${PARS[2]} '{printf("%.0f\t%.0f\t%.0f\t%d\t%d\n", $1, $2, $3, $4=rid, $5=tid)}' ${FILE} >/tmp/xyz
		mv /tmp/xyz ${NEWFILENAME}
		mv ${FILE} ${STATIC_DATADIR}/_BACKUP
		MSG_SUCCESS "${PUREFILE} HAS BEEN HARMONISED -> ${PARS[1]}_${PUREFILE}.steering!"
	fi
done

chmod 660 ${STATIC_DATADIR}/_BACKUP/*.xyz 2>/dev/null	# adjust file permissions

if [ ${RUN_MERGE} -eq "0" ] && [ -f ${STATIC_XYZ} ] # if (1) no new XYZ file available and (2) *.static found
	then
		MSG_SUCCESS "No NEW FILES TO ADD."
elif [ ${RUN_MERGE} -eq "1" ] || [ ! -f ${STATIC_XYZ} ] # if (1) new XYZ file available or (2) no *.static found
	then
		MSG_INFO "Merging dynamic data..."
		rm ${STATIC_XYZ} 2>/dev/null
		rm ${STATIC_XYZ_SURF} 2>/dev/null
		rm ${STATIC_RID} 2>/dev/null
		rm ${STATIC_TID} 2>/dev/null

		if compgen -G ${STATIC_MANUAL_POINTS}*.steering > /dev/null; then
			MSG_INFO "Merging high res manual data"
			cat ${STATIC_MANUAL_POINTS}*.steering >> ${STATIC_XYZ}
		fi
		if compgen -G ${STATIC_VARIOUS_POINTS}*.steering > /dev/null; then
			cat ${STATIC_VARIOUS_POINTS}*.steering >> ${STATIC_XYZ}
		fi    
		if compgen -G ${STATIC_BEDMACHINE_POINTS}*.steering > /dev/null; then
			MSG_INFO "Merging Bedmachine points data"
			cat ${STATIC_BEDMACHINE_POINTS}*.steering >> ${STATIC_XYZ}
		fi
		if compgen -G ${STATIC_BEDMACHINE_INTERP}*.steering > /dev/null; then
			MSG_INFO "Merging Bedmachine interpolated data"
			cat ${STATIC_BEDMACHINE_INTERP}*.steering >> ${STATIC_XYZ}
		fi

		MSG_INFO "SPLITTING AND TRUNCATING DATA..."	
		awk -F'\t' '!_[$1$2]++' ${STATIC_XYZ} > _tmp && mv -f _tmp ${STATIC_XYZ}
		awk -F'\t' '{ printf("%d\t%d\t%d\n", $1, $2, $4) }' ${STATIC_XYZ} > ${STATIC_RID}
		awk -F'\t' '{ printf("%d\t%d\t%d\n", $1, $2, $5) }' ${STATIC_XYZ} > ${STATIC_TID}
		awk -F'\t' '{ printf("%d\t%d\t%d\n", $1, $2, $3) }' ${STATIC_XYZ} > _tmp && mv -f _tmp ${STATIC_XYZ}
		awk -F'\t' -v limit=${SURF_UPPER_LIMIT} '$3 < limit' ${STATIC_XYZ} > _tmp && mv -f _tmp ${STATIC_XYZ_SURF} #create subset for surface less than UPPER_LIMIT (prevents GMT warnings and shortens runtime of gmt surface)

		MSG_INFO "RUNNING BLOCKMEDIAN ON ${STATIC_XYZ_SURF}..."
		module load ${GMT}
		gmt blockmedian ${STATIC_XYZ_SURF} -Q -R${BM_REGION} -I${BM_SIZE} -r > _tmp && mv -f _tmp ${STATIC_XYZ_SURF}
		module unload ${GMT}
		rm gmt.history 2>/dev/null
else
	MSG_WARNING "SOMETHING WENT TERRIBLY WRONG"
fi
