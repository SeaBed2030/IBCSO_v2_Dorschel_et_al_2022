#!/usr/bin/bash
# This script is using a different approach than all other scripts
# It's creating individual shell scripts that call gmt commands for min, max, q25, q75 and median xyv files
# --> creates surfaces (create_Surf_Script) and nearest neighbours (create_Neighbour_Script)	

source "SEABED2030.config"
RUN_NAME="D2"

CHAIN=$1 

MSG_INFO "HOUSEKEEPING:  Setting up directories involved in product process..."
PDIR=$(ls -d ${PRODUCTDIR}/* | tail -1) #get latest product
timestamp=${PDIR##*/}   # Returns "/from/hear/to"

XYVDIR=$PDIR/$XYVPRODUCT
GRIDDIR=$PDIR/$GRIDPRODUCT
RASTERDIR=$PDIR/$RASTERPRODUCT
SHAPEDIR=$PDIR/$SHAPEPRODUCT
REPDIR=$PDIR/$REPORTPRODUCT
LOGDIR=$REPDIR/LOGS/

mkdir ${GRIDDIR}
mkdir ${RASTERDIR}
mkdir ${LOGDIR}
mkdir empty_dir
rsync empty_dir/ $LOGDIR/
rsync empty_dir/ $GRIDDIR/
rsync empty_dir/ $RASTERDIR/
rm -r empty_dir
MSG_SUCCESS "HOUSEKEEPING: Done."

MSG_INFO "PREPARING STEERING POINTS FILE DATABASE."
getSteeringPoints
MSG_SUCCESS "DONE."

#5 files: median, q25, q75, min, max, sum of weights, tid, mask times 2: low / high
MEDIAN_LO=${PDIR}/${XYVPRODUCT}/${timestamp}_median_low.xyv
MEDIAN_HI=${PDIR}/${XYVPRODUCT}/${timestamp}_median_high.xyv 
Q25_LO=${PDIR}/${XYVPRODUCT}/${timestamp}_q25_low.xyv 
Q25_HI=${PDIR}/${XYVPRODUCT}/${timestamp}_q25_high.xyv 
Q75_LO=${PDIR}/${XYVPRODUCT}/${timestamp}_q75_low.xyv 
Q75_HI=${PDIR}/${XYVPRODUCT}/${timestamp}_q75_high.xyv 
MIN_LO=${PDIR}/${XYVPRODUCT}/${timestamp}_min_low.xyv 
MIN_HI=${PDIR}/${XYVPRODUCT}/${timestamp}_min_high.xyv 
MAX_LO=${PDIR}/${XYVPRODUCT}/${timestamp}_max_low.xyv 
MAX_HI=${PDIR}/${XYVPRODUCT}/${timestamp}_max_high.xyv 
TID_LO=${PDIR}/${XYVPRODUCT}/${timestamp}_tid_low.xyv 
TID_HI=${PDIR}/${XYVPRODUCT}/${timestamp}_tid_high.xyv 
RID_LO=${PDIR}/${XYVPRODUCT}/${timestamp}_rid_low.xyv
#SUM_WEIGHT_LO=${PDIR}/${XYVPRODUCT}/${timestamp}_sum_weight_low.xyv
WEIGHT_LO=${PDIR}/${XYVPRODUCT}/${timestamp}_dataset_weight_low.xyv

create_Surf_Script (){
	INFILE=$1 #first argument of function (xyv filename)
	PREFIX=${timestamp}_${2}
	PUREFILE=${INFILE##*/}
	PUREFILE=${PUREFILE%.*}
	SSD_FILE_UNSORTED=${SSD_DIR}/${PUREFILE}_SURF_UNSORTED.xyv
	SSD_FILE=${SSD_DIR}/${PUREFILE}_SURF.xyv
	LOG_FILE=${SSD_DIR}/${PUREFILE}_SURF.log
	SRCFILE=${XYVDIR}/${PREFIX}_Surface_script.sh	
	
	SURFRAW=${SSD_DIR}/${PREFIX}_raw_surf.grd
	SURFCUT=${SSD_DIR}/${PREFIX}_cut_surf.grd
	SURFFILTER=${SSD_DIR}/${PREFIX}_filtered_surf.grd
	SURFSAMPLED=${SSD_DIR}/${PREFIX}_surf.grd
	
	echo "#!/usr/bin/bash" > $SRCFILE
	echo "# ------------   Surface creation   ------------------" >> $SRCFILE
	echo "" >> $SRCFILE
	
	echo "cd $SSD_DIR" >> $SRCFILE #again, weird stdout bug!
	
	echo "cp $INFILE $SSD_FILE_UNSORTED" >> $SRCFILE
	echo "cat $STATIC_XYZ_SURF >> $SSD_FILE_UNSORTED" >> $SRCFILE #add steering points to file
	echo "" >> $SRCFILE
	
	echo "mv ${SSD_FILE_UNSORTED} ${SSD_FILE}" >> $SRCFILE
	echo "rm $SSD_FILE_UNSORTED 2>/dev/null" >> $SRCFILE
	echo "" >> $SRCFILE
	
	echo "gmt gmtset IO_NC4_DEFLATION_LEVEL 3" >> $SRCFILE
	echo "" >> $SRCFILE
	
	#log gmt output to file?
	echo "#Surface the merged masterfile for the BM region." >> $SRCFILE
	echo "srun gmt surface $SSD_FILE -R$BM_REGION -I$SURF_RESO_RAW -N$SURF_N_MAX_ITER -T$SURF_TENSION -C$SURF_CONV_LIMIT -G${SURFRAW}=ns -Lu${SURF_UPPER_LIMIT} -r " >> $SRCFILE
	echo "" >> $SRCFILE
	
	
	echo "#Filter the grid from surface." >> $SRCFILE
	echo "srun gmt grdfilter $SURFRAW -G$SURFFILTER -D0 -Fc$SURF_FILTER">> $SRCFILE
	echo "" >> $SRCFILE
	
	echo "#Resample the filtered grid to final resolution." >> $SRCFILE
	echo "srun gmt grdsample $SURFFILTER -G$SURFSAMPLED -R$BM_REGION -I$SURF_RESO" >> $SRCFILE
	echo "" >> $SRCFILE
	
	echo "# ------------   cleanup   ------------------" >> $SRCFILE
	echo "mv $SURFSAMPLED $GRIDDIR" >> $SRCFILE
	echo "mv $LOG_FILE $GRIDDIR 2>/dev/null" >> $SRCFILE
	echo "rm $SURFRAW 2>/dev/null" >> $SRCFILE
	echo "rm $SURFCUT 2>/dev/null" >> $SRCFILE
	echo "rm $SURFFILTER 2>/dev/null" >> $SRCFILE
	echo "rm $SSD_FILE 2>/dev/null" >> $SRCFILE
}

create_Neighbour_Script (){ #see create_Surf_Script above
	INFILE=$1
	PREFIX=${timestamp}_${2}	
	MASK=$3 #use grdmath yes oder no?
	PUREFILE=${INFILE##*/}
	PUREFILE=${PUREFILE%.*}
	SSD_FILE_UNSORTED=${SSD_DIR}/${PUREFILE}_NN_UNSORTED.xyv
	SSD_FILE=${SSD_DIR}/${PUREFILE}_NN.xyv
	
	SRCFILE=${XYVDIR}/${PREFIX}_NearestNeighbour_script.sh
	
	NN_RAW=${SSD_DIR}/${PREFIX}_NN_raw.grd
	NN_OUT=${SSD_DIR}/${PREFIX}_NN.grd
	
	echo "#!/usr/bin/bash" > $SRCFILE
	echo "# ------------   Nearest Neighbour   ------------------" >> $SRCFILE
	echo "" >> $SRCFILE
	
	echo "cd $SSD_DIR" >> $SRCFILE #again, weird stdout bug!
	
	echo "cp $INFILE $SSD_FILE_UNSORTED" >> $SRCFILE
	echo "cat ${STATIC_XYZ} >> $SSD_FILE_UNSORTED" >> $SRCFILE #add steering points to file
	echo "" >> $SRCFILE
		
	echo "mv ${SSD_FILE_UNSORTED} ${SSD_FILE}" >> $SRCFILE
	echo "rm $SSD_FILE_UNSORTED 2>/dev/null" >> $SRCFILE
	echo "" >> $SRCFILE

	echo "gmt gmtset IO_NC4_DEFLATION_LEVEL 3  #is this still neccessary?" >> $SRCFILE	
	echo "" >> $SRCFILE

	echo "#Run nearest neighbour on the merged masterfile for the BM region." >> $SRCFILE
	echo "srun gmt nearneighbor $SSD_FILE -R$BM_REGION -N$NN_NEIGHBOURS -S$NN_SEARCHRADIUS -I$NN_RESO -G${NN_RAW}=ns -r" >> $SRCFILE
	echo "" >> $SRCFILE

	echo "#Cut and filter the grid from surface." >> $SRCFILE #no filterig?
	echo "srun gmt grdcut $NN_RAW -R$BM_REGION -G$NN_OUT=ns" >> $SRCFILE
	echo "" >> $SRCFILE

	if [[ "${MASK}" -eq 1 ]]
		then
			MASKFILE=$SSD_DIR/${PREFIX}_mask_NN.grd
			echo "#Run mask filter on the merged masterfile for the BM region." >> $SRCFILE
			echo "srun gmt grdmath -R${BM_REGION} ${NN_OUT} ISFINITE = ${MASKFILE}=ns" >> $SRCFILE # =ns: int16 format
			echo "mv $MASKFILE $GRIDDIR" >> $SRCFILE
			echo "" >> $SRCFILE
	fi
	
	echo "mv $NN_OUT $GRIDDIR" >> $SRCFILE
	echo "rm $NN_RAW 2>/dev/null" >> $SRCFILE
	echo "rm $SSD_FILE 2>/dev/null" >> $SRCFILE
}

create_Grid_Script (){ #see create_Surf_Script above
	INFILE=$1
	PREFIX=${timestamp}_${2}
	MASK=$3 #use grdmath yes oder no?
	
	PUREFILE=${INFILE##*/}
	PUREFILE=${PUREFILE%.*}
	SSD_FILE_UNSORTED=${SSD_DIR}/${PUREFILE}_GRID_UNSORTED.xyv
	SSD_FILE=${SSD_DIR}/${PUREFILE}_GRID.xyv
	
	SRCFILE=${XYVDIR}/${PREFIX}_grid_script.sh
	
	GRID=${SSD_DIR}/${PREFIX}.grd
	
	echo "#!/usr/bin/bash" > $SRCFILE
	echo "# ------------   Mask grid creation   ------------------" >> $SRCFILE
	echo "" >> $SRCFILE
	
	echo "cd $SSD_DIR" >> $SRCFILE #again, weird stdout bug!
	
	echo "cp $INFILE $SSD_FILE_UNSORTED" >> $SRCFILE
	if [[ "$2" == "tid_low" ]]; then # check for data type (here TID)
		echo "cat ${STATIC_TID} >> $SSD_FILE_UNSORTED" >> $SRCFILE #add steering points to file
	elif [[ "$2" == "rid_low" ]]; then # check for data type (here RID)
		echo "cat ${STATIC_RID} >> $SSD_FILE_UNSORTED" >> $SRCFILE #add steering points to file
	fi
	echo "" >> $SRCFILE
	
	echo "mv ${SSD_FILE_UNSORTED} ${SSD_FILE}" >> $SRCFILE
	echo "rm $SSD_FILE_UNSORTED 2>/dev/null" >> $SRCFILE
	echo "" >> $SRCFILE
	
	echo "gmt gmtset IO_NC4_DEFLATION_LEVEL 3  #is this still neccessary?" >> $SRCFILE
	echo "" >> $SRCFILE
	
	echo "#Run nearest neighbour on the merged masterfile for the BM region." >> $SRCFILE
	echo "srun gmt xyz2grd -Af ${SSD_FILE} -I${SURF_RESO} -R${BM_REGION} -G${GRID}=ns -di-32768 -r >/dev/null" >> $SRCFILE	# -Af: select first point in "grid cell", =ns: int16 format, -di: set NoData to -32768
	echo "" >> $SRCFILE	
	
	
	if [[ "${MASK}" -eq 1 ]]
		then
			MASKFILE=$SSD_DIR/${PREFIX}_mask.grd
			echo "#Run mask filter on the merged masterfile for the BM region." >> $SRCFILE
			echo "srun gmt grdmath -R${BM_REGION} ${GRID} ISFINITE = ${MASKFILE}=ns" >> $SRCFILE	# =ns: int16 format
			echo "mv ${MASKFILE} ${GRIDDIR}" >> $SRCFILE
			echo "" >> $SRCFILE
	fi
	
	echo "# ------------   cleanup   ------------------" >> $SRCFILE
	echo "mv ${GRID} ${GRIDDIR}" >> $SRCFILE
	echo "rm ${GRID} 2>/dev/null" >> $SRCFILE
	echo "rm ${SSD_FILE} 2>/dev/null" >> $SRCFILE
}

MSG_INFO "Creating surface scripts for low res data in: ${GRIDDIR}"
create_Surf_Script ${MEDIAN_LO} "median_low"
create_Surf_Script ${Q25_LO} "q25_low"
create_Surf_Script ${Q75_LO} "q75_low"
create_Surf_Script ${MIN_LO} "min_low" 
create_Surf_Script ${MAX_LO} "max_low"
MSG_SUCCESS "Done."

MSG_INFO "Creating Nearest Neighbour scripts for high res data in: ${GRIDDIR}"
create_Neighbour_Script ${MEDIAN_HI} "median_high" 1 #median high nn and mask
create_Neighbour_Script ${Q25_HI} "q25_high" 1
create_Neighbour_Script ${Q75_HI} "q75_high" 1
create_Neighbour_Script ${MIN_HI} "min_high" 1
create_Neighbour_Script ${MAX_HI} "max_high" 1
MSG_SUCCESS "Done."

MSG_INFO "Creating Grid scripts for discrete data in: ${GRIDDIR}"
create_Grid_Script ${TID_HI} "tid_high" 1
create_Grid_Script ${TID_LO} "tid_low" 1
create_Grid_Script ${RID_LO} "rid_low" 1
create_Grid_Script ${WEIGHT_LO} "weights_low"
MSG_SUCCESS "Done."

chmod u+x,g+x ${XYVDIR}/*.sh

if [[ "$CHAIN" == "1" ]]
then
	source ${SCRIPTDIR}/D3_run_gridding_script.sh 1
fi