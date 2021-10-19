#!/usr/bin/bash
source "SEABED2030.config"
RUN_NAME="D3"

scancel -n${RUN_NAME}_SURF
scancel -n${RUN_NAME}_NN
scancel -n${RUN_NAME}_GRID

CHAIN=$1

#create and clean log directory
PDIR=$(ls -d $PRODUCTDIR/* | tail -1) #get latest product
XYVDIR=$PDIR/$XYVPRODUCT
GRIDDIR=$PDIR/$GRIDPRODUCT
REPDIR=$PDIR/$REPORTPRODUCT
LOGDIR=${REPDIR}/LOGS/${RUN_NAME}
mkdir ${LOGDIR}
rm -f ${LOGDIR}/* 2>/dev/null
rm -f ${GRIDDIR}/*.grd 2>/dev/null

MSG_INFO "Starting calculation of 'surface' grids..."
NUMBER_OF_SURFACE_SCRIPTS=$(GET_NUMBER_OF_FILES "${XYVDIR}/*_Surface_script.sh")
sbatch -J${RUN_NAME}_SURF --array=1-$NUMBER_OF_SURFACE_SCRIPTS --time=04:00:00 --partition=mini,fat,xfat --mem=10G --mail-user=${MAIL_PROJECT_DEV} --mail-type=${MAIL_ERROR_EXIT} --output=$LOGDIR/${RUN_NAME}_SURF_%a.slurmlog --error=$LOGDIR/${RUN_NAME}_SURF_%a.slurmerr ${SRUNDIR}/D3_run_gridding_script.srun 'SURF'

MSG_INFO "Starting calculation of 'nearneighbor' grids..."
NUMBER_OF_NN_SCRIPTS=$(GET_NUMBER_OF_FILES "${XYVDIR}/*_NearestNeighbour_script.sh")
sbatch -J${RUN_NAME}_NN --array=1-$NUMBER_OF_NN_SCRIPTS --time=00:30:00 --partition=xfat,fat --mem=35G --qos='short' --mail-user=${MAIL_PROJECT_DEV} --mail-type=${MAIL_ERROR_EXIT} --output=$LOGDIR/${RUN_NAME}_NN_%a.slurmlog --error=$LOGDIR/${RUN_NAME}_NN_%a.slurmerr ${SRUNDIR}/D3_run_gridding_script.srun 'NN'

MSG_INFO "Starting calculation of additional grids..."
NUMBER_OF_GRID_SCRIPTS=$(GET_NUMBER_OF_FILES "${XYVDIR}/*_grid_script.sh")
sbatch -J${RUN_NAME}_GRID --array=1-$NUMBER_OF_GRID_SCRIPTS --time=00:20:00 --partition=mini,fat,xfat --mem=10G --qos='short' --mail-user=${MAIL_PROJECT_DEV} --mail-type=${MAIL_ERROR_EXIT} --output=$LOGDIR/${RUN_NAME}_GRID_%a.slurmlog --error=$LOGDIR/${RUN_NAME}_GRID_%a.slurmerr ${SRUNDIR}/D3_run_gridding_script.srun 'GRID'

MSG_BATCH ${RUN_NAME}

CHAIN_SCRIPTS $CHAIN "${SCRIPTDIR}/D4_product_to_GTIFF.sh" $RUN_NAME # call next script in processing queue (if evoked as part of processing chain)
