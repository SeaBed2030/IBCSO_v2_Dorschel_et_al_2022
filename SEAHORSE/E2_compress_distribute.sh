#!/usr/bin/bash
source "SEABED2030.config"

RUN_NAME="E2"

MSG_INFO "Setting up directories for product creation..."
PDIR=$(ls -d $PRODUCTDIR/* | tail -1) #get latest product
timestamp=${PDIR##*/}
REPDIR=${PDIR}/REPORT
LOGDIR=${REPDIR}/LOGS
PLOTDIR=${REPDIR}/PLOTS
RASTERPRODUCT=${PDIR}/${RASTERPRODUCT}

OUT_NAME="IBCSO_current_version"
END_USER_INTERNAL=${END_USER_INTERNAL}/IBCSO

ZIPDIR=/tmp/IBCSO
rm -r ${ZIPDIR} 2>/dev/null
mkdir ${ZIPDIR}
TMP=${ZIPDIR}/${OUT_NAME}
mkdir ${TMP}
REP=${TMP}/REPORT
mkdir ${REP}
RAS=${TMP}/RASTER
mkdir ${RAS}
TAB=${TMP}/TABLES
mkdir ${TAB}

mkdir ${END_USER_PRODUCT}
mkdir ${END_USER_COVERAGE}

rm ${END_USER_PRODUCT}/*.zip 2>/dev/null
rm -r ${END_USER_PRODUCT}/${OUT_NAME}/ 2>/dev/null
rm -r ${END_USER_INTERNAL} 2>/dev/null
mkdir ${END_USER_INTERNAL}

MSG_SUCCESS "Done."
MSG_INFO "Downloading metadata & tile table..."
METADATA_TABLE=${TAB}/IBCSO_metadata.csv #overwrite defaults
TILEEXTENT_FILE=${TAB}/IBCSO_tiles.csv #overwrite default
SYNC_DATA

MSG_INFO "Copying SEAHORSE report and plots..."
REPFILE=${REP}/SEAHORSE_report.csv
cp ${REPDIR}/*_SEAHORSE_report.csv ${REPFILE}
cp -ar ${PLOTDIR} ${REP}

MSG_INFO "Copying raster files..."
# essential grids for distribution
cp ${RASTERPRODUCT}/${timestamp}_rid_low_SRTM.tif ${RAS}/IBCSO_RID.tif
cp ${RASTERPRODUCT}/${timestamp}_tid_low_SRTM.tif ${RAS}/IBCSO_TID.tif
cp ${RASTERPRODUCT}/${timestamp}_IBCSO_bed.tif ${RAS}/IBCSO_bed.tif
cp ${RASTERPRODUCT}/${timestamp}_IBCSO_bed_WGS84.tif ${RAS}/IBCSO_bed_WGS84.tif
cp ${RASTERPRODUCT}/${timestamp}_IBCSO_ice-surface.tif ${RAS}/IBCSO_ice-surface.tif
cp ${RASTERPRODUCT}/${timestamp}_IBCSO_ice-surface_WGS84.tif ${RAS}/IBCSO_ice-surface_WGS84.tif

# for internal QC
cp ${RASTERPRODUCT}/${timestamp}_median_composite_with_SRTM.tif ${END_USER_INTERNAL}
cp ${RASTERPRODUCT}/${timestamp}_median_composite.tif ${END_USER_INTERNAL}
cp ${RASTERPRODUCT}/${timestamp}_IBCSO_bed.tif ${END_USER_INTERNAL}
cp ${RASTERPRODUCT}/${timestamp}_IBCSO_ice-surface.tif ${END_USER_INTERNAL}
cp ${RASTERPRODUCT}/${timestamp}_IBCSO_composite_std_dev.tif ${END_USER_INTERNAL}
cp ${RASTERPRODUCT}/${timestamp}_rid_low.tif ${END_USER_INTERNAL}
cp ${RASTERPRODUCT}/${timestamp}_tid_low.tif ${END_USER_INTERNAL}
cp ${RASTERPRODUCT}/${timestamp}_rid_low_SRTM.tif ${END_USER_INTERNAL}
cp ${RASTERPRODUCT}/${timestamp}_tid_low_SRTM.tif ${END_USER_INTERNAL}
cp ${RASTERPRODUCT}/${timestamp}_SRTM15_infill_mask.tif ${END_USER_INTERNAL}
cp ${RASTERPRODUCT}/${timestamp}_ice-surface_mask.tif ${END_USER_INTERNAL}
cp ${RASTERPRODUCT}/${timestamp}_ocean_mask.tif ${END_USER_INTERNAL}

READ_ME=${TMP}/readme.txt
NUMBER_OF_CRUISES=($(ls ${XYZDIR}/*.xyz))
NUMBER_OF_CRUISES=${#NUMBER_OF_CRUISES[@]}
echo -e "AWI IBCSO SEABED2030 \t\t\nPRODUCT README\nVERSION: ${timestamp}\nBASED ON: ${NUMBER_OF_CRUISES} data sets\n\nContact:\n${CONTACTS}\n" > ${READ_ME}
echo -e "Results are based on blockmedian run on cruise level with the following settings:" >> ${READ_ME}
echo -e "	-R (Region): ${BM_REGION}" >> ${READ_ME}
echo -e "	-I (Window size): ${BM_SIZE}" >> ${READ_ME}
echo -e "	-Q (Quick)" >> ${READ_ME}
echo -e "	from GMT Version ${GMT}" >> ${READ_ME}
echo -e "x,y are in \"${IBCSO_PROJ4}\" (\"EPSG: ${IBCSO_EPSG}\")\nz is negative\n" >> ${READ_ME}
echo -e "Metadata is provided in file: TABLES/IBCSO_metadata.csv\nusing \\t (tab) as column delimiter and \. as decimal separator\n" >> ${READ_ME}
echo -e "using the following columns:" >> ${READ_ME}
echo -e "	xyz_filename:	Link to xyz filename provided by AWI" >> ${READ_ME}
echo -e "	dataset_name:	filename as stored in archive at AWI" >> ${READ_ME}
echo -e "	dataset_tid:	dataset type identifier" >> ${READ_ME}
echo -e "	weight:	dataset weight (based on subjective assessment)" >> ${READ_ME}
echo -e "	contrib_org:	acronym of contributing organisation\n" >> ${READ_ME}

#compress & distribute
MSG_INFO "Compressing data and sending to ${END_USER_PRODUCT}."
cd ${ZIPDIR}
zip -rq ${ZIPDIR}/${OUT_NAME}.zip ./* 2>/dev/null
mv ${ZIPDIR}/${OUT_NAME}.zip ${END_USER_PRODUCT}
rm -r ${ZIPDIR}
chgrp seabed2030 ${END_USER_PRODUCT}/${OUT_NAME}.zip #set to seabed2030 group
chmod 664 ${END_USER_PRODUCT}/${OUT_NAME}.zip #set to read for others!
cd ${END_USER_PRODUCT}
unzip ${END_USER_PRODUCT}/${OUT_NAME}.zip 
MSG_SUCCESS "A new product package archive is available at\n${END_USER_PRODUCT}"
MSG_MAIL_ALL "SEABED2030 - NEW PRODUCT AVAILABLE" "A new product package archive is available at\n>${END_USER_PRODUCT}/"
