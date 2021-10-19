#!/usr/bin/bash
source '/isibhv/projects/seabed2030/SEAHORSE/SCRIPTS/SEABED2030.config' #load global config file
SHOW_LOGO
RUN_NAME="C4"

CHAIN=$1

MSG_INFO "HOUSEKEEPING FOR STAGE C4..."
#rm -r ${LARGEBLOCKDIR} 2>/dev/null
rm -r ${WORK_LARGEBLOCKDIR} 2>/dev/null
find ${LOGBASEDIR} -name "C*.slurmerr" -empty -type f -delete 2>/dev/null
find ${WORK_BLOCKDIR}/*.extent -empty -type f -delete # delete files with auxiliary information (updated SQL database in C3)

CREATE_STAGE_REPORT "C"    # create report CSV file from all logs of stage
MSG_SUCCESS "HOUSEKEEPING DONE."

MSG_MAIL_INFO "Finished STAGE C (weighted statistics)" "Hola,\nStage >> C << just finished. Please continue with the next stage (D)."