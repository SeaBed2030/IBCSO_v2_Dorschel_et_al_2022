#-----------------------------------------------------------
#   SEABED2030 - A5
#   Update metadata table with information from harmonisation
#
#   (C) 2021 Sacha Viquerat, Fynn Warnke, Alfred Wegener Institute Bremerhaven, Germany
#   sacha.vsop@gmail.com
#-----------------------------------------------------------

import os
import csv
import sys
import glob
from GENERAL.lib.MySQL import MySQL_Handler


def update_MySQL(merged_harmonised_xyz:str, sqlHandler:MySQL_Handler, import_time_sql)->None:
    """
    Update MySQL database with information from extentsfile.
    """
    rid = os.path.basename(merged_harmonised_xyz)
    rid = rid.split('_')[0]
    query_update = f'UPDATE metadata SET date_harmonised = {import_time_sql} WHERE dataset_rid = "{rid}";'
    sqlHandler.query(query_update)
    return

if __name__ =='__main__':
    
    if len(sys.argv) > 1: # check if there are any commandline arguments
        folder_name = sys.argv[1].rstrip() #first argument of the script, input file name provided by sruns, remove trailing whitespace
        timestamp = sys.argv[2].rstrip()
    else:
        print('[ERROR]    No input argument(s) given!')
        sys.exit(1)
    
    files = glob.glob(os.path.join(folder_name,'*.mxyz'))
    
    mysql = MySQL_Handler()
    import_time, import_time_sql = mysql.setTimeStamp(timestamp, format='%Y-%m-%d_%H%M%S')
    
    for f in files:
        update_MySQL(f, mysql, import_time_sql)
    
