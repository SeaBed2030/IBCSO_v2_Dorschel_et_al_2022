#-----------------------------------------------------------
#   SEABED2030 - Q1
#   Check if dataset in xyz is in metadata
#
#   (C) 2020 Sacha Viquerat, Fynn Warnke, Alfred Wegener Institute Bremerhaven, Germany
#   sacha.vsop@gmail.com
#-----------------------------------------------------------

import os
import sys
import glob

from lib.MySQL import MySQL_Handler

def checkMySQL(dataset_rid):

    """Check mysqldatabase if dataset_rid is in metadata.
    Moves file if there is no entry.
    Also retrieves some info on each file (size, creation and mod date)."""
    
    rows_count = handler.query(f'select count(*) as items from metadata where dataset_rid = {dataset_rid}')[0]['items'] # only interested in number of entries
    #add handler for none
    if rows_count == 1:
        info = os.stat(fileName)
        filesize_MB = info.st_size/1024/1024 # convert bytes to megabytes
        createTime = handler.fromTimeStamp(os.path.getctime(fileName))
        editTime = handler.fromTimeStamp(os.path.getmtime(fileName)) #getmtime: get last file modification time
        updateDict = [{'in_database': '"y"', 'date_edit': editTime, 'filesize_MB': filesize_MB,'where': f'dataset_rid = {dataset_rid}'}]
        handler.updateMySQL(updateDict,'metadata')
    elif rows_count == 0: 
        fName = os.path.basename(fileName)
        print(f'Moving {fName} to {os.path.join(oldDataDir,fName)}')
        os.rename(fileName,os.path.join(oldDataDir,fName))
    elif rows_count > 1: 
        print(f'Warning: multiple entries for {fName} (though technically impossoble)!')

if __name__ =='__main__':
    handler = MySQL_Handler()
    
    folderName = sys.argv[1].rstrip() 
    oldDataDir = sys.argv[2].rstrip() 
    files = glob.glob(os.path.join(folderName,"*.xyz"))
    handler.query('update metadata set in_database ="n";')    
    handler.query('update metadata set filesize_MB = 0;')    
    for fileName in files:
        dataset_rid = os.path.basename(fileName).split('_')[0]
        checkMySQL(dataset_rid)
