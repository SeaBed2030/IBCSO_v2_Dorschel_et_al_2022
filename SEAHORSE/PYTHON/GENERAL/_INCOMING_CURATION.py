#-----------------------------------------------------------
#   SEABED2030 - Q1
#   Check if data in incoming dir is in metadata
#   Check if there tile edits for a new incoming file
#   If not, move it to QA folder
#
#   (C) 2020 Sacha Viquerat, Fynn Warnke, Alfred Wegener Institute Bremerhaven, Germany
#   sacha.vsop@gmail.com
#-----------------------------------------------------------

import os
import sys
import glob
import shutil
import argparse

from pathlib import Path

from lib.MySQL import MySQL_Handler

def curateIncoming(fileName, tracefolder=None):

    """Check mysqldatabase if file name is already in metadata.
    Changes file extension to .xyz_notindb if there is no entry."""
    
    fName = os.path.basename(fileName)
    rec = handler.query(f'select count(*) as items,dataset_rid from metadata where dataset_name = "{fName}"')[0]
    rows_count = rec['items'] # number of entries
    rid = rec['dataset_rid']
    if rows_count is None:
        print(f'Unknown error in {fName}')
        return -1
    elif rows_count == 1: 
        print(f'{fName} is in metadata.\n\tSetting harmonisation and tiling dates to NULL.')
        handler.query(f'update metadata set date_harmonised = NULL where dataset_name = "{fName}"') #set date_harmonised to NULL (will be harmonised afterwards)
        handler.query(f'update metadata set date_tiled = NULL where dataset_name = "{fName}"') #set date_tiled to NULL (will be tiled much later)
    elif rows_count == 0: 
        newName = os.path.join(rejectFolder,fName)
        print(f'moving {fName} to {rejectFolder}')
        os.rename(fileName,os.path.join(rejectFolder,fName))
    elif rows_count > 1: 
        print(f'Warning: multiple entries for {fName} (though technically impossible)!')
    
    if tracefolder is not None:
        dirpath = Path(tracefolder,str(rid)) #check if a trace exists for that rid
        if dirpath.exists() and dirpath.is_dir():
            newpath = Path(tracefolder,f'DELETE_{rid}') #check if a trace exists for that rid
            print(f'Renaming {dirpath} -> {newpath}')
            shutil.move(dirpath,newpath)
            
def defineInputArguments():
    parser = argparse.ArgumentParser(description='Curate incoming data for consistency with metadata and traces.',usage='Lorem ipsum')
    parser.add_argument('--incomingfolder', '-i', nargs='?', type=str, help='Folder containing incoming data', default=None, required=True)
    parser.add_argument('--oldfolder', '-o', nargs='?', type=str, help='Folder containing incoming data not in Metadata', default=None, required=True)
    parser.add_argument('--tracefolder', '-t', nargs='?', type=str, help='Folder containing traces', default=None, required=False)
    return parser

if __name__ =='__main__':
    handler = MySQL_Handler()
    parser = defineInputArguments()
    args = parser.parse_args()
    
    if not len(sys.argv) > 1:
        print('\nNo arguments passed!\n')
        quit()
    
    folderName = args.incomingfolder
    rejectFolder = args.oldfolder
    files = glob.glob(os.path.join(folderName,"*.xyz"))

    for fileName in files:
        curateIncoming(fileName,args.tracefolder)
