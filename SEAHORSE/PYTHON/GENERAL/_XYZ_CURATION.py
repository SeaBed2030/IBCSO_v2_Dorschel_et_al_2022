#-----------------------------------------------------------
#   SEABED2030
#   Make sure filename weight and database weight are in sync
#
#   (C) 2020 Sacha Viquerat, Fynn Warnke, Alfred Wegener Institute Bremerhaven, Germany
#   sacha.vsop@gmail.com
#-----------------------------------------------------------

import os
import sys
import glob
import argparse

from lib.MySQL import MySQL_Handler

def getWeight(dataset_rid):

    """Get weight for dataset_rid and number of query results from the MySQL data base (if dataset_rid is in metadata)."""
    
    result = handler.query(f'select dataset_rid, weight, count(*) as items from metadata where dataset_rid = {dataset_rid}')[0]
    if result['items'] == 0:
        print(f'{dataset_rid} is not in MySQL Metadata') 
        return None
    elif result['items'] > 1: 
        print(f'Warning: multiple entries for {fName} (though technically impossible)!')
        return None
    else: 
        return(result['weight'])
        
def splitXYZfile(fName,sep='_w')->dict:
    baseName = os.path.basename(fileName)
    dirName = os.path.dirname(fileName)
    rid = baseName.split(sep)[0]
    weight = baseName.split(sep)[1]
    weight = weight.split('.')[0]
    out = dict(directory = dirName,baseName = baseName, dataset_rid = str(rid),dataset_weight = str(weight))
    return(out)
    
def combineXYZfile(dataset_rid,dataset_weight,folder=None,sep='_',weightSep='w',suffix='xyz')->str:
    fileName = f'{dataset_rid}{sep}{weightSep}{dataset_weight}.{suffix}'
    if folder is None:
        return fileName
    else:
        return os.path.join(folder,fileName)

def defineInputArguments():
    parser = argparse.ArgumentParser(description='Curate xyz data for consistency with metadata.',usage='Lorem ipsum')
    return parser

if __name__ =='__main__':
    handler = MySQL_Handler()
    
    folderName = sys.argv[1].rstrip() 
    
    files = glob.glob(os.path.join(folderName,"*.xyz"))  
    for fileName in files:
        dataInfo = splitXYZfile(fileName)
        metadataWeight = getWeight(dataInfo['dataset_rid'])
        if str(metadataWeight) != str(dataInfo['dataset_weight']):
            print(f'Found update for rid: {dataInfo["dataset_rid"]}:')
            print(f'file: {dataInfo["dataset_weight"]}  DB: {metadataWeight}')
            newFileName = combineXYZfile(dataInfo['dataset_rid'],metadataWeight,folder=dataInfo['directory'])
            print(f'renaming {fileName} to {newFileName}\n')
            os.rename(fileName,newFileName)

