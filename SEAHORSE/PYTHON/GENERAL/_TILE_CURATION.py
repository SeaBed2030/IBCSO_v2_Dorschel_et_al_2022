#-----------------------------------------------------------
#   SEABED2030 - _TILE_CURATION
#   Lorem Ipsum
#
#   (C) 2020 Sacha Viquerat, Fynn Warnke, Alfred Wegener Institute Bremerhaven, Germany
#   sacha.vsop@gmail.com
#-----------------------------------------------------------

import os
import sys
import glob
import argparse

from lib.MySQL import MySQL_Handler

def updateFileStatsMySQL(tiles):

    """Lorem ipsum."""
    
    print('Updating info_tiles with summary of file metadata (physical tile files)...')
    handler.query('update info_tiles set filesize_MB = 0, number_of_cruises = 0, sum_weight = 0, avg_weight=0;')
    updateDict=[]
    for fileName in tiles:
        tile = os.path.basename(fileName).split('_')[1]
        info = os.stat(fileName)
        filesize_MB = info.st_size/1024/1024
        date_edit = handler.fromTimeStamp(os.path.getmtime(fileName))
        date_creation = handler.fromTimeStamp(os.path.getctime(fileName))
        updateDict.append({'filesize_MB': filesize_MB, 'date_edit': date_edit, 'date_creation': date_creation, 'where': f'ID = {tile}'})
    handler.updateMySQL(updateDict,'info_tiles')

def tileCruiseUpdate():

    """Lorem ipsum."""
    
    print('Updating info_tiles with summary of cruise metadata (from featured cruises)...')
    feat_cruises=handler.query('select ID, featured_cruises from info_tiles;')
    updateDict=[]
    for row in feat_cruises:
        tile=row['ID']
        cruises=row['featured_cruises']
        if cruises is None: 
            updateDict.append({'number_of_cruises': 0, 'avg_weight': 0, 'sum_weight': 0, 'where': f'ID = {tile}'})
            continue
        cruises=cruises.split(';')
        weight=handler.query(f'select weight from metadata where dataset_rid in ({",".join(cruises)});')
        weights=[]
        for r in weight:
            weights.append(r['weight'])
        sumWeights=sum(weights)
        avgWeight = sum(weights) / len(weights)
        updateDict.append({'sum_weight': sumWeights, 'avg_weight': avgWeight, 'where': f'ID = {tile}'})
    handler.updateMySQL(updateDict,'info_tiles')    

def save_existing_tiles(fileName:str, handler:MySQL_Handler)->None:
    pass

    
def defineInputArguments():
    parser = argparse.ArgumentParser(description='Curate tile database', usage='Lorem ipsum')
    parser.add_argument('tileFolder', type=str, help='Location of the tile file database.', default=None)
    parser.add_argument('--fileStats', '-S', help='Extract file metadata.', action="store_true")
    parser.add_argument('--cruiseStats', '-C', help='Add info about containing cruises', action="store_true")
    parser.add_argument('--tileExists', '-X',type=str, help='download list of existing tiles', default=None)
    return parser
    
if __name__ =='__main__':
    handler = MySQL_Handler(DEBUG=False)
    parser = defineInputArguments()
    args = parser.parse_args()
    
    if args.tileExists is not None:
        save_existing_tiles(args.tileExists,handler)
        sys.exit()
    
    tileDir = args.tileFolder
    tiles = glob.glob(os.path.join(tileDir,"*.tile"))
    
    if args.fileStats:
        updateFileStatsMySQL(tiles)    
    if args.cruiseStats:
        tileCruiseUpdate()
