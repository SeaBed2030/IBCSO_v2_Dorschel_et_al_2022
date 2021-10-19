#-----------------------------------------------------------
#   SEABED2030
#   Download dynamic data from MySQL database and save it as csv
#
#   (C) 2020 Sacha Viquerat, Fynn Warnke, Alfred Wegener Institute Bremerhaven, Germany
#   sacha.vsop@gmail.com
#-----------------------------------------------------------

import glob
import sys
import os
import shutil
import argparse
import csv

from lib.MySQL import MySQL_Handler #custom MySQL library
 
def getMetadata(outFile, table='metadata', limit='\t', ALL_RECORDS=False):
    
    """Get data from MySQL and write tiledefinition to dir (defaults to SCRIPTS/DATA folder) for later use."""
    
    if ALL_RECORDS:
        query = f'select * from {table};'
    else:
        query = f'select * from {table} where SEAHORSE_INCLUDE = 1;'
    records = handler.query(query)
    handler.writeRecords(outFile,records,limit=limit)

def getRID(outFile, columns = ['dataset_rid','dataset_name','dataset_tid','contrib_org','weight'],limit='\t'):
    
    """ DOCSTRING MISSING """
    records = handler.query(f'select {",".join(columns)} from metadata;')
    handler.writeRecords(outFile,records,limit=limit)

def getTiles(outFile,limit=','):
    
    """Get data from MySQL and write tiledefinition to dir (defaults to SCRIPTS/DATA folder) for later use."""
    
    records = handler.query(f'select * from info_tiles;')
    handler.writeRecords(outFile,records,limit=limit)
    
def getSteering(outFile,limit='\t'):

    """Get data from MySQL and write tiledefinition to dir (defaults to SCRIPTS/DATA folder) for later use."""
    records = handler.query(f'select dataset_rid,dataset_name,dataset_tid from metadata where dataset_rid<10000;')
    handler.writeRecords(outFile,records,limit=limit, HEADER=False)

        
def getUniqueTiles(outFile, joinChar=' '):

    """Get data from MySQL and write tiledefinition to dir (defaults to SCRIPTS/DATA folder) for later use."""
    
    records = handler.query(f'select ID from info_tiles group by ID;')
    records = [r['ID'] for r in records]
    records = joinChar.join([str(r) for r in records]) #hardcoded - read in bash
    with open(outFile, 'w') as f:
        f.write(records)
        f.write('\n')

def getUniqueBTiles(outFile:str, handler:MySQL_Handler):
    
    """Get data from MySQL and write tiledefinition to dir (defaults to SCRIPTS/DATA folder) for later use."""    
    records = handler.query(f'select basicTile from info_tiles where filesize_MB > 0 group by basicTile;') #filesize comes from curation script and is safe
    records = [ str( r['basicTile'] ) for r in records ]
    records = ' '.join( [r for r in records] ) #hardcoded - read in bash
    with open(outFile, 'w', newline='\n') as f:
        f.writelines(records)
        f.write('\n')
   
def getTable(schemaName, outFile, limit='\t', HEADER = True):

    """Save schema to file."""

    records = handler.getTable(schemaName)
    handler.writeRecords(outFile, records, HEADER=HEADER, limit=limit)
    
def getTileQA(outFile, limit='\t', HEADER=True):
    records = handler.query(f'SELECT * FROM info_tiles AS a LEFT JOIN QA_tile_edits AS b ON a.ID=b.tileID;')
    handler.writeRecords(outFile, records, HEADER=HEADER, limit=limit)

    
def defineInputArguments():
    parser = argparse.ArgumentParser(description='Download dynamic SEABED2030 data from MySQL Server',
                                     usage='Lorem ipsum')
    parser.add_argument('--coastline', '-C', nargs='?', type=str, help='Coastline point data location (x y 0).', default=None, required=False)
    parser.add_argument('--tiles', '-T', nargs='?', type=str, help='Tile definition (IDs and extent).', default=None, required=False)
    parser.add_argument('--btileID', '-B', nargs='?', type=str, help='Get unique basic tile ID\'s.', default = None, required=False)
    parser.add_argument('--tileID', '-I', nargs='?', type=str, help='Get unique small tile ID\'s.', default = None, required=False)
    parser.add_argument('--metadata', '-M', nargs='?', type=str, help='Get metadata.', default = None, required=False)
    parser.add_argument('--metadata_fixed_cols', '-Mf', nargs='?', type=str, help='Get metadata view with fixed columns', default = None, required=False)
    parser.add_argument('--metadata_all', '-Ma', action='store_true', help='Get all metadata.', required=False)
    parser.add_argument('--steering_points', '-S', nargs='?', type=str, help='Get steering point metadata.', default = None, required=False)
    parser.add_argument('--rid', '-R', nargs='?', type=str, help='Get regional identifier data (RID).', default = None, required=False)
    parser.add_argument('--table', '-Q', nargs='?', type=str, help='Get user defined schema.', default = None, required=False)
    parser.add_argument('--output', nargs='?', type=str, help='Filename for user defined schema download.', default = None, required=False)
    parser.add_argument('--tileQA', nargs='?', type=str, help='Filename for tile_info QA data.', default = None, required=False)
    parser.add_argument('--curate', nargs='*', type=str, help='Curate tables.', default = None, required=False)
    return parser


if __name__ == '__main__':
    handler = MySQL_Handler()
    parser = defineInputArguments()
    args = parser.parse_args()
    
    if not len(sys.argv) > 1:
        print('\nNo arguments passed!\n')
        quit()

    if args.curate is not None:
        if len(args.curate)==0: 
            print('Please provide at least one table for curation!')
            quit()
        for tbl in args.curate:
            print(f'Curating MySQL table {tbl}')
            handler.curate_MySQL(tbl)
            
    if args.metadata is not None:
        print(f'Downloading metadata into {args.metadata}')
        if args.metadata_all:
            print(f'Downloading metadata (including ALL records) into {args.metadata}')
        getMetadata(args.metadata,ALL_RECORDS=args.metadata_all)
    
    if args.metadata_fixed_cols is not None:
        print(f'Downloading metadata with fixed column order into {args.metadata_fixed_cols}')
        if args.metadata_all:
            print(f'Downloading metadata (including ALL records) into {args.metadata_fixed_cols}')
        getMetadata(args.metadata_fixed_cols,table='metadata_fixed_cols',ALL_RECORDS=args.metadata_all)
    
    if args.tiles is not None:
        print(f'Downloading tiles into {args.tiles}')
        getTiles(args.tiles) 
    
    if args.steering_points is not None:
        print(f'Downloading tiles into {args.steering_points}')
        getSteering(args.steering_points)
        
    if args.rid is not None:
        print(f'Downloading rid data into {args.rid}')
        getRID(args.rid)
        
    if args.tileID is not None:
        print(f'Downloading small tile IDs into {args.tileID}')
        getUniqueTiles(args.tileID)
        
    if args.btileID is not None:
        print(f'Downloading basic tile IDs into {args.btileID}')
        getUniqueBTiles(args.btileID,handler)    
        
    if args.tileQA is not None:
        print(f'Downloading tile_info QA into {args.tileQA}')
        getTileQA(args.tileQA)  
        
    if args.table is not None:
        if args.output is not None:
            print(f'Downloading schema {args.table} into {args.output}')
            getTable(args.table,args.output)
