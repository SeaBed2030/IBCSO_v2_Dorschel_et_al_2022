#-----------------------------------------------------------
#   SEABED2030 - B3
#   Update metadata table with extent information from B2 (tiling)
#
#   (C) 2021 Sacha Viquerat, Alfred Wegener Institute Bremerhaven, Germany
#   sacha.vsop@gmail.com & fynn.warnke@awi.de
#-----------------------------------------------------------

import os
import sys
import glob
import argparse

import pandas as pd

from GENERAL.lib.MySQL import MySQL_Handler

def read_minmax_files(dir_files):
    """
    Read all '*.minmax' extent files in input directory and combine values for all splits.
    
    Parameters
    ----------
    dir_files : str
        Input directory with all '*.minmax' metadata files
    
    Returns
    -------
    results : dict
        Dictionary with "dataset_rid" as key and tuple of (minmax, time_tiling) as item
    """
    files_minmax = glob.glob(os.path.join(dir_files,'*.minmax'))
    names = ['xmin', 'xmax', 'ymin', 'ymax', 'zmin', 'zmax', 'rid', 'time_tiling']
    df = pd.concat([pd.read_csv(f, sep=' ', header=None, names=names) for f in files_minmax], ignore_index=True)
    grouped = df.groupby('rid')
    
    results = {}
    for rid, g in grouped:
        minmax = [g['xmin'].min(), g['xmax'].max(), g['ymin'].min(), g['ymax'].max(), g['zmin'].min(), g['zmax'].max()]
        minmax = [int(v) for v in minmax]
        time_tiling = g['time_tiling'].mean() # get tiling timestamp (epoch time)
        results[rid] = (minmax, time_tiling)
    
    return results

def get_cruise_RID_per_tile(dir_files):
    """
    Read all '*.til' files and extract cruise RID from filename
    
    Parameters
    ----------
    dir_files : str
        Input directory with all '*.til' metadata files
    
    Returns
    -------
    results : dict
        Dictionary with "tile ID" as key and string of all featured RIDs (e.g. '11102;12578;11001') as item
    """
    files = glob.glob(os.path.join(dir_files,'*.til'))
    
    results = {}
    for f in files:
        f_dir, f_name = os.path.split(f)
        f_name_split = f_name.split('_')
        tile_id = int(f_name_split[1]) # extract tile ID from filename
        rid = f_name_split[3] # extract RID from filename
        
        rid_entries = results.get(tile_id)
        if rid_entries is None:
            results[tile_id] = rid
        else:
            new_entries = rid_entries + ';' + rid
            results[tile_id] = new_entries
        
    return results

def update_minmax(results_dict, sql_handler):
    """
    Update mysql with extents for X,Y,Z data of every cruise RID
    
    Parameters
    ----------
    results_dict : dict
        Dictionary with "dataset_rid" as key and tuple of (minmax values, time_tiling) as item
    sql_handler : SQL_Handler
        Handler for SQL access
    """
    sql_handler.query('UPDATE metadata SET x_min=NULL, x_max=NULL, y_min=NULL, y_max=NULL, z_min=NULL, z_max=NULL, date_tiled=NULL;')
    
    for rid, (values, time_tiling) in results_dict.items():
        query_update = f'UPDATE metadata SET x_min = {values[0]}, x_max = {values[1]}, y_min = {values[2]}, y_max = {values[3]}, z_min = {values[4]}, z_max = {values[5]} WHERE dataset_rid = "{rid}";'
        sql_handler.query(query_update)
        time_tiled = sql_handler.fromTimeStamp(time_tiling)
        sql_handler.query(f'UPDATE metadata SET date_tiled = {time_tiled} WHERE dataset_rid = "{rid}";')
        
def update_featured_cruises(results_dict, sql_handler):
    """
    Update mysql with cruise RIDs located in each tile
    
    Parameters
    ----------
    results_dict : dict
        Dictionary with "tile ID" as key and string of all featured RIDs (e.g. '11102;12578;11001') as item
    sql_handler : SQL_Handler
        Handler for SQL access
    """
    sql_handler.query('UPDATE info_tiles SET featured_cruises=NULL, number_of_cruises=0, filesize_MB=0, avg_weight=0, sum_weight=0;')
    
    for tile_id, values in results_dict.items():
        n_cruises = len(values.split(';'))
        query_update = f'UPDATE info_tiles SET featured_cruises="{values}", number_of_cruises={n_cruises} WHERE ID = "{tile_id}";'
        sql_handler.query(query_update)

def defineInputArguments():
    parser = argparse.ArgumentParser(description='sync minmax and tile info with db')
    parser.add_argument('--minmax', '-m', nargs='?', type=str, help='Folder containing minmax files')
    parser.add_argument('--tiledir', '-t', nargs='?', type=str, help='Folder containing tiles')
    return parser

if __name__ =='__main__':

    parser = defineInputArguments()
    args = parser.parse_args()    
    minmax_dir=args.minmax
    tile_dir=args.tiledir
    results_minmax = read_minmax_files(minmax_dir)                   # combine minmax values per cruise RID
    results_featured_cruises = get_cruise_RID_per_tile(tile_dir)     # extract RIDs per tile
    
    sql_handler = MySQL_Handler()                                         # create mysql handler
    update_minmax(results_minmax, sql_handler)                            # update metadata table with min/max values
    update_featured_cruises(results_featured_cruises, sql_handler)        # update info_tiles table with info about featured cruises
    