#-------------------------------------------------------------------------------
# Author:      fwarnke
#
# Created:     Mon Jun 21 10:35:49 2021
# Updated:     
# Institute:   Alfred Wegener Institute (AWI), Bremerhaven
# Contact:     fynn.warnke@awi.de (fynn.warnke@yahoo.de)
#-------------------------------------------------------------------------------

import os
import argparse
import numpy as np
import rasterio

from GENERAL.lib.MySQL import MySQL_Handler

def define_input_args():
    parser = argparse.ArgumentParser(description='Update "in_current_product" column in "metadata" table with count of grid cells for each unique RID.')
    parser.add_argument('rid', type=str, help='IBCSO RID grid')
    parser.add_argument('ocean_mask', type=str, help='IBCSO ocean mask')
    parser.add_argument('--verbose', '-v', type=int, nargs='?', default=0, const=0, choices=[0, 1, 2],
                        help='Level of output verbosity (default: 0)')
    return parser

def log_message(ref_lvl:int, *msg_args)->None:
    """Print log messages depending on level of verbosity"""
    if ref_lvl <= verbosity:
        print(*msg_args)

def update_cells_per_rid(rids, cnts, sql_handler)->None:
    """
    Update metadata column "in_current_product" with number of grid cells in product
    that origin from each unique RID.
    
    Parameters
    ----------
    rids : numpy.array
        Array of all unique RID values that appear in the current product.
    cnts : numpy.array
        Array of counts corresponding to unique RIDs.
    
    """
    sql_handler.query('UPDATE metadata SET in_current_product = 0;') # reset column values
    
    for rid, cnt in zip(rids, cnts): # loop over unique RIDs
        query_update = f'UPDATE metadata SET in_current_product = {cnt} WHERE dataset_rid = {rid};'
        sql_handler.query(query_update)


if __name__ == '__main__':
    # get input arguments
    parser = define_input_args()
    args = parser.parse_args()
    
    global verbosity
    verbosity = args.verbose
    
    # read RID GeoTIFF as numpy.array
    log_message(2, f'[INFO]   Reading RID GeoTIFF < {args.rid} > into numpy.array')
    with rasterio.open(args.rid, driver='GTiFF') as src_rid:
        array_rid = src_rid.read(1)
    
    # read ocean mask GeoTIFF as numpy.array
    log_message(2, f'[INFO]   Reading ocean mask GeoTIFF < {args.ocean_mask} > into numpy.array')
    with rasterio.open(args.ocean_mask, driver='GTiFF') as src_ocean:
        array_ocean_mask = src_ocean.read(1)
    
    # get unique RIDs and respective counts for current product
    log_message(2, '[INFO]   Extract unique RIDs and respective counts')
    rid_uniq, rid_uniq_cnts = np.unique(array_rid, return_counts=True)
    condition = (rid_uniq >= 10000) # filter for unique datasets (without steering points)
    rid_uniq, rid_uniq_cnts = rid_uniq[condition], rid_uniq_cnts[condition]
    log_message(1, f'[INFO]   Total amount of unique RIDs: {rid_uniq.size}')
    
    # get total amount of cells with data (RID >= 10000)
    rid_total_data_cells = np.count_nonzero(array_rid >= 10000)
    log_message(0, f'[INFO]   Total amount of cells with data (aka RID value): >> {rid_total_data_cells} <<')
    
    # update 'metadata' table on SQL database
    sql_handler = MySQL_Handler() # create SQL_handler
    update_cells_per_rid(rid_uniq, rid_uniq_cnts, sql_handler)
    log_message(0, '[SUCCESS]   Updated count of grid cells associated with unique RIDs in "metadata" table')
