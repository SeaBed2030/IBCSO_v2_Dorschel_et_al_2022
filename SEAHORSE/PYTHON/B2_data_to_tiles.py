#-----------------------------------------------------------
#   SEABED2030 - B2
#   Split dataset into tiles
#
#   (C) 2020 Fynn Warnke, Sacha Viquerat Alfred Wegener Institute Bremerhaven, Germany
#   fwarnke@awi.de, sacha.vsop@gmail.com
#-----------------------------------------------------------

import os
import sys
import glob
import time
import datetime
import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def log_message(ref_lvl:int, *msg_args)->None:
    """Print log messages depending on level of verbosity"""
    if ref_lvl <= verbosity:
        print(*msg_args)

def define_input_args():
    parser = argparse.ArgumentParser(description='This script assigns each data point of the input file to a specific tile.')
    parser.add_argument('input_file', type=str, help='Input cruise split (*.xyzsplit)')
    parser.add_argument('tile_file', type=str, help='Input surface GeoTIFF (low resolution)')
    parser.add_argument('--verbose', '-v', type=int, nargs='?', default=0, const=0, choices=[0, 1, 2],
                        help='Level of output verbosity (default=0)')
    parser.add_argument('--output-removed', '-or', action='store_true', help='Write removed lines to files.')
    return parser

def timeit(func):
    """Decorator function to measure runtime of given function."""
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        results = func(*args, **kwargs)
        end_time = time.perf_counter() - start_time
        log_message(2, f'[TIME]    {func.__name__}():\t{int(end_time//60):d} min {(end_time%60):.3f} sec')
        return results
    return wrapper

def get_filesize(path_file):
    return Path(path_file).stat().st_size

def clean_server(pattern:str)->None:
    """
    Clean the server in case there's some reminders from previous runs.
    
    Parameters
    ----------
    pattern : str
        Path including wildcard to match all xyzsplits of given tile ID
    """
    # create list of all xyzsplits found
    files = glob.glob(pattern)
    if len(files) > 0: #this should also work for empty lists!
        for f in files:
            try:
                os.remove(f)
            except:
                pass

@timeit
def load_tile_extents(tile_file):
    """
    Load file containing tile extents from disk into pandas.DataFrame.
    
    Parameters
    ----------
    tile_file : str
        Path to file with tile extents on disk
        
    Returns
    ----------
    tile_def : pandas.DataFrame
        Modified table of tile extents with computed tile center coordinates
    
    """
    # read tile extents from CSV file
    tile_def = pd.read_csv(tile_file, sep=',', usecols=[0,1,2,3,4,5], dtype=np.float32)
    
    # compute center coordinates of tiles (to later match tile labels)
    tile_def['x_tiles'] = tile_def[['West','East']].mean(axis=1).astype('int')
    tile_def['y_tiles'] = tile_def[['North','South']].mean(axis=1).astype('int')
    
    return tile_def

@timeit
def bin_data(df, tile_extents):
    """
    Assign center coordinates of corresponding tile for each data point (row) using binning via "pd.cut".
    
    Parameters
    ----------
    df : pandas.DataFrame
        Chunk of input data to process
    tile_extents : pandas.DataFrame
        Bounding box coordinates of all tiles
        
    Returns
    ----------
    df : pandas.DataFrame
        Data chunck with assigned tile center coordinates
    
    """
    # create bin range for both X and Y coordinates --> used for groupby tile operation!
    xmin, xmax = int(tile_extents['West'].min()),  int(tile_extents['East'].max())
    ymin, ymax = int(tile_extents['South'].min()), int(tile_extents['North'].max())
    log_message(2, f'[INFO]   tile range (X): {xmin} m, {xmax} m')
    log_message(2, f'[INFO]   tile range (Y): {ymin} m, {ymax} m')
    
    tile_size_x = np.abs(xmax - int(tile_extents['West'].max()))  # get tile size (m) in X-coordinate direction
    tile_size_y = np.abs(ymax - int(tile_extents['South'].max())) # get tile size (m) in Y-coordinate direction
    log_message(2, f'[INFO]   tile size (X): {tile_size_x} m')
    log_message(2, f'[INFO]   tile size (Y): {tile_size_y} m')
    
    tile_range_x = np.arange(xmin, xmax + tile_size_x, tile_size_x, dtype='int')
    tile_range_y = np.arange(ymin, ymax + tile_size_y, tile_size_y, dtype='int')
    
    # create tile center coordinates for labeling
    tile_range_x_label = np.arange(xmin + tile_size_x/2, xmax + tile_size_x/2, tile_size_x, dtype='int')
    tile_range_y_label = np.arange(ymin + tile_size_y/2, ymax + tile_size_y/2, tile_size_y, dtype='int')
    
    # perform binning into tiles along X and Y axis
    df['x_tiles'] = pd.cut(df['x'].values, bins=tile_range_x, labels=tile_range_x_label, include_lowest=True)
    df['y_tiles'] = pd.cut(df['y'].values, bins=tile_range_y, labels=tile_range_y_label, include_lowest=True)
    
    return df

@timeit
def assign_tile_ID(data, tile_def, weight, rid, depth_threshold=-9999):
    """
    Merge dataframes with data and tile extent information
    
    Parameters
    ----------
    data : pandas.DataFrame
        Chunk of input data to process
    tile_def : pandas.DataFrame
        Modified table of tile extents with computed tile center coordinates
    weight : int
        Weight of cruise from filename (e.g. "10001_w20" -> "20")
    rid : int
        RID value from filename (e.g. "10001_w20" -> "10001")
    depth_threshold : int
        Threshold depth, larger values are excluded from tiling (default: -9999)
        
    Returns
    ----------
    data_merge : pandas.DataFrame
        Merged dataframe with [x,y,z,x_tiles,y_tiles,ID,basicTile,weight,rid] columns and
        without rows feature "NaN" and depths < depth_threshold (-9999).
    
    """
    # length of original dataframe
    cnt_rows = len(data)
    log_message(1, f'[INFO]   Total values:\t\t\t\t{cnt_rows:>9}')
    
    # Drop all rows with no tile ID (outside of IBCSO area)
    mask_nan = data.isnull().any(axis=1)
    data_nan = data[mask_nan].copy()
    data.drop(data[mask_nan].index, inplace=True)
    cnt_nan = len(data)
    log_message(1, f'[INFO]   NaN values (e.g. outside):\t{cnt_rows - cnt_nan:>9}')
    
    # Merge ID column of tile_def with input dataframe
    data_merge = data.merge(tile_def[['ID','basicTile','x_tiles','y_tiles']], on=['x_tiles','y_tiles'], how='left')
    log_message(2, f'[INFO]   Merge tile ID with data chunks')
    
    # Drop all rows with depth exceeding user limit
    mask_invalid_depth = (data_merge['z'] <= depth_threshold)
    data_invalid_depth = data_merge[mask_invalid_depth].copy()
    data_merge.drop(data_merge[mask_invalid_depth].index, inplace=True)
    cnt_invalid_depths = len (data_merge)
    log_message(1, f'[INFO]   Invalid depth values:\t\t{cnt_nan - cnt_invalid_depths:>9}')
    
    # set weight and RID of input file
    data_merge['weight'] = weight
    data_merge['rid'] = rid
    
    return data_merge, data_nan, data_invalid_depth

@timeit
def write_discarted_lines(df, out_type, prefix, output_dir)->None:
    """Wrapper function to write removed lines to files for later QC."""
    
    # Check for column names to use
    if out_type == 'NaN':
        columns = ['x','y','z','rid']
    elif out_type == 'invalid_depths':
        columns = ['x','y','z','rid','ID', 'basicTile']
    
    # export only if dataframe is not empty
    if not df.empty:
        # add RID column
        df['rid'] = rid
        out_name = os.path.join(output_dir, f'{prefix}_{out_type}.removed')
        df.to_csv(out_name, sep=' ', columns=columns, float_format='%.0f',
                  index=False, header=False, mode='a', line_terminator='\n')

def compare_min_max(min_ref, max_ref, min_new, max_new):
    """Compare min and max values from previous chunk with current one."""
    if min_new < min_ref:
        min_out = min_new
    else:
        min_out = min_ref
    if max_new > max_ref:
        max_out = max_new
    else:
        max_out = max_ref
    return min_out, max_out

@timeit
def write_min_max_values(xmin, xmax, ymin, ymax, zmin, zmax, rid, prefix, output_dir)->None:
    """Wrapper function to write min and max values to file."""
    
    # get tiling time (UTC) as epoch timestamp
    time_tiling = datetime.datetime.now(datetime.timezone.utc).timestamp()
    
    out_name = os.path.join(output_dir, f'{prefix}_xyz.minmax')
    with open(out_name, 'w', newline='\n') as fout:
        fout.write(' '.join([str(int(v)) for v in [xmin, xmax, ymin, ymax, zmin, zmax, rid, time_tiling]]) + '\n')

@timeit
def write_sorted_unique_tiles(tile_list:list, prefix, output_dir)->None:
    """Wrapper function to write sorted and unique tile IDs to file."""
    
    tiles_uniq_sorted = sorted(list(set(tile_list)))
    
    out_name = os.path.join(output_dir, f'{prefix}.uniquetiles')
    with open(out_name, 'w', newline='\n') as fout:
        fout.write('\n'.join(str(int(tile)) for tile in tiles_uniq_sorted) + '\n')

@timeit
def split_to_tiles(input_file, rid, weight, tile_file, output_dir, raw_name, write_removed_lines):
    """
    Main function wrapping all actual work.
    """
    # create search patterns to delete
    pattern_tiles = os.path.join(output_dir, f'*{raw_name}*.til')
    pattern_removed_lines = os.path.join(output_dir, '*.removed')
    pattern_unique_tile_list = os.path.join(output_dir, '*.uniquetiles')
    # clean the server in case there are remainders!
    for pattern in [pattern_tiles, pattern_removed_lines, pattern_unique_tile_list]:
        clean_server(pattern)
    
    # load tile definitions from csv into numpy array
    log_message(2, '[INFO]   Load tile extents from file')
    tile_extents = load_tile_extents(tile_file)
    
    # read data into pandas DataFrame
    log_message(2, '[INFO]   Read data into pandas DataFrame')
    sep = r'[,;\t\s+]'
    data_iterator = pd.read_csv(input_file, sep=sep, dtype=np.float32, header=None, names=['x','y','z'],
                                engine='python', error_bad_lines=False, warn_bad_lines=True,
                                chunksize=1_000_000, iterator=True)
    
    # initialize min and max values (reversed for comparison!)
    xmin, xmax = int(tile_extents['East'].max()), int(tile_extents['West'].min()) 
    ymin, ymax = int(tile_extents['North'].max()), int(tile_extents['South'].min())
    zmax, zmin = 0, -9999
    
    # initialize list to hold tile IDs
    tile_list = []
    
    # process each chunk individually
    for idx, data_chunk in enumerate(data_iterator):
        log_message(1, f'\n[INFO]   Processing chunk < {idx} >')
        # bin each point into corresponding tile
        log_message(2, '[INFO]   Assign tile ID to each point (2D binning)')
        data = bin_data(data_chunk, tile_extents)
        
        # assign tile ID to each row in df
        log_message(2, '[INFO]   Assign tile ID to each row in DataFrame')
        data, data_nan, data_invalid_depth = assign_tile_ID(data, tile_extents, weight, rid, depth_threshold=-9999)
        
        # get min and max values for X, Y & Z
        xmin, xmax = compare_min_max(xmin, xmax, data['x'].min(), data['x'].max())
        ymin, ymax = compare_min_max(ymin, ymax, data['y'].min(), data['y'].max())
        zmax, zmin = compare_min_max(zmax, zmin, data['z'].max(), data['z'].min())
        
        # groupby tile ID (actual 2D binning)
        log_message(2, '[INFO]   Group data by tile ID')
        tiles = data.groupby('ID')
        
        # append assigned tile IDs to list of all tile IDs
        tile_list.extend(list(tiles.groups.keys()))
        
        # write removed lines to log files
        if write_removed_lines == True:
            log_message(1, '[INFO]   Writing removed lines to files...')
            write_discarted_lines(data_nan, out_type='NaN', prefix=raw_name, output_dir=output_dir)
            write_discarted_lines(data_invalid_depth, out_type='invalid_depths', prefix=raw_name, output_dir=output_dir)
            
        # loop over individual tile IDs and write data to disk
        for tile_id, tile in tiles:
            log_message(2, f'[INFO]   Writing tile < {tile_id:.0f} > to disk...')
            basic_tile = tile['basicTile'].iloc[0]
            out_name = os.path.join(output_dir, f'tile_{int(tile_id)}_{int(basic_tile)}_{raw_name}.til')
            tile.to_csv(out_name, sep=' ', columns=['x','y','z','weight','rid'], float_format='%.0f',
                        index=False, header=False, mode='a', line_terminator='\n')
    
    log_message(2, f'[INFO]   xmin: {int(xmin)}, xmax: {int(xmax)}, ymin: {int(ymin)}, ymax: {int(ymax)}, zmin: {int(zmin)}, zmax: {int(zmax)}')
    log_message(1, f'[INFO]   Writing MIN and MAX values for X,Y and Z to file "{raw_name}_xyz.minmax"...')
    write_min_max_values(xmin, xmax, ymin, ymax, zmin, zmax, rid, prefix=raw_name, output_dir=output_dir)
    
    log_message(1, f'[INFO]   Writing sorted and unique tile IDs to file "{raw_name}.uniquetiles"...')
    write_sorted_unique_tiles(tile_list, prefix=raw_name, output_dir=output_dir)
    
    log_message(1, "")


if __name__ =='__main__':
    # get input arguments
    parser = define_input_args()
    args = parser.parse_args()
    
    # first argument of the script (input file)
    input_file = args.input_file
    # second argument of the script (reference file with tile extents)
    tile_file = args.tile_file
    # verbosity
    global verbosity
    verbosity = args.verbose
    # write removed lines to files?
    write_removed_lines = args.output_removed
    
    # get the output directory
    output_dir = os.path.dirname(input_file)
    
    # get the input_file without the path; e.g. 741_w20#aa.xyzsplit
    basename = os.path.basename(input_file)
    # pure filename; e.g.: 741_w20#aa
    raw_name = os.path.splitext(basename)[0]
    
    # new split for chunks is #, e.g.: 741_w20#aa.xyzsplit -> 741_w20#aa
    components = raw_name.split('.')
    # now split into rid and weight (using '_') -> 741, w20#aa
    components = components[0].split('_')
    # first part of filename is rid
    rid = components[0]
    # after harmonisation, it should be guaranteed that ALL files are rid_weight! e.g.: w20#aa -> 20
    weight = components[1].split('#')[0]
    weight = weight.split('w')[1]
    
    # check for data in file
    FILESIZE = get_filesize(input_file)
    if FILESIZE == 0:
        log_message(1, f'[WARNING]  Cruise file with RID < {rid} > is empty. Skipped further processing!')
        sys.exit(0)     # exit program with signal "success" (to enable dependency sbatch to run)
    
    # === assign and write to output tile ===
    split_to_tiles(input_file, rid, weight, tile_file, output_dir, raw_name, write_removed_lines)
    