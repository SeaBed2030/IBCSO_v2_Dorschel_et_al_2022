#-------------------------------------------------------------------------------
# Author:      fwarnke
#
# Created:     Fri Nov 27 08:58:23 2020
# Updated:     
# Institute:   Alfred Wegener Institute (AWI), Bremerhaven
# Contact:     fynn.warnke@awi.de (fynn.warnke@yahoo.de)
#-------------------------------------------------------------------------------

import os
import sys
import time
import argparse

import xarray as xr
import rioxarray

from dask.distributed import Client, LocalCluster, performance_report

from GENERAL.bending.functions_bending import (create_transition_zone_dask, fill_transition_zone_dask,
                                               calc_weighting_functions_dask, combine_data_dask)
try:
    import dask_memusage
    profile_mem = True
except ImportError:
    profile_mem = False

#%% FUNCTIONS

def defineInputArguments():
    parser = argparse.ArgumentParser(description='Merging two GeoTIFFs into single file using custom bending algorithm.')
    parser.add_argument('surface', help='Input surface GeoTIFF (low resolution)')
    parser.add_argument('nearneighbor', help='Input nearest neighbor GeoTIFF (high resolution)')
    parser.add_argument('mask', help='Input mask GeoTIFF (for high resolution data)')
    parser.add_argument('type', type=str, help='Type of input/output data')
    parser.add_argument('--buffer', '-b', nargs='?', type=int, help='Buffer size (N x N)', default=5, required=True)
    parser.add_argument('--version', '-v', nargs='?', type=str, help='Specify algorithm version', choices=['standard', 'smooth'], default='standard')
    parser.add_argument('--percentage', '-p', nargs='?', type=float, help='Percentage of high resolution data to incorporate (format: 0.x)', default=0.2)
    parser.add_argument('--outdir', '-o', nargs='?', type=str, help='Output directory for computed GeoTIFFs')
    return parser

#%% MAIN

if __name__ == '__main__':
    
    # ==================== VARIABLES ====================
    # parse arguments
    parser = defineInputArguments()
    args = parser.parse_args()
    
    buffer_size = args.buffer if args.buffer >= 3 else 3        # check buffersize
    type_stats = args.type                                      # get type (e.g. median)
    
    if 0 <= args.percentage <= 0.5:                             # check percentage
        percentage = float(args.percentage)
    else:
        sys.exit('[ERROR]    Choose reasonable percentage value (between 0.0 and 0.5)!\n')
    
    if percentage == 0 and args.version == 'smooth':            # check version
        version = 'standard'
        print('[INFO]      Percentage of high resolution data was set to 0.0 >>> changed version to "standard"!\n')
    else:
        version = args.version
    
    # check output directory
    if args.outdir == None:
        output_dir = os.path.dirname(args.surface)
    else:
        output_dir = args.outdir
    
    timestamp = '_'.join(os.path.basename(args.surface).split('_')[:-3])    # get timestamp of run
    
    chunks = (1, 4800, 4800)   # 4 byte (32 bit) * 4800 * 4800 = 92.16 MB
    kwargs_tif = {'tiled':True, 'blockxsize':chunks[2], 'blockysize':chunks[1],
                  'compress':'deflate', 'predictor':2, 'zlevel':6}
    
    # print info block to log
    print(f'[INFO]   type:           {type_stats}')
    print(f'[INFO]   version:        {version}')
    print(f'[INFO]   percentage:     {percentage}')
    print(f'[INFO]   buffer size:    {buffer_size}')
    print(f'[INFO]   dask chunks:    {chunks}\n')
    print(f'[INFO]   surface:        {args.surface}')
    print(f'[INFO]   nearneigbor:    {args.nearneighbor}')
    print(f'[INFO]   mask:           {args.mask}')
    print(f'[INFO]   output dir:     {output_dir}\n')
    
    #%% create dask cluster
    config = {'n_workers': 1,
              'processes': False,       # use Threads!
              'local_directory': os.environ.get('SSD_DIR')}
    cluster = LocalCluster(**config)
    if profile_mem: # create csv of min/max MEM usage of each dask task
        dask_memusage.install(cluster.scheduler, os.path.join(output_dir,'D5_memory_usage.csv'))
    client = Client(cluster, timeout=60)    # connect dask client to slurm cluster
    
    with performance_report(filename=os.path.join(output_dir, f'D5_{type_stats}.html')):
        #%% READ DATA
        print('[STATUS]   Read low resolution data >> surface')
        data_surface = rioxarray.open_rasterio(args.surface, chunks=chunks, default_name='surface')
        
        print('[STATUS]   Read high resolution data >> nearneighbor')
        data_nearneighbor = rioxarray.open_rasterio(args.nearneighbor, chunks=chunks, default_name='surface')
        
        print('[STATUS]   Read IBCSO data mask (of nearneigbor)')
        mask = rioxarray.open_rasterio(args.mask, chunks=chunks, default_name='surface')
        
        #%% CONVERT XARRAY TO DASK.ARRAY
        print('[STATUS]   Extract dask.array from xarray.DataArray')
        data_surface_dask = data_surface.squeeze(drop=True).data.persist()
        data_nearneighbor_dask = data_nearneighbor.squeeze(drop=True).data.persist()
        mask_dask = mask.squeeze(drop=True).data.persist()
        
        #%% CREATE TRANSITION ZONE
        print('[STATUS]   Create transition zone between low and high resolution data')
        _, mask_dilation, mask_erosion, mask_diff, footprint = create_transition_zone_dask(mask_dask, buffer_size, percentage, version=version)
        
        #%% FILL TRANSITION ZONE
        print('[STATUS]   Fill transition zone by convolution of both datasets')
        data_nearneighbor_transition = fill_transition_zone_dask(mask_dask, mask_erosion, data_nearneighbor_dask, data_surface_dask, footprint, version=version)
        
        #%% CALCULATE WEIGHTING FUNCTION
        print('[STATUS]   Generate hyperbolic weighting functions (1/d^2)')
        dist_inner_func, dist_outer_func = calc_weighting_functions_dask(mask_dask, mask_dilation, mask_erosion, buffer_size, version=version)
        
        #%% COMBINE DATA
        print('[STATUS]   Combine low resolution, high resolution and convolved (transition zone) data into single array')
        data_output = combine_data_dask(data_nearneighbor_transition, data_surface_dask, 
                                        mask_dask, mask_dilation, mask_diff, #None,
                                        dist_inner_func, dist_outer_func)
        
        #%% OUTPUT FINAL PRODUCT
        print(f'[SUCCESS]   Save composite data to file < {timestamp}_{type_stats}_composite.tif >')
        data_output_xr = xr.DataArray(data=data_output, coords=mask.squeeze().coords, attrs=mask.attrs,
                                      name='IBCSO').expand_dims(dim='band')
        data_output_xr.rio.to_raster(os.path.join(output_dir, f'{timestamp}_{type_stats}_composite.tif'),
                                     dtype='int16', nodata=-32768, **kwargs_tif)
        
        print(f'[SUCCESS]   Save transition zone to file < {timestamp}_{type_stats}_transition-zone.tif >')
        mask_diff_xr = xr.DataArray(data=mask_diff, coords=mask.squeeze().coords, name='transition_zone').expand_dims(dim='band')
        mask_diff_xr.rio.to_raster(os.path.join(output_dir, f'{timestamp}_{type_stats}_transition-zone.tif'), **kwargs_tif)
        
    #%% shutdown dask
    print('[STATUS]   Shutdown dask cluster and client\n')
    client.shutdown()
    cluster.close()