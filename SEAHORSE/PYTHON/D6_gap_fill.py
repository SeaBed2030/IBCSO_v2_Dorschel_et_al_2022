#-------------------------------------------------------------------------------
# Author:      fwarnke
#
# Created:     Thu Nov 26 17:46:26 2020
# Updated:     
# Institute:   Alfred Wegener Institute (AWI), Bremerhaven
# Contact:     fynn.warnke@awi.de (fynn.warnke@yahoo.de)
#-------------------------------------------------------------------------------

import os
import argparse
import warnings

import numpy as np

import xarray as xr
import rioxarray
import pygmt

import dask
from dask.distributed import Client, LocalCluster, performance_report

from GENERAL.bending.functions_bending import (create_transition_zone_dask, 
                                               calc_weighting_functions_dask, 
                                               combine_data_dask)
try:
    import dask_memusage
    profile_mem = True
except ImportError:
    profile_mem = False

#%%
def calc_factor_xr(data_composite, data_background, mask, nan_value=np.nan):
    """
    Calculate (masked) factor grid from IBCSO composite and SRTM15 data.

    Parameters
    ----------
    data_composite : np.ndarray (block of xarray.DataArray)
        Lazily read IBCSO grid.
    data_background : np.ndarray (block of xarray.DataArray)
        Lazily read SRTM15 grid.
    mask : np.ndarray (block of xarray.DataArray)
        Lazily read IBCSO data mask.
    nan_value : (int, float, np.nan), optional
        NaN value used to fill masked grid cells. The default is np.nan.

    Returns
    -------
    factor_mask : np.ndarray (block of xarray.DataArray)
        Masked factor grid filled with nan_value.
    
    """
    # calculate factor array
    with np.errstate(divide='ignore', invalid='ignore'):
        factor = data_composite/data_background # data_composite/data_background
    # mask factor array in cells with no valid IBCSO data
    factor[np.where(mask==0)] = nan_value
    # restrict factor values to specific range
    factor[factor > 1.5] = 1.5 # 1.5
    factor[factor < 0.5] = 0.5 # 0.5
    
    return factor

def blockmedian_chunkwise(array, bm_cells:int, chunks:tuple):
    """
    Perform blockmedian reduction for DataArray chunks using window of (bm_cells x bm_cells).
    
    Parameters
    ----------
    array : xarray.DataArray
        DESCRIPTION.
    bm_cells : int
        Number of aggregated cells.
    chunks : tuple
        Tuple of chunks to split output (band, x, y)
    
    Returns
    -------
    array_bm : xarray.DataArray
        Blockmedian reduced input array (by bm_cells).
    
    Reference:
    http://xarray.pydata.org/en/stable/generated/xarray.DataArray.coarsen.html
    """
    array_bm = array.coarsen(y=bm_cells, x=bm_cells, 
                             coord_func={'y':'mean', 'x':'mean'}).median(skipna=True)#.chunk(chunks)
    
    # update 'spatial_reference' attribute to account for changed resolution
    gt = [float(g) for g in array_bm['spatial_ref'].attrs['GeoTransform'].split()]
    gt[1] = gt[1] * blocksize
    gt[-1] = gt[-1] * blocksize
    gt_bm = ' '.join([str(g) for g in gt])
    array_bm['spatial_ref'] = array_bm['spatial_ref'].rio.update_attrs({'GeoTransform':gt_bm})
    
    return array_bm

def preprocess_blockmedian_array(array_bm):
    """
    Preprocess xarray.DataArray for usage in pygmt.surface.
    
    Parameters
    ----------
    array_bm : xarray.DataArray
        Blockmedian-filtered factor array.
    
    Returns
    -------
    array_out : np.ndarray
        2D numpy array with X,Y and Z as columns.
    
    """
    with warnings.catch_warnings():
        warnings.filterwarnings(action='ignore', message='All-NaN slice encountered') # catch sepcified warning
        array_out = array_bm.squeeze(dim='band', drop=True).drop('spatial_ref').stack(dims=['x','y'])
        array_out = array_out.to_dataframe(name='factor_bm').reset_index(drop=False)
        array_out = array_out.replace([np.inf, -np.inf], np.nan).dropna(axis=0, how='any')
        array_out = array_out.to_numpy(dtype=np.float32) # convert to 2D array
    
    return array_out

def adjust_background(data_background, factor):
    """Wrapper for background adjustment."""
    data_adjusted = data_background * factor
    
    # add attributes for export
    attrs = data_background.attrs
    for k in list(attrs.keys()):
        if k.startswith('STATISTICS_'):
            del attrs[k]
    data_adjusted = data_adjusted.assign_attrs(attrs)
    
    return data_adjusted

def define_input_arguments():
    parser = argparse.ArgumentParser(description='Merging IBCSO composite grid and SRTM15 data using custom bending algorithm.')
    parser.add_argument('composite', help='IBCSO composite GeoTIFF')
    parser.add_argument('background', help='SRTM15 V2 GeoTIFF')
    parser.add_argument('mask', help='Mask of IBCSO data (only high-resolution; TID >=15)')
    parser.add_argument('--mask_srtm', help='Mask where to use SRTM15+ grid (1: use, 0: do NOT)')
    parser.add_argument('--blocksize_bm', '-bs', nargs='?', type=int, help='Blockmedian window size (N x N)', default=20)
    parser.add_argument('--buffer', '-bu', nargs='?', type=int, help='Buffer size for transition zone', default=20)
    parser.add_argument('--version', '-v', nargs='?', type=str, help='Specify algorithm version', choices=['standard', 'smooth'], default='standard')
    parser.add_argument('--percentage', '-p', nargs='?', type=float, help='Percentage of high resolution data to incorporate (format: 0.x)', default=0.0)
    parser.add_argument('--outdir', '-o', nargs='?', type=str, help='Output directory for files to write')
    parser.add_argument('--type', type=str, help='Type of input/output data', default='median')
    return parser

#%%
if __name__ == '__main__':
    
    # ==================== VARIABLES ====================
    # parse arguments
    parser = define_input_arguments()
    args = parser.parse_args()
    
    blocksize = args.blocksize_bm               # define blockmedian extent; blocksize (cells) * resolution (500 m)
    buffer_size = args.buffer                   # define buffer size for creation of transition zone
    type_stats = args.type                      # get type (e.g. median)
    version = args.version                      # set bending version
    timestamp = '_'.join(os.path.basename(args.composite).split('_')[:-2])    # get timestamp of run
    
    if args.outdir == None:                     # check output directory
        output_dir = os.path.dirname(args.composite)
    else:
        output_dir = args.outdir
    
    verbosity = 'e'                                 # e: error messages only, t: timings, i: infos, w: warnings (verbose=True)
    region = '-4800000/4800000/-4800000/4800000'    # GMT region
    resolution = 500                                # set resolution of grids
    res_surface = resolution * blocksize            # resolution of surface spline interpolation
    res_filter = res_surface * 2                    # resolution of grdfilter's cosine filter
    chunksize = 4800
    chunks = (1, chunksize, chunksize)   # 4 byte (32 bit) * 4800 * 4800 = 92.16 MB
    
    # GeoTIFF settings
    kwargs_tif = {'tiled':True, 'blockxsize':chunksize, 'blockysize':chunksize,
                  'compress':'deflate', 'predictor':2, 'zlevel':6}
    
    # GMT settings
    pygmt_kwargs = dict(
        IO_GRIDFILE_FORMAT='nf',        # GMT netCDF format (32-bit integer, COARDS, CF-1.5) -> factor is float
        IO_NC4_CHUNK_SIZE=chunksize,    # netCDF chunksize
        IO_NC4_DEFLATION_LEVEL=6        # compression level 0 (none) to 9 (highest)
        )
    pygmt.config(**pygmt_kwargs)        # apply GMT settings
    
    # print info block to log
    print(f'[INFO]   type:               {type_stats}')
    print(f'[INFO]   composite:          {args.composite}')
    print(f'[INFO]   background:         {args.background}')
    print(f'[INFO]   mask:               {args.mask}')
    print(f'[INFO]   mask (SRTM):        {args.mask_srtm}')
    print(f'[INFO]   output dir:         {output_dir}\n')
    
    print(f'[INFO]   blockmedian size:   {blocksize}')
    print(f'[INFO]   buffer size:        {buffer_size}')
    print(f'[INFO]   version:            {version}')
    print(f'[INFO]   percentage:         {args.percentage}')
    print(f'[INFO]   resolution:         {resolution}')
    print(f'[INFO]   res_surface:        {res_surface}')
    print(f'[INFO]   res_filter:         {res_filter}')
    print(f'[INFO]   dask chunks:        {chunks}\n')
    
    #%% create dask cluster
    config = {'n_workers': 1,
              'processes': False,           # use Threads!
              'local_directory': os.environ.get('SSD_DIR')}
              
    cluster = LocalCluster(**config)        # create LocalCluster on compute node
    if profile_mem: # create csv of min/max MEM usage of each dask task
        dask_memusage.install(cluster.scheduler, os.path.join(output_dir,'D6_memory_usage.csv'))
    client = Client(cluster, timeout=60)    # connect dask client to slurm cluster
    
    with performance_report(filename=os.path.join(output_dir, f'D6_{type_stats}.html')):
        #%% READ DATA
        print('[STATUS]   Read IBCSO composite data')
        data_composite = rioxarray.open_rasterio(args.composite, chunks=chunks, default_name='composite')
        
        print('[STATUS]   Read SRTM15 V2 background data')
        data_background = rioxarray.open_rasterio(args.background, chunks=chunks, default_name='background')
        
        print('[STATUS]   Read IBCSO data mask (of nearneigbor)')
        mask = rioxarray.open_rasterio(args.mask, chunks=chunks, default_name='mask_ibcso').astype('int8')
        
        #%% CALCULATE FACTOR
        print('[STATUS]   Calculate FACTOR grid (masked by IBCSO RID grid)')
        factor = xr.apply_ufunc(calc_factor_xr, data_composite, data_background, mask, 
                                dask='parallelized', output_dtypes=['float32'], 
                                kwargs={'nan_value':np.nan}).rename('factor')
        
        #%% BLOCKMEDIAN OF FACTOR
        print('[STATUS]   Calculate BLOCKMEDIAN for masked factor grid')
        factor_bm = blockmedian_chunkwise(factor, bm_cells=blocksize, chunks=chunks).rename('factor_bm')
        
        #%% INTERPOLATE FACTOR (BLOCKMEDIAN)
        print('[STATUS]   Preprocess blockmedian factor grid (convert to numpy array with X,Y,Z columns)')
        factor_preproc = preprocess_blockmedian_array(factor_bm)
        
        print('[STATUS]   Interpolate factor residuals using pygmt.surface')
        out_surface = os.path.join(output_dir, f'{timestamp}_{type_stats}_factor_surface.nc')
        factor_surface = pygmt.surface(data=factor_preproc,
                                    spacing=f'{res_surface}',
                                    region=f'{region}',
                                    verbose=f'{verbosity}',
                                    **{'r':'',      # pixel registration
                                        'T':0.35,   # tension factor
                                        'C':0.001,  # convergence limit
                                        'Ll':'d',      # limit lower output solution to minimum input value
                                        'Lu':'d',      # limit upper output solution to maximum input value
                                        },
                                    outfile=out_surface)
        
        #%% FILTER INTERPOLATED FACTOR
        print('[STATUS]   Filter interpolated factor grid using pygmt.grdfilter')
        print('[STATUS]   >> save temporary file to disk')
        out_filtered = os.path.join(output_dir, f'{timestamp}_{type_stats}_factor_filtered.nc')
        factor_filtered = pygmt.grdfilter(grid=out_surface, #  factor_surface
                                        filter=f'c{res_filter}',                  # cosine arch filter (width=20000m)
                                        distance='0',                             # set distance unit equal to grid units (meter)
                                        #spacing='500', 
                                        region=f'{region}',
                                        verbose=f'{verbosity}',
                                        outgrid=out_filtered)
        
        #%% RESAMPLE FILTERED GRID
        print(f'[STATUS]   Resample factor grid to {resolution} m (bicubic)')
        out_resampled = os.path.join(output_dir, f'{timestamp}_{type_stats}_factor_resampled.nc')
        with pygmt.clib.Session() as session: # call GMT grdsample from python script -> no direct PyGMT API yet
            session.call_module('grdsample', f'{out_filtered} -G{out_resampled} -I{resolution} -R{region} -V{verbosity}')
        
        #%% ADJUST SRTM15 with FACTOR SURFACE (interpolated, filtered)
        print('[STATUS]   Read filtered factor grid using rioxarray')
        #factor_filtered = rioxarray.open_rasterio(out_filtered, chunks=chunks, default_name='factor_filtered')
        factor_filtered = rioxarray.open_rasterio(out_resampled, chunks=chunks, default_name='factor_filtered_resampled')
        
        print('[STATUS]   Export filtered factor surface grid as GeoTIFF')
        factor_filtered.assign_coords({'spatial_ref':data_composite['spatial_ref']}).rio.to_raster(os.path.join(output_dir, f'{timestamp}_factor_surf_filtered.tif'), **kwargs_tif)
        
        print('[STATUS]   Adjust SRTM15 grid using filtered factor grid')
        data_background_adj = adjust_background(data_background, factor_filtered)
        
        print('[STATUS]   Export adjusted SRTM15 grid as GeoTIFF')
        bg_path, bg_file = os.path.split(args.background)
        bg_name, bg_suffix = bg_file.split('.')
        out_adjusted = os.path.join(output_dir, f'{timestamp}_{bg_name}_adj.{bg_suffix}')
        data_background_adj.rio.to_raster(out_adjusted, dtype=np.int16, nodata=-32768, **kwargs_tif)
        
        #%% CONVERT XARRAY TO DASK.ARRAY
        print('[STATUS]   Open adjusted SRTM15 grid using rioxarray')
        data_bg_adj = rioxarray.open_rasterio(out_adjusted, chunks=chunks, default_name='SRTM15_adjusted')
        
        print('[STATUS]   Convert xarray to dask.array (drop dimension "band")')
        data_composite_dask = data_composite.squeeze(drop=True).data.persist()
        data_background_dask = data_bg_adj.squeeze(drop=True).data.persist()
        mask_dask = mask.squeeze(drop=True).data.persist()
        
        if args.mask_srtm is not None:   # check if additional mask was provided
            mask_srtm = rioxarray.open_rasterio(args.mask_srtm, chunks=chunks, default_name='mask_srtm').astype('int8').squeeze(drop=True).data
        else:
            mask_srtm = args.mask_srtm
        
        #%% CREATE TRANSITION ZONE
        print('[STATUS]   Create transition zone between low and high resolution data')
        mask_dask, mask_dilation, mask_erosion, mask_diff_dilation, _ = create_transition_zone_dask(mask_dask, buffer_size, version=version, 
                                                                                                    mask_srtm=mask_srtm, remove_small_patches=True, remove_divisor=5)
        
        #%% CALCULATE WEIGHTING FUNCTION
        print('[STATUS]   Generate hyperbolic weighting functions (1/d^2)')
        dist_inner_func, dist_outer_func = calc_weighting_functions_dask(mask_dask, mask_dilation, mask_erosion, buffer_size, version=version)
        
        #%% COMBINE IBCSO AND SRTM15
        print('[STATUS]   Combine low resolution, high resolution and convolved (transition zone) data into single array')
        # , mask_srtm_usage
        data_output = combine_data_dask(data_composite_dask, data_background_dask, 
                                        mask_dask, mask_dilation, mask_diff_dilation, #mask_srtm,
                                        dist_inner_func, dist_outer_func)
        
        #%% OUTPUT FINAL PRODUCT
        print(f'[SUCCESS]   Save gap-filled data to file > {timestamp}_{type_stats}_composite_with_SRTM.tif')
        data_output_xr = xr.DataArray(data=data_output, coords=mask.squeeze().coords, attrs=mask.attrs,
                                    name='IBCSO').expand_dims(dim='band')
        data_output_xr.rio.to_raster(os.path.join(output_dir, f'{timestamp}_{type_stats}_composite_with_SRTM.tif'),
                                    dtype='int16', nodata=-32768, **kwargs_tif)
        # export SRTM15+ infill mask
        mask_srtm_usage = dask.array.where(mask_dilation==1, 0, 1) # invert quasi-boolean raster (1: SRTM15 used, 0: not used)
        mask_srtm_usage_xr = xr.DataArray(data=mask_srtm_usage, coords=mask.squeeze().coords, attrs=mask.attrs,
                                          name='SRTM15_infill_mask').expand_dims(dim='band')
        mask_srtm_usage_xr.rio.to_raster(os.path.join(output_dir, f'{timestamp}_SRTM15_infill_mask.tif'),
                                         dtype='int16', nodata=-32768, **kwargs_tif)
        # DEBUG: export "mask_diff_dilation"
        ##mask_diff_dilation_xr = xr.DataArray(data=mask_diff_dilation, coords=mask.squeeze().coords, attrs=mask.attrs,
        ##                                     name='mask_diff_dilation').expand_dims(dim='band')
        ##mask_diff_dilation_xr.rio.to_raster(os.path.join(output_dir, f'{timestamp}_mask_diff_dilation.tif'),
        ##                                    dtype='int16', nodata=-32768, **kwargs_tif)
        
    #%% shutdown dask
    print('[INFO]   Shutdown dask cluster and client\n')
    client.shutdown()
    cluster.close()
    