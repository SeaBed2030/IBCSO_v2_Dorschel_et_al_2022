#-------------------------------------------------------------------------------
# Author:      fwarnke
#
# Created:     27-11-2020
# Updated:     07-06-2021
# Institute:   Alfred Wegener Institute (AWI), Bremerhaven
# Contact:     fynn.warnke@awi.de (fynn.warnke@yahoo.de)
#-------------------------------------------------------------------------------

import numpy as np
import dask
from scipy.ndimage import (binary_dilation, binary_erosion, distance_transform_edt, convolve)

def get_circular_buffer(buffersize:int):
    """
    Create circular buffer as boolean numpy.arry from given buffer size.
    Inspired from https://stackoverflow.com/a/8650741
    
    Parameters
    ----------
    buffersize : int 
        Buffer size for transition zone (aka radius).
    
    Returns
    -------
    kernel : np.array
        Numpy array of circular buffer (quasi-boolean, with 1 and 0).
    """
    # get diameter from buffersize
    diameter = buffersize*2 + 1
    # initialize kernel structure
    kernel = np.zeros((diameter, diameter))
    # create circular mask (indices)
    y,x = np.ogrid[-buffersize:diameter-buffersize, -buffersize:diameter-buffersize]
    mask = x**2 + y**2 <= buffersize**2 + int(buffersize/2)
    # set indices of circular buffer to 1
    kernel[mask] = 1
        
    return kernel

def create_transition_zone_dask(mask_data, buffer_size, percentage=0.0, version='standard', 
                                mask_srtm=None, remove_small_patches=False, remove_divisor=5):
    """
    Create transition zone between IBCSO composite data and SRTM15 background grid based on data mask.
    
    Parameters
    ----------
    mask_data : dask.array
        Lazy loaded IBCSO data mask (from GeoTIFF).
    buffer_size : int
        Buffer size from command line input.
    percentage : float, optional
        Share of original data on transition zone. The default is 0.2.
    version : str, optional
        Either 'standard' (only on background grid) or 'smooth' (on both composite and background grid). 
        The default is 'standard'.
    remove_small_patches : bool
        Perform binary_closing operation to remove small SRTM15 infill patches 
        (based on structure with size: buffer_size//2). The default is 'False'.
    remove_divisor : int
        Divisor of buffer_size (divident) to calculate reduced footprint for removal of small patches. 
        The default is '5' (1/5 of buffer_size [radius]).
    
    Returns
    -------
    mask_dask : dask.array
        IBCSO data mask (optional: updated with mask_srtm).
    mask_dilation : dask.array
        Dilated IBCSO data mask.
    mask_erosion : dask.array
        Eroded IBCSO data mask.
    mask_diff_dilation : dask.array
        Difference data mask from dilation and erosion.
    footprint : np.array
        Structure used for morphological operations.
    """
    # create circular buffer footprint
    footprint = get_circular_buffer(buffer_size)
    
    # optional: mask SRTM15 specific areas
    if mask_srtm is not None:
        print('[INFO]   Masking areas where *NOT* to use SRTM15+ grid')
        # update data mask in areas where SRTM15 should *NOT* be used
        # (mask_data==0) --> (mask_data==1) where SRTM15 *NOT* be used (mask_srtm==0)
        mask = dask.array.where(mask_srtm==0, 1, mask_data)
    else:
        mask = mask_data
    
    if version == 'standard':
        kwargs = {'structure':footprint, 'iterations':1, 'border_value':0}
        
        # perform binary dilation chunkwise (1: data + buffered areas [dilation], 0: else)
        mask_dilation = mask.map_overlap(binary_dilation, depth=buffer_size, boundary='none', trim=True,
                                         dtype=np.int8, name='mask_dilation', **kwargs)
        
        if remove_small_patches:
            print('[INFO]   Removing small SRTM15 infill patches')
            remove_divisor = int(remove_divisor) # make sure divisor is int
            footprint_cleaning = get_circular_buffer(int(buffer_size//remove_divisor))
            if len(footprint_cleaning) == 1: footprint_cleaning = np.ones((3,3), dtype=np.int8) # fallback value if buffer_size small

            # perform binary_closing to eliminate small patches where SRTM15 grid would be used
            kwargs_dilation = {'structure':footprint_cleaning, 'iterations':1, 'border_value':0}
            tmp = mask_dilation.map_overlap(binary_dilation, depth=buffer_size, boundary='none', trim=True,
                                            dtype=np.int8, name='mask_dilation_cleaned', **kwargs_dilation)
            kwargs_erosion = {'structure':footprint_cleaning, 'iterations':1, 'border_value':1} # border_values must be 1 --> avoid edge effects!
            mask_cleaned = tmp.map_overlap(binary_erosion, depth=buffer_size, boundary='none', trim=True,
                                           dtype=np.int8, name='mask_dilation_cleaned', **kwargs_erosion)
            
            # calc additional mask of buffered areas using logical binary check (1: buffered areas [dilation], 0: data + else)
            mask_diff = dask.array.logical_xor(mask_cleaned, mask, **{'dtype':np.int8})
            
            return mask, mask_cleaned, None, mask_diff, footprint
        else:
            # calc additional mask of buffered areas using logical binary check (1: buffered areas [dilation], 0: data + else)
            mask_diff = dask.array.logical_xor(mask_dilation, mask, **{'dtype':np.int8})
        
            return mask, mask_dilation, None, mask_diff, footprint
    
    elif version == 'smooth':
        # calculate individual buffer sizes
        buffer_size = buffer_size * 2 + 1
        buffer_erosion = np.rint(buffer_size * percentage).astype(np.int) if np.rint(buffer_size * percentage).astype(np.int) >= 3 else 3
        buffer_dilation = (buffer_size - buffer_erosion) if (buffer_size - buffer_erosion)%2 == 1 else (buffer_size - buffer_erosion) + 1
        
        # ----- DILATION --> surface grid area -----
        footprint_dilation = get_circular_buffer(buffer_dilation)
        kwargs_dil = {'structure':footprint_dilation, 'iterations':1, 'border_value':0}
        mask_dilation = mask.map_overlap(binary_dilation, depth=buffer_size, boundary='none', trim=True,
                                         dtype=np.int8, name='mask_dilation', **kwargs_dil)
        
        # ----- EROSION --> nearneighbor grid area -----
        footprint_erosion = get_circular_buffer(buffer_erosion)
        kwargs_ero = {'structure':footprint_erosion, 'iterations':1, 'border_value':0}
        mask_erosion = mask.map_overlap(binary_erosion, depth=buffer_size, boundary=1, trim=True,
                                        dtype=np.int8, name='mask_erosion', **kwargs_ero).astype(np.int8)
        
        # ----- only TRANSITION ZONE -----
        # calc additional mask using dask-backed logical binary check
        mask_diff = dask.array.logical_xor(mask_dilation, mask_erosion, **{'dtype':np.int8})
        
        return mask, mask_dilation, mask_erosion, mask_diff, footprint

def fill_transition_zone_dask(mask, mask_erosion, data_nn, data_s, footprint, version='standard'):
    """
    Compute data values for used in transition zone using a convolution of merged nearneighbor and surface data.

    Parameters
    ----------
    mask : dask.array
        Lazy loaded IBCSO data mask (from GeoTIFF).
    mask_erosion : dask.array
        Eroded IBCSO data mask.
    data_nn : dask.array
        High-resolution raster data (sparse grid). Computed using 'gmt nearneighbor'.
    data_s : dask.array
        Low-resolution background raster data (interpolated grid). Computed using 'gmt surface'.
    footprint : np.array
        Footprint for moving window operations (N x N).
    version : str, optional
        Define algorihtm method to use. Either 'standard' (only on background grid) 
        or 'smooth' (both on high-res and background grid).The default is 'standard'.
    
    Returns
    -------
    data_out : dask.array
        Convolved data with inserte high-resolution data.
    
    """
    # combine surface and nearest neighbor data into single array for convolution
    data_merge = dask.array.where(mask==1, data_nn, data_s).astype('int32')
    
    # define parameter for dask.array.map_overlap
    depth = tuple(np.array(footprint.shape) // 2) # depth should be at least(!) half the footprint size to avoid issues!
    depth = dict(zip(range(data_merge.ndim), depth))
    boundary = 'none'
    # convolve combined nearneighbor and surface data
    data_tmp = dask.array.map_overlap(convolve, data_merge, depth=depth, boundary=boundary, trim=True,
                                      dtype=np.float32, name='convolve_temp', meta=data_merge._meta, 
                                      weights=footprint, mode='reflect')
    # convolve dummy grid (used as divisor)
    data_kernel = dask.array.map_overlap(convolve, dask.array.ones_like(data_merge), depth=depth, boundary=boundary, trim=True,
                                         dtype=np.float32, name='convolve_kernel', meta=data_merge._meta,
                                         weights=footprint, mode='constant', cval=1.0)
    # get actual convolved nearneighbor and surface data
    data_convolve = dask.array.true_divide(data_tmp, data_kernel)
    
    # [standard]: fill only surface transition zone
    if version == 'standard':
        data_out = dask.array.where(mask==1, data_nn, data_convolve)
        return data_out
    # [smooth]: fill both surface and nearneighbor transition zones
    elif version == 'smooth':
        data_out = dask.array.where(mask_erosion==1, data_nn, data_convolve)
        return data_out

def calc_weighting_functions_dask(mask, mask_dilation, mask_erosion, buffer_size, version='standard'):
    """
    Calculate euclidean distances from zero values of sparse high-resolution data mask (== 0) to actual data cells (mask == 1).
    Vis versa for dilated mask (low-resolution data). Closest cells to each mask equal "1" and decreasing values with increasing distance.
    This function returns "inner" and "outer" weighting factor grids calculated using  ahyperbolic weighting function 1/(d*d) based on the euclidean distance matrices.
    
    Parameters
    ----------
    mask : dask.array
        Lazy loaded IBCSO data mask.
    mask_dilation : dask.array
        Dilated IBCSO data mask.
    mask_erosion : dask.array
        Eroded IBCSO data mask.
    version : str
        Either 'standard' (only on background grid) or 'smooth' (both on composite and background grid). 
        The default is 'standard'.
    
    Returns
    -------
    dist_inner_sq : dask.array
        Inner weighting function.
    dist_outer_sq : dask.array
        Outer weighting function.
    """
    def _weight_func(d):
        with np.errstate(divide='ignore', invalid='ignore'):
            out = 1/(d*d)
        return out
    
    # map_overlap settings
    depth = {0:buffer_size, 1:buffer_size}
    boundary = 'none'
    
    if version == 'standard':
        # distance from inner part (*inverted* mask used!)
        dist_inner = dask.array.map_overlap(distance_transform_edt, abs(mask - 1), depth=depth, boundary=boundary, trim=True, dtype=np.float32, name='dist_inner')
    elif version == 'smooth':
        # distance from inner part (*inverted* mask_erosion used!)
        dist_inner = dask.array.map_overlap(distance_transform_edt, abs(mask_erosion - 1), depth=depth, boundary=boundary, trim=True, dtype=np.float32, name='dist_inner')
        
    # inner weighting factor
    dist_inner_sq = dask.array.map_overlap(_weight_func, dist_inner, depth=depth, boundary=boundary, trim=True, dtype=np.float32, name='dist_inner_sq')
    
    # distance from outer part
    dist_outer = dask.array.map_overlap(distance_transform_edt, mask_dilation, depth=depth, boundary=boundary, trim=True, dtype=np.float32, name='dist_outer')
    
    # outer weighting factor
    dist_outer_sq = dask.array.map_overlap(_weight_func, dist_outer, depth=depth, boundary=boundary, trim=True, dtype=np.float32, name='dist_outer_sq')
    
    return dist_inner_sq, dist_outer_sq

def combine_data_dask(data_ibcso, data_bg, mask, mask_morph, mask_diff, dist_in_sq, dist_out_sq): #mask_srtm
    """
    Combine input data grids with computed transition zone data using weighting function. 
    
    Returns
    -------
    data_out : dask.array
        Final array with IBCSO, SRTM15 and combined data.

    """
    def calc_combined_chunkwise(dist_in_sq, dist_out_sq, data_ibcso, data_bg):
        with np.errstate(invalid='ignore'):
            out = (dist_in_sq*data_ibcso + dist_out_sq*data_bg) / (dist_in_sq + dist_out_sq)
        return out
    
    data_combined = dask.array.map_blocks(calc_combined_chunkwise, 
                                          dist_in_sq, dist_out_sq, data_ibcso, data_bg,
                                          dtype=np.float32, name='data_combined')
    
    data_out = dask.array.where(mask_morph == 0, data_bg, data_ibcso)           # outside IBCSO coverage (SRTM15)
    data_out = dask.array.where(mask_diff == 1, data_combined, data_out)        # transition zone (convolved data)
    
    return data_out
