# The International Bathymetric Chart of the Southern Ocean (IBCSO) Version 2

*Dorschel et al. (2022)*

#### Contact:

ibcso@awi.de  
southern-ocean@seabed.org

# Short overview of Python scripts

### [A2_harmonise_data](./A2_harmonise_data.py)
This python script performs the following steps:
1. Retrieve RID for given input file.
2. Read line by line from each XYZ split
3. Check for column separators and brute force replace all `tab`, `;`, `,` to `whitespace`.
4. Round all depth values coordinates to the nearest meter
5. Remove all lines that cannot be salvaged (more or less than 3 columns after column separator replacement, multiple decimal symbols or special characters that cannot be interpreted in a line)
6. Export harmonized lines into a file using following name template: [**dataset_rid**]_w[**weight**] and the split identifier

### [A5_update_metadata](./A5_update_metadata.py)

Update the SQL database with information from the harmonized data.

### [B2_data_to_tiles](./B2_data_to_tiles.py)
This script assigns each data point of the given input file to a specific tile whose extents are defined in the *info_tiles* table in the SQL database. The following steps are performed:
1. Load tile extents from SQL database dump file
1. Read ASCII XYZ file using `pandas` (as iterator over multiple chunks for memory efficiency)
1. Assign tile IDs
1. Remove points outside extent and depths > threshold
1. **optional**: Write removed points to files for later QC
1. Write points for each assigned tile to individual output files ("tile_{tile_ID}\_{basic_tile_ID}_{raw_name}.til")
1. Write min/max values for X and Y coordinates and depth for each unique RID (input file)

### [B4_update_metadata](./B4_update_metadata.py)

- Update *metadata* table in SQL database with min/max values of X and Y coordinates and depth (Z) for each RID 
- Update *info_tiles* table in SQL database with list of featured RIDs, tile size (MB), and the tile creation date 

### [C3_sync_bm_stats](./C3_sync_bm_stats.py) and [C3_augment_bm](./C3_augment_bm.py)

1. Read selected metadata information (from SQL database dump)
1. Augment *blockmedian* (iterate over each input line)
1. Write augmented data to output files (`*.xyv`)



### [D5_product_bending.py](./D5_product_bending.py)

This python script performs the bending of two input grids (one sparse and one interpolated). In order to work with limited resources and preserve scalability (with increased cell resolution in the future) the python library `dask` is utilized to read all input files chunkwise and lazy. `dask` creates a task graph that will be computed using a specified scheduler (here *distributed*) to speed-up computations while enabling "larger then memory" operations. More about `dask` can be found in their [documentation](https://docs.dask.org/en/latest/).  
Furthermore, `xarray` and `rioxarray` are used on top of dask to read the GeoTIFFs and retain metadata information about projection and geotransformation.

Required input files for the bending are:
- low-resolution grid (`*_low_surf.tif`)
- high-resolution grid (`*_high_NN.tif`)
- mask of valid data (`*_high_mask_NN.tif`)
- buffer size (as number of cells)
- bending version
    - *standard* (transition zone only in cells of low-resolution grid)
	- *smooth* (transition zone in both high- and low-resolution grid)
- percentage of high-resolution grid cells to build transition zone

The script perform the following steps:
1. Create transition zone between both grids
1. Fill transition zone for sparse high-resolution input grid (required for later steps)
1. Calculate weighting factor grids
1. Combine both grids using filled transition zone

#### create_transition_zone_dask()
1. create circular footprint from buffer size (for moving window operations)
1. **optional**: mask areas where background grid should not be used (not applicable here!)
1. select alorithm version:
    - *standard*: perform `binary_dilation`
    - *smooth*: perform `binary_dilation` and subsequent `binary_erosion`  
1. calculate transition zone (quasi-boolean) from difference of input data mask and "buffered" mask using (according to selected version)

#### fill_transition_zone_dask()
1. combine high-resolution (sparse, nearneighbor) and low-resolution (interpolated, surface) data into single array (for convolution).  This is needed to extend the sparse high-resolution grid.
2. convolve combined nearneighbor and surface data (results in artifically high values)
3. convolve dummy grid (only ones in array) to use as divisor in next step
4. get actual convolved grid by utilizing `dask.array.true_divide` (results in correct depths)
5. fill transition zone based on chosen version (*standard* or *smooth*)

#### calc_weighting_functions_dask()
Calculate euclidean distances from zero values of sparse high-resolution data mask (== 0) to actual data cells (mask == 1).
Vis versa for dilated mask (low-resolution data). Closest cells to each mask equal "1" and decreasing values with increasing distance.
This function returns "inner" and "outer" weighting factor grids calculated using  ahyperbolic weighting function 1/(d*d) based on the euclidean distance matrices.

1. use `scipy.ndimage.distance_transform_edt` to calculate distances from high-resolution data mask
    - *standard*: original data mask (from sparse nearneighbor output) --> transition zone **not** part of high-res data!
    - *smooth*: calculated, eroded data mask --> transition zone **also** part of high-res data!
1. use `scipy.ndimage.distance_transform_edt` to calculate distances from low-resolution data mask (mask_dilation)
1. calculate inner/outer weighting factor grids

#### combine_data_dask()
Combine input data grids with computed transition zone data using a weighting function:  

$`\frac{factor_i * data_n + factor_o * data_b}{factor_i * factor_o}`$  

with *factor_i* as the inner weighting factor, *factor_o* as the outer weighting factor, *data_n* as sparse high-resolution grid and *data_b* as interpolated low-resolution grid (e.g. from `gmt surface`).

### [D6_gap_fill.py](./D6_gap_fill.py)  
This python script performs the bending of two input grids (one sparse and one interpolated). In order to work with limited resources and preserve scalability (with increased cell resolution in the future) the python library `dask` is utilized to read all input files chunkwise and lazy. `dask` creates a task graph that will be computed using a specified scheduler (here *distributed*) to speed-up computations while enabling "larger then memory" operations. More about `dask` can be found in their [documentation](https://docs.dask.org/en/latest/).  
Furthermore, `xarray` and `rioxarray` are used on top of dask to read the GeoTIFFs and retain metadata information about projection and geotransformation.

Required input files for the bending are:
- low-resolution *background* grid (`SRTM15_PS65.tif`)
- high-resolution *composite* grid (`*_median_composite.tif`)
- mask of valid data (`*_tid_low_mask.tif`)
- mask where **not** to use SRTM15+ (`SRTM15_mask.tif`)
- blocksize for blockmedian (as number of cells)
- buffer size (as number of cells)
- bending version, here *standard* (transition zone only in cells of *background* SRTM15+ grid)

The script perform the following steps:
1. Read input data (using `rioxarray`)
1. Calculate offset factor between IBCSO composite and SRTM15+ grids (using `calc_factor_xr()`)
1. Compute blockmedian of masked factor grid (using `blockmedian_chunkwise()`)
1. Generate surface spline interpolation (using `pygmt.surface`)
1. Filter output grid (using `pygmt.grdfilter`)
1. Resample filtered grid (using `gmt grdsample` called via `pygmt.Session`)
1. Adjust SRTM15+ grid to IBCSO depths (using `adjust_background()`)
1. Create transition zone between both grids (refer to [D5: create_transition_zone_dask](#create_transition_zone_dask))
1. Calculate weighting factor grids (refer to [D5: calc_weighting_functions_dask](#calc_weighting_functions_dask))
1. Combine both grids using filled transition zone (refer to [D5: combine_data_dask](#combine_data_dask))

#### calc_factor_xr()
Calculate (masked) factor grid from IBCSO composite and SRTM15 data: $`factor = data_c/data_b`$ with composite grid as *data_c* and background grid as *data_b*.  
Here, the factor grid is restricted to cell where measured data points exist (using data mask) and clipped at $`factor > 1.5`$ and $`factor < 0.5`$.  

#### blockmedian_chunkwise()
Perform blockmedian reduction for DataArray chunks using window of (blocksize x blocksize) and `dask.array.coarsen`.

#### adjust_background()
Adjust SRTM15+ grid to IBCSO depths by multiplying original SRTM15+ grid with interpolated and smoothed offset factor grid.

[D7_cnt_cells_per_uniq_RID](./D7_cnt_cells_per_uniq_RID.py)

- Update SQL metadata column "in_current_product" with number of grid cells in product that origin from each unique RID.

