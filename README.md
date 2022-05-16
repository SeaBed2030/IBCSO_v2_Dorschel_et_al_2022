# The International Bathymetric Chart of the Southern Ocean (IBCSO) Version 2
*Dorschel et al. (2022)*

https://doi.org/10.1038/s41597-022-01366-7

### Homepage:
www.ibcso.org

#### Contact:
ibcso@awi.de  
southern-ocean@seabed.org

Twitter: @ibcso

## Script repository
This repository contains all scripts that were used in the creation of the manuscript and the analysis contained therein. This repository contains all scripts   used in the main work flow (**SEAHORSE**) in an abstracted form. This means that you will not be able to use it as is - **SEAHORSE** is heavily adapted to work seamlessly in our **HPC** ecosystem. You might use it as a template to create your own derivative of the work flow. 

This repository also contains the comparison between **IBCSO v2** and **IBCSO v1** / **SRTM15+ v2.2**. These can be run using the Python and R scripts. However, since the data creation requires intensive processing of large tables, we implemented an **HPC** solution. The Python functions can be run separately to create the required data sets for the visualization though.

This repository contains the following folders:

   - SEAHORSE
     - all `shell`, `srun` and `python` scripts used in the main work flow (**SEAHORSE**) to create a gridded map from xyz files
   - COMPARISON
     - contains the scripts for both comparisons in the paper:
     - discrepancy map of sub areas
     - comparison between **IBCSO v2** and **IBCSO v1** / **SRTM15+ v2.2**

# Software References:
### GDAL 3.1.4
GDAL/OGR contributors. (2020). *GDAL/OGR Geospatial Data Abstraction software Library*. Retrieved from https://gdal.org

### GMT 6.1.1
Wessel, P., Luis, J. F., Uieda, L., Scharroo, R., Wobbe, F., Smith, W. H. F., & Tian, D. (2019). The Generic Mapping Tools version 6. Geochemistry, Geophysics, Geosystems, 20, 5556–5564. https://doi.org/10.1029/2019GC008515

### dask 2.30.0
Dask Development Team (2016). Dask: Library for dynamic task scheduling
URL https://dask.org

### numpy 1.20.1
Harris, C.R., Millman, K.J., van der Walt, S.J. et al. Array programming with NumPy. Nature 585, 357–362 (2020). DOI: 10.1038/s41586-020-2649-2. https://www.nature.com/articles/s41586-020-2649-2

### pandas 1.2.2
The pandas Development Team (2021). pandas-dev/pandas: Pandas 1.2.2
URL https://pandas.pydata.org/ doi: https://doi.org/10.5281/zenodo.4524629

### rasterio 1.2.0
Gillies, S., & others. (2019). Rasterio: geospatial raster I/O for Python programmers. Retrieved from "https://github.com/rasterio/rasterio"

### shapely 1.7.1
Gillies, S., & others. (2007). Shapely: manipulation and analysis of geometric objects. Retrieved from "https://github.com/Toblerity/Shapely"

### Python3
Van Rossum, G., & Drake, F. L. (2009). Python 3 Reference Manual. Scotts Valley, CA: CreateSpace.

### SEAHORSE is using the following libraries under LGPL:
#### psycopg2 2.8.6
