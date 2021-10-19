# The International Bathymetric Chart of the Southern Ocean (IBCSO) Version 2
*Dorschel et al. 2022*

#### Contact:
ibcso@awi.de

southern-ocean@seabed.org

## Script repository
This repository contains all scripts that were used in the creation of the manuscript and the analysis contained therein. This repository contains all scripts   used in the main work flow (**SEAHORSE**) in an abstracted form. This means that you will not be able to use it as is - **SEAHORSE** is heavily adapted to work seamlessly in our **HPC** ecosystem. You might use it as a template to create your own derivative of the work flow. 

This repository also contains the comparison between **IBCSO v2** and **IBCSO v1** / **SRTM15+ v2.2**. These can be run using the Python and R scripts. However, since the data creation requires intensive processing of large tables, we implemented an **HPC** solution. The Python functions can be run separately to create the required data sets for the visualization though.

This repository contains the following folders:

   - SEAHORSE
     - all shell, srun and python scripts used in the main work flow (**SEAHORSE**) to create a gridded map from xyz files
   - COMPARISON
     - contains the scripts for both comparisons in the paper:
     - discrepancy map of sub areas
     - comparison between **IBCSO v2** and **IBCSO v1** / **SRTM15+ v2.2**
