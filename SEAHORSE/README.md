# The International Bathymetric Chart of the Southern Ocean (IBCSO) Version 2

*Dorschel et al. (2022)*

#### Contact:

ibcso@awi.de  
southern-ocean@seabed.org

# Summary of SEAHORSE STUFF

- add description of what's happening
- description on our hpc environment

## `conda` environment

We heavily rely on both `python` and `GDAL` routines for several steps of the **SEAHORSE** workflow. In order to provide a consistent and maintainable programming environment, we use a custom `conda` environment ([seabed2030](./seabed2030.yml)).

This `yml` file lists the required dependencies, however, since the availability of some packages is dependent on your operating system (in our case the Linux distribution CentOS), the environment file is intended only to provides a guideline. 

The usage of this environment is indicated in the scripts by the following lines:

​	`module load ${CONDA}` - loads *miniconda* (HPC specific)
​	`source activate ${CONDA_ENV}` - activates the custom [seabed2030](./seabed2030.yml) conda environment (*refer to [SEABED2030.config](./SEABED2030.config)*)

