#!/usr/bin/bash

module purge
module load R/3.6.1 gdal/2.4.4 centoslibs proj intel.compiler proj/4.9.3 2>/dev/null
Rscript ./_R//IBCSO_installer.R > install.log
module purge