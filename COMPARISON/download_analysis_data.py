# download raster files for analysis and store:
import os
import urllib.request
from shutil import copyfile



filenames = {
'IBCSO_v1': "IBCSO_v1_bed_only_ocean.tif",
'IBCSO_v2': "IBCSO_v2_bed_only_ocean.tif",
'IBCSO_v2_60': "IBCSO_v2_bed_only_ocean_60deg.tif",
'SRTM15': "SRTM15plus_V2-2_only_ocean.tif"
}

links = {
'IBCSO_v1': """https://www.dropbox.com/s/lusgh805uhz3dus/IBCSO_v1_bed_only_ocean.tif?dl=0""",
'IBCSO_v2': """""",
'IBCSO_v2_60': """""",
'SRTM15': """"""
}

for data, filename in filenames.items():
    url =links[data]
    target = os.path.join(os.getcwd(),"with IBCSO1 and SRTM","DATA",filename)
    urllib.request.urlretrieve(url, target)
    print(data)
    print(url)
    print(target)
    target2= os.path.join(os.getcwd(),"discrepancy sub areas","SHAPES",filename)
    copyfile(target, target2)

    
# into SBD2K3_MANU_SCRIPTS\COMPARISON\with IBCSO1 and SRTM\DATA
# IBCSO_v1_bed_only_ocean.tif
# IBCSO_v2_bed_only_ocean.tif
# IBCSO_v2_bed_only_ocean_60deg.tif
# SRTM15plus_V2-2_only_ocean.tif

# #into SBD2K3_MANU_SCRIPTS\COMPARISON\discrepancy sub areas\SHAPES
# IBCSO_v1_bed_only_ocean.tif
# IBCSO_v2_bed_only_ocean.tif
# SRTM15plus_V2-2_only_ocean.tif