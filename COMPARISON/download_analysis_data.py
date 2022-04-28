# download raster files for analysis and store:
import os
import urllib.request
from shutil import copyfile

# this will result in the following structure:
# .\COMPARISON\with IBCSO1 and SRTM\DATA
# IBCSO_v1_bed_only_ocean.tif
# IBCSO_v2_bed_only_ocean.tif
# IBCSO_v2_bed_only_ocean_60deg.tif
# SRTM15plus_V2-2_only_ocean.tif

# .\COMPARISON\discrepancy sub areas\SHAPES
# IBCSO_v1_bed_only_ocean.tif
# IBCSO_v2_bed_only_ocean.tif
# SRTM15plus_V2-2_only_ocean.tif

filenames = {
'IBCSO_v1': "IBCSO_v1_bed_only_ocean.tif",
'IBCSO_v2': "IBCSO_v2_bed_only_ocean.tif",
'IBCSO_v2_60': "IBCSO_v2_bed_only_ocean_60deg.tif",
'SRTM15': "SRTM15plus_V2-2_only_ocean.tif"
}

links = {
'IBCSO_v1': """https://figshare.com/ndownloader/files/31364884?private_link=14d4c73a9f88fdff301f""",
'IBCSO_v2': """https://figshare.com/ndownloader/files/31323850?private_link=14d4c73a9f88fdff301f""",
'IBCSO_v2_60': """https://figshare.com/ndownloader/files/31323853?private_link=14d4c73a9f88fdff301f""",
'SRTM15': """https://figshare.com/ndownloader/files/31323856?private_link=14d4c73a9f88fdff301f"""
}

IBCSO_SRTM_COMP_PATH = os.path.join(os.getcwd(),"with IBCSO1 and SRTM","DATA")
IBCSO_subarea_COMP_PATH = os.path.join(os.getcwd(),"discrepancy sub areas","SHAPES")

os.makedirs(IBCSO_SRTM_COMP_PATH, exist_ok=True)
os.makedirs(IBCSO_subarea_COMP_PATH, exist_ok=True)

for data, filename in filenames.items():
    
    url = links[data]
    filename = filenames[data]
    
    target = os.path.join(IBCSO_SRTM_COMP_PATH,filename)
    urllib.request.urlretrieve(url, target)
    
    if data != 'IBCSO_v2_60':
        target2= os.path.join(IBCSO_subarea_COMP_PATH,filename)
        copyfile(target, target2)

    print(data)
    print(url)
    print(target)