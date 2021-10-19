"""
Set band description(s) for specified GeoTIFF file

usage:
    python set_band_desc.py /path/to/file.tif band desc [band desc...]
where:
    band = band number to set (starting from 1)
    desc = band description string (enclose in "double quotes" if it contains spaces)
example:
    python gdal_set_band_names.py /path/to/ibcso.tif 1 "Band 1 desc"
    
credits:
    https://gis.stackexchange.com/a/290806

"""
import sys
from osgeo import gdal

def set_band_descriptions(filepath, bands):
    """
    filepath : str
        path/virtual path/uri to raster
    bands : tuple of tuples
        ((band, description), (band, description),...)
    """
    ds = gdal.Open(filepath, gdal.GA_Update)
    for band, desc in bands:
        rb = ds.GetRasterBand(band)
        rb.SetDescription(desc)
    ds = None # trigger GDAL to apply changes

if __name__ == '__main__':
    filepath = sys.argv[1]
    bands = [int(i) for i in sys.argv[2::2]]
    names = sys.argv[3::2]
    set_band_descriptions(filepath, zip(bands, names))