#-----------------------------------------------------------
#   SEABED2030 - C3
#   Take aggregated info from blockmedian process and update tiles
#
#   (C) 2020 Sacha Viquerat, Fynn Warnke, Alfred Wegener Institute Bremerhaven, Germany
#   sacha.vsop@gmail.com
#-----------------------------------------------------------

import sys
import os
import argparse
import glob
import csv

from GENERAL.lib.MySQL import MySQL_Handler #custom MySQL library


def defineInputArguments():
    parser = argparse.ArgumentParser(description='Update tile database with blockmedian info')
    parser.add_argument('--infolder', '-i', nargs='?', type=str, help='folder of extent file location.', default=None, required=True)
    parser.add_argument('--suffix', '-s', nargs='?', type=str, help='Lines file suffix (defauts to "extent").', default='extent', required=False)
    return parser

def get_files(infolder:str,suffix:str):
    files = glob.glob(os.path.join(infolder,f"*.{suffix}"))
    return(files)

def convert_to_table(fileList:[str],sep='\t'):
    data = []
    for file in fileList:
        with open(file,'r',newline = '\n') as f:
            linedata = dict()
            for line in f.readlines():
                line = line.strip()
                line=line.split(' ')
                linedata[line[0]]=line[1]
            linedata['ID']=line[2]
            linedata['btile']=line[3]
            linedata['type']=line[4]
            linedata['no_cruises']=len(linedata['cruises'].split(';'))
            
            original_file=f'{os.path.splitext(file)[0]}.bm'
            linedata['creationtime']=os.path.getctime(original_file)
            linedata['filesize']=os.path.getsize(original_file)/1024/1024
        data.append(linedata)
    return data

def update_db(handler:MySQL_Handler, data:[dict]):
    query = f"TRUNCATE info_bm;"
    handler.query(query)
    for tile in data:
        query=f"""REPLACE into info_bm (ID, basicTile, bm_windows, bm_featured_cruises, bm_number_of_cruises,x_min,x_max,y_min,y_max,z_min,z_max,bm_type,bm_date_creation,bm_filesize_MB) values({tile['ID']}, {tile['btile']}, {tile['bm_windows']}, "{tile['cruises']}", {tile['no_cruises']}, {tile['xmin']},{tile['xmax']}, {tile['ymin']},{tile['ymax']}, {tile['zmin']},{tile['zmax']},"{tile['type']}",FROM_UNIXTIME({tile['creationtime']}),{tile['filesize']});"""
        handler.query(query)
        
if __name__ == '__main__':
    handler = MySQL_Handler()
    parser = defineInputArguments()
    args = parser.parse_args()

    if not len(sys.argv) > 1:
        print('\nNo arguments passed!\n')
        quit()

    folder = args.infolder
    suffix = args.suffix
    extentfiles = get_files(folder, suffix)
    data = convert_to_table(extentfiles)
    update_db(handler,data)
    print('Done.')
    
