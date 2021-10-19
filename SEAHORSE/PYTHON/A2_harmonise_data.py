#-----------------------------------------------------------
#   SEABED2030 - A2
#   Harmonise cruise data
#
#   (C) 2020 Sacha Viquerat, Fynn Warnke Alfred Wegener Institute Bremerhaven, Germany
#   sacha.vsop@gmail.com
#-----------------------------------------------------------

import os
import sys
import re
import csv
import pyproj

def getFromDictArray(dictArray,keyName,value):
    for pos, di in enumerate(dictArray):
        test = di.get(keyName,-1)
        if  test.lower() == value.lower():
            return(di)
    return None
    
def get_metadata(filename):
    """
    retrieve data relevant to harmonisation from MySQL metadata dump.
    
    Parameters
    ----------
    filename : str
        Input path pointing to metadata file from SQL dump.
        
    Returns
    -------
    metadata : list of dicts
        List of single dictionary per row in input file
    """
    metadata = []
    with open(filename, 'r') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            metadata.append(row)
    return metadata

def brute_force_split(line, desired_sep=' '):
    """
    Replace all characters except ' ' (whitespace) to homogenise line separators. 
    Combine all combinations of delimiters (\t;).
    Do not replace ',' it might be adecimal separator (duh).
    Also replace duplicate signs (--) with single sign (-).
    Then: split line using ' ' separator.
    """
    
    line = re.sub('[\t\;\s\,]+', desired_sep,line.strip()) # regex: + permutates across character set in square brackets (tab, space, colon, comma)
    line = re.sub('[-]+','-', line) # regex: replace multiple signs with single
    return line.split(desired_sep)

def clean_number(number_string, NEG = 0):

    """Take number_string and replace all dubious characters,
    extra digits and do all sorts of auto-fixes to obtain a clean floast number."""
    number_string = re.sub("[\,]+",".",number_string) #first set decimal to point
    number = re.sub("[^-0123456789\.]","",number_string)
    if number in ['','-']: return None
    if number.count('.')>1: #what should we do about 1234.00.00 (HI513.xyz?)
        intPart = number.split('.')[0] #take the integer part
        decPart = number.split('.')[1] #take the decimal part (discard the remaining splits)
        number = f"{intPart}.{decPart}"
    number = float(number) 
    if number is None: return None
    #if NEG: number = -abs(number)
    return(number)

def harmonise(filename, records) -> list:#, outproj)->list:

    """Main harmonisation loop. Read line from source data and make sure it fits 
    the desired seabed2030 format. columns x y z separated by ',' and integer precision.
    Coordinates in seabed2030 projection, negative depths."""
    
    xCol = 0 #int(records['xCol'])-1 #column of the x value
    yCol = 1 #int(records['yCol'])-1 #column of the y value
    zCol = 2 #int(records['zCol'])-1 #column of the z value
    ZNEG = 1 #records['zNeg'] #column of the zNeg
    RID = records['dataset_rid'] #use regional identifier as new filename
    weight = records['weight']
    newFilename=f'{os.path.splitext(filename)[0]}#{RID}_w{weight}.hxyz' #hxyz harmonised xyz file
    errorName=f'{os.path.splitext(filename)[0]}#{RID}_w{weight}.harmerrors' #hxyz harmonised xyz file
    
    if weight is None:
        return "weight is missing", f'{os.path.splitext(filename)[0]}#{RID}_w-1.harmerrors' #hxyz harmonised xyz file, -1
    
    MSGERROR=[]
    MSGWARNING=[]
    ErrArea = False
    ErrLine = None
    print('WARNING:\tUsing dangerous file operation for writing (saving time)!')
    print('WARNING:\tAuto-fixing xyz errors (decluttering log files)!')
    
    with open(filename,'r') as f:
        harmonised = open(newFilename,'w+') #create and write
        for cnt, line in enumerate(f):
            originalLine = f'{line.rstrip()}'
            line = brute_force_split(line)

            #check length of line splits
            if len(line) < (max([xCol,yCol,zCol]) + 1): 
                if not ErrArea:
                    ErrArea = True
                    ErrLine = cnt
                continue 
           
            x1, y1, z1 = line[xCol],line[yCol],line[zCol] #assign x,y, depth
            
            try:
                x, y, z = clean_number(x1),clean_number(y1),clean_number(z1) #assign x,y, depth - int version
            except: 
                if not ErrArea:
                    ErrArea = True
                    ErrLine = cnt
                continue
            if None in [x,y,z]: #if we can't auto fix all numbers, skip this line and go to the next
                if not ErrArea:
                    ErrArea = True
                    ErrLine = cnt
                continue
                
            #check depth    
            if z>0:
                z*=-1
            if abs(z) > 11000:
                if not ErrArea:
                    ErrArea = True
                    ErrLine = cnt
                continue
                    
            x,y,z = int(x),int(y),int(z) #discard decimals
            
            newLine = ' '.join(str(e) for e in (x, y, z)) #build a harmonised line
            harmonised.write(f'{newLine}\n') #write the line, finish by adding a newline character
                        
            if ErrArea:
                MSGERROR.append(f'{RID}\tERROR\t{os.path.basename(fileName)}\t{ErrLine}\t{cnt}\tline is malformed')
                ErrArea = False
                ErrLine = None
        harmonised.close()  
    return MSGERROR, errorName, cnt

if __name__ =='__main__':
    
    if len(sys.argv) > 1: # check if there are any commandline arguments
        filename = sys.argv[1].rstrip() #first argument of the script, input file name provided by sruns, remove trailing whitespace
        metafile = sys.argv[2].rstrip()
    else:
        print('[ERROR]    No input argument(s) given!')
        sys.exit(1)
    
    dataset_name = os.path.splitext(os.path.basename(filename))[0]
    dataset_name = f"{dataset_name.split('#')[0]}.xyz"

    metadata = get_metadata(metafile)
    records = getFromDictArray(metadata, 'dataset_name', dataset_name)
    
    if records is None:
        print(f'ERROR:\tNo metadata found for dataset {dataset_name}!')
        sys.exit(1)
    
    MSG, errorName, linecount = harmonise(filename, records)
    
    if len(MSG) > 0:
        MSG.append(f"{records['dataset_rid']}\tINFO\t{os.path.basename(filename)}\t{linecount}\t{linecount}\ttotal lines")
        with open(errorName  ,'w+') as f: #create and write
            for line in MSG:
                f.write(f'{line}\n')
