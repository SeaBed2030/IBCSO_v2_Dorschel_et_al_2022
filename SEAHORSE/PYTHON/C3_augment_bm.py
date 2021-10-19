#-----------------------------------------------------------
#   SEABED2030 - C3
#   Take aggregated info from blockmedian process and augment with metadata
#
#   (C) 2020 Sacha Viquerat, Fynn Warnke, Alfred Wegener Institute Bremerhaven, Germany
#   sacha.vsop@gmail.com
#-----------------------------------------------------------

import sys
import os
    
def get_translation(codefile):
    """
    Load data from rid codefile
    """
    rid=[]
    lines=[]
    header=None
    with open(codefile, 'r') as f:
        for i,row in enumerate(f):
            row=row.strip()
            if i==0: 
                header = row
                continue
            info = row.split('\t')
            rid.append(info.pop(0))
            lines.append(list(info))
    return rid, lines

def augment_BM(inFile,outFile,rid,metadata, delim='\t'):
    """
    Augment each line in blockmedian by metadata attributes.
    """
    with open(outFile,'w+') as o:    
        with open(inFile,'r+') as f:
            for i,line in enumerate(f):
                #if i==0: continue #header row no it'S not dummy!
                line=line.strip()
                line = line.split(delim)
                bm_rid = str(line[2])
                if bm_rid in rid: pos = rid.index(bm_rid)
                else: pos = None    
                if pos is None:
                    data=['0', '0', '0', '0', '0'] #we need dummy data otherwise empty lines will shift everything around
                else:
                    data=metadata[pos]
                o.writelines(delim.join(data))
                o.writelines('\n')

if __name__ == '__main__':
    
    if len(sys.argv) > 1: # check if there are any commandline arguments
        inFile = sys.argv[1].rstrip() # input file "*.bmz"
        outFile = sys.argv[2].rstrip() # output file "*.bmplus"
        codefile = sys.argv[3].rstrip() # metadata table extract (from SQL dump)
    else:
        print('[ERROR]    No input argument(s) given!')
        sys.exit(1)
    
    rid, metadata = get_translation(codefile) # 
    
    augment_BM(inFile, outFile, rid, metadata)
    
