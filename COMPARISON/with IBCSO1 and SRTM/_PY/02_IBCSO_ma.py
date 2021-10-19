import pandas as pd
import numpy as np
import sys, os
from datetime import datetime

def read_chunkwise(filename, window_size):
    #alternative: chunksize window size
    x_, y_ = [], []
    for chunk in pd.read_csv(filename, chunksize=window_size,sep='\t', header = None):
        chunk.columns = ["x", "y"]
        x_.append(chunk.x.mean())
        y_.append(chunk.y.mean())
    out = pd.DataFrame({'x_': x_, 'y_': y_})
    return(out)
        
def moving_avg(df:pd.DataFrame, window_size:int)->pd.DataFrame:
    mult = 1. / window_size
    df['bin'] = [ np.trunc( x * mult + mult ) for x in range( len( df ) ) ]
    
    x_ =df.groupby('bin').x.mean()
    y_ =df.groupby('bin').y.mean()
    
    frame = { 'x_': x_, 'y_': y_}
    ma = pd.DataFrame(frame)
    #ma.dropna(inplace=True)
    ma['y_diff'] = ma['y_'] - ma['x_']
    return(ma)

if __name__=='__main__':
    infile = sys.argv[1]
    window_size_a = int(sys.argv[2])
    window_size_b = int(sys.argv[3])
    outfile1=f"""{os.path.splitext(infile)[0]}_ma_a"""
    outfile2=f"""{os.path.splitext(infile)[0]}_ma_b"""

    print(f'{datetime.now()} reading data')
    #ma1 = read_chunkwise(infile, window_size_a)
    #ma2 = read_chunkwise(infile, window_size_a*window_size_b)
    
    df = pd.read_csv(infile,sep='\t', header = None)
    df.columns = ["x", "y"]
    print(f'{datetime.now()} calculating ma')
    ma_1 = moving_avg(df,window_size_a)
    ma_2 = moving_avg(df,window_size_a*window_size_b)
    print(f'{datetime.now()} saving data')
    ma_1.to_csv(path_or_buf=f"""{outfile1}.csv""", sep='\t',index=False, decimal='.')
    ma_2.to_csv(path_or_buf=f"""{outfile2}.csv""", sep='\t',index=False, decimal='.')