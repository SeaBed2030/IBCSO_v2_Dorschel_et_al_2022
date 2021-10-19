#-----------------------------------------------------------
#   SEABED2030
#   Download dynamic data from MySQL database and save it as csv
#
#   (C) 2020 Sacha Viquerat, Alfred Wegener Institute Bremerhaven, Germany
#   sacha.vsop@gmail.com
#-----------------------------------------------------------

import glob
import sys

from lib.MySQL import MySQL_Handler #custom MySQL library


if __name__ == '__main__':
    handler=MySQL_Handler()
    query=sys.argv[1]
    values=handler.query(query)
    for I in values:
        for k,v in I.items():
            print(f'{k} {str(v)}')
    
    