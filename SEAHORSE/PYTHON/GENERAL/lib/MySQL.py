#-----------------------------------------------------------
#   SEABED2030 - MySQL handler class
#   #
#   (C) 2020 Sacha Viquerat, Alfred Wegener Institute Bremerhaven, Germany
#   sacha.vsop@gmail.com
#-----------------------------------------------------------

"""
####################################################################
#                                                                  #
#                          AWI Bathymetry                          #
#                       SEABED2030 - MySQL handler class           #
#                                                                  #
####################################################################

Provides basic functions for MySQL Interactions
"""

#=============================================================================
#    SCRIPT INFO
#=============================================================================

__author__ = 'Sacha Viquerat'
__version__ = '0.1'
__date__ = '2020-10-16'
__email__ = 'sacha.vsop@gmail.com'
__status__ = 'Developement'

#=============================================================================
#    IMPORT MODULES
#=============================================================================


import os
import csv
import re
import pymysql

from datetime import datetime
from typing import Optional

class MySQL_Handler():
    """ Lorem Ipsum """
       
    def __init__(self, host:str = 'host', user:str = 'username', password:str = 'password', db:str = 'database_name', DEBUG:bool = False):
        self.MYSQL_HOST = host
        self.MYSQL_USER = user
        self.MYSQL_PASS = password
        self.MYSQL_DB = db
        self.lastResult = None
        self.connect()
        self.DEBUG = DEBUG
    
    def connect(self):
        self.connection = pymysql.connect(host=self.MYSQL_HOST,user=self.MYSQL_USER,password=self.MYSQL_PASS,db=self.MYSQL_DB,cursorclass=pymysql.cursors.DictCursor)

    @staticmethod
    def dict2csv(data:[dict], sep='\t', dec='.', endLine='\n')->[str]:
        fileContent = [f'{sep.join(data[0].keys())}{endLine}']
        for e in data:
            fileContent.append(f'{sep.join([str(e) for e in e.values()])}{endLine}' )
        return fileContent
    
    @staticmethod
    def csv2dict(fileName:str,delimiter=',')->[dict]:
        """Lorem Ipsum

        Parameters
        ----------
        fileName : str
            Lorem Ipsum
        delimiter : str (default: ',')
            Lorem Ipsum

        """
        with open(fileName, 'r') as f:
            dialect = csv.Sniffer().sniff(f.readline(), [',','\t',' '])
            dialect.quoting=csv.QUOTE_NONE
            f.seek(0)
            reader = csv.DictReader(f, dialect)
            out = [r for r in reader]
        return(out)
            
    @staticmethod
    def printTable(myDict, colList=None):
       """ Pretty print a list of dictionaries (myDict) as a dynamically sized table.
       If column names (colList) aren't specified, they will show in random order.
       Author: Thierry Husson - Use it as you want but don't blame me.
       """
       if not colList: colList = list(myDict[0].keys() if myDict else [])
       myList = [colList] # 1st row = header
       for item in myDict: myList.append([str(item[col] if item[col] is not None else '') for col in colList])
       colSize = [max(map(len,col)) for col in zip(*myList)]
       formatStr = ' | '.join(["{{:<{}}}".format(i) for i in colSize])
       myList.insert(1, '\t') # Seperating line
       for item in myList: print(formatStr.format(*item))
       
    def query(self, query:str)->Optional[list]:
        """Establish connection and run query
        Parameters
        ----------
        query : str
            MySQL query string to be run against MySQL connection

        """
        if self.DEBUG: 
            print(query)
            return
            
        with self.connection.cursor() as cursor: 
            try:
                cursor.execute(query)
                self.connection.commit()
                res = cursor.fetchall()
            except:
                self.connection.rollback()
                res = None
        self.lastResult = res
        return res

    def getTable(self, table_name:str)->Optional[list]:
        query = f'select * from {table_name};'
        return self.query(query)
        
    def getInformation(self,table_name:str)->Optional[list]:
        query = f'select * from information_schema.COLUMNS where TABLE_SCHEMA = "{self.MYSQL_DB}" and TABLE_NAME = "{table_name}";'
        return self.query(query)
        
    def curate_MySQL(self, table_name:str='metadata',NULLIFY=True, TRIM=True):
        columns = self.getInformation(table_name)
        if len(columns)==0: 
            print(f'Table "{table_name}" not found on DB: {self.MYSQL_DB}!')
            return 1
        for col in columns:
            COLUMN_NAME,IS_NULLABLE,DATA_TYPE = col['COLUMN_NAME'],col['IS_NULLABLE'],col['DATA_TYPE']
            IS_CHAR = re.search('.*char.*', DATA_TYPE)
            if IS_CHAR:
                if TRIM:
                    sql=f"""UPDATE {self.MYSQL_DB}.{table_name} set {COLUMN_NAME} = REPLACE(REPLACE(REPLACE({COLUMN_NAME}, '\\t', ''), '\\r', ''), '\\n', '');"""
                    self.query(sql)
                    sql=f"""UPDATE {self.MYSQL_DB}.{table_name} set {COLUMN_NAME} = TRIM({COLUMN_NAME});"""
                    self.query(sql)
                if NULLIFY and IS_NULLABLE == 'YES':
                    sql=f"""update {self.MYSQL_DB}.{table_name} set {COLUMN_NAME} = NULL where {COLUMN_NAME} in ('',' ','""','\\'\\'');"""
                    self.query(sql)
        return 0
                    
    def getCreate(self, tablename:str)->Optional[list]:
        query = f'SHOW CREATE TABLE {tablename};'
        return self.query(query)[0]
        
    def getSQL(self, table_name:str)->Optional[list]:
        sql = f'SHOW CREATE TABLE {table_name};'
        create = self.query(sql)[0]['Create Table']
        create = create.replace("\n", "")
        sql = [f'{create};']
        data = self.getTable(table_name)
        columns = ', '.join(data[0].keys())
        for line in data:
            values = [int(x) if isinstance(x, bool) else x for x in line.values()]
            newline=f'INSERT INTO {table_name} ({columns}) VALUES ({",".join(values)});'
            sql.append(newline)
        return '\n'.join(sql)

        
    def getColumnNames(self,tableName:str)->Optional[list]:
        columns = self.query(f'SHOW columns FROM {tableName};')
        if columns is None: return None
        else: return [col['Field'] for col in columns]
            
    def insertFromDict(self, data:[dict],tableName:str)->[str]:
        query = []
        columns = list(data[0].keys())
        values = []
        print(columns)
        for e in data:
            pass
            
    def createTimeStamp(self,timeStamp:datetime = datetime.utcnow(), format:str = '%Y-%m-%d %H:%M:%S' )->str:
        timeString = timeStamp.strftime(format)
        sql = f'str_to_date("{timeString}","%Y-%m-%d %H:%i:%s")'
        return timeString, sql
        
    def setTimeStamp(self,timeStamp:str, format:str= '%Y-%m-%d %H:%M:%S', targetformat:str = '%Y-%m-%d %H:%M:%S' )->str:
        timeStamp = datetime.strptime(timeStamp, format)
        timeString = timeStamp.strftime(targetformat)
        sql = f'str_to_date("{timeString}","%Y-%m-%d %H:%i:%s")'
        return timeString, sql
        
    def fromTimeStamp(self, unixtime):
        return f'from_unixtime({unixtime})'

    def updateMySQL(self, dictArray:[dict], tableName:str='metadata')->[str]:
        queryLog=[]
        for line in dictArray:
            WHERE = ';'
            if 'where' in line: 
                WHERE = f' where {line["where"]};'
                del(line['where'])
            SETS = ', '.join([f'{key} = {item}' for key,item in line.items()])
            queryUpdate = f'update {tableName} set {SETS} {WHERE}'
            self.query(queryUpdate)
            queryLog.append(queryUpdate)
        return(queryLog)    
    
    @staticmethod
    def columnsToDefinitions(data:[dict])->dict:
        print(data[0].keys())
    
    def createTable(schemaName:str, columnDefinition={}):
        columns=None
        query=f'create table {schemaName} ()'
        
    def writeRecords(self,outFile, records, limit=',', HEADER=True):
        with open(outFile, 'w', newline='') as f:
            w = csv.DictWriter(f, records[0].keys(), delimiter=limit, lineterminator='\n')
            if HEADER : w.writeheader()
            w.writerows(records)
            
    def csvToMySQL(self,fileName:str, delimiter:str=',', schemaName = None, header:str=None):
        if schemaName is None:
            schemaName=os.path.basename(os.path.splitext(fileName)[0])
        data = self.csv2dict(fileName,delimiter)
        print([d for d in data] [0])
