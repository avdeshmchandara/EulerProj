# -*- coding: utf-8 -*-
"""
Created on Thu Aug  9 14:10:26 2018

@author: SINGA72
"""

'''
Created on October 6, 2015

@author: Anil Pratap Singh

1. GLOBAL Utilities
2. GENERAL Utilities
3. SQL Utilities
4. CSV To Sqlite
5. Logging System
6. Hist1D
7. Hist1S
'''
import os
import sys
import sqlite3
import traceback
import time
import datetime
import math
import numpy as np
import shlex

#@# GLOBAL Definitions
llog = None
sqllog = None

#@# GENERAL Utilities
def replace_all (string,seq,repl):
    while seq in string:
        string = string.replace(seq,repl)
    return string


def replace_many(string,seqL,replL):
    for seq,rep in zip(seqL,replL):
        string = string.replace(seq,rep)
    return string
    
def print_error(err,place=''):
    '''
    A utility function for better diagnostic reporting.
    '''
    exc_type, exc_value, exc_traceback = sys.exc_info()
    traceback.print_tb(exc_traceback, limit=5, file=sys.stdout)
    p = sys._getframe(1).f_code.co_name
    if place == '' : place = p
    sys.stderr.write(('ERROR ('+place+'):  %s\n') % str(err))
    
    
def clean_text(text, splitChar=','):
    '''
     1. remove unicode sentinetls u'xxx'
     2. remove (, ) characters 
     3. remove (\\) characters
     4. under development
     '''
    text = text.strip()
    ##Getting rid of unicode sentinel
    text = text.replace('u\'','')
    ##Following Characters mess-up with SQL
    text = text.replace('\'','')
    text = text.replace('(','')
    text = text.replace(')','')
    text = text.replace('.','')
    text = text.replace(' ','')
    text = text.replace('%','')
    text = text.replace('-','')
    ##print "clean_text",text
    return text, text.split(splitChar)


def sqlite_table_info(tableName):
    return "PRAGMA table_info(?tableName?)".replace("?tableName?",tableName)


def create_table_qry(tabName, varDict={}, uniqueIdFlag=False,uIdName='PYDAN_ROW_NUM'):
    try:
        query = ' CREATE  TABLE '+tabName+''' ( \n'''
        if(uniqueIdFlag == True):
            query = query + uIdName+'        VARCHAR,\n '
        varterms = []
        #for var in varNames:
        #    if var not in varTypes: varTypes[var]='VARCHAR'       
#        if (len(varTypes)==0):
#            for var in varNames: varTypes[var]='VARCHAR'
        #print (varDict)
        for varname in varDict:
            vartype = varDict[varname]            
            varterms.append('       '.join([varname,vartype]))
        query = query+ ',\n '.join(varterms)
        query = query+'\n )'
    
        return query
    except Exception as err:
        print_error(err, 'euler.create_table_qry')        
        

def clean_string(text,replaceHyphen=True):
    try:
        text = text.strip()
        ##Getting rid of unicode sentinel
        text = text.replace('u\'','')
        ##Following Characters mess-up with SQL
        text = text.replace('\'','')
        text = text.replace('(','')
        text = text.replace(')','')
        text = text.replace('.','')
        ##text = text.replace(' ','')
        text = text.replace('%','')
        if(replaceHyphen==True):
            text = text.replace('-','')
        text =text.replace('"','')
        ##print "clean_text",text
        ##Removing all non-ascii characters.
        text = ''.join([i for i in text if ord(i)<128])
        return text
    except Exception as err:
        print_error(err, 'euler.clean_string') 

#@# SQL Utili
def connection(url):
    '''
    Provides a connection object to sqlite.
    Registers some missing functions to sqlite.
    '''
    try:
        conn = sqlite3.connect(url)
        conn.create_function('log',func=math.log,narg=1)
        conn.create_function('alog',func=math.exp,narg=1)
        conn.create_function('sqrt',func=math.sqrt,narg=1)
        conn.create_function('pow',func=math.pow,narg=2)
        return conn
    except Exception as err:
        print_error(err,'euler.connection')

def attach_dbase(conn, name,obase):
    conn.execute("ATTACH '"+obase+"' AS "+name+';')
    conn.commit()
    
def get_bysql(query,conn,verbose=False):
    'Returns list of lists data fetched by query rowXcolumns format'
    try:
        #query = query.replace('?srcName?',tabName)
        if verbose==True : print (query)
        results = conn.cursor().execute(query)
        dataSet = []
        for r in results:
            rList = list(r)
            dataSet.append(rList)
        return dataSet  
    except Exception as err:
        
        print ("\n\n****Exception Encountered****\n")
        print ('Not Happy With \n'+query)
        print_error(err,'euler.get_bysql')
        
def run(query,conn,mute=False,nice_print=False, verbose=False):
    'Returns list of lists data fetched by query rowXcolumns format'
    if mute == True : return
    try:
        #query = query.replace('?srcName?',tabName)
        if verbose==True : print (query)
        results = conn.cursor().execute(query)
        
        if results == None: return 
        if results.description == None : return
        columns = [d[0] for d in results.description]
        columns = ','.join(columns)
        print ('='*len(columns))
        print(columns)
        print ('='*len(columns))
        dataSet = []
        for r in results:
            rList = [str(ri) for ri in list(r)]
            print (','.join(rList))
        print ('='*len(columns)+'\n')
        return dataSet  
    except Exception as err:
        
        print ("\n\n****Exception Encountered****\n")
        print ('Not Happy With \n'+query)
        print_error(err,'Euler.run')
        

"""
def get_bysql2(query,conn,verbose=False):
    'Returns list of lists data fetched by query rowXcolumns format'
    try:
        #query = query.replace('?srcName?',tabName)
        if verbose==True : print (query)
        results = conn.cursor().execute(query)
        if results != None
        dataSet = []
        for r in results:
            rList = list(r)
            dataSet.append(rList)
        return dataSet  
    except Exception as err:
        
        print ("\n\n****Exception Encountered****\n")
        print ('Not Happy With \n'+query)
        print_error(err,'euler.get_bysql')

"""
def print_logo():
    logo='''
    ************************************************
    *                    EULER                     *
    *    A SQLITE POWERED DATA SCIENCE TOOLKIT     *
    *          SINGH.AP79@GMAIL.NOSPAM.COM         * 
    ************************************************
    '''
    logo = '\n'.join([l.strip() for l in logo.split('\n')])
    print (logo)
print_logo()
        
def download_csv_bysql(query,conn,write_to,verbose=False):
    'Returns list of lists data fetched by query rowXcolumns format'
    try:
        #query = query.replace('?srcName?',tabName)
        if verbose==True : print (query)
        write_to  = open(write_to,'w')
        query = query.replace(';','')
        results = conn.cursor().execute(query)
        columns = [d[0] for d in results.description]
        write_to.write(','.join(columns)+'\n')
        counter = 0
        for r in results:
            if (counter-1)%10000 == 0: print ('Downloaded :',counter,' records.')
            rList = [str(ri) for ri in list(r)]
            rList = ','.join(rList)+'\n'
            write_to.write(rList)
            counter = counter+1
            
        write_to.close()    
    except Exception as err:
        print ("\n\n****Exception Encountered****\n")
        print ('Not Happy With \n'+query)
        print_error(err,'euler.download_csv_bysql')
        
        
def get_ncols_bysql(query,n,conn,verbose=False,col_keys=None):
    'Returns list of lists data fetched by query in columnXrow format'
    try:
        if n==1 : return get_1col_bysql(query, conn)
        res = get_bysql(query,conn,verbose=verbose)
       
        if len(res) == 0: 
            res = [[]]*n
           
            return res 
        data_cols = []
        for i in range(0,n):    
            data_cols.append([line[i] for line in res])
        if col_keys == None:
            return tuple(data_cols)
        else: return dict(zip(col_keys,data_cols))
    except Exception as err:
        print_error(err,'euler.get_ncols_bysql')
        
 
def get_1col_bysql(query,conn):
    try:
        'Return list data fetched by query for a single output'
        res = get_bysql(query, conn)
        col1 = [line[0] for line in res]
        return col1
    except Exception as err:
        print_error(err,'euler.get_1col_bysql')



def execute(script,conn,verbose=1):
    try:
        cursor = conn.cursor()
        for sql in script.split(';'):
            if sql.strip() == "":
                pass
            else:       
                try:
                    if (llog != None): llog.write_log(sql)
                    if verbose > 0: print (sql)
                    cursor.execute(sql)
                    conn.commit()
                except Exception as err:
                    print_error(err,'euler.execute')
                    print ('''
                    Not happy with       
                    '''+sql)
                    if 'DROP ' not in sql: sys.exit(-9999)
                    
        return 0
    except Exception as err:
        print_error(err,'Euler.execute')

def mv_db1_db2(conn,name,tabName):
    sql = ''' 
    CREATE TABLE ?name?.?tabName? as SELECT * FROM ?tabName? t2;
    '''
    sql = sql.replace('?name?',name)
    sql = sql.replace('?tabName?',tabName)
    execute(sql,conn,verbose=1)
    conn.commit()
    
"""
keep working...
""" 
def print_data_dict(data_dict,order=''):
    order = order.split(',')
    if len(order)==0:
        order = data_dict.keys()
    data_array = []
    for k in order:
        data_array.append(data_dict[k])
    print (', '.join(order))
    
    rows = 0.0
    cols = len(data_array)
    if cols > 0: rows = len(data_array[0])
    
    for row in range(0,rows):
        pass
    
    


    
#@# CSV To Sqlite
class pycsv_reader:
    '''
     csv reader
    '''
   
    def __init__(self,fName,headers=[],separator = ',',firstLineIsHeader=True, 
                 reNames={}):
        self.pFirstLineIsHeader = firstLineIsHeader
        self.pSeparator = separator
        self.pFname = fName
        if len(headers) !=0 and firstLineIsHeader==True:
            raise RuntimeError('Ambiguous Header Specifications')
        if os.path.exists(self.pFname):
            self.pFile = open(self.pFname,'r')
            #firstLine   = self.pFile.readline().split(self.pSeparator)
            headLine = self.pFile.readline()
            for key in reNames.keys():
                headLine=headLine.replace(key,reNames[key])
            firstLine = headLine.strip().split(self.pSeparator)
            firstLine = [word.strip() for word in firstLine]
            
            #print firstLine
            if len(headers) == 0 and firstLineIsHeader==False:
                numCol = len(firstLine)
                for n in range (1,numCol):
                    self.pHeader.append(str(n))
            elif len(headers) == 0 and firstLineIsHeader==True:
                self.pHeader = firstLine
            elif len(headers) != 0 and firstLineIsHeader==False:
                #if len(firstLine) == len(headers):
                self.pHeader = headers
                #else:
                #    raise RuntimeError("Not implemented yet")
            self.pHeader = map (str.strip, self.pHeader)
            self.pHeader = map (clean_string,self.pHeader)
            ##The clean_text return two values: Check how it works here.
            ## Not easy to use map so let us replace things by list comprehension 
            ##self.pHeader = map (clean_text.,self.pHeader)
            ##@ANIL I don't want it this ugly
            tempHeader = []
            for cname in self.pHeader:
                a, h = clean_text(cname,self.pSeparator)
                a = a.replace('"','')
                tempHeader.append(a)
            self.pHeader = tempHeader
        elif IOError:
            print ("Unable to find file: "+str(fName))
    
    def __del__(self):
        '''
        Returning the resources.
        '''
        self.pFirstLineIsHeader = None
        self.pSeparator = None
        self.pFname = None
        self.pHeader = None
        self.close()
        self.pFile = None
        
    def close(self):
        try:
            self.pFile.close()            
        except Exception as err:
            sys.stderr.write('ERROR: %s\n' % str(err))


            
    def toStr(self):
        print (self.pFname)
        for col in self.pHeader:
            print (col)   
         
   
    
    def  to_database(self,tabName,database,varNames=[],varTypes=None,ur=False, 
                     replaceList=[],useshlex=False,nestedcommareplace=None,
                     reportEvery = 100,
                     verbose = False
                     ):
        '''writes the csv file to a table.
           ---returns a pydset object associated with table
        '''
          
        
        try:
            pConn = sqlite3.connect(database)
            print ('drop table if exists '+tabName)
            pConn.execute('drop table if exists '+tabName)
            print ('Here!')
            pConn.commit()
            if len(varNames)==0:
                varNames = self.pHeader
            if varTypes == None:
                varTypes = {}
                
            for var in varNames:
                #print var
                if var not in varTypes: varTypes[var]='VARCHAR'
                else: varTypes[var]=varTypes[var].strip()


            query = create_table_qry(tabName,varDict=varTypes
                                            ,uniqueIdFlag=False
                                           )
            
            if verbose == True: print (query)
            pConn.execute(query)
            print ("Done")
            pConn.commit()
            
            manyLines = []
            q = ['?']*len(varNames)
            qr = '('+','.join(q)+')'
            query = 'insert into '+tabName+'('+','.join(varNames)+') values '+qr
            
            #print (query)
            pConn.execute('PRAGMA synchronous=OFF');
            counter = 0;            
            for row in self.pFile:
                if (counter%int(reportEvery) == 0):
                    print ("Lines Read: ",counter)
                row = row.strip()
                if row == '': continue
                line = None
                if useshlex==True:
                    if nestedcommareplace == None:
                        #rasieException
                        print_error('pydata.shlex.screwup')
                        return
                    
                    row = row.replace(",,",",'',")
                    splitter = shlex.shlex(row, posix=True)
                    splitter.whitespace=','
                    splitter.whitespace_split = True
                    line = list(splitter)
                    line = [l.replace(',',nestedcommareplace) for l in line]
                    #print ('shlex'+'-'.join(line))
                else: line = row.split(self.pSeparator)
                if len(line)==0: continue
                  
                else:
                    for replacement in replaceList:
                        key = replacement.strip().split(":")[0]
                        val = replacement.strip().split(":")[1]
                        line =  [t.replace(key,val) for t in line]
                        
                    line = [l.replace("\xa0", " ") for l in line]
                    
                    manyLines.append(tuple(line))
                    #print (line)
                    if (counter%1)==0:
                        if counter == 0:
                            pass
                        else:
                            if verbose ==  True: print (query,manyLines)
                            pConn.cursor().executemany(query,manyLines)
                            
                            manyLines =[]
                    #if(select):
                    counter = counter+1
                
            if len(manyLines)>0:
                pConn.executemany(query,manyLines)
            else: self.pFile.close()
            ##Move the cursor back to top of the csv file.
            pConn.commit()
                    
        except Exception as err:
                print_error(err,"pycsv_reader.to_database")
            
    def reader(self, numLines=-9999):
        rows = []
        nLines = 0
        for row in self.pFile:
            r,rowList = clean_text(row, self.pSeparator)
            rows.append(rowList)
            nLines = nLines+1
            if(numLines != -9999 and nLines>=numLines):
                break
        self.pFile.seek(0,0)
        if (self.pFirstLineIsHeader == True):
            self.pFile.readline()
        return rows
    
    def dict_reader(self,numLines):
        data = self.reader(numLines)
        dataDict = []
        for row in data:
            ##Row is a list of variables.
            ##We zip it to headers to form a dictionary.
            dictRow = dict(zip(self.pHeader,row))
            dataDict.append(dictRow)
        return dataDict




def csvToSqlite(csvfile,sqlitefile,tabName):
    try:
        csv = pycsv_reader(csvfile)
        csv.to_database(tabName,database=sqlitefile)        
        csv.close()
    except Exception as err:
        print_error(err)
        

def list2DToSqlite(headerList1D,dataList2D,tabName,dbFile,tempFile):
    try:
        file = open(tempFile,'w')
        file.write(','.join(headerList1D)+'\n')
        for week in dataList2D:
            week = [str(w) for w in week]
            file.write(','.join(week)+'\n')
        file.close()
        csvToSqlite(tempFile,dbFile,tabName)
        
        ##Cleanup the temporary file.
        if os.path.exists(tempFile):
            os.remove(tempFile)

    except Exception as err:
        print_error(err)
   
def dict_tosqlite(headcolDict,tabName,conn):
    try:
        columns = headcolDict.keys()
        if len(columns) == 0:
            raise Exception ('''Exception: No Valid Column Names Found.''')
        
        column_type = [ isinstance(col,str)  for col in columns]
        if not (all(column_type)) :
            raise Exception ('''Exception: Non string value specified for headers''')

        #Sanity Checks on data.
        number_of_rows = []
        for column in columns:
            number_of_rows.append(len(headcolDict[column]))
            
        if sum(number_of_rows) != number_of_rows[0]*len(number_of_rows):
            raise Exception ('''Exception: All Columns must have same number of rows. ''')

        sql = '''
        drop table if exists ?tabName?;
        CREATE TABLE ?tabName? (
        ?vardefs?
        );
        '''
        sql = sql.replace('?tabName?',tabName)
        vardefs = []
        for vvar in columns:
            vardefs.append('?vvar? TEXT'.replace('?vvar?',vvar))
        vardefs = ','.join(vardefs)
        sql = sql.replace('?vardefs?',vardefs)
        
        ## Insert the data into the table.
        insrt = 'INSERT INTO ?tabName? (?cols?) values (?vals?)'
        insrt = insrt.replace('?tabName?',tabName)
        cols  = ', '.join(columns)
        insrt = insrt.replace('?cols?',cols)
        data_cols = [headcolDict[col] for col in columns]
        data_rows = list(map(list, zip(*data_cols))) #transpose cols to rows.
        inserts = []
        for row in data_rows:
            row = ', '.join(["'"+str(dat)+"'" for dat in row])
            inserts.append(insrt.replace('?vals?',row))
        
        #print (inserts)
        sql = sql+';\n'.join(inserts)
        sql = replace_all(sql,'  ',' ')
        execute(script=sql,conn=conn,verbose=0)
    except Exception as err:
        print_error(err,'euler.dict_tosqlite')
        
        

def create_temp_table(data_dict,tabName,conn=None):
    try:
        if conn is None:
            conn = connection(':memory:')
        dict_tosqlite(data_dict,tabName,conn)
        return conn, tabName
    except Exception as err:
        print_error(err,'euler.create_temp_table')
        if conn != None:
            conn.close()
        raise err
        
        
def drop_temp_table(tabName,conn=None):
        if conn is None:
            conn = connection(':memory:')
        try:
            conn.execute('drop table if exists '+tabName)
        except Exception as err:
            print_error(err,'euler.drop_temp_table')
            raise err

#@# LOGGING System
##Open a logging system.
class logcabin:
    def __init__(self):
        logbook = None
        start_time = None
        start_time_string = None
    
    def open_log(self,fname):
        self.start_time = time.time()
        self.logbook = open(fname,'w')
        self.start_time_string =  datetime.datetime.fromtimestamp(self.start_time).strftime('%Y-%m-%d %H:%M:%S')
        self.logbook.write("--Starting Execution at: "+self.start_time_string+"\n")

    def close_log(self):
        end_time = time.time()
        end_time_string =  datetime.datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S')
        print (self.start_time_string, end_time_string)
        if self.logbook != None and self.start_time != None:
            #tdiff = datetime.datetime.fromtimestamp(end_time-self.start_time).strftime('%H:%M:%S')
            self.logbook.write("\n--Start Time: "+self.start_time_string+", End Time: "+end_time_string+"\n\n")
            self.logbook.write("---Total Duration of Run: ?")
            self.logbook.write("\n\n---++++End Of Execution+++++\n\n")
            self.logbook.close()

    def write_log(self,msg):
        msg = "\n--++++++++++++++++++++++++++++++++++++++++\n"+msg+"\n"
        self.logbook.write(msg)
        
        
#@# Hist1D
def sql_cleanupHist1D(conn,sql,varName,bins):
    sql = '''
    drop table if exists ?histName?;
    DROP TABLE IF EXISTS ?varName?_binned ;
    drop table if exists ?histName?;
    '''
    
def sql_discrtizeVar(tabName,varName,bins,elseVal=None):
    lbins = bins[:-1]
    hbins = bins[1: ]
    sql = 'WHEN ?varName? >= ?lbin? AND ?varName?<?hbin? THEN ?lbin?'
    sql = sql.replace('?varName?',varName)
    sql_bag = ['CASE']
    for lbin,hbin in zip(lbins,hbins):
        sql1 = sql.replace('?lbin?',str(lbin))
        sql1 = sql1.replace('?hbin?',str(hbin))
        sql_bag.append(sql1)
    if elseVal != None:
        sql_bag = sql_bag+['ELSE '+str(elseVal)]
    sql_bag += ['END '+varName]
    return '\n'.join(sql_bag)

    
def sql_mkHist1D(conn,sql,varName,bins,
                 histName,
                 countKey='count(*)',
                 flavor='sqlite'):
    """
    Generates a 1D histogram using data selected by
    sql query. The parameter bins is of array type.
    """
    #flavor = 'tdata' 
    #First we need to create an emply histogram in SQL
    sql1='''
    --?sqlsas?DROP TABLE ?ifexists? ?histName?;
    --?sqlsas?DROP TABLE ?ifexists? ?varName?_binned ;
    --?sqlsas?DROP TABLE ?ifexists? ?histName?;    
    --?sqlsas?DROP TABLE ?ifexists? ?histName?_bins;
    --?sqlsas?CREATE TABLE ?histName?_bins
    
    --?tdata?CREATE MULTISET VOLATILE TABLE ?histName?_bins 
     (   
      bin  Numeric,
      lbin Numeric,
      hbin Numeric,
      freq Numeric  
    );
   --?tdata?PRIMARY INDEX(LBIN,HBIN)
   --?tdata?ON COMMIT PRESERVE ROWS;

    ?sqlInsert?;
    
    --?tdata?COLLECT STATS ON HMAXADJST_IL_BINS index(lbin,hbin);
    --?sqlsas?CREATE TABLE ?varName?_binned AS
    
    --?tdata?CREATE volatile TABLE ?varName?_binned AS (
    SELECT 
     
    bin, ?countKey? as FREQ
    FROM (
    SELECT 
    T1.*,
    T2.bin as bin
    FROM
    (?sql?) as T1
    JOIN
    ?histName?_bins as T2
    ON
    T1.?varName? >= T2.lbin 
    AND T1.?varName? < T2.hbin
    ) T3 GROUP BY bin 
    --?tdata?)
    --?tdata?WITH DATA
    --?tdata?ON COMMIT PRESERVE ROWS    
    
    ;    
    
    --?sqlsas?CREATE TABLE ?histName? AS
    --?tdata?CREATE MULTISET TABLE ssd_pnr.?histName?
    --?tdata?, NO FALLBACK, NO BEFORE JOURNAL, NO AFTER JOURNAL AS (   
    SELECT 
    bin,lbin,hbin,
    CASE WHEN FREQ IS NULL THEN 0 ELSE FREQ END as FREQ
    FROM(
    SELECT 
    T1.bin as bin,
    T1.lbin as lbin,
    T1.hbin as hbin,
    T2.FREQ as freq
    FROM
    ?histName?_bins as T1
    LEFT JOIN
    ?varName?_binned as T2
    ON
    T1.bin = T2.bin
    )T3
    --?tdata?)
    --?tdata?WITH DATA
    --?tdata?PRIMARY INDEX(BIN)
    --?tdata?ON COMMIT PRESERVE ROWS  
    ;
    
    DROP TABLE ?varName?_binned ;
    DROP TABLE ?histName?_bins;
    '''
    
    if flavor != 'sqlite':
        sql1 = sql1.replace('?ifexists?', ' ')
    else : sql1 = sql1.replace('?ifexists?', ' IF EXISTS ')

    if flavor in ('sqlite','sas'):
        flavor = 'sqlsas'
    
    
    sql1 = sql1.replace('--?'+flavor+'?','')
    sql1 = sql1.replace('?histName?',histName)
    sql1 = sql1.replace('?countKey?',countKey)
    #Now add the lbin, hbin boundaries to the table.
    sql2 = 'INSERT INTO ?histName?_bins VALUES(?bin?,?lbin?,?hbin?,0.0)'
    bIndex = 0
    bbin = 1
    inserts = []
    for bIndex in range(0,len(bins)-1):
        lbin = bins[bIndex]
        hbin = bins[bIndex+1]
        
        sqlInsert = sql2.replace('?lbin?',str(lbin))
        sqlInsert = sqlInsert.replace('?bin?',str(bbin))
        sqlInsert = sqlInsert.replace('?hbin?',str(hbin))
        sqlInsert = sqlInsert.replace('?histName?', histName)
        bbin = bbin+1
        inserts.append(sqlInsert)
        
    sql1 = sql1.replace('?sqlInsert?',';'.join(inserts)
                       )
    sql1 = sql1.replace('?sql?',sql)
    sql1 = sql1.replace('?histName?',histName)
    sql1 = sql1.replace('?varName?',varName)
    
    
    sql_list = sql1.split('\n')
    sql_l2 = []
    for line in sql_list:
        line = line.strip()
        if len(line)==0: continue
        if ''.join(line[:3])=='--?':continue
        sql_l2.append(line)
    sql1 = '\n'.join(sql_l2)
    sql1 = sql1.replace(';',';\n')
    
    #Now we have a script.. run it one by one    
    if conn != None: execute(sql1, conn, verbose=False) 
    #conn.cursor().executescript(sql1)
    return sql1


def sql_saveHist1D(conn,histname,bins,freqs):
    """
    Saves 1D histogram in RAM to the sqlite system
    """
    lbins = bins[:-1]
    hbins = bins[0:]
    
    sql='''
        DROP TABLE IF EXISTS ?histname?;
        CREATE TABLE ?histname? (
            lbin numeric,
            hbin numeric,
            freq numeric        
        );
        ?insert?;
    '''
    inserts = []
    for lbin,hbin,freq in zip(lbins,hbins,freqs):
        sql1 = 'INSERT INTO ?histname? VALUES (?lbin?,?hbin?,?freq?)'
        sql1 = sql1.replace('?lbin?',str(lbin))
        sql1 = sql1.replace('?hbin?',str(hbin))
        sql1 = sql1.replace('?freq?',str(freq))
        inserts.append(sql1)
        
    insert = ';\n'.join(inserts)
    sql = sql.replace('?insert?',insert)
    sql = sql.replace('?histname?',histname)
    if conn != None: 
        conn.cursor().executescript(sql1)
        conn.commit()
    return sql.replace('  ','')

def csv_saveHist1D(fileName,histName,bins,freqs):
    
    f = open(fileName,'w')
    lbins = bins[:-1]
    hbins = bins[0:]
    for lbin,hbin,freq in zip(lbins,hbins,freqs):
        f.write(','.join([lbin,hbin,freq])+'\n')
    f.close()
        

def csv_rdHist1D(filename):
    '''
    Reads a 1D histogram from fixed format CSV file.
    Sno, lbins, hbins, freq \n
    '''
    lbins = []
    hbins = []
    freqs = []
    file = open(filename)
    header = file.readline()
    for line in file:
        line = line.replace('\n','').split(',')
        lbins.append(float(line[1]))
        hbins.append(float(line[2]))
        freqs.append(float(line[3]))
    
    bincenters = [lbin+0.5*(hbin-lbin) for lbin,hbin in zip(lbins,hbins)]
    bins = lbins+[hbins[-1]]
    return bincenters, bins, freqs


def sql_rdHist1D(conn,histName,outfile=None):
    '''
    Reads a 1D histogram from SQLITE system.
    '''
    query='select 1.0*lbin,1.0*hbin,1.0*freq from ?outfile??histName? order by 1*lbin'
    query = query.replace('?histName?',histName)
    
    if outfile==None: outfile=''
    else: outfile = outfile+'.'
    
    query  = query.replace('?outfile?',outfile)
    
    data =  get_ncols_bysql(query,3,conn)
    lbins = data[0]
    hbins = data[1]
    freqs  = data[2]
    bincenters = [lbin+0.5*(hbin-lbin) for lbin,hbin in zip(lbins,hbins)]
    bins = lbins+[hbins[-1]]    
    return bincenters.copy(), bins.copy(), freqs

def sql_dropHist1D(conn,histName):
    conn.cursor().execute('DROP TABLE IF EXISTS '+histName)  
    conn.commit()
    
##SOME STATISTICS
def cumsum(fList):
    cum_freq = []
    cfq = 0
    for freq in fList:
        cfq =  cfq + freq
        cum_freq.append(cfq)
    return cum_freq
    
    
def cum_Hist1D(bincenters,bins,freqs):
    cum_freq = cumsum(freqs)
    return bincenters.copy(),bins.copy(),cum_freq
        
    
def mean_Hist1D(bincenters,bins,freqs):
    sum_freq = 0.
    sum_bc = 0.
    if bincenters == None:
        bincenters = [b1+0.5*(b2-b1) for b1,b2 in zip(bins[:-1],bins[1:])]
    for bc,freq in zip(bincenters,freqs):
        sum_freq = sum_freq+freq
        sum_bc = sum_bc+bc*freq
    
    if sum_freq == 0 : return 0.
    else: return sum_bc/sum_freq
    
def var_Hist1D(bincenters,bins,freqs):
    
    sumX = 0
    sum_freq = 0
    if bincenters == None:
        bincenters = [b1+0.5*(b2-b1) for b1,b2 in zip(bins[:-1],bins[1:])]
        #print ('bincenters: ',bincenters)
    mu = mean_Hist1D(bincenters,bins,freqs)
    for bc, freq in zip(bincenters,freqs):
        sumX = sumX + freq*(bc-mu)**2
        sum_freq = sum_freq+freq
    if sum_freq == 0 : return 0
    else: return sumX/sum_freq

def stddev_Hist1D(bincenters,bins,freqs):
    return math.sqrt(var_Hist1D(bincenters, bins, freqs))


def skew_Hist1D(bincenters,bins,freqs):
    mu     = mean_Hist1D(bincenters, bins, freqs)
    stddev = stddev_Hist1D(bincenters, bins, freqs)
    sumX =0.
    n = 0.
    for bc,f in zip(bincenters,freqs):
        sumX = sumX+f*(bc-mu)*(bc-mu)*(bc-mu)
        n = n+f
    sumX = sumX/(n*stddev*stddev*stddev)

    return sumX
    
def max_Hist1D(bins,freqs):
    lbins = bins[:-1]
    hbins = bins[0:]
    lbin_max = lbins[0]
    hbin_max = hbins[0]
    freq_max = freqs[0]
    for lb,hb,f in zip(bins,freqs):
        if f> freq_max :
            freq_max = f
            lbin_max = lb
            hbin_max = hb
     
    return lbin_max,hbin_max,freq_max
    
    
def min_Hist1D(bins,freqs):
    lbins = bins[:-1]
    hbins = bins[0:]
    lbin_min = lbins[0]
    hbin_min = hbins[0]
    freq_min = freqs[0]
    for lb,hb,f in zip(bins,freqs):
        if f < freq_min :
            freq_min = f
            lbin_min = lb
            hbin_min = hb
     
    return lbin_min,hbin_min,freq_min    
    
def integral_Hist1D(x1,x2,bins,freqs):
    lbins = bins[:-1]
    hbins = bins[0:]
    if x1 == None : x1 = lbins[0]
    if x2 == None : x2 = lbins[-1]
    sumX = 0
    for lbin,hbin,f in zip(lbins,hbins,freqs):
        width = hbin-lbin
        sumX = sumX+width*f
    return sumX 

def scale_Hist1D(bincenters,bins,freqs,s):
    scal_f = []
    for f in freqs:
        scal_f.append(s*f)
    return bincenters.copy(),bins.copy(),scal_f

## Some functions involving two histograms.

def add_Hist1D(bincenters,bins,freq1,freq2,w1=1.,w2=1.):
    tot_f = []
    for b,f1,f2 in zip(bins,freq1,freq2):
        tot_f.append(w1*f1+w2*f2)        
    return bincenters.copy(),bins.copy(),tot_f


def divide_Hist1D(bincenters,bins,freq1,freq2):
    div_f = []
    for b,f1,f2 in zip(bins,freq1,freq2):
        quot = 0.
        if f2>0:
            quot = (1.0*f1)/f2
        div_f.append(quot)
    return bincenters.copy(),bins.copy(),div_f
        
   
def chi2_unscaledHist1D(binccenters,bins,freq1,freq2):
    sum2 = 0
    ndf = 0
    for f1,f2 in zip(freq1,freq2):
        sum2 = sum2+ ((f2-f1)*(f1-f1))/(f1)
        ndf  = ndf+1
    return sum2,ndf
    


#@# Hist1S
def sql_mkHist1S(conn,sql,varName,binns,histName,countKey='*',outfile=None):
    """
    Generates a 1D histogram using data selected by
    sql query. The parameter bins is of array type.
    """ 
    #First we need to create an emply histogram in SQL
    sql1='''
    DROP TABLE  if exists ?histName?;
    DROP TABLE  if exists ?histName?_binned;
    DROP TABLE  if exists ?outfile??histName?_Hist1S;
    
    CREATE TABLE ?histName?(
      bin  Text,
      binId Numeric,
      freq Numeric
    );
        
    ?sqlInsert?;

      CREATE TABLE ?histName?_binned AS
      SELECT 
      bin, COUNT(*) as FREQ
      FROM (
      SELECT 
       T1.*,
       T2.bin as bin
      FROM
       (?sql?) as T1
       JOIN
       ?histName? as T2
       ON
       T1.?varName? = T2.bin 
    ) T3 GROUP BY bin 
    
    ;
    
    CREATE TABLE ?outfile??histName?_Hist1S AS
    SELECT 
     bin,
     binId,
     CASE WHEN FREQ IS NULL THEN 0 ELSE FREQ END as FREQ
    FROM(
    SELECT 
     T1.bin as bin,
     T1.binId as binId,
     T2.FREQ as freq
    FROM
     ?histName? as T1
     LEFT JOIN
     ?histName?_binned as T2
     ON
      T1.bin = T2.bin
     )T3
      
    ;
    
    DROP TABLE ?histName?_binned ;
    DROP TABLE ?histName?;
    '''
    sql1 = sql1.replace('?histName?',histName)
    if outfile == None: outfile = ''
    else: outfile = ''.join([outfile,'.'])

    sql1 = sql1.replace('?outfile?',outfile)
    
    #Now add the lbin, hbin boundaries to the table.
    sql2 = "INSERT INTO ?histName? VALUES('?bin?',?id?,0.0)"
    bIndex = 0

    inserts = []
    #print (binns)
    i=0
    for binn in binns:
        i = i+1
        sqlInsert = sql2.replace('?bin?',str(binn))
        sqlInsert = sqlInsert.replace('?id?',str(i))
        sqlInsert = sqlInsert.replace('?histName?', histName)
        inserts.append(sqlInsert)
        
    sql1 = sql1.replace('?sqlInsert?',';\n'.join(inserts)
                       )
    sql1 = sql1.replace('?sql?',sql)
    sql1 = sql1.replace('?histName?',histName)
    sql1 = sql1.replace('?varName?',varName)
    
    #Now we have a script.. run it one by one 
    #print(sql1)   
    if conn != None: 
        conn.cursor().executescript(sql1)
        
    return sql1


def sql_rdHist1S(conn,histName,orderby=None,outfile=None):
    '''
    Reads a 1D histogram from SQLITE system.
    '''
    if orderby == None: orderby = 'bin'
    histName  = histName+'_Hist1S'
    query='select bin,binId,freq from ?outfile??histName? order by ?orderby?'
    query = query.replace('?histName?',histName)
    query = query.replace('?orderby?',orderby)
    
    if outfile==None: outfile=''
    else: outfile = outfile+'.'
    
    query  = query.replace('?outfile?',outfile)

    
    data = get_ncols_bysql(query,3,conn)
    binLabels = data[0]
    binIds = data[1]
    freqs  = data[2]
    return  binLabels, binIds,freqs

###------------------------------------------------
##SOME FUNCTIONS FOR HIST2D that can be later developed.
def sql_mkHist2D(conn,var1,var2,where,tabName):
    sql='''
    drop table if exists hist_?var1?_?var2?;
    create table hist_?var1?_?var2? as
    SELECT var1,var2,
    case when freq is NULL then 0 else freq end freq
    FROM
    (
    SELECT 
    T3.var1 as var1,
    T3.var2 as var2,
    T4.freq as freq
    FROM
    (
    select distinct T1.var1 var1, T2.var2 var2 from
     (select distinct ?var1? var1 from ?tabName?) T1
     join
     (select distinct ?var2? var2 from ?tabName?) T2
     on 1=1 
    ) T3
    LEFT JOIN
    (
    SELECT ?var1? var1, ?var2? var2, count(*) freq
    from ?tabName? ?where? group by ?var1?,?var2?
    ) T4
    ON
    T3.var1 = T4.var1
    AND T3.var2 = T4.var2
    ) T5 order by var1,var2;
    '''
    sql = sql.replace("?tabName?",tabName)
    sql = sql.replace('?var1?',var1)
    sql = sql.replace('?var2?',var2)
    sql = sql.replace('?where?',where)
    #print (sql)
    conn.cursor().executescript(sql)
    conn.commit()
    
    sql = 'select distinct var1 from hist_?var1?_?var2? order by var1 asc'
    sql = sql.replace('?var1?',var1)
    sql = sql.replace('?var2?',var2)
    x = get_ncols_bysql(sql,1,conn)[0]
    sql = 'select distinct var2 from hist_?var1?_?var2? order by var2 asc'
    sql = sql.replace('?var1?',var1)
    sql = sql.replace('?var2?',var2)   
    y = get_ncols_bysql(sql,1,conn)[0]
    sql = 'select freq from hist_?var1?_?var2? order by var1 asc,var2 asc'
    sql = sql.replace('?var1?',var1)
    sql = sql.replace('?var2?',var2)   
    z = get_ncols_bysql(sql,1,conn)[0]
    
    #print (x)
    z = np.array(z).reshape(len(x),len(y)).tolist()
    '''
    fig = plt.figure()
    ax = fig.add_subplot(1,1,1)
    ax.set_xticks(x)
    ax.set_yticks(y)
    ax.imshow(z,cmap='hot')
    '''
    return x,y,z


def Chi2_Hist2D_2(conn,hist):
    sql = '''
    drop table if exists ?hist?_prob1;
    create table ?hist?_prob1 as
    SELECT 
    TA.VAR1 as var1,
    TB.VAR2 as var2,
    TA.n1*TB.n2 as ni_nj
     
    FROM
    (
    select 
    t.var1 as var1, 
    sum(freq)  as n1
    from 
    ?hist? as t
    group by var1
    ) TA
    join
    (
    select 
    t.var2 as var2, 
    sum(freq)  as n2
    from 
    ?hist? as t
    group by var2
    ) TB
    on
    1=1
    ;
      
    UPDATE ?hist?_prob1 set ni_nj= (1.0*ni_nj)/(SELECT sum(freq) FROM ?hist?);
    '''
    sql = sql.replace('?hist?',hist)
    print (sql)
    
    
def Chi2_Hist2D(conn,hist):
    sql='''
    
    drop table if exists ?hist?_prob1;
    create table ?hist?_prob1 as
    select 
    t1.var1 as var1, 
    t1.var2 as var2,
    case
     when p_ij>0 then (1.0*freq)/p_ij else 0
    end p_ij
    from
    (
     select 
     var1, 
     var2, 
     freq, 
     p_ij
     from ?hist? tA 
     join (select sum(freq) as p_ij from ?hist?) tB
     on 1=1
    ) t1
     
    ;

    drop table if exists ?hist?_prob2;
    create table ?hist?_prob2 as
    select 
    t1.var1 as var1,
    t2.var2 as var2,
    t1.p_i as p_i,
    t2.p_j as p_j
    FROM
    (
    select 
    var1, 
   
    sum(p_ij) as p_i
    from 
    ?hist?_prob1
    group by var1
    ) t1
    join
    (
    select 
    var2, 
   
    sum(p_ij) as p_j
    from 
    ?hist?_prob1
    group by var2
    ) t2
    on
    1=1
     
    ;
    
    drop table if exists ?hist?_prob3;
    create table ?hist?_prob3 as
    select var1,var2,
    case 
     when p_i*p_j > 0 then ((p_ij - p_i*p_j)*(p_ij - p_i*p_j))/ (p_i*p_j) else 0
    end chi2
    from(
    select 
    t1.var1 as var1,
    t2.var2 as var2,
    t1.p_ij as p_ij,
    t2.p_i as p_i,
    t2.p_j as p_j
    from
     ?hist?_prob1 as t1
    join
     ?hist?_prob2 as t2
    on
     1=1
     and t1.var1 = t2.var1
     and t1.var2 = t2.var2     
    ) B
    ;     
    
    drop table if exists ?hist?_prob1;
    drop table if exists ?hist?_prob2;
    
    drop table if exists ?hist?_chi2;
    create table ?hist?_chi2 as
    select 
     case
      when dfrdm>0 then sqrt((1.0*chi2)/(dfrdm)) else 9999
     end chi2
    from (
    select 
    min(count(distinct var1)-1,count( distinct var2)-1) dfrdm, 
    sum(chi2) chi2
    from  ?hist?_prob3)t1;
    '''
   
    sql = sql.replace('?hist?',hist)
    print(sql)
    conn.cursor().executescript(sql)

#@# MATPLOTLIB Utilities
from matplotlib import pyplot as plt

def TCanvas(title="",xlabel="",ylabel=""):
    fig = plt.figure()
    ax  = fig.add_subplot(1,1,1)
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    return ax,fig

def add_hist(ax, bins,freq):
    return ax.hist(bins[:-1],bins,weights=freq)
    
import random

def add_vals_ontop(ax,bins,freq,threshold=0,showprecents=False,
                   suppress_bins = []
                   ):
    freq_p = [round((100.*v)/sum(freq),0) for v in freq]
    
    #If binspression is list is present.
    if len(suppress_bins) > 0:
        for binn in suppress_bins:
            freq_p[binn] = threshold -1 
            freq[binn] = threshold -1
 
    for j in range(0,len(bins)-1):
        where = random.uniform(0.1,0.7)
        xcenter = bins[j]+where*(bins[j+1]-bins[j])
        f = freq[j]
        p = int(freq_p[j])
        
        
        if p>threshold and showprecents==True:
            ax.text(xcenter,f,str(p)+'%',ha='center',va='bottom',rotation=90)
        else:
            if (f>threshold):
                ax.text(xcenter,f,str(int(f)),ha='center',va='bottom',fontsize=8,rotation=45)



def add_text_ontop(ax,bins,freq,txtlist):
    
    for j in range(0,len(bins)-1):
        where = random.uniform(0.3,0.7)
        xcenter = bins[j]+where*(bins[j+1]-bins[j])
        
        f = freq[j]
        p = txtlist[j]
        
        ax.text(xcenter,f,str(p),
                ha='center',va='bottom',rotation=45,fontsize=8)
  
        
            
#@# Time Series
def first_diffs(llist):
    return [llist[i+1]-llist[i] for i in range(0,len(llist)-1)]


def list_diff(llist1,llist2):
    ll = [ll1i-ll2i for ll1i,ll2i in zip(llist1,llist2)]
    return ll

#Some list manipulations.
def normalize(llist, value):
    
    ssum = sum(llist)
    if sum == 0 : return 0.
    else: return [(float(x)*value)/ssum for x in llist]

def slope_line(slope,intercept,x0,x1):
    steps = (x1-x0)*100
    x = []
    y = []
    for i in range(0,steps+1):
        xi = x0+i*(.01)
        yi = slope*xi + intercept
        x.append(xi)
        y.append(yi)
    return x,y
    
    
def reorder_key_vals(keys_list,values_list,key_order_list, default_value=0):
    
    for key,val in zip(keys_list,values_list):
        if not isinstance(key,str):
            print ('''
                   Warning: Non String Key Encountered
                   To Do:   Throw An Exception Here.       
                   ''')
            return
        
        if not (isinstance(val,int) or isinstance(val,float)):
            print ('''
                   Warning: Non Numberic Value Encountered. 
                   To Do:  Anil throw an exception here.
                   ''')
            return
        
    key_value_dict = dict(zip(keys_list,values_list))
    #Let us find if any of the keys in missing from order list.
    key_missing = [key for key in keys_list if key not in key_order_list]
    #Append missing keys to the order list
    key_order_list_copy = key_order_list.copy()+key_missing
    
    #Now get the parallel lists.
    values_list_new = []
    for key in key_order_list_copy :
        if key in keys_list:
            values_list_new.append(key_value_dict[key])
        else:
            values_list_new.append(default_value)
    
    return key_order_list_copy, values_list_new

"""

def pullData(tabName_src,
             tabName_dest, 
             dest_database,
             verbose=False):
    sql = '''
    SELECT *
    FROM 
    ?histName_src? as t1
    --order by bin asc
    '''
    sql = sql.replace('?histName_src?',histName_src)
    
    if verbose==True: print(sql)
    res = cursor.execute(sql)
    #mydata = cnt.fetchall()
    columns = ','.join([col[0] for col in cursor.description])
    file = open(saveDir+histName_dest+'.csv','w')
    file.write(columns+'\n')
    lines_counter = 0
    while True: 
        row = cursor.fetchone()
        if row == None: break
        if (lines_counter% 100000==0): print (lines_counter)
        lines_counter = lines_counter+1
        row = ','.join([str(col) for col in row])+'\n'
        #print (row)
        file.write(row)
    file.close()
    pcsv = eu.pycsv_reader(saveDir+histName_dest+'.csv')
    pcsv.to_database(histName_dest,database)
    
    import os
    os.remove(saveDir+histName_dest+'.csv')

    print ('Done!')
    
"""


"""

## TERADATA.
import teradata

def Tconnection(username='U373001',password='Ershjuly2018'):
    udaExec = teradata.UdaExec (appName="dpull", version="1.0",logConsole=False)
    session = udaExec.connect(method="ODBC",dsn="tdata", username=username, password=password);
    cursor =  session.cursor()
    return session,cursor

def Texecute(script,conn,verbose=1,saftey=True):
    if saftey == True: return
    for sql in script.split(';'):
        if sql.strip() == "":
            pass
        else:       
            try:
                if (llog != None): llog.write_log(sql)
                if verbose > 0: print (sql)
                conn.execute(sql)
                #conn.commit()
            except Exception as err:
                print_error(err,'euler.execute')
                print ("NOT HAPPY WITH: \n"+sql)
                
    return 0


def Tget_bysql(sql,tconn,dbase,tabDest,verbose=False,saftey=True,csvFormat=False):
    if saftey == True: return
   
    cursor = tconn
    database  = dbase
    histName_dest = tabDest 
    
    #sql = sql.replace('?histName_src?',histName_src)
    
    if verbose==True: print(sql)
    res = cursor.execute(sql)
    #mydata = cnt.fetchall()
    columns = ','.join([col[0] for col in cursor.description])
    file = open(histName_dest+'.csv','w')
    file.write(columns+'\n')
    lines_counter = 0
    while True: 
        row = cursor.fetchone()
        if row == None: break
        if (lines_counter% 100000==0): print (lines_counter)
        lines_counter = lines_counter+1
        row = ','.join([str(col) for col in row])+'\n'
        #print (row)
        file.write(row)
    file.close()
    if csvFormat == False:
        pcsv = pycsv_reader(histName_dest+'.csv')
        pcsv.to_database(histName_dest,database)
        
        import os
        os.remove(histName_dest+'.csv')

    print ('Done!')
"""    



"""
import psycopg2

configuration = { 'dbname': 'iot', 
                     'user':'SINGA72',
                     'pwd':'ChangeMe2',
                     'host':'whr-gis-data-services.ctltmdu79zqh.us-east-1.redshift.amazonaws.com',
                     'port':'5439'
                   }



def create_conn(*args,**kwargs):
   config = kwargs['config']
   try:
       conn=psycopg2.connect(dbname=config['dbname'], host=config['host'], port=config['port'], user=config['user'], password=config['pwd'])
       return conn
   except Exception as err:
       #print(err.message)
       print_error(err,'create_conn')
  

def run_sql(sql,readonly="0"):
   conn = create_conn(config=configuration)
   cur = conn.cursor()
   try:
       print("-->Started at: " + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') )
       print(sql);
       if readonly=="1":
           conn.set_session(readonly=True, autocommit=True)
       cur.execute(sql)
       if readonly!="1":
           conn.commit()
       print("-->Ended at: " + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') )
   except Exception as err:
       if readonly!="1":
           conn.rollback()
       print(err.message)
   cur.close()
   conn.close()
"""













