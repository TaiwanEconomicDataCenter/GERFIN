# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import math, re, sys, calendar, os, copy, time
import pandas as pd
import numpy as np
from datetime import datetime, date

ENCODING = 'utf-8-sig'
data_path = './output/'
NAME = 'EIKON_'

def ERROR(error_text):
    print('\n\n= ! = '+error_text+'\n\n')
    with open('./ERROR.log','w', encoding=ENCODING) as f:    #用with一次性完成open、close檔案
        f.write(error_text)
    sys.exit()
def readExcelFile(dir, default=pd.DataFrame(), acceptNoFile=True, \
             header_=None,skiprows_=None,index_col_=None,sheet_name_=None):
    try:
        t = pd.read_excel(dir,sheet_name=sheet_name_, header=header_,index_col=index_col_,skiprows=skiprows_)
        #print(t)
        return t
    except FileNotFoundError:
        if acceptNoFile:
            return default
        else:
            ERROR('找不到檔案：'+dir)
    except:
        try: #檔案編碼格式不同
            t = pd.read_excel(dir, header=header_,skiprows=skiprows_,index_col=index_col_,sheet_name=sheet_name_)
            #print(t)
            return t
        except:
            return default  #有檔案但是讀不了:多半是沒有限制式，使skiprow後為空。 一律用預設值

def CONCATE(df_key, DB_D, DB_name_D, Day_list):
    
    DB_TABLE = 'DB_'
    DB_CODE = 'data'
    
    tStart = time.time()
    print('Reading file: '+NAME+'key, Time: ', int(time.time() - tStart),'s'+'\n')
    KEY_DATA_t = readExcelFile(data_path+NAME+'key.xlsx', header_ = 0, acceptNoFile=False, index_col_=0, sheet_name_=NAME+'key')
    try:
        with open(data_path+'database_num.txt','r',encoding=ENCODING) as f:  #用with一次性完成open、close檔案
            database_num = int(f.read().replace('\n', ''))
        DATA_BASE_t = {}
        for i in range(1,database_num+1):
            print('Reading file: '+NAME+'database_'+str(i)+', Time: ', int(time.time() - tStart),'s'+'\n')
            DB_t = readExcelFile(data_path+NAME+'database_'+str(i)+'.xlsx', header_ = 0, index_col_=0, acceptNoFile=False, sheet_name_=None)
            for d in DB_t.keys():
                DATA_BASE_t[d] = DB_t[d]
    except FileNotFoundError:
        DATA_BASE_t = {}
        print('Reading file: '+NAME+'database, Time: ', int(time.time() - tStart),'s'+'\n')
        DB_t = readExcelFile(data_path+NAME+'database.xlsx', header_ = 0, index_col_=0, acceptNoFile=False, sheet_name_=None)
        for d in DB_t.keys():
            DATA_BASE_t[d] = DB_t[d]
    #DATA_BASE_t = readExcelFile(data_path+'EIKON_database.xlsx', header_ = 0, index_col_=0, acceptNoFile=False)
    
    print('Concating file: '+NAME+'key, Time: ', int(time.time() - tStart),'s'+'\n')
    KEY_DATA_t = pd.concat([KEY_DATA_t, df_key], ignore_index=True)
    
    print('Concating file: '+NAME+'database, Time: ', int(time.time() - tStart),'s'+'\n')
    for d in DB_name_D:
        sys.stdout.write("\rConcating sheet: "+str(d))
        sys.stdout.flush()
        if d in DATA_BASE_t.keys():
            DATA_BASE_t[d] = DATA_BASE_t[d].join(DB_D[d])
        else:
            DATA_BASE_t[d] = DB_D[d]
    sys.stdout.write("\n")

    print('Time: ', int(time.time() - tStart),'s'+'\n')
    KEY_DATA_t = KEY_DATA_t.sort_values(by=['name', 'db_table'], ignore_index=True)
    
    key_data = list(KEY_DATA_t['name'])
    repeated = 0
    repeated_index = []
    for i in range(1, len(KEY_DATA_t)):
        if key_data[i] == key_data[i-1]:
            repeated += 1
            repeated_index.append(i-1)
            #print(i,' ',i-1)
            key = KEY_DATA_t.iloc[i-1]    
            DATA_BASE_t[key['db_table']] = DATA_BASE_t[key['db_table']].drop(columns = key['db_code'])
        sys.stdout.write("\r"+str(repeated)+" repeated data key(s) found")
        sys.stdout.flush()
    sys.stdout.write("\n")
    #for i in repeated_index:
    #    print(key_data[i])
    for i in repeated_index:
        sys.stdout.write("\rDropping repeated data key(s): "+str(i))
        sys.stdout.flush()
        KEY_DATA_t = KEY_DATA_t.drop([i])
    sys.stdout.write("\n")
    KEY_DATA_t.reset_index(drop=True, inplace=True)
    #print(KEY_DATA_t)
    if KEY_DATA_t.iloc[0]['snl'] != 1:
        KEY_DATA_t.loc[0, 'snl'] = 1
    for s in range(1,KEY_DATA_t.shape[0]):
        sys.stdout.write("\rSetting new snls: "+str(s))
        sys.stdout.flush()
        KEY_DATA_t.loc[s, 'snl'] = KEY_DATA_t.loc[0, 'snl'] + s
    sys.stdout.write("\n")
    #if repeated > 0:
    print('Setting new files, Time: ', int(time.time() - tStart),'s'+'\n')
    
    start_code_D = 1
    start_table_D = 1
    DB_D_new = {}
    db_table_D_t = pd.DataFrame(index = Day_list, columns = [])
    DB_name_D_new = []
    db_table_new = 0
    db_code_new = 0
    for f in range(KEY_DATA_t.shape[0]):
        sys.stdout.write("\rSetting new keys: "+str(db_table_new)+" "+str(db_code_new))
        sys.stdout.flush()
        if start_code_D >= 200:
            DB_D_new[db_table_D] = db_table_D_t
            DB_name_D_new.append(db_table_D)
            start_table_D += 1
            start_code_D = 1
            db_table_D_t = pd.DataFrame(index = Day_list, columns = [])
        db_table_D = DB_TABLE+'D_'+str(start_table_D).rjust(4,'0')
        db_code_D = DB_CODE+str(start_code_D).rjust(3,'0')
        db_table_D_t[db_code_D] = DATA_BASE_t[KEY_DATA_t.iloc[f]['db_table']][KEY_DATA_t.iloc[f]['db_code']]
        KEY_DATA_t.loc[f, 'db_table'] = db_table_D
        KEY_DATA_t.loc[f, 'db_code'] = db_code_D
        start_code_D += 1
        db_table_new = db_table_D
        db_code_new = db_code_D
        
        if f == KEY_DATA_t.shape[0]-1:
            if db_table_D_t.empty == False:
                DB_D_new[db_table_D] = db_table_D_t
                DB_name_D_new.append(db_table_D)

    sys.stdout.write("\n")
    DB_D = DB_D_new
    DB_name_D = DB_name_D_new

    print('Concating new files: '+NAME+'database, Time: ', int(time.time() - tStart),'s'+'\n')
    DATA_BASE_t = {}
    for d in DB_name_D:
        sys.stdout.write("\rConcating sheet: "+str(d))
        sys.stdout.flush()
        DATA_BASE_t[d] = DB_D[d]
    sys.stdout.write("\n")
    print(KEY_DATA_t)
    print('Time: ', int(time.time() - tStart),'s'+'\n')

    return (KEY_DATA_t, DATA_BASE_t, DB_name_D)
