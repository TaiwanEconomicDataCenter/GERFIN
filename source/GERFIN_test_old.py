# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import math, re, sys, calendar, os, copy, time
import pandas as pd
import numpy as np
from datetime import datetime, date

ENCODING = 'utf-8-sig'
data_path = './output/'
NAME = 'EIKON'
NAME1 = ''
NAME2 = '_old'

def ERROR(error_text):
    print('\n\n= ! = '+error_text+'\n\n')
    with open('./ERROR.log','w', encoding=ENCODING) as f:    #用with一次性完成open、close檔案
        f.write(error_text)
    sys.exit()
def readExcelFile(dir, default=pd.DataFrame(), acceptNoFile=True, \
             header_=None,skiprows_=None,index_col_=None,sheet_name_=None):
    try:
        t = pd.read_excel(dir, header=header_,skiprows=skiprows_,index_col=index_col_,sheet_name=sheet_name_)
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

#def CONCATE(df_key, DB_A, DB_Q, DB_M, DB_name_A, DB_name_Q, DB_name_M):
    
DB_TABLE = 'DB_'
DB_CODE = 'data'
this_year = datetime.now().year + 1
Year_list = [tmp for tmp in range(1947,this_year)]
Quarter_list = []
for q in range(1947,this_year):
    for r in range(1,5):
        Quarter_list.append(str(q)+'-Q'+str(r))
Month_list = []
for y in range(1947,this_year):
    for m in range(1,13):
        Month_list.append(str(y)+'-'+str(m).rjust(2,'0'))

tStart = time.time()
print('Reading file: '+NAME+'_key'+NAME1+', Time: ', int(time.time() - tStart),'s'+'\n')
KEY_DATA_t = readExcelFile(data_path+NAME+'_key'+NAME1+'.xlsx', header_ = 0, acceptNoFile=False, index_col_=0, sheet_name_=NAME+'_key')
print('Reading file: '+NAME+'_key'+NAME2+', Time: ', int(time.time() - tStart),'s'+'\n')
df_key = readExcelFile(data_path+NAME+'_key'+NAME2+'.xlsx', header_ = 0, acceptNoFile=False, index_col_=0, sheet_name_=NAME+'_key')
#print('Reading file: EIKON_database, Time: ', int(time.time() - tStart),'s'+'\n')
#DATA_BASE_t = readExcelFile(data_path+'EIKON_database.xlsx', header_ = 0, index_col_=0, acceptNoFile=False)

print('Concating file: '+NAME+'_key'+NAME1+', Time: ', int(time.time() - tStart),'s'+'\n')
KEY_DATA_t = pd.concat([KEY_DATA_t, df_key], ignore_index=True)
"""
print('Concating file: EIKON_database, Time: ', int(time.time() - tStart),'s'+'\n')
for d in DB_name_A:
    sys.stdout.write("\rConcating sheet: "+str(d))
    sys.stdout.flush()
    if d in DATA_BASE_t.keys():
        DATA_BASE_t[d] = DATA_BASE_t[d].join(DB_A[d])
    else:
        DATA_BASE_t[d] = DB_A[d]
sys.stdout.write("\n")
for d in DB_name_Q:
    sys.stdout.write("\rConcating sheet: "+str(d))
    sys.stdout.flush()
    if d in DATA_BASE_t.keys():
        DATA_BASE_t[d] = DATA_BASE_t[d].join(DB_Q[d])
    else:
        DATA_BASE_t[d] = DB_Q[d]
sys.stdout.write("\n")
for d in DB_name_M:
    sys.stdout.write("\rConcating sheet: "+str(d))
    sys.stdout.flush()
    if d in DATA_BASE_t.keys():
        DATA_BASE_t[d] = DATA_BASE_t[d].join(DB_M[d])
    else:
        DATA_BASE_t[d] = DB_M[d]
sys.stdout.write("\n")
"""
print('Time: ', int(time.time() - tStart),'s'+'\n')
KEY_DATA_t = KEY_DATA_t.sort_values(by=['name', 'db_table'], ignore_index=True)
unrepeated = 0
#unrepeated_index = []
for i in range(1, len(KEY_DATA_t)):
    if KEY_DATA_t['name'][i] != KEY_DATA_t['name'][i-1] and KEY_DATA_t['name'][i] != KEY_DATA_t['name'][i+1]:
        #if str(KEY_DATA_t['last'][i]) >= '2010':
        print(list(KEY_DATA_t.iloc[i]),'\n')
        unrepeated += 1
        #repeated_index.append(i)
        #print(KEY_DATA_t['name'][i],' ',KEY_DATA_t['name'][i-1])
        #key = KEY_DATA_t.iloc[i]
        #DATA_BASE_t[key['db_table']] = DATA_BASE_t[key['db_table']].drop(columns = key['db_code'])
        #unrepeated_index.append(i)
        
    #sys.stdout.write("\r"+str(repeated)+" repeated data key(s) found")
    #sys.stdout.flush()
#sys.stdout.write("\n")
print('unrepeated: ', unrepeated)
#for i in unrepeated_index:
    #sys.stdout.write("\rDropping repeated data key(s): "+str(i))
    #sys.stdout.flush()
    #KEY_DATA_t = KEY_DATA_t.drop([i])
#sys.stdout.write("\n")
"""
KEY_DATA_t.reset_index(drop=True, inplace=True)
if KEY_DATA_t.iloc[0]['snl'] != 1:
    KEY_DATA_t.loc[0, 'snl'] = 1
for s in range(1,KEY_DATA_t.shape[0]):
    sys.stdout.write("\rSetting new snls: "+str(s))
    sys.stdout.flush()
    KEY_DATA_t.loc[s, 'snl'] = KEY_DATA_t.loc[0, 'snl'] + s
sys.stdout.write("\n")
"""