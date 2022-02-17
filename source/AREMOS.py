# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import math, re, sys, calendar, os, copy, time
import pandas as pd
import numpy as np
from datetime import datetime, date

ENCODING = 'utf-8-sig'
data_path = './output/'
NAME = 'IMFNCB'
#NAME2 = '_new2'

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


"""   
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
"""
tStart = time.time()
print('Reading file: gerfin, Time: ', int(time.time() - tStart),'s'+'\n')
GERFIN = readExcelFile(data_path+'gerfin.xlsx', header_ = 0, acceptNoFile=False, index_col_=0, sheet_name_='gerfin')
print('Reading file: AREMOS data, Time: ', int(time.time() - tStart),'s'+'\n')
AREMOS = readExcelFile(data_path+NAME+'.xlsx', header_ = 0, acceptNoFile=False, sheet_name_='工作表1')
#print('Reading file: MEI_database, Time: ', int(time.time() - tStart),'s'+'\n')
#DATA_BASE_t = readExcelFile(data_path+'MEI_database.xlsx', header_ = 0, index_col_=0, acceptNoFile=False)
print(GERFIN)
print(AREMOS)

nA = AREMOS.shape[1]
nG = GERFIN.shape[0]
base = []
base.append('base currency')
quote = []
quote.append('quote currency')
currency = []
for i in range(1,nA):
    column = AREMOS.columns[i]+'.D'
    for g in range(nG):
        if column == GERFIN['code'][g]:
            base.append(GERFIN['base currency'][g])
            quote.append(GERFIN['quote currency'][g])
            break
currency.append(quote)
currency.append(base)
description = pd.DataFrame(currency, columns = list(AREMOS.columns))

print('Concating file: AREMOS data, Time: ', int(time.time() - tStart),'s'+'\n')
AREMOS = pd.concat([description, AREMOS], ignore_index=True)
for d in range(AREMOS.shape[0]):
    AREMOS.loc[d, 'DATE'] = str(AREMOS.loc[d, 'DATE']).replace(' 00:00:00','')
print(AREMOS)

AREMOS.to_excel(data_path+NAME+"_new.xlsx", sheet_name=NAME)
"""
print('Time: ', int(time.time() - tStart),'s'+'\n')
GERFIN = GERFIN.sort_values(by=['name', 'db_table'], ignore_index=True)
unrepeated = 0
#unrepeated_index = []
for i in range(1, len(GERFIN)):
    if GERFIN['name'][i] != GERFIN['name'][i-1] and GERFIN['name'][i] != GERFIN['name'][i+1]:
        print(list(GERFIN.iloc[i]),'\n')
        unrepeated += 1
        #repeated_index.append(i)
        #print(GERFIN['name'][i],' ',GERFIN['name'][i-1])
        #key = GERFIN.iloc[i]
        #DATA_BASE_t[key['db_table']] = DATA_BASE_t[key['db_table']].drop(columns = key['db_code'])
        #unrepeated_index.append(i)
        
    #sys.stdout.write("\r"+str(repeated)+" repeated data key(s) found")
    #sys.stdout.flush()
#sys.stdout.write("\n")
print('unrepeated: ', unrepeated)
"""