# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import math, re, sys, calendar, os, copy, time
import pandas as pd
import numpy as np
from datetime import datetime, date

ENCODING = 'utf-8-sig'
out_path = './output/'
column = ['code', 'frequency', 'from', 'to', 'description', 'base currency', 'quote currency', 'source', 'attribute']

"""
tStart = time.time()
print('Reading file: QNIA_key'+NAME1+', Time: ', int(time.time() - tStart),'s'+'\n')
KEY_DATA_t = readExcelFile(data_path+'QNIA_key'+NAME1+'.xlsx', header_ = 0, acceptNoFile=False, index_col_=0, sheet_name_='QNIA_key')
print('Reading file: QNIA_key'+NAME2+', Time: ', int(time.time() - tStart),'s'+'\n')
df_key = readExcelFile(data_path+'QNIA_key'+NAME2+'.xlsx', header_ = 0, acceptNoFile=False, index_col_=0, sheet_name_='QNIA_key')
#print('Reading file: MEI_database, Time: ', int(time.time() - tStart),'s'+'\n')
#DATA_BASE_t = readExcelFile(data_path+'MEI_database.xlsx', header_ = 0, index_col_=0, acceptNoFile=False)
"""
with open('./gerfin.txt','r') as f:
    lines = f.readlines()
for l in range(len(lines)):
    lines[l] = lines[l].replace('\n','')
#print(lines)

frequency = 'DAILY'
gerfin = []
last = ''
countS = 0
for l in lines:
    if not l or l == ' ':
        if last.find('SERIES') < 0 and last.find('DAILY Data') < 0:
            g_t = []
            code = ''
            fromt = ''
            to = ''
            description = ''
            base = ''
            quote = ''
            source = ''
            attribute = ''
            count = 0
    elif l.find('#') >= 0:
        continue
    else:
        if l.find('SERIES') >= 0:
            countS+=1
            loc1 = l.find(':')+1
            loc2 = l.find('.D')+2
            code = l[loc1:loc2]
            g_t.append(code)
        elif l.find('DAILY Data') >= 0:
            g_t.append(frequency)
            loc3 = l.find('from')+5
            loc4 = l.find('to')-2
            loc5 = l.find('to')+3
            loc6 = l.find('2019')+4
            fromt = l[loc3:loc4]
            to = l[loc5:].replace('2019 ','2019')
            g_t.append(fromt)
            g_t.append(to)
        elif l.find('Exchange Rate:') >= 0:
            g_t.append(l)
            loc7 = l.find('- ')+2
            loc8 = l.find('- ', loc7)
            loc9 = l.find('- ', loc7)+2
            loc10 = l.find(':')+2
            loc11 = l.find(' per')
            loc12 = l.find('per')+4
            loc13 = l.find('- ')-1
            source = l[loc7:loc8]
            attribute = l[loc9:]
            base = l[loc12:loc13]
            quote = l[loc10:loc11]
            g_t.append(base)
            g_t.append(quote)
            g_t.append(source)
            g_t.append(attribute)
        else:
            g_t.append(l)
        
        count +=1
    if count >= 3:
        gerfin.append(g_t)
    last = l
#print(gerfin)
"""
for g in gerfin:
    if len(g) > 7:
        print(g)
""" 
print(countS)
ger = pd.DataFrame(gerfin, columns=column)
print(ger)
ger.to_excel(out_path+"gerfin.xlsx", sheet_name='gerfin')



"""
print('Concating file: QNIA_key'+NAME1+', Time: ', int(time.time() - tStart),'s'+'\n')
KEY_DATA_t = pd.concat([KEY_DATA_t, df_key], ignore_index=True)

print('Concating file: MEI_database, Time: ', int(time.time() - tStart),'s'+'\n')
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

print('Time: ', int(time.time() - tStart),'s'+'\n')
KEY_DATA_t = KEY_DATA_t.sort_values(by=['name', 'db_table'], ignore_index=True)
unrepeated = 0
#unrepeated_index = []
for i in range(1, len(KEY_DATA_t)):
    if KEY_DATA_t['name'][i] != KEY_DATA_t['name'][i-1] and KEY_DATA_t['name'][i] != KEY_DATA_t['name'][i+1]:
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

KEY_DATA_t.reset_index(drop=True, inplace=True)
if KEY_DATA_t.iloc[0]['snl'] != 1:
    KEY_DATA_t.loc[0, 'snl'] = 1
for s in range(1,KEY_DATA_t.shape[0]):
    sys.stdout.write("\rSetting new snls: "+str(s))
    sys.stdout.flush()
    KEY_DATA_t.loc[s, 'snl'] = KEY_DATA_t.loc[0, 'snl'] + s
sys.stdout.write("\n")
"""
