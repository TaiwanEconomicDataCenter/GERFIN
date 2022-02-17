# !/usr/bin/env python3
# -*- coding: utf-8 -*-
# pylint: disable=E1101
import math, re, sys, calendar, os, copy, time, logging
import pandas as pd
import numpy as np
from datetime import datetime, date
#from cif_new import createDataFrameFromOECD
import GERFIN_concat as CCT
from GERFIN_concat import ERROR, MERGE, NEW_KEYS, CONCATE, UPDATE, readFile, readExcelFile
import GERFIN_test as test
from GERFIN_test import GERFIN_identity
FORMAT = '%(asctime)s %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT, handlers=[logging.FileHandler("LOG.log", 'w', CCT.ENCODING)], datefmt='%Y-%m-%d %I:%M:%S %p')

ENCODING = CCT.ENCODING

start_year = 1957
NAME = 'EIKON_'
data_path = './data/'
out_path = "./output/"
databank = NAME[:-1]
find_unknown = False
main_suf = '?'
merge_suf = '?'
dealing_start_year = 1957
start_year = 1957
maximum = 9
merging = bool(int(input('Merging data file (1/0): ')))
updating = bool(int(input('Updating TOT file (1/0): ')))
if merging and updating:
    ERROR('Cannot do merging and updating at the same time.')
elif merging or updating:
    merge_suf = input('Be Merged(Original) data suffix: ')
    main_suf = input('Main(Updated) data suffix: ')
else:
    find_unknown = bool(int(input('Check if new items exist (1/0): ')))
    if find_unknown == False:
        maximum = 100
        dealing_start_year = int(input("Dealing with data from year: "))
        start_year = dealing_start_year-2
excel_suffix = CCT.excel_suffix
with open(out_path+NAME+'TOT_name.txt','r',encoding='ANSI') as f:
    DF_suffix = f.read()
main_file = readExcelFile(out_path+NAME+'key'+main_suf+'.xlsx', header_ = 0, index_col_=0, sheet_name_=NAME+'key')
merge_file = readExcelFile(out_path+NAME+'key'+merge_suf+'.xlsx', header_ = 0, index_col_=0, sheet_name_=NAME+'key')
key_list = ['databank', 'name', 'old_name', 'db_table', 'db_code', 'desc_e', 'desc_c', 'freq', 'start', 'last', 'base', 'quote', 'snl', 'source', 'form_e', 'form_c']
#merge_file = readExcelFile(out_path+'EIKON_key.xlsx', header_ = 0, sheet_name_='EIKON_key')
#merge_database = readExcelFile(out_path+'EIKON_database'+'.xlsx', header_ = 0, index_col_=0, sheet_name_=None)
frequency = 'D'
start_file = 1
last_file = 3
update = datetime.today()
for i in range(len(key_list)):
    if key_list[i] == 'snl':
        snl_pos = i
        break
tStart = time.time()
LOG = ['excel_suffix', 'merging', 'updating', 'find_unknown','dealing_start_year']
for key in LOG:
    logging.info(key+': '+str(locals()[key])+'\n')
log = logging.getLogger()
stream = logging.StreamHandler(sys.stdout)
stream.setFormatter(logging.Formatter('%(message)s'))
log.addHandler(stream)
sys.stdout.write("\n\n")
if merging:
    logging.info('Process: File Merging\n')
elif updating:
    logging.info('Process: File Updating\n')
else:
    logging.info('Data Processing\n')

def takeFirst(alist):
	return alist[0]

Datatype = readFile(data_path+'Datatype.csv', header_ = 0)
Datatype = Datatype.set_index('Symbol').to_dict()
source_FromUSD = readFile(data_path+'sourceFROM.csv', header_ = 0)
source_ToUSD = readFile(data_path+'sourceTO.csv', header_ = 0)
source_USD = pd.concat([source_FromUSD, source_ToUSD], ignore_index=True)
source_USD = source_USD.set_index('Symbol').to_dict()
Currency = readFile(data_path+'Currency2.csv', header_ = 0)
Currency = Currency.set_index('Code').to_dict()

def CURRENCY_CODE(code):
    if code in Currency['Country_Code']:
        return str(Currency['Country_Code'][code]).rjust(3,'0')
    else:
        return 'not_exists'
def CURRENCY(code):
    if code in Currency['Name']:
        return str(Currency['Name'][code])
    else:
        ERROR('貨幣代碼錯誤: '+code)
"""
def SOURCE(code):
    if code in source_USD['Source']:
        return str(source_USD['Source'][code])
    else:
        ERROR('來源代碼錯誤: '+code)
"""
FREQNAME = {'D':'daily'}
FREQLIST = {}
FREQLIST['D'] = pd.date_range(start = str(start_year)+'-01-01', end = update).strftime('%Y-%m-%d').tolist()
FREQLIST['D'].reverse()

KEY_DATA = []
DATA_BASE_dict = {}
db_table_t_dict = {}
DB_name_dict = {}
for f in FREQNAME:
    DATA_BASE_dict[f] = {}
    db_table_t_dict[f] = pd.DataFrame(index = FREQLIST[f], columns = [])
    DB_name_dict[f] = []
DB_TABLE = 'DB_'
DB_CODE = 'data'

table_num_dict = {}
code_num_dict = {}
if merge_file.empty == False and merging == True and updating == False:
    logging.info('Merging File: '+out_path+NAME+'key'+merge_suf+'.xlsx, Time:'+str(int(time.time() - tStart))+' s'+'\n')
    snl = int(merge_file['snl'][merge_file.shape[0]-1]+1)
    for f in FREQNAME:
        table_num_dict[f], code_num_dict[f] = MERGE(merge_file, DB_TABLE, DB_CODE, f)
    if main_file.empty == False:
        logging.info('Main File Exists: '+out_path+NAME+'key'+main_suf+'.xlsx, Time:'+str(int(time.time() - tStart))+' s'+'\n')
        try:
            with open(out_path+NAME+'database_num'+main_suf+'.txt','r',encoding=ENCODING) as f:  #用with一次性完成open、close檔案
                database_num = int(f.read().replace('\n', ''))
            main_database = {}
            for i in range(1,database_num+1):
                logging.info('Reading file: '+NAME+'database_'+str(i)+main_suf+', Time:'+str(int(time.time() - tStart))+' s'+'\n')
                DB_t = readExcelFile(out_path+NAME+'database_'+str(i)+main_suf+'.xlsx', header_ = 0, index_col_=0, acceptNoFile=False, sheet_name_=None)
                for d in DB_t.keys():
                    main_database[d] = DB_t[d]
        except:
            logging.info('Reading file: '+NAME+'database'+main_suf+'.xlsx, Time:'+str(int(time.time() - tStart))+' s'+'\n')
            main_database = readExcelFile(out_path+NAME+'database'+main_suf+'.xlsx', header_ = 0, index_col_=0, acceptNoFile=False)
        for s in range(main_file.shape[0]):
            sys.stdout.write("\rSetting snls: "+str(s+snl))
            sys.stdout.flush()
            main_file.loc[s, 'snl'] = s+snl
        sys.stdout.write("\n")
        logging.info('Setting files, Time:'+str(int(time.time() - tStart))+' s'+'\n')
        db_table_new = 0
        db_code_new = 0
        for f in range(main_file.shape[0]):
            sys.stdout.write("\rSetting new keys: "+str(db_table_new)+" "+str(db_code_new))
            sys.stdout.flush()
            freq = main_file.iloc[f]['freq']
            df_key, DATA_BASE_dict[freq], DB_name_dict[freq], db_table_t_dict[freq], table_num_dict[freq], code_num_dict[freq], db_table_new, db_code_new = \
                NEW_KEYS(f, freq, FREQLIST, DB_TABLE, DB_CODE, main_file, main_database, db_table_t_dict[freq], table_num_dict[freq], code_num_dict[freq], DATA_BASE_dict[freq], DB_name_dict[freq])
        sys.stdout.write("\n")
        for f in FREQNAME:
            if db_table_t_dict[f].empty == False:
                DATA_BASE_dict[f][DB_TABLE+f+'_'+str(table_num_dict[f]).rjust(4,'0')] = db_table_t_dict[f]
                DB_name_dict[f].append(DB_TABLE+f+'_'+str(table_num_dict[f]).rjust(4,'0'))
else:    
    snl = 1
    for f in FREQNAME:
        table_num_dict[f] = 1
        code_num_dict[f] = 1

#print(GERFIN_t.head(10))
if updating == False and DF_suffix != merge_suf:
    logging.info('Reading file: '+NAME+'key'+DF_suffix+', Time: '+str(int(time.time() - tStart))+' s'+'\n')
    DF_KEY = readExcelFile(out_path+NAME+'key'+DF_suffix+'.xlsx', header_ = 0, acceptNoFile=False, index_col_=0, sheet_name_=NAME+'key')
    DF_KEY = DF_KEY.set_index('name')
elif updating == False and DF_suffix == merge_suf:
    DF_KEY = merge_file
    DF_KEY = DF_KEY.set_index('name')

def EIKON_DATA(i, loc1, loc2, name, sheet, EIKON_t, code_num, table_num, KEY_DATA, DATA_BASE, db_table_t, DB_name, snl, freqlist, frequency, source):
    freqlen = len(freqlist)
    NonValue = ['nan','','0']
    if code_num >= 200:
        db_table = DB_TABLE+frequency+'_'+str(table_num).rjust(4,'0')
        DATA_BASE[db_table] = db_table_t
        DB_name.append(db_table)
        table_num += 1
        code_num = 1
        db_table_t = pd.DataFrame(index = freqlist, columns = [])

    old_name = str(EIKON_t[sheet].columns[i][1])

    value = list(EIKON_t[sheet][EIKON_t[sheet].columns[i]])
    index = EIKON_t[sheet][EIKON_t[sheet].columns[i]].index
    db_table = DB_TABLE+frequency+'_'+str(table_num).rjust(4,'0')
    db_code = DB_CODE+str(code_num).rjust(3,'0')
    db_table_t[db_code] = ['' for tmp in range(freqlen)]
    
    start_found = False
    last_found = False
    found = False
    for k in range(len(value)):
        try:
            freq_index = index[k].strftime('%Y-%m-%d')
        except AttributeError:
            freq_index = index[k]
        if freq_index in db_table_t.index and ((find_unknown == False and int(str(freq_index)[:4]) >= dealing_start_year) or find_unknown == True):
            if str(value[k]) in NonValue:
                db_table_t[db_code][freq_index] = ''
            else:
                found = True
                db_table_t[db_code][freq_index] = value[k]
                if start_found == False:
                    try:
                        start = index[k].strftime('%Y-%m-%d')
                    except AttributeError:
                        start = index[k]
                    start_found = True
        else:
            continue
    
    try:
        last = db_table_t[db_code].loc[~db_table_t[db_code].isin(NonValue)].index[0]
    except IndexError:
        if found == True:
            ERROR('last not found: '+str(name))
    if start_found == False:
        if found == True:
            ERROR('start not found: '+str(name))
    if found == False:
        start = 'Nan'
        last = 'Nan'
    #ERROR(str(sheet)+' '+str(EIKON_t[sheet].columns[i]))

    dtype = str(EIKON_t[sheet].columns[i][1])[loc1+1:loc2]
    form_e = str(Datatype['Name'][dtype])+', '+str(Datatype['Type'][dtype])
    desc_e = str(source_USD['Category'][code])+': '+str(source_USD['Full Name'][code]).replace('to', 'per', 1).replace('Tous', 'per US ').replace('To_us_$', 'per US dollar').replace('?', '$', 1).replace("'", ' ').replace('US#', 'US pound')+', '+form_e+', '+'source from '+str(source_USD['Source'][code])
    if str(source_USD['Full Name'][code]).find('USD /') >= 0 or str(source_USD['Full Name'][code]).find('USD/') >= 0 or str(source_USD['Full Name'][code]).find('US Dollar /') >= 0:
        if source_USD['From Currency'][code] == 'United States Dollar':
            base = source_USD['From Currency'][code]
            quote = source_USD['To Currency'][code]
        else:
            base = source_USD['To Currency'][code]
            quote = source_USD['From Currency'][code]
    elif str(source_USD['Full Name'][code]).find('/ USD') >= 0 or str(source_USD['Full Name'][code]).find('/USD') >= 0:
        if source_USD['From Currency'][code] == 'United States Dollar':
            base = source_USD['To Currency'][code]
            quote = source_USD['From Currency'][code]
        else:
            base = source_USD['From Currency'][code]
            quote = source_USD['To Currency'][code]
    else:
        base = source_USD['To Currency'][code]
        quote = source_USD['From Currency'][code]
    desc_c = ''
    freq = frequency
    
    if str(source_USD['Full Name'][code]).find('Butterfly') >= 0 or str(source_USD['Full Name'][code]).find('Reversal') >= 0:
        form_c = 'Options'
    elif str(source_USD['Full Name'][code]).find('Forecast') >= 0:
        form_c = 'Forecast'
    elif str(source_USD['Full Name'][code]).find('FX Volatility') >= 0:
        form_c = 'FX Volatility'
    elif str(source_USD['Full Name'][code]).find('Hourly') >= 0:
        form_c = 'Hourly Rate'
    elif str(source_USD['Full Name'][code]).find('Ptax') >= 0:
        form_c = 'Ptax Rate'    
    elif str(source_USD['Full Name'][code]).find('Forw') >= 0 or str(source_USD['Full Name'][code]).find('FW') >= 0 or str(source_USD['Full Name'][code]).find('MF') >= 0 or str(source_USD['Full Name'][code]).find('YF') >= 0 \
        or str(source_USD['Full Name'][code]).find('Week') >= 0 or str(source_USD['Full Name'][code]).find('Month') >= 0 or str(source_USD['Full Name'][code]).find('Year') >= 0 or str(source_USD['Full Name'][code]).find('Overnight') >= 0 \
        or str(source_USD['Full Name'][code]).find('Tomorrow Next') >= 0 or str(source_USD['Full Name'][code]).find('MONTH') >= 0:
        form_c = 'Forward'
    else:
        form_c = ''
    
    key_tmp= [databank, name, old_name, db_table, db_code, desc_e, desc_c, freq, start, last, base, quote, snl, source, form_e, form_c]
    KEY_DATA.append(key_tmp)
    snl += 1

    code_num += 1

    return code_num, table_num, DATA_BASE, db_table_t, DB_name, snl

###########################################################################  Main Function  ###########################################################################
new_item_counts = 0

for g in range(start_file,last_file+1):
    if main_file.empty == False:
        break
    logging.info('Reading file: '+NAME+str(g)+' Time: '+str(int(time.time() - tStart))+' s'+'\n')
    EIKON_t = readExcelFile(data_path+NAME+str(g)+'.xlsx', header_ = [0,1,2], sheet_name_= None)
    
    for sheet in EIKON_t:
        if CURRENCY_CODE(sheet) == 'not_exists':
            continue
        logging.info('Reading sheet: '+CURRENCY(sheet)+' Time: '+str(int(time.time() - tStart))+' s'+'\n')
        EIKON_t[sheet].set_index(EIKON_t[sheet].columns[0], inplace = True)
        if EIKON_t[sheet].index[0] > EIKON_t[sheet].index[1]:
            EIKON_t[sheet] = EIKON_t[sheet][::-1]
        nG = EIKON_t[sheet].shape[1]
            
        for i in range(nG):
            sys.stdout.write("\rLoading...("+str(round((i+1)*100/nG, 1))+"%)*")
            sys.stdout.flush()
            
            if EIKON_t[sheet].columns[i][0] == '#ERROR':
                continue
            
            loc1 = str(EIKON_t[sheet].columns[i][1]).find('(')
            loc2 = str(EIKON_t[sheet].columns[i][1]).find(')')
            code = str(EIKON_t[sheet].columns[i][1])[:loc1]
            source = str(source_USD['Source'][code])
            if source != 'WM/Reuters':
                continue
            
            name = frequency+CURRENCY_CODE(sheet)+str(EIKON_t[sheet].columns[i][1]).replace('(','').replace(')','')+'.d'
            if (name in DF_KEY.index and find_unknown == True) or (name not in DF_KEY.index and find_unknown == False):
                continue
            elif name not in DF_KEY.index and find_unknown == True:
                new_item_counts+=1
            
            code_num_dict[frequency], table_num_dict[frequency], DATA_BASE_dict[frequency], db_table_t_dict[frequency], DB_name_dict[frequency], snl = \
                EIKON_DATA(i, loc1, loc2, name, sheet, EIKON_t, code_num_dict[frequency], table_num_dict[frequency], KEY_DATA, DATA_BASE_dict[frequency], db_table_t_dict[frequency],\
                    DB_name_dict[frequency], snl, FREQLIST[frequency], frequency, source)
                
        sys.stdout.write("\n\n")
        if find_unknown == True:
            logging.info('Total New Items Found: '+str(new_item_counts)+' Time: '+str(int(time.time() - tStart))+' s'+'\n')  

for f in FREQNAME:
    if main_file.empty == False:
        break
    if db_table_t_dict[f].empty == False:
        DATA_BASE_dict[f][DB_TABLE+f+'_'+str(table_num_dict[f]).rjust(4,'0')] = db_table_t_dict[f]
        DB_name_dict[f].append(DB_TABLE+f+'_'+str(table_num_dict[f]).rjust(4,'0'))

print('Time: ', int(time.time() - tStart),'s'+'\n')
if main_file.empty == True:
    df_key = pd.DataFrame(KEY_DATA, columns = key_list)
else:
    if merge_file.empty == True:
        ERROR('Missing Merge File')
if updating == True:
    df_key, DATA_BASE_dict = UPDATE(merge_file, main_file, key_list, NAME, out_path, merge_suf, main_suf, FREQLIST=FREQLIST)
else:
    if df_key.empty and find_unknown == False:
        ERROR('Empty dataframe')
    elif df_key.empty and find_unknown == True:
        ERROR('No new items were found.')
    df_key, DATA_BASE_dict = CONCATE(NAME, merge_suf, out_path, DB_TABLE, DB_CODE, FREQNAME, FREQLIST, tStart, df_key, merge_file, DATA_BASE_dict, DB_name_dict, find_unknown=find_unknown)

logging.info(df_key)
#logging.info(DATA_BASE_t)
DB_name = []
if updating == True:
    for key in DATA_BASE_dict.keys():
        DB_name.append(key)
else:
    for f in FREQNAME:
        for key in DATA_BASE_dict[f].keys():
            DB_name.append(key)

print('Time: ', int(time.time() - tStart),'s'+'\n')
df_key.to_excel(out_path+NAME+"key"+excel_suffix+".xlsx", sheet_name=NAME+'key')
database_num = int(((len(DB_name)-1)/maximum))+1
for d in range(1, database_num+1):
    if database_num > 1:
        with pd.ExcelWriter(out_path+NAME+"database_"+str(d)+excel_suffix+".xlsx") as writer: # pylint: disable=abstract-class-instantiated
            logging.info('Outputing file: '+NAME+"database_"+str(d))
            if maximum*d > len(DB_name):
                for db in range(maximum*(d-1), len(DB_name)):
                    sys.stdout.write("\rOutputing sheet: "+str(DB_name[db])+'  Time: '+str(int(time.time() - tStart))+'s')
                    sys.stdout.flush()
                    if updating == True:
                        if DATA_BASE_dict[DB_name[db]].empty == False:
                            DATA_BASE_dict[DB_name[db]].to_excel(writer, sheet_name = DB_name[db])
                    else:
                        for f in FREQNAME:
                            if DB_name[db] in DATA_BASE_dict[f].keys() and DATA_BASE_dict[f][DB_name[db]].empty == False:
                                DATA_BASE_dict[f][DB_name[db]].to_excel(writer, sheet_name = DB_name[db])
                writer.save()
                sys.stdout.write("\n")
            else:
                for db in range(maximum*(d-1), maximum*d):
                    sys.stdout.write("\rOutputing sheet: "+str(DB_name[db])+'  Time: '+str(int(time.time() - tStart))+'s')
                    sys.stdout.flush()
                    if updating == True:
                        if DATA_BASE_dict[DB_name[db]].empty == False:
                            DATA_BASE_dict[DB_name[db]].to_excel(writer, sheet_name = DB_name[db])
                    else:
                        for f in FREQNAME:
                            if DB_name[db] in DATA_BASE_dict[f].keys() and DATA_BASE_dict[f][DB_name[db]].empty == False:
                                DATA_BASE_dict[f][DB_name[db]].to_excel(writer, sheet_name = DB_name[db])
                writer.save()
                sys.stdout.write("\n")
    else:
        with pd.ExcelWriter(out_path+NAME+"database"+excel_suffix+".xlsx") as writer: # pylint: disable=abstract-class-instantiated
            if updating == True:
                for key in DATA_BASE_dict:
                    sys.stdout.write("\rOutputing sheet: "+str(d))
                    sys.stdout.flush()
                    if DATA_BASE_dict[key].empty == False:
                        DATA_BASE_dict[key].to_excel(writer, sheet_name = key)
            else:
                for f in FREQNAME:
                    for key in DATA_BASE_dict[f]:
                        sys.stdout.write("\rOutputing sheet: "+str(key))
                        sys.stdout.flush()
                        if DATA_BASE_dict[f][key].empty == False:
                            DATA_BASE_dict[f][key].to_excel(writer, sheet_name = key)
sys.stdout.write("\n")
logging.info('\ndatabase_num = '+str(database_num))
if database_num > 1:
    with open(out_path+NAME+'database_num'+excel_suffix+'.txt','w', encoding=ENCODING) as f:    #用with一次性完成open、close檔案
        f.write(str(database_num))

print('Time: ', int(time.time() - tStart),'s'+'\n')
if updating == False:
    if find_unknown == True:
        checkNotFound = False
    else:
        checkNotFound = True
    unknown_list, toolong_list, update_list, unfound_list = GERFIN_identity(out_path, df_key, DF_KEY, checkNotFound=checkNotFound, checkDESC=True, tStart=tStart, start_year=dealing_start_year)