# !/usr/bin/env python3
# -*- coding: utf-8 -*-
# pylint: disable=E1101
import math, re, sys, calendar, os, copy, time, zipfile, logging
import pandas as pd
import numpy as np
import requests as rq
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import webdriver_manager
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime, date
import GERFIN_concat as CCT
from GERFIN_concat import ERROR, MERGE, NEW_KEYS, CONCATE, UPDATE, readFile, readExcelFile, PRESENT, GERFIN_WEB, SELECT_DF_KEY, SELECT_DATABASES, INSERT_TABLES
import GERFIN_test as test
from GERFIN_test import GERFIN_identity
from pandas.errors import ParserError
FORMAT = '%(asctime)s %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT, handlers=[logging.FileHandler("LOG.log", 'w', CCT.ENCODING)], datefmt='%Y-%m-%d %I:%M:%S %p')

ENCODING = CCT.ENCODING

NAME = 'GERFIN_'
EIKON_NAME = 'EIKON_'
data_path = './data/'
out_path = "./output/"
databank = NAME[:-1]
find_unknown = False
main_suf = '?'
merge_suf = '?'
dealing_start_year = 1957
start_year = 1957
maximum = 6
merging = False
updating = False
data_processing = bool(int(input('Processing data (1/0): ')))
excel_suffix = CCT.excel_suffix
#main_file = readExcelFile(out_path+NAME+'key'+main_suf+'.xlsx', header_ = 0, index_col_=0, sheet_name_=NAME+'key')
#merge_file = readExcelFile(out_path+NAME+'key'+merge_suf+'.xlsx', header_ = 0, index_col_=0, sheet_name_=NAME+'key')
LOG = ['excel_suffix', 'merging', 'updating', 'find_unknown','dealing_start_year']
for key in LOG:
    logging.info(key+': '+str(locals()[key])+'\n')
log = logging.getLogger()
stream = logging.StreamHandler(sys.stdout)
stream.setFormatter(logging.Formatter('%(message)s'))
log.addHandler(stream)
key_list = ['databank', 'name', 'db_table', 'db_code', 'desc_e', 'old_name', 'freq', 'start', 'last', 'base', 'quote', 'snl', 'source', 'form_e', 'form_c']
frequency = 'D'
start_file = 1
last_file = 4
EIKON_start_file = 1
EIKON_last_file = 3
update = datetime.today()
for i in range(len(key_list)):
    if key_list[i] == 'snl':
        snl_pos = i
        break
tStart = time.time()

def takeFirst(alist):
	return alist[0]

AREMOS_gerfin = readExcelFile(data_path+'AREMOS_gerfin.xlsx', header_ = [0], sheet_name_='AREMOS_gerfin')
Currency = readFile(data_path+'Currency.csv', header_ = 0)
Currency = Currency.set_index('Code').to_dict()
Currency2 = readFile(data_path+'Currency2.csv', header_ = 0)
Currency2 = Currency2.set_index('Code').to_dict()
Datatype = readFile(data_path+'Datatype.csv', header_ = 0)
Datatype = Datatype.set_index('Symbol').to_dict()
source_FromUSD = readFile(data_path+'sourceFROM.csv', header_ = 0)
source_ToUSD = readFile(data_path+'sourceTO.csv', header_ = 0)
source_USD = pd.concat([source_FromUSD, source_ToUSD], ignore_index=True)
source_USD = source_USD.set_index('Symbol').to_dict()
def CURRENCY(code):
    if code in Currency['Name']:
        return str(Currency['Name'][code])
    else:
        ERROR('貨幣代碼錯誤: '+code)
def CURRENCY2(code):
    if code in Currency2['Name']:
        return str(Currency2['Name'][code])
    else:
        ERROR('貨幣代碼錯誤: '+code)
def CURRENCY_CODE(code):
    if code in Currency2['Country_Code']:
        return str(Currency2['Country_Code'][code]).rjust(3,'0')
    else:
        return 'not_exists'

FREQNAME = {'D':'daily'}
table_num_dict = {}
code_num_dict = {}
if data_processing:
    find_unknown = bool(int(input('Check if new items exist (1/0): ')))
    if find_unknown == False:
        dealing_start_year = int(input("Dealing with data from year: "))
        start_year = dealing_start_year-2
    sys.stdout.write("\n\n")
    logging.info('Data Processing\n')
    main_file = pd.DataFrame()
    merge_file = pd.DataFrame()
    snl = 1
    for f in FREQNAME:
        table_num_dict[f] = 1
        code_num_dict[f] = 1

FREQLIST = {}
FREQLIST['D'] = pd.date_range(start = str(start_year)+'-01-01', end = update).strftime('%Y-%m-%d').tolist()
FREQLIST['D'].reverse()

KEY_DATA = []
DATA_BASE_main = {}
db_table_t_dict = {}
DB_name_dict = {}
for f in FREQNAME:
    DATA_BASE_main[f] = {}
    db_table_t_dict[f] = pd.DataFrame(index = FREQLIST[f], columns = [])
    DB_name_dict[f] = []
DB_TABLE = 'DB_'
DB_CODE = 'data'

merge_file_loaded = False
if excel_suffix == 'mysql':
    df_key = SELECT_DF_KEY(databank)
    DATA_BASE_dict = SELECT_DATABASES(databank)
    merge_file_loaded = True
while data_processing == False:
    while True:
        try:
            merging = bool(int(input('Merging data file = 1/Updating data file = 0: ')))
            updating = not merging
            if merge_file_loaded == False:
                merge_suf = input('Be Merged(Original) data suffix: ')
                if os.path.isfile(out_path+NAME+'key'+merge_suf+'.xlsx') == False:
                    raise FileNotFoundError
            main_suf = input('Main(Updated) data suffix: ')
            if os.path.isfile(out_path+NAME+'key'+main_suf+'.xlsx') == False:
                raise FileNotFoundError
        except:
            print('= ! = Incorrect Input'+'\n')
        else:
            break
    sys.stdout.write("\n\n")
    if merging:
        logging.info('Process: File Merging\n')
    elif updating:
        logging.info('Process: File Updating\n')
    logging.info('Reading main key: '+NAME+'key'+main_suf+'.xlsx, Time: '+str(int(time.time() - tStart))+' s'+'\n')
    main_file = readExcelFile(out_path+NAME+'key'+main_suf+'.xlsx', header_ = 0, index_col_=0, sheet_name_=NAME+'key', acceptNoFile=False)
    if main_file.empty:
        ERROR('Empty updated_file')
    try:
        with open(out_path+NAME+'database_num'+main_suf+'.txt','r',encoding=ENCODING) as f:  #用with一次性完成open、close檔案
            database_num = int(f.read().replace('\n', ''))
        main_database = {}
        for i in range(1,database_num+1):
            logging.info('Reading main database: '+NAME+'database_'+str(i)+main_suf+', Time: '+str(int(time.time() - tStart))+' s'+'\n')
            DB_t = readExcelFile(out_path+NAME+'database_'+str(i)+main_suf+'.xlsx', header_ = 0, index_col_=0, acceptNoFile=False, sheet_name_=None)
            for d in DB_t.keys():
                main_database[d] = DB_t[d]
    except:
        logging.info('Reading main database: '+NAME+'database'+main_suf+'.xlsx, Time: '+str(int(time.time() - tStart))+' s'+'\n')
        main_database = readExcelFile(out_path+NAME+'database'+main_suf+'.xlsx', header_ = 0, index_col_=0, acceptNoFile=False)
    if merge_file_loaded:
        merge_file = df_key
        merge_database = DATA_BASE_dict
    else:
        logging.info('Reading original key: '+NAME+'key'+merge_suf+'.xlsx, Time: '+str(int(time.time() - tStart))+' s'+'\n')
        merge_file = readExcelFile(out_path+NAME+'key'+merge_suf+'.xlsx', header_ = 0, index_col_=0, sheet_name_=NAME+'key', acceptNoFile=False)
        if merge_file.empty:
            ERROR('Empty original_file')
        try:
            with open(out_path+NAME+'database_num'+merge_suf+'.txt','r',encoding=ENCODING) as f:  #用with一次性完成open、close檔案
                database_num = int(f.read().replace('\n', ''))
            merge_database = {}
            for i in range(1,database_num+1):
                logging.info('Reading original database: '+NAME+'database_'+str(i)+merge_suf+', Time: '+str(int(time.time() - tStart))+' s'+'\n')
                DB_t = readExcelFile(out_path+NAME+'database_'+str(i)+merge_suf+'.xlsx', header_ = 0, index_col_=0, acceptNoFile=False, sheet_name_=None)
                for d in DB_t.keys():
                    merge_database[d] = DB_t[d]
        except:
            logging.info('Reading original database: '+NAME+'database'+merge_suf+', Time: '+str(int(time.time() - tStart))+' s'+'\n')
            merge_database = readExcelFile(out_path+NAME+'database'+merge_suf+'.xlsx', header_ = 0, index_col_=0, acceptNoFile=False)
    #if merge_file.empty == False and merging == True and updating == False:
    if merging:
        logging.info('Merging File, Time: '+str(int(time.time() - tStart))+' s'+'\n')
        snl = int(merge_file['snl'][merge_file.shape[0]-1]+1)
        for f in FREQNAME:
            table_num_dict[f], code_num_dict[f] = MERGE(merge_file, DB_TABLE, DB_CODE, f)
        #if main_file.empty == False:
        #logging.info('Main File Exists: '+out_path+NAME+'key'+main_suf+'.xlsx, Time: '+str(int(time.time() - tStart))+' s'+'\n')
        for s in range(main_file.shape[0]):
            sys.stdout.write("\rSetting snls: "+str(s+snl))
            sys.stdout.flush()
            main_file.loc[s, 'snl'] = s+snl
        sys.stdout.write("\n")
        logging.info('Setting files, Time: '+str(int(time.time() - tStart))+' s'+'\n')
        db_table_new = 0
        db_code_new = 0
        for f in range(main_file.shape[0]):
            sys.stdout.write("\rSetting new keys: "+str(db_table_new)+" "+str(db_code_new))
            sys.stdout.flush()
            freq = main_file.iloc[f]['freq']
            df_key, DATA_BASE_main[freq], DB_name_dict[freq], db_table_t_dict[freq], table_num_dict[freq], code_num_dict[freq], db_table_new, db_code_new = \
                NEW_KEYS(f, freq, FREQLIST, DB_TABLE, DB_CODE, main_file, main_database, db_table_t_dict[freq], table_num_dict[freq], code_num_dict[freq], DATA_BASE_main[freq], DB_name_dict[freq])
        sys.stdout.write("\n")
        for f in FREQNAME:
            if db_table_t_dict[f].empty == False:
                DATA_BASE_main[f][DB_TABLE+f+'_'+str(table_num_dict[f]).rjust(4,'0')] = db_table_t_dict[f]
                DB_name_dict[f].append(DB_TABLE+f+'_'+str(table_num_dict[f]).rjust(4,'0'))
        df_key, DATA_BASE_dict = CONCATE(NAME, merge_suf, out_path, DB_TABLE, DB_CODE, FREQNAME, FREQLIST, tStart, df_key, merge_file, DATA_BASE_main, DB_name_dict, DATA_BASE_t=merge_database)
        for f in FREQNAME:
            DATA_BASE_main[f] = {}
            db_table_t_dict[f] = pd.DataFrame(index = FREQLIST[f], columns = [])
            DB_name_dict[f] = []
    elif updating:
        if 'table_id' in key_list:
            key_list.remove('table_id')
        df_key, DATA_BASE_dict = UPDATE(merge_file, main_file, key_list, NAME, out_path, merge_suf, main_suf, FREQLIST=FREQLIST, original_database=merge_database, updated_database=main_database)
    merge_file_loaded = True
    while True:
        try:
            continuing = bool(int(input('Merge or Update Another File With the Same Original File (1/0): ')))
        except:
            print('= ! = Incorrect Input'+'\n')
        else:
            break
    if continuing == False:
        break

#print(GERFIN_t.head(10))
if updating == False:
    DF_KEY = SELECT_DF_KEY(databank)
    DF_KEY = DF_KEY.set_index('name')
    #print(DF_KEY)

def GERFIN_DATA(i, name, GERFIN_t, code_num, table_num, KEY_DATA, DATA_BASE, db_table_t, DB_name, snl, freqlist, frequency, source, AREMOS_key=None, AREMOS_key2=None):
    freqlen = len(freqlist)
    NonValue = ['nan','-','.','0','']
    if code_num >= 200:
        db_table2 = DB_TABLE+frequency+'_'+str(table_num).rjust(4,'0')
        DATA_BASE[db_table2] = db_table_t
        DB_name.append(db_table2)
        table_num += 1
        code_num = 1
        db_table_t = pd.DataFrame(index = freqlist, columns = [])
    
    value = list(GERFIN_t[GERFIN_t.columns[i]])
    index = GERFIN_t[GERFIN_t.columns[i]].index
    new_table = False
    db_table = DB_TABLE+frequency+'_'+str(table_num).rjust(4,'0')
    db_code = DB_CODE+str(code_num).rjust(3,'0')
    #db_table_t[db_code] = ['' for tmp in range(freqlen)]
    db_table_t = pd.concat([db_table_t, pd.DataFrame(['' for tmp in range(freqlen)], index=freqlist, columns=[db_code])], axis=1)
    if AREMOS_key2 != None:
        code_num += 1
        if code_num >= 200:
            new_table = True
            DATA_BASE[db_table] = db_table_t
            DB_name.append(db_table)
            table_num += 1
            code_num = 1
            db_table_t2 = pd.DataFrame(index = freqlist, columns = [])
            db_table2 = DB_TABLE+frequency+'_'+str(table_num).rjust(4,'0')
            db_code2 = DB_CODE+str(code_num).rjust(3,'0')
            #db_table_t2[db_code2] = ['' for tmp in range(freqlen)]
            db_table_t2 = pd.concat([db_table_t2, pd.DataFrame(['' for tmp in range(freqlen)], index=freqlist, columns=[db_code2])], axis=1)
        else:
            db_table2 = DB_TABLE+frequency+'_'+str(table_num).rjust(4,'0')
            db_code2 = DB_CODE+str(code_num).rjust(3,'0')
            #db_table_t[db_code2] = ['' for tmp in range(freqlen)]
            db_table_t = pd.concat([db_table_t, pd.DataFrame(['' for tmp in range(freqlen)], index=freqlist, columns=[db_code2])], axis=1)
    start_found = False
    last_found = False
    found = False
    for k in range(len(value)):
        try:
            freq_index = index[k].strftime('%Y-%m-%d')
        except AttributeError:
            freq_index = index[k]
        if freq_index in db_table_t.index and ((find_unknown == False and int(str(freq_index)[:4]) >= dealing_start_year) or find_unknown == True):
            if str(value[k]) in NonValue or bool(re.search(r'^0(\.0+)*$', str(value[k]))):
                db_table_t[db_code][freq_index] = ''
                if new_table == True:
                    db_table_t2[db_code2][freq_index] = ''
                elif AREMOS_key2 != None:
                    db_table_t[db_code2][freq_index] = ''
            else:
                found = True
                db_table_t[db_code][freq_index] = float(value[k])
                if new_table == True:
                    db_table_t2[db_code2][freq_index] = round(1/float(value[k]), 4)
                elif AREMOS_key2 != None:
                    db_table_t[db_code2][freq_index] = round(1/float(value[k]), 4)
                if start_found == False:
                    try:
                        start = index[k].strftime('%Y-%m-%d')
                    except AttributeError:
                        start = index[k]
                    start2 = start
                    start_found = True
        else:
            continue
    
    try:
        last = db_table_t[db_code].loc[~db_table_t[db_code].isin(NonValue)].index[0]
        last2 = last
    except IndexError:
        if found == True:
            ERROR('last not found: '+str(name))
    if start_found == False:
        if found == True:
            ERROR('start not found: '+str(name))                
    if found == False:
        logging.debug('Data Not Found: '+str(GERFIN_t.columns[i]))
        start = 'Nan'
        last = 'Nan'
        start2 = start
        last2 = last
    if new_table == True:
        db_table_t = db_table_t2

    desc_e = str(AREMOS_key['description'][0])
    base = str(AREMOS_key['base currency'][0])
    quote = str(AREMOS_key['quote currency'][0])
    desc_c = ''
    form_e = str(AREMOS_key['attribute'][0])
    form_c = ''
    if AREMOS_key2 != None:
        desc_e2 = str(AREMOS_key2['description'][0])
        base2 = str(AREMOS_key2['base currency'][0])
        quote2 = str(AREMOS_key2['quote currency'][0])
        desc_c2 = ''
        form_c2 = ''
    
    key_tmp= [databank, name, db_table, db_code, desc_e, desc_c, frequency, start, last, base, quote, snl, source, form_e, form_c]
    KEY_DATA.append(key_tmp)
    snl += 1
    if AREMOS_key2 != None:
        key_tmp2= [databank, name2, db_table2, db_code2, desc_e2, desc_c2, frequency, start2, last2, base2, quote2, snl, source, form_e, form_c2]
        KEY_DATA.append(key_tmp2)
        snl += 1

    code_num += 1

    return code_num, table_num, DATA_BASE, db_table_t, DB_name, snl

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
    #db_table_t[db_code] = ['' for tmp in range(freqlen)]
    db_table_t = pd.concat([db_table_t, pd.DataFrame(['' for tmp in range(freqlen)], index=freqlist, columns=[db_code])], axis=1)
    
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
    #desc_c = ''
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
    
    key_tmp= [databank, name, db_table, db_code, desc_e, old_name, freq, start, last, base, quote, snl, source, form_e, form_c]
    KEY_DATA.append(key_tmp)
    snl += 1

    code_num += 1

    return code_num, table_num, DATA_BASE, db_table_t, DB_name, snl

###########################################################################  Main Function  ###########################################################################
new_item_counts = 0
chrome = None
zip_list = []

for g in range(start_file,last_file+1):
    if data_processing == False:
        break
    # if chrome == None:
    #     options = Options()
    #     options.add_argument("--disable-notifications")
    #     options.add_argument("--disable-popup-blocking")
    #     options.add_argument("ignore-certificate-errors")
    #     options.add_experimental_option("excludeSwitches", ["enable-logging"])
    #     chrome = webdriver.Chrome(ChromeDriverManager().install(), options=options)
    #     chrome.set_window_position(980,0)
    logging.info('Reading file: '+NAME+str(g)+' Time: '+str(int(time.time() - tStart))+' s'+'\n')
    if g == 1 or g == 4:
        file_path = data_path+NAME+str(g)+'.csv'
        if PRESENT(file_path):
            try:
                GERFIN_t = readFile(data_path+NAME+str(g)+'.csv', header_=[0], index_col_=0)
            except ParserError:
                skip = [0,1]
                GERFIN_t = readFile(data_path+NAME+str(g)+'.csv', header_=[0], index_col_=0, skiprows_=skip)
        else:
            if g == 1:
                url = 'https://sdw.ecb.europa.eu/browse.do?node=9691296'
            elif g == 4:
                url = 'https://sdw.ecb.europa.eu/browse.do?node=9691297'
            # skip = [1,2,3,4]
            skip = None
            GERFIN_t = GERFIN_WEB(chrome, g, file_name=NAME+str(g), url=url, header=[0], index_col=0, skiprows=skip, output=True, start_year=dealing_start_year)
        
        if str(GERFIN_t.index[0]).find('/') >= 0:
            new_index = []
            for ind in GERFIN_t.index:
                new_index.append(pd.to_datetime(ind))
            GERFIN_t = GERFIN_t.reindex(new_index)
        if GERFIN_t.index[10] > GERFIN_t.index[11]:
            GERFIN_t = GERFIN_t[::-1]
        if str(GERFIN_t.index[10]).strip()[:4] < str(dealing_start_year) and str(GERFIN_t.index[-10]).strip()[:4] < str(dealing_start_year):
            logging.info('Data not in range\n')
            continue
        
        nG = GERFIN_t.shape[1]
        #print(GERFIN_t)        
        for i in range(nG):
            sys.stdout.write("\rLoading...("+str(round((i+1)*100/nG, 1))+"%)*")
            sys.stdout.flush()

            source = 'Official ECB & EUROSTAT Reference'
            AREMOS_key = AREMOS_gerfin.loc[AREMOS_gerfin['source'] == source].loc[AREMOS_gerfin['quote currency'] == str(GERFIN_t.columns[i])].to_dict('list')
            AREMOS_key2 = AREMOS_gerfin.loc[AREMOS_gerfin['source'] == source].loc[AREMOS_gerfin['base currency'] == str(GERFIN_t.columns[i])].to_dict('list')
            if pd.DataFrame(AREMOS_key).empty == True:
                continue
            name = str(AREMOS_key['code'][0])
            name2 = str(AREMOS_key2['code'][0])
            if (name in DF_KEY.index and name2 in DF_KEY.index and find_unknown == True) or (name not in DF_KEY.index and name2 not in DF_KEY.index and find_unknown == False):
                continue
            elif name not in DF_KEY.index and name2 not in DF_KEY.index and find_unknown == True:
                new_item_counts+=2
            
            code_num_dict[frequency], table_num_dict[frequency], DATA_BASE_main[frequency], db_table_t_dict[frequency], DB_name_dict[frequency], snl = \
                  GERFIN_DATA(i, name, GERFIN_t, code_num_dict[frequency], table_num_dict[frequency], KEY_DATA, DATA_BASE_main[frequency], db_table_t_dict[frequency],\
                       DB_name_dict[frequency], snl, FREQLIST[frequency], frequency, source, AREMOS_key=AREMOS_key, AREMOS_key2=AREMOS_key2)
    elif g == 2:
        file_path = data_path+NAME+str(g)+'.csv'
        if PRESENT(file_path):
            GERFIN_t = readFile(data_path+NAME+str(g)+'.csv', header_=[0,1,2], index_col_=0, skiprows_=[3,4], skipfooter_=1)
        else:
            chrome.set_window_size(1080, 1020)
            #url = 'https://www.bundesbank.de/dynamic/action/en/statistics/time-series-databases/time-series-databases/759784/759784?listId=www_s331_xdrd'
            url = 'https://www.bundesbank.de/dynamic/action/en/statistics/time-series-databases/time-series-databases/759784/759784?listId=www_sdks_xdrd'
            GERFIN_t = GERFIN_WEB(chrome, g, file_name=NAME+str(g), url=url, header=[0,1,2], index_col=0, skiprows=[3,4], csv=True, start_year=dealing_start_year)
        if GERFIN_t.index[0] > GERFIN_t.index[1]:
            GERFIN_t = GERFIN_t[::-1]
        
        nG = GERFIN_t.shape[1]
        #print(GERFIN_t)        
        for i in range(nG):
            sys.stdout.write("\rLoading...("+str(round((i+1)*100/nG, 1))+"%)*")
            sys.stdout.flush()

            if str(GERFIN_t.columns[i][0]).find('FLAGS') >= 0:
                continue
            source = 'Fin. Market Indicative Reference'
            AREMOS_key = AREMOS_gerfin.loc[AREMOS_gerfin['source'] == source].loc[AREMOS_gerfin['quote currency'] == CURRENCY(GERFIN_t.columns[i][2])].to_dict('list')
            AREMOS_key2 = AREMOS_gerfin.loc[AREMOS_gerfin['source'] == source].loc[AREMOS_gerfin['base currency'] == CURRENCY(GERFIN_t.columns[i][2])].to_dict('list')
            if pd.DataFrame(AREMOS_key).empty == True:
                continue
            name = str(AREMOS_key['code'][0])
            name2 = str(AREMOS_key2['code'][0])
            if (name in DF_KEY.index and name2 in DF_KEY.index and find_unknown == True) or (name not in DF_KEY.index and name2 not in DF_KEY.index and find_unknown == False):
                continue
            elif name not in DF_KEY.index and name2 not in DF_KEY.index and find_unknown == True:
                new_item_counts+=2
            
            code_num_dict[frequency], table_num_dict[frequency], DATA_BASE_main[frequency], db_table_t_dict[frequency], DB_name_dict[frequency], snl = \
                  GERFIN_DATA(i, name, GERFIN_t, code_num_dict[frequency], table_num_dict[frequency], KEY_DATA, DATA_BASE_main[frequency], db_table_t_dict[frequency],\
                       DB_name_dict[frequency], snl, FREQLIST[frequency], frequency, source, AREMOS_key=AREMOS_key, AREMOS_key2=AREMOS_key2)    
    elif g == 3:
        Zip_file = NAME+str(g)
        file_path = data_path+Zip_file+'.zip'
        present_file_existed = PRESENT(file_path)
        if Zip_file not in zip_list:
            if present_file_existed == True:
                zipname = Zip_file
            else:
                zipname = GERFIN_WEB(chrome, g, file_name=Zip_file, url='https://research.stlouisfed.org/useraccount/datalists', Zip=True)
            zip_list.append(zipname)
        zf = zipfile.ZipFile(file_path,'r')
        GERFIN_t = readExcelFile(zf.open(databank+'.xls'), header_ =[0], index_col_=0, sheet_name_='Daily')
        README_t = readExcelFile(zf.open(databank+'.xls'), sheet_name_='README')
        README = list(README_t[0])
        #GERFIN_t = GERFIN_WEB(chrome, g, url='https://research.stlouisfed.org/useraccount/datalists', header=[0], index_col=0, Zip=True)
        #GERFIN_t = readExcelFile(data_path+NAME+str(g)+'.xls', header_ =0, index_col_=0, sheet_name_='Daily')
        #README_t = readExcelFile(data_path+NAME+str(g)+'.xls', sheet_name_='README')
        #README = list(README_t[0])
        if GERFIN_t.index[0] > GERFIN_t.index[1]:
            GERFIN_t = GERFIN_t[::-1]
        
        nG = GERFIN_t.shape[1]
        nR = len(README)
        #print(GERFIN_t)        
        for i in range(nG):
            sys.stdout.write("\rLoading...("+str(round((i+1)*100/nG, 1))+"%)*")
            sys.stdout.flush()
            if str(GERFIN_t.columns[i]).find('DEX') < 0:
                continue
            for r in range(nR):
                if README[r] == GERFIN_t.columns[i]:
                    for rr in range(r,nR):
                        if README[rr] == 'Units:':
                            if str(GERFIN_t.columns[i]).find('DEXUS') >= 0:
                                loc1 = README[rr+1].find('One ')
                                currency = README[rr+1][loc1+4:]
                            else:
                                loc1 = README[rr+1].find(' to')
                                currency = README[rr+1][:loc1]
                            break
                    break
            
            source = 'FRB NY'
            AREMOS_key = AREMOS_gerfin.loc[AREMOS_gerfin['source'] == source].loc[AREMOS_gerfin['quote currency'] == currency].to_dict('list')
            if pd.DataFrame(AREMOS_key).empty == True:
                continue
            name = str(AREMOS_key['code'][0])
            if (name in DF_KEY.index and find_unknown == True) or (name not in DF_KEY.index and find_unknown == False):
                continue
            elif name not in DF_KEY.index and find_unknown == True:
                new_item_counts+=1
            
            code_num_dict[frequency], table_num_dict[frequency], DATA_BASE_main[frequency], db_table_t_dict[frequency], DB_name_dict[frequency], snl = \
                    GERFIN_DATA(i, name, GERFIN_t, code_num_dict[frequency], table_num_dict[frequency], KEY_DATA, DATA_BASE_main[frequency], db_table_t_dict[frequency],\
                        DB_name_dict[frequency], snl, FREQLIST[frequency], frequency, source, AREMOS_key=AREMOS_key)
                
    sys.stdout.write("\n\n")
    if find_unknown == True:
        logging.info('Total New Items Found: '+str(new_item_counts)+' Time: '+str(int(time.time() - tStart))+' s'+'\n')  
if chrome != None:
    chrome.quit()
    chrome = None

for g in range(EIKON_start_file,EIKON_last_file+1):
    if data_processing == False:
        break
    logging.info('Reading file: '+EIKON_NAME+str(g)+' Time: '+str(int(time.time() - tStart))+' s'+'\n')
    EIKON_t = readExcelFile(data_path+EIKON_NAME+str(g)+'.xlsx', header_ = [0,1,2], sheet_name_= None)
    
    for sheet in EIKON_t:
        if CURRENCY_CODE(sheet) == 'not_exists':
            continue
        logging.info('Reading sheet: '+CURRENCY2(sheet)+' Time: '+str(int(time.time() - tStart))+' s'+'\n')
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
            
            code_num_dict[frequency], table_num_dict[frequency], DATA_BASE_main[frequency], db_table_t_dict[frequency], DB_name_dict[frequency], snl = \
                EIKON_DATA(i, loc1, loc2, name, sheet, EIKON_t, code_num_dict[frequency], table_num_dict[frequency], KEY_DATA, DATA_BASE_main[frequency], db_table_t_dict[frequency],\
                    DB_name_dict[frequency], snl, FREQLIST[frequency], frequency, source)
                
        sys.stdout.write("\n\n")
        if find_unknown == True:
            logging.info('Total New Items Found: '+str(new_item_counts)+' Time: '+str(int(time.time() - tStart))+' s'+'\n')  

print('Time: ', int(time.time() - tStart),'s'+'\n')
if data_processing:
    for f in FREQNAME:
        if main_file.empty == False:
            break
        if db_table_t_dict[f].empty == False:
            DATA_BASE_main[f][DB_TABLE+f+'_'+str(table_num_dict[f]).rjust(4,'0')] = db_table_t_dict[f]
            DB_name_dict[f].append(DB_TABLE+f+'_'+str(table_num_dict[f]).rjust(4,'0'))
    df_key = pd.DataFrame(KEY_DATA, columns = key_list)
    if df_key.empty and find_unknown == False:
        ERROR('Empty dataframe')
    elif df_key.empty and find_unknown == True:
        ERROR('No new items were found.')
    df_key, DATA_BASE_dict = CONCATE(NAME, merge_suf, out_path, DB_TABLE, DB_CODE, FREQNAME, FREQLIST, tStart, df_key, merge_file, DATA_BASE_main, DB_name_dict)

logging.info(df_key)
#logging.info(DATA_BASE_t)
DB_name = []
for key in DATA_BASE_dict.keys():
    DB_name.append(key)

print('Time: ', int(time.time() - tStart),'s'+'\n')
if excel_suffix == 'mysql':
    INSERT_TABLES(databank, df_key, DATA_BASE_dict)
else:
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
                        #if updating == True:
                        if DATA_BASE_dict[DB_name[db]].empty == False:
                            DATA_BASE_dict[DB_name[db]].to_excel(writer, sheet_name = DB_name[db])
                    writer.save()
                    sys.stdout.write("\n")
                else:
                    for db in range(maximum*(d-1), maximum*d):
                        sys.stdout.write("\rOutputing sheet: "+str(DB_name[db])+'  Time: '+str(int(time.time() - tStart))+'s')
                        sys.stdout.flush()
                        #if updating == True:
                        if DATA_BASE_dict[DB_name[db]].empty == False:
                            DATA_BASE_dict[DB_name[db]].to_excel(writer, sheet_name = DB_name[db])
                    writer.save()
                    sys.stdout.write("\n")
        else:
            with pd.ExcelWriter(out_path+NAME+"database"+excel_suffix+".xlsx") as writer: # pylint: disable=abstract-class-instantiated
                #if updating == True:
                for key in DATA_BASE_dict:
                    sys.stdout.write("\rOutputing sheet: "+str(d))
                    sys.stdout.flush()
                    if DATA_BASE_dict[key].empty == False:
                        DATA_BASE_dict[key].to_excel(writer, sheet_name = key)
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