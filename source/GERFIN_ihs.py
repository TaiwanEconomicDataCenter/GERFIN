# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import math, re, sys, calendar, os, copy, time
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
#from GERFIN_concat import CONCATE, readExcelFile

ENCODING = 'utf-8-sig'

start_year = 2018
latest = False
SUFFIX = ''
NAME = 'GERFIN_myihs_'+str(start_year)+SUFFIX
data_path = './data2/'
out_path = "./output/"
databank = 'GERFIN'
key_list = ['databank', 'name', 'db_table', 'db_code', 'desc_e', 'desc_c', 'freq', 'start', 'last', 'base', 'quote', 'snl', 'source', 'form_e', 'form_c']
#merge_file = readExcelFile(out_path+'GERFIN_key.xlsx', header_ = 0, sheet_name_='GERFIN_key')
this_year = start_year+1 #datetime.now().year + 1
if latest == True:
    update = datetime.today()
else:
    update = str(start_year+1)+'-01-31'
for i in range(len(key_list)):
    if key_list[i] == 'snl':
        snl_pos = i
        break

# 回報錯誤、儲存錯誤檔案並結束程式
def ERROR(error_text):
    print('\n\n= ! = '+error_text+'\n\n')
    with open('./ERROR.log','w', encoding=ENCODING) as f:    #用with一次性完成open、close檔案
        f.write(error_text)
    sys.exit()

def readFile(dir, default=pd.DataFrame(), acceptNoFile=False, \
             header_=None,skiprows_=None,index_col_=None,skipfooter_=0,encoding_=ENCODING):
    try:
        t = pd.read_csv(dir, header=header_,skiprows=skiprows_,index_col=index_col_,skipfooter=skipfooter_,\
                        encoding=encoding_,engine='python')
        #print(t)
        return t
    except FileNotFoundError:
        if acceptNoFile:
            return default
        else:
            ERROR('找不到檔案：'+dir)
    except:
        try: #檔案編碼格式不同
            t = pd.read_csv(dir, header=header_,skiprows=skiprows_,index_col=index_col_,\
                        engine='python')
            #print(t)
            return t
        except:
            return default  #有檔案但是讀不了:多半是沒有限制式，使skiprow後為空。 一律用預設值

def readExcelFile(dir, default=pd.DataFrame(), acceptNoFile=True, \
             header_=None,skiprows_=None,index_col_=None,usecols_=None,skipfooter_=0,sheet_name_=None):
    try:
        t = pd.read_excel(dir,sheet_name=sheet_name_, header=header_,index_col=index_col_,skiprows=skiprows_,skipfooter=skipfooter_,usecols=usecols_)
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

def takeFirst(alist):
    return alist[0]

AREMOS_gerfin = readExcelFile(data_path+'gerfin.xlsx', header_ = [0], sheet_name_='gerfin')
Country = readFile(data_path+'Country.csv', header_ = 0)
CRC = Country.set_index('Country_Code').to_dict()
USD = ['REXA','REXE','REX']
EURO = ['EURDECB','EUREECB','EURD','EURE']
SDR = ['SDRA','SDRE']
OPP = ['REXD','REXI','EURECB','EURIECB','EURI','SDRDA','SDRDE','EUR']
AVG = ['EURDECB','EURECB','SDRA','SDRDA','REXA','REXD','EURD','EUR','REX','W']
END = ['EUREECB','EURIECB','SDRE','SDRDE','REXE','REXI','EURE','EURI']
def IHSBASE(name, suffix):
    if name[:1] == 'A':
        name = name+'.A'
        suffix = suffix+'.A'
    opp = ''
    for key in EURO:
        if name.find(key+suffix) >= 0:
            opp = False
            return 'Euro'
    for key in SDR:
        if name.find(key+suffix) >= 0:
            opp = False
            return 'Special Drawing Rights (SDR)'
    for key in OPP:
        if name.find(key+suffix) >= 0:
            opp = True
    for key in USD:
        if name.find(key+suffix) >= 0:
            opp = False
            return 'United States Dollar (USD)'
    if opp == True:
        code = name[1:4]
        if code == '001':
            code = '1'
        if code in CRC['Currency_Name']:
            return str(CRC['Currency_Name'][code])
        else:
            return ''
    else:
        return '' 
def IHSFORM(name, suffix):
    if name[:1] == 'A':
        name = name+'.A'
        suffix = suffix+'.A'
    for key in AVG:
        if name.find(key+suffix) >= 0:
            return 'Average of observations through period (A)'
    for key in END:
        if name.find(key+suffix) >= 0:
            return 'End of period (E)'
    
    return ''

def OLD_LEGACY(code):
    if code in CRC['Old_legacy_currency']:
        return str(CRC['Old_legacy_currency'][code])
    else:
        return code

Day_list = pd.date_range(start = str(start_year)+'-01-01', end = update).strftime('%Y-%m-%d')
nD = len(Day_list)
KEY_DATA = []
SORT_DATA_D = []
DATA_BASE_D = {}
db_table_D_t = pd.DataFrame(index = Day_list, columns = [])
DB_name_D = []
DB_TABLE = 'DB_'
DB_CODE = 'data'
    
table_num_D = 1
code_num_D = 1
snl = 1
if code_num_D == 200:
    code_num_D = 1
start_snl = snl
start_table_D = table_num_D
start_code_D = code_num_D
CONTINUE = []

#before1 = ['FOREIGN EXCHANGE',') PER','DATA)',')FROM','SOURCE','NOTE','RATESDR','RATESEMI','RATEEND','RATES','MARKET RATE','OFFICIAL RATE','PRINCIPAL RATE','USING','ONWARDD','WEDOLLAR','ESOFFICIAL','MILLIONS','NSAINTERNATIONAL','aA','aE','ReservesClaims','DollarsUnit','DollarSource','www.imf.org','FUNDCURRENCY','DATAU.S.','ORLUXEMBOURG','EMUEURO','Y DATA',' AS','HOUSEHOLDSCANNOT','NACIONALWHICH','WITH ',"#IES",'#']
#after1 = [' FOREIGN EXCHANGE ',') PER ','DATA): ',') FROM',', SOURCE',', NOTE','RATE SDR','RATE SEMI','RATE END','RATES ','MARKET RATE ','OFFICIAL RATE ','PRINCIPAL RATE ','USING ','ONWARD D','WE DOLLAR','ES OFFICIAL',' MILLIONS','NSA INTERNATIONAL','a A','a E','Reserves, Claims','Dollars; Unit','Dollar; Source','','FUND CURRENCY','DATA U.S.','OR LUXEMBOURG','EMU EURO','Y DATA ',' AS ','HOUSEHOLDS CANNOT','NACIONAL WHICH',' WITH ','IES',' ']
before2 = ['Ecb','1 Ecu','Sdr','Ifs','Ihs','Imf','Iso','Exchange S ','Rate S ','Am','Pm','Of ',"People S","People'S",'Usd','Us ','#Name?eekly','#Name?','Cfa','Cfp','Fx','Rate,,','Rate,','Nsa','Cofer','And ', 'In ',')Total','Or ','Luf','Emu ','Rexa','Rexeurd','Rexe','Rexeure','Rexi','Rexeuri','Subsidizedby','Ft','Idc']
after2 = ['ECB','1 ECU','SDR','IFS','IHS','IMF','ISO','Exchanges ','Rates ','am','pm','of ',"People's","People's",'USD','US ','weekly','','CFA','CFP','Foreign Exchange','Rate,','Rate.','NSA','COFER','and ','in ','): Total','or ','LUF','EMU ','REXA','REXEURD','REXE','REXEURE','REXI','REXEURI','Subsidized by','FT','IDC']
before3 = ['CYPrus','EURo']
after3 = ['Cyprus','Euro']

def GERFIN_DATA(ind, GERFIN_t, AREMOS_gerfin, value, index, code_num, table_num, KEY_DATA, SORT_DATA, DATA_BASE, db_table_t, DB_name, snl, freqlist, frequency, freqnum=None, freqsuffix=[], keysuffix=[], suffix='', wed=False):
    freqlen = len(freqlist)
    NonValue = 'nan'
    if code_num >= 200:
        db_table = DB_TABLE+frequency+'_'+str(table_num).rjust(4,'0')
        DATA_BASE[db_table] = db_table_t
        DB_name.append(db_table)
        table_num += 1
        code_num = 1
        db_table_t = pd.DataFrame(index = freqlist, columns = [])

    name = GERFIN_t.index[ind].replace('.HIST','').replace('.ARCH','')
    if GERFIN_t.iloc[i]['Frequency'] == 'Annual':
        name = name.replace('.A','')

    AREMOS_key = AREMOS_gerfin.loc[AREMOS_gerfin['code'] == name].to_dict('list')
    if pd.DataFrame(AREMOS_key).empty == True:
        CONTINUE.append(name)
        db_table = DB_TABLE+frequency+'_'+str(table_num).rjust(4,'0')
        return code_num, table_num, SORT_DATA, DATA_BASE, db_table, db_table_t, DB_name, snl
    
    db_table = DB_TABLE+frequency+'_'+str(table_num).rjust(4,'0')
    db_code = DB_CODE+str(code_num).rjust(3,'0')
    db_table_t[db_code] = ['' for tmp in range(freqlen)]
    desc_e = str(GERFIN_t.loc[GERFIN_t.index[ind], 'Long Label']).replace('\n',' ')
    desc_e = desc_e.title()
    for ph in range(len(before2)):
        desc_e = desc_e.replace(before2[ph],after2[ph])
    loc2 = desc_e.find('ISO Code:')+10
    loc3 = loc2+3
    loc4 = desc_e.find('ISO Codes:')+11
    loc5 = loc4+3
    if loc2-10 >= 0:
        desc_e = desc_e.replace(desc_e[loc2:loc3],desc_e[loc2:loc3].upper())
    if loc4-11 >= 0:
        desc_e = desc_e.replace(desc_e[loc4:loc5],desc_e[loc4:loc5].upper())
    for ph in range(len(before3)):
        desc_e = desc_e.replace(before3[ph],after3[ph])
    base = IHSBASE(name, suffix)
    form_e = IHSFORM(name, suffix)
    quote = str(GERFIN_t.loc[GERFIN_t.index[ind], 'Unit'])
    source = str(GERFIN_t.loc[GERFIN_t.index[ind], 'Source'])
    desc_c = ''
    form_c = ''
    
    start_found = False
    last_found = False
    found = False
    for k in range(len(value)):
        if not not keysuffix:
            for word in range(len(keysuffix)):
                if str(index[k]).find(keysuffix[word]) >= 0:
                    freq_index = str(index[k])[:freqnum]+freqsuffix[word]
                    if frequency == 'A':
                        freq_index = int(freq_index)
                    break
                else:
                    freq_index = 'Nan'
        else:
            if frequency == 'W':
                try:
                    if wed == True:
                        freq_index = (date.fromisoformat(index[k])-timedelta(days=4)).strftime('%Y-%m-%d')
                    else:
                        freq_index = (date.fromisoformat(index[k])-timedelta(days=6)).strftime('%Y-%m-%d')
                except ValueError:
                    freq_index = 'Nan'
            elif frequency == 'D':
                try:
                    freq_index = date.fromisoformat(index[k]).strftime('%Y-%m-%d')
                except ValueError:
                    freq_index = 'Nan'
            else:
                freq_index = 'Nan'
        if freq_index in db_table_t.index:
            if str(value[k]) == NonValue:
                db_table_t[db_code][freq_index] = ''
            else:
                found = True
                db_table_t[db_code][freq_index] = float(value[k])
                if start_found == False and found == True:
                    if frequency == 'A':
                        start = int(freq_index)
                    else:
                        start = str(freq_index)
                    start_found = True
                if start_found == True:
                    if k == len(value)-1:
                        if frequency == 'A':
                            last = int(freq_index)
                        else:
                            last = str(freq_index)
                        last_found = True
                    else:
                        for st in range(k+1, len(value)):
                            if not not keysuffix:
                                for word in range(len(keysuffix)):
                                    if str(index[st]).find(keysuffix[word]) >= 0:
                                        if str(value[st]) != NonValue:
                                            last_found = False
                                            break
                                        else:
                                            last_found = True
                                    else:
                                        last_found = True
                            else:
                                if str(value[st]) != NonValue:
                                    last_found = False
                                else:
                                    last_found = True
                            if last_found == False:
                                break
                        if last_found == True:
                            if frequency == 'A':
                                last = int(freq_index)
                            else:
                                last = str(freq_index)
        else:
            continue

    if start_found == False:
        if found == True:
            ERROR('start not found: '+str(name))
    elif last_found == False:
        if found == True:
            ERROR('last not found: '+str(name))
    if found == False:
        start = 'Nan'
        last = 'Nan'               

    key_tmp= [databank, name, db_table, db_code, desc_e, desc_c, frequency, start, last, base, quote, snl, source, form_e, form_c]
    KEY_DATA.append(key_tmp)
    sort_tmp = [name, snl, db_table, db_code, start]
    SORT_DATA.append(sort_tmp)
    snl += 1

    code_num += 1
    
    return code_num, table_num, SORT_DATA, DATA_BASE, db_table, db_table_t, DB_name, snl

#print(GERFIN_t.head(10))
tStart = time.time()


SHEET_NAME = ['Daily_5_week','Daily_7_week']
for freq in SHEET_NAME:
    print('Reading file: '+NAME+', sheet: '+freq+' Time: ', int(time.time() - tStart),'s'+'\n')
    GERFIN_t = readExcelFile(data_path+NAME+'.xlsx', header_ =0, index_col_=3, skiprows_=[0,1], skipfooter_=10, sheet_name_=freq)
    #print(GERFIN_t)
    index = []
    for dex in GERFIN_t.columns:
        if type(dex) == datetime:
            index.append(dex.strftime('%Y-%m-%d'))
        else:
            index.append(dex)
    
    nG = GERFIN_t.shape[0]
    print('Total Columns:',nG,'Time: ', int(time.time() - tStart),'s'+'\n')        
    for i in range(nG):
        sys.stdout.write("\rLoading...("+str(round((i+1)*100/nG, 1))+"%)*")
        sys.stdout.flush()

        value = list(GERFIN_t.iloc[i])
        if str(GERFIN_t.iloc[i]['Frequency']) == 'Daily (5/week)':
            frequency = 'D'
            code_num_D, table_num_D, SORT_DATA_D, DATA_BASE_D, db_table_D, db_table_D_t, DB_name_D, snl = GERFIN_DATA(i, GERFIN_t, AREMOS_gerfin, value, index, code_num_D, table_num_D, KEY_DATA, SORT_DATA_D, DATA_BASE_D, db_table_D_t, DB_name_D, snl, Day_list, frequency, suffix='.D')
        elif str(GERFIN_t.iloc[i]['Frequency']) == 'Daily (7/week)':
            frequency = 'D'
            code_num_D, table_num_D, SORT_DATA_D, DATA_BASE_D, db_table_D, db_table_D_t, DB_name_D, snl = GERFIN_DATA(i, GERFIN_t, AREMOS_gerfin, value, index, code_num_D, table_num_D, KEY_DATA, SORT_DATA_D, DATA_BASE_D, db_table_D_t, DB_name_D, snl, Day_list, frequency, suffix='.D')
            
    sys.stdout.write("\n\n") 

if db_table_D_t.empty == False:
    DATA_BASE_D[db_table_D] = db_table_D_t
    DB_name_D.append(db_table_D)      

print('Time: ', int(time.time() - tStart),'s'+'\n')    
SORT_DATA_D.sort(key=takeFirst)
repeated_D = 0
for i in range(1, len(SORT_DATA_D)):
    if SORT_DATA_D[i][0] == SORT_DATA_D[i-1][0]:
        repeated_D += 1
        if str(SORT_DATA_D[i-1][4]) == 'Nan':
            target = i-1
            try:
                DATA_BASE_D[SORT_DATA_D[target][2]].drop(columns = SORT_DATA_D[target][3])
            except KeyError:
                target = i
        else:
            target = i
        #print(SORT_DATA_D[i][0],' ',SORT_DATA_D[i-1][1],' ',SORT_DATA_D[i][1],' ',SORT_DATA_D[i][2],' ',SORT_DATA_D[i][3])
        for key in KEY_DATA:
            if key[snl_pos] == SORT_DATA_D[target][1]:
                #print(key)
                KEY_DATA.remove(key) 
                break
        DATA_BASE_D[SORT_DATA_D[target][2]] = DATA_BASE_D[SORT_DATA_D[target][2]].drop(columns = SORT_DATA_D[target][3])
        if DATA_BASE_D[SORT_DATA_D[target][2]].empty == True:
            DB_name_D.remove(SORT_DATA_D[target][2])
    sys.stdout.write("\r"+str(repeated_D)+" repeated daily data key(s) found")
    sys.stdout.flush()
sys.stdout.write("\n")

print('Time: ', int(time.time() - tStart),'s'+'\n')
df_key = pd.DataFrame(KEY_DATA, columns = key_list)
df_key = df_key.sort_values(by=['name', 'db_table'], ignore_index=True)
if df_key.iloc[0]['snl'] != start_snl:
    df_key.loc[0, 'snl'] = start_snl
for s in range(1,df_key.shape[0]):
    sys.stdout.write("\rSetting new snls: "+str(s))
    sys.stdout.flush()
    df_key.loc[s, 'snl'] = df_key.loc[0, 'snl'] + s
sys.stdout.write("\n")
#if repeated_D > 0:
print('Setting new files, Time: ', int(time.time() - tStart),'s'+'\n')

DATA_BASE_D_new = {}
db_table_D_t = pd.DataFrame(index = Day_list, columns = [])
DB_name_D_new = []
db_table_new = 0
db_code_new = 0
for f in range(df_key.shape[0]):
    sys.stdout.write("\rSetting new keys: "+str(db_table_new)+" "+str(db_code_new))
    sys.stdout.flush()
    if df_key.iloc[f]['freq'] == 'D':
        if start_code_D >= 200:
            DATA_BASE_D_new[db_table_D] = db_table_D_t
            DB_name_D_new.append(db_table_D)
            start_table_D += 1
            start_code_D = 1
            db_table_D_t = pd.DataFrame(index = Day_list, columns = [])
        db_table_D = DB_TABLE+'D_'+str(start_table_D).rjust(4,'0')
        db_code_D = DB_CODE+str(start_code_D).rjust(3,'0')
        db_table_D_t[db_code_D] = DATA_BASE_D[df_key.iloc[f]['db_table']][df_key.iloc[f]['db_code']]
        df_key.loc[f, 'db_table'] = db_table_D
        df_key.loc[f, 'db_code'] = db_code_D
        start_code_D += 1
        db_table_new = db_table_D
        db_code_new = db_code_D
    
    if f == df_key.shape[0]-1:
        if db_table_D_t.empty == False:
            DATA_BASE_D_new[db_table_D] = db_table_D_t
            DB_name_D_new.append(db_table_D)

sys.stdout.write("\n")
DATA_BASE_D = DATA_BASE_D_new
DB_name_D = DB_name_D_new

print(df_key)
#print(DATA_BASE_t)

print('Time: ', int(time.time() - tStart),'s'+'\n')
df_key.to_excel(out_path+NAME+"_key.xlsx", sheet_name=NAME+'_key')
with pd.ExcelWriter(out_path+NAME+"_database.xlsx") as writer: # pylint: disable=abstract-class-instantiated
    for d in DB_name_D:
        sys.stdout.write("\rOutputing sheet: "+str(d))
        sys.stdout.flush()
        if DATA_BASE_D[d].empty == False:
            DATA_BASE_D[d].to_excel(writer, sheet_name = d)
sys.stdout.write("\n")

print('Time: ', int(time.time() - tStart),'s'+'\n')
#print('Total items not found: ',len(CONTINUE), '\n')

OLCurrency = []
SDR = []
LEFT = []
DF_NAME = list(df_key['name'])
freq_list = ['A','M','Q','S','W']
for i in range(AREMOS_gerfin.shape[0]):
    if str(AREMOS_gerfin.loc[i, 'code']) not in DF_NAME:
        LEFT.append(AREMOS_gerfin.loc[i, 'code'])
    if OLD_LEGACY(str(AREMOS_gerfin.loc[i, 'code'])[1:4]) == 'Y' and str(AREMOS_gerfin.loc[i, 'code'])[:1] in freq_list and str(AREMOS_gerfin.loc[i, 'code']).find('REX') >= 0:
        if str(AREMOS_gerfin.loc[i, 'code']) not in DF_NAME:
            OLCurrency.append(AREMOS_gerfin.loc[i, 'code'])
    elif OLD_LEGACY(str(AREMOS_gerfin.loc[i, 'code'])[1:4]) == 'S' and str(AREMOS_gerfin.loc[i, 'code'])[:1] in freq_list and str(AREMOS_gerfin.loc[i, 'code']).find('REX') >= 0:
        if str(AREMOS_gerfin.loc[i, 'code']) not in DF_NAME:
            SDR.append(AREMOS_gerfin.loc[i, 'code'])
print('Total Old Legacy Currency items not found: ', len(OLCurrency), '\n')
print('Total International Monetary Fund (IMF) SDRs items not found: ', len(SDR), '\n')
print('Items not found: ', len(LEFT), '\n')
print('Time: ', int(time.time() - tStart),'s'+'\n')
