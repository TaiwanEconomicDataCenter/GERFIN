# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import math, re, sys, calendar, os, copy, time, numpy, logging
import pandas as pd
import numpy as np
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import GERFIN_concat as CCT
from GERFIN_concat import ERROR, readExcelFile

ENCODING = CCT.ENCODING
data_path = "./output/"

if CCT.excel_suffix != "0":
    local = False
    checkDESC = True
else:
    local = True #bool(int(input('Check from local data (1/0): ')))
    checkDESC = bool(int(input('Check data description (1/0): ')))

def GERFIN_identity(data_path, df_key, DF_KEY, checkNotFound=False, checkDESC=True, checkOnly='', checkIgnore=[], tStart=time.time(), start_year=1901):
    
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    FORMAT = '%(asctime)s %(message)s'
    logging.basicConfig(level=logging.INFO, format=FORMAT, handlers=[logging.FileHandler("TEST.log", 'w', CCT.ENCODING)], datefmt='%Y-%m-%d %I:%M:%S %p')
    logging.info('Checking Identities: '+str(int(time.time() - tStart))+'s'+'\n')
    df_key = df_key.set_index('name')
    
    unknown = 0
    unknown_list = []
    unknown_earliest = str(datetime.today().year)
    toolong = 0
    toolong_list = []
    toomanymonths = False
    includingweekly = False
    notsame = 0
    notsame_earliest = str(datetime.today().year)
    notsame_dict = {}
    updated = 0
    update_list = []
    CHECK = ['desc_e', 'desc_c', 'freq', 'base', 'quote', 'source', 'form_e', 'form_c']
    for c in CHECK:
        notsame_dict[c] = 0
    UPDATE = ['last']
    for ind in df_key.index:
        sys.stdout.write("\rChecking Index: "+ind+" ")
        sys.stdout.flush()
        if str(df_key.loc[ind, 'desc_e']).find(checkOnly) < 0:
            continue
        to_be_ignore = False
        for ignore in checkIgnore:
            if str(df_key.loc[ind, 'desc_e']).find(ignore) >= 0:
                to_be_ignore = True
                break
        if to_be_ignore == True:
            continue
        if len(ind) > 17:
            toolong_list.append(ind)
            toolong += 1
        if df_key.loc[ind, 'freq'] == 'M' and str(df_key.loc[ind, 'last']) !='Nan':
            if datetime.strptime(df_key.loc[ind, 'last'], '%Y-%m')-relativedelta(months=1000) > datetime.strptime(df_key.loc[ind, 'start'], '%Y-%m'):
                toomanymonths = True
        if df_key.loc[ind, 'freq'] == 'W':
            includingweekly = True
        if ind not in DF_KEY.index:
            #logging.info('Index Unknown: '+ind)
            unknown_list.append([ind, df_key.loc[ind, 'start']])
            if str(df_key.loc[ind, 'start'])[:4] < unknown_earliest:
                unknown_earliest = str(df_key.loc[ind, 'start'])[:4]
            unknown += 1
        else:
            for check in CHECK:
                if str(df_key.loc[ind, check]).strip().lower() != str(DF_KEY.loc[ind, check]).strip().lower():
                    if check == 'start' and (str(DF_KEY.loc[ind, check]).strip() == 'Nan' or str(df_key.loc[ind, check]).strip() < str(DF_KEY.loc[ind, check]).strip()):
                        continue
                    elif str(DF_KEY.loc[ind, check]).strip() == 'nan' and str(df_key.loc[ind, check]).strip() == '':
                        continue
                    elif checkDESC == False and (check == 'desc_e' or check == 'desc_c' or check == 'form_e' or check == 'form_c'):
                        continue
                    logging.info('Index '+ind+' '+check+' error')
                    if check == 'desc_e' and str(df_key.loc[ind, check]).replace(str(DF_KEY.loc[ind, check]), '') != str(df_key.loc[ind, check]):
                        logging.info('df_key(not equal part) = '+str(df_key.loc[ind, check]).replace(str(DF_KEY.loc[ind, check]), ''))
                    else:
                        logging.info('DF_KEY = '+str(DF_KEY.loc[ind, check]))
                        logging.info('df_key = '+str(df_key.loc[ind, check]))
                    notsame += 1
                    notsame_dict[check] += 1
                    if str(df_key.loc[ind, 'start'])[:4] < notsame_earliest:
                        notsame_earliest = str(df_key.loc[ind, 'start'])[:4]
            for update in UPDATE:
                if type(df_key.loc[ind, update]) != type(DF_KEY.loc[ind, update]) and df_key.loc[ind, update] != DF_KEY.loc[ind, update]:
                    if str(DF_KEY.loc[ind, update]).strip() == 'Nan' or ((type(DF_KEY.loc[ind, update]) == int or type(DF_KEY.loc[ind, update]) == numpy.int64) and (type(df_key.loc[ind, update]) == int or type(df_key.loc[ind, update]) == numpy.int64)):
                        continue
                    elif str(df_key.loc[ind, update]).strip() == 'Nan' and int(str(DF_KEY.loc[ind, update]).strip()[:4]) < start_year:
                        continue
                    logging.info('Index '+ind+' '+'Incorrect Time Type')
                    logging.info('DF_KEY = '+str(DF_KEY.loc[ind, update])+', type = '+str(type(DF_KEY.loc[ind, update])))
                    logging.info('df_key = '+str(df_key.loc[ind, update])+', type = '+str(type(df_key.loc[ind, update])))
                    continue
                if df_key.loc[ind, update] != DF_KEY.loc[ind, update] and str(DF_KEY.loc[ind, update]).strip() == 'Nan':
                    continue
                if df_key.loc[ind, update] > DF_KEY.loc[ind, update] and str(df_key.loc[ind, update]).strip() != 'Nan':
                    update_list.append(ind)
                    updated += 1
                elif df_key.loc[ind, update] < DF_KEY.loc[ind, update]:
                    logging.info('The program did not fetch the latest data for: '+ind)  
    sys.stdout.write("\n")
    log = logging.getLogger()
    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(logging.Formatter('%(message)s'))
    log.addHandler(stream)
    logging.info('unknown: '+str(unknown))
    if unknown != 0:
        logging.info('unknown earliest: '+str(unknown_earliest))
    logging.info('name too long: '+str(toolong))
    logging.info('inconsistent: '+str(notsame))
    if notsame != 0:
        for c in CHECK:
            logging.info('Total '+c+' inconsistent: '+str(notsame_dict[c]))
        logging.info('inconsistent earliest: '+str(notsame_earliest))
    logging.info('updated: '+str(updated))
    logging.info('including weekly: '+str(includingweekly))
    logging.info('too many months: '+str(toomanymonths))

    unfound = 0
    unfound_list = []
    if checkNotFound == True:
        for ind in DF_KEY.index:
            sys.stdout.write("\rChecking Index: "+ind+" ")
            sys.stdout.flush()
            if ind not in df_key.index:
                #print('Index Not Found: '+ind)
                unfound_list.append(ind)
                unfound += 1
        sys.stdout.write("\n")
        logging.info('unfound: '+str(unfound))
    
    print('Time: '+str(int(time.time() - tStart))+' s'+'\n')
    return unknown_list, toolong_list, update_list, unfound_list

if local == True:
    NAME = input('Bank (EIKON/GERFIN): ')
    with open(data_path+NAME+'_TOT_name.txt','r',encoding='ANSI') as f:
        DF_suffix = f.read()
    main_suf = input('Main data suffix: ')
    styr = int(input('Dealing Start Year of Main data: '))
    logging.info('Reading file: '+NAME+'_key'+main_suf+'\n')
    df_key = readExcelFile(data_path+NAME+'_key'+main_suf+'.xlsx', header_ = 0, acceptNoFile=False, index_col_=0, sheet_name_=NAME+'_key')
    logging.info('Reading TOT file: '+NAME+'_key'+DF_suffix+'\n')
    DF_KEY = readExcelFile(data_path+NAME+'_key'+DF_suffix+'.xlsx', header_ = 0, acceptNoFile=False, index_col_=0, sheet_name_=NAME+'_key')
    DF_KEY = DF_KEY.set_index('name') 
    unknown_list, toolong_list, update_list, unfound_list = GERFIN_identity(data_path, df_key, DF_KEY, checkDESC=checkDESC, start_year=styr)
