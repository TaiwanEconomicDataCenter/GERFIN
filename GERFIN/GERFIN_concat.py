# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import math, re, sys, calendar, os, copy, time, shutil, logging, traceback
import pandas as pd
import numpy as np
import requests as rq
from datetime import datetime, date
from urllib.error import HTTPError
from pathlib import Path
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import StaleElementReferenceException
from selenium.common.exceptions import ElementClickInterceptedException
from selenium.common.exceptions import JavascriptException
from selenium.common.exceptions import UnexpectedAlertPresentException
import webdriver_manager
from webdriver_manager.chrome import ChromeDriverManager
sys.path.append('../../TO_DB/TO_DB')
from TO_DB import SELECT_DF_KEY, SELECT_DATABASES, INSERT_TABLES

ENCODING = 'utf-8-sig'
data_path = "./data/"
out_path = "./output/"
excel_suffix = input('Output file suffix or mysql (If test identity press 0): ')
if excel_suffix.lower().strip() == 'mysql':
    excel_suffix = 'mysql'

def ERROR(error_text, waiting=False):
    if waiting == True:
        sys.stdout.write("\r"+error_text)
        sys.stdout.flush()
    else:
        sys.stdout.write('\n\n')
        logging.error('= ! = '+error_text)
        sys.stdout.write('\n\n')
    sys.exit()

def readFile(dir, default=pd.DataFrame(), acceptNoFile=False,header_=None,names_=None,skiprows_=None,index_col_=None,usecols_=None,skipfooter_=0,nrows_=None,encoding_=ENCODING,engine_='python',sep_=None, wait=False):
    try:
        t = pd.read_csv(dir, header=header_,skiprows=skiprows_,index_col=index_col_,skipfooter=skipfooter_,\
                        names=names_,usecols=usecols_,nrows=nrows_,encoding=encoding_,engine=engine_,sep=sep_)
        #print(t)
        return t
    except (OSError, FileNotFoundError):
        if acceptNoFile:
            return default
        else:
            if wait == True:
                ERROR('Waiting for Download...', waiting=True)
            else:
                ERROR('找不到檔案：'+dir)
    except HTTPError as err:
        if acceptNoFile:
            return default
        else:
            ERROR(str(err))
    except:
        try: #檔案編碼格式不同
            t = pd.read_csv(dir, header=header_,skiprows=skiprows_,index_col=index_col_,skipfooter=skipfooter_,\
                        names=names_,usecols=usecols_,nrows=nrows_,engine=engine_,sep=sep_)
            #print(t)
            return t
        except UnicodeDecodeError as err:
            ERROR(str(err))

def readExcelFile(dir, default=pd.DataFrame(), acceptNoFile=True, na_filter_=True, \
             header_=None,names_=None,skiprows_=None,index_col_=None,usecols_=None,skipfooter_=0,nrows_=None,sheet_name_=None,engine_=None,wait=False):
    try:
        t = pd.read_excel(dir,sheet_name=sheet_name_, header=header_,names=names_,index_col=index_col_,skiprows=skiprows_,skipfooter=skipfooter_,usecols=usecols_,nrows=nrows_,na_filter=na_filter_,engine=engine_)
        #print(t)
        return t
    except (OSError, FileNotFoundError):
        if acceptNoFile:
            return default
        else:
            if wait == True:
                ERROR('Waiting for Download...', waiting=True)
            else:
                ERROR('找不到檔案：'+dir)
    except:
        try: #檔案編碼格式不同
            t = pd.read_excel(dir,sheet_name=sheet_name_, header=header_,names=names_,index_col=index_col_,skiprows=skiprows_,skipfooter=skipfooter_,usecols=usecols_,nrows=nrows_,na_filter=na_filter_)
            #print(t)
            return t
        except UnicodeDecodeError as err:
            ERROR(str(err))

def PRESENT(file_path):
    if os.path.isfile(file_path) and datetime.fromtimestamp(os.path.getmtime(file_path)).strftime('%Y-%V') == datetime.today().strftime('%Y-%V'):
        logging.info('Present File Exists. Reading Data From Default Path.\n')
        return True
    else:
        return False

def GERFIN_WEBDRIVER(chrome, file_name, header=None, index_col=None, skiprows=None, usecols=None, names=None, csv=True, Zip=False):

    chrome.execute_script("window.open()")
    chrome.switch_to.window(chrome.window_handles[-1])
    chrome.get('chrome://downloads')
    time.sleep(3)
    try:
        if chrome.execute_script("return document.querySelector('downloads-manager').shadowRoot.querySelector('#downloadsList downloads-item').shadowRoot.querySelector('div#content #tag')").text == '已刪除':
            ERROR('The file was not properly downloaded')
    except JavascriptException:
        ERROR('The file was not properly downloaded')
    excel_file = chrome.execute_script("return document.querySelector('downloads-manager').shadowRoot.querySelector('#downloadsList downloads-item').shadowRoot.querySelector('div#content  #file-link').text")
    new_file_name = file_name+re.sub(r'.+?(\..+)$', r"\1", excel_file)
    chrome.close()
    chrome.switch_to.window(chrome.window_handles[0])
    GERFIN_t = pd.DataFrame()
    while True:
        try:
            if Zip == True:
                GERFIN_zip = new_file_name
                if os.path.isfile((Path.home() / "Downloads" / excel_file)) == False:
                    sys.stdout.write("\rWaiting for Download...")
                    sys.stdout.flush()
                    raise FileNotFoundError
            else:
                if csv == True:
                    GERFIN_t = readFile((Path.home() / "Downloads" / excel_file).as_posix(), header_=header, index_col_=index_col, skiprows_=skiprows, acceptNoFile=False, usecols_=usecols, names_=names, wait=True)
                else:
                    GERFIN_t = readExcelFile((Path.home() / "Downloads" / excel_file).as_posix(), header_=header, index_col_=index_col, skiprows_=skiprows, sheet_name_=0, acceptNoFile=False, usecols_=usecols, names_=names, wait=True)
            if type(GERFIN_t) != dict and GERFIN_t.empty == True and Zip == False:
                break
        except:
            time.sleep(1)
        else:
            sys.stdout.write('\nDownload Complete\n\n')
            if os.path.isfile((Path.home() / "Downloads" / new_file_name)) and excel_file != new_file_name:
                os.remove((Path.home() / "Downloads" / new_file_name))
            os.rename((Path.home() / "Downloads" / excel_file), (Path.home() / "Downloads" / new_file_name))
            if os.path.isfile(data_path+new_file_name):
                if datetime.fromtimestamp(os.path.getmtime(data_path+new_file_name)).strftime('%Y-%m') ==\
                     datetime.fromtimestamp(os.path.getmtime((Path.home() / "Downloads" / new_file_name))).strftime('%Y-%m'):
                    os.remove(data_path+new_file_name)
                else:
                    if os.path.isfile(data_path+'old/'+new_file_name):
                        os.remove(data_path+'old/'+new_file_name)
                    shutil.move(data_path+new_file_name, data_path+'old/'+new_file_name)
            shutil.move((Path.home() / "Downloads" / new_file_name), data_path+new_file_name)
            break
    if type(GERFIN_t) != dict and GERFIN_t.empty == True and Zip == False:
        ERROR('Empty DataFrame')

    if Zip == True:
        return GERFIN_zip
    else:
        return GERFIN_t

def GERFIN_WEB_LINK(chrome, fname, keyword, get_attribute='href', text_match=False):
    
    link_list = WebDriverWait(chrome, 10).until(EC.presence_of_all_elements_located((By.XPATH, './/*[@href]')))
    link_found = False
    for link in link_list:
        if (text_match == True and link.text.find(keyword) >= 0) or (text_match == False and link.get_attribute(get_attribute).find(keyword) >= 0):
            link_found = True
            link.click()
            break
    link_meassage = None
    if link_found == False:
        if text_match == True:
            key_string = link.text
        else:
            key_string = link.get_attribute(get_attribute)
        link_meassage = 'Link Not Found in key string: '+key_string+', key = '+keyword
    return link_found, link_meassage

def GERFIN_WEB(chrome, g, file_name, url, header=None, index_col=0, skiprows=None, csv=False, output=False, Zip=False, start_year=None):

    link_found = False
    link_message = None
    logging.info('Downloading file: GERFIN_'+str(g)+'\n')
    chrome.get(url)

    y = 0
    DOWN = 2
    height = chrome.execute_script("return document.documentElement.scrollHeight")
    while True:
        if link_found == True:
            break
        try:
            chrome.execute_script("window.scrollTo(0,"+str(y)+")")
            if g == 1 or g == 4:
                #選取頻率
                WebDriverWait(chrome, 15).until(EC.element_to_be_clickable((By.XPATH, './/div[select[@name="FREQ.18"]]/div/ul'))).click()
                for d in range(DOWN):
                    ActionChains(chrome).send_keys(Keys.DOWN).perform()
                ActionChains(chrome).send_keys(Keys.ENTER).click(chrome.find_element_by_xpath('.//div[select[@name="FREQ.18"]]/div/ul/li/input')).perform() #確認選取
                #檢查是否有沒選到的頻率或是選到不該選的頻率
                WebDriverWait(chrome, 15).until(EC.visibility_of_element_located((By.XPATH, './/div[select[@name="FREQ.18"]]/div/ul/li[@class="select2-search-choice"]/div')))
                if chrome.find_element_by_xpath('.//div[select[@name="FREQ.18"]]/div/ul/li[@class="select2-search-choice"]/div').text != 'D':
                    chrome.refresh()
                    raise FileNotFoundError
                #讀取資料表
                sys.stdout.write("\rWaiting for Download...")
                sys.stdout.flush()
                chrome.find_element_by_xpath('.//a[@class="dataTable"]').click()
                while True:
                    sys.stdout.write("\rWaiting for Download...")
                    sys.stdout.flush()
                    try:
                        #WebDriverWait(chrome, 10).until(EC.visibility_of_element_located((By.XPATH, './/input[@name="start"]'))).send_keys('01-01-'+str(start_year))
                        GERFIN_t = pd.read_html(chrome.find_element_by_id('dataTableID').get_attribute('outerHTML'), header=header, index_col=index_col, skiprows=skiprows)[0]
                        GERFIN_t.index.name = 'Period'
                    except NoSuchElementException:
                        time.sleep(1)
                    else:
                        sys.stdout.write('\nDownload Complete\n\n')
                        break
                #ActionChains(chrome).send_keys(Keys.ENTER).perform()
                link_found = True
            elif g == 2:
                chrome.refresh()
                try:
                    WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.XPATH, './/a[text()="Remove all"]')))
                except TimeoutException:
                    WebDriverWait(chrome, 10).until(EC.element_to_be_clickable((By.XPATH, './/a[text()="Add all"]'))).click()
                while True:
                    try:
                        #chrome.execute_script("window.scrollTo(0,100)")
                        link_found, link_meassage = GERFIN_WEB_LINK(chrome, url, keyword='data-basket')
                    except (UnexpectedAlertPresentException, ElementClickInterceptedException):
                        time.sleep(0.5)
                    else:
                        break
                chrome.execute_script("window.scrollTo(0,200)")
                WebDriverWait(chrome, 10).until(EC.visibility_of_element_located((By.XPATH, './/input[@name="its_from"]'))).send_keys(str(start_year))
                target = WebDriverWait(chrome, 5).until(EC.element_to_be_clickable((By.XPATH, './/label[contains(., "English")]'))).location_once_scrolled_into_view
                WebDriverWait(chrome, 5).until(EC.element_to_be_clickable((By.XPATH, './/label[contains(., "English")]'))).click()
                chrome.refresh()
                chrome.execute_script("window.scrollTo(0,0)")
                chrome.find_element_by_xpath('.//input[@value="Go to download"]').click()
                link_found, link_meassage = GERFIN_WEB_LINK(chrome, url, keyword='Daily series', text_match=True)
            elif g == 3:
                email = open(data_path+'email.txt','r',encoding='ANSI').read()
                password = open(data_path+'password.txt','r',encoding='ANSI').read()
                try:
                    WebDriverWait(chrome, 10).until(EC.visibility_of_element_located((By.ID, 'eml'))).send_keys(email)
                    WebDriverWait(chrome, 5).until(EC.visibility_of_element_located((By.ID, 'pw'))).send_keys(password)
                    time.sleep(1)
                    WebDriverWait(chrome, 5).until(EC.element_to_be_clickable((By.XPATH, './/input[@type="submit"]'))).click()
                    time.sleep(2)
                except TimeoutException:
                    time.sleep(0)
                chrome.refresh()
                WebDriverWait(chrome, 10).until(EC.visibility_of_element_located((By.ID, 'content-table')))
                time.sleep(2)
                link_found, link_meassage = GERFIN_WEB_LINK(chrome, url, keyword='GERFIN', text_match=True)
                time.sleep(2)
                link_found, link_meassage = GERFIN_WEB_LINK(chrome, url, keyword='download')
                WebDriverWait(chrome, 2).until(EC.element_to_be_clickable((By.XPATH, './/input[@name="download_data"]'))).click()
                link_found = True
            if link_found == False:
                raise FileNotFoundError
        except (FileNotFoundError, TimeoutException, StaleElementReferenceException, ElementClickInterceptedException):
            print(str(traceback.format_exc())[:2000])
            if g == 1 or g == 4:
                DOWN+=1
            else:
                y+=500
            if (y > height and link_found == False) or DOWN > 5:
                if link_message != None:
                    ERROR(link_message)
                else:
                    if DOWN > 5:
                        ERROR('Frequency of Daily was not able to be selected.')
                    else:
                        ERROR('Download File Not Found.')
        except Exception as e:
            ERROR(str(e))
        else:
            break
    time.sleep(3)
    if output == True:
        GERFIN_t.to_csv(data_path+file_name+'.csv')
    else:
        GERFIN_t = GERFIN_WEBDRIVER(chrome, file_name, header=header, index_col=index_col, skiprows=skiprows, csv=csv, Zip=Zip)

    return GERFIN_t

def MERGE(merge_file, DB_TABLE, DB_CODE, freq):
    i = 0
    found = False
    while found == False:
        i += 1
        if DB_TABLE+freq+'_'+str(i).rjust(4,'0') not in list(merge_file['db_table']) and i > 1:
            found = True
            code_t = []
            for c in range(merge_file.shape[0]):
                if merge_file['db_table'][c] == DB_TABLE+freq+'_'+str(i-1).rjust(4,'0'):
                    code_t.append(merge_file['db_code'][c])
            if max(code_t) == DB_CODE+str(199).rjust(3,'0'):
                table_num = i
                code_num = 1
            else:
                table_num = i-1
                code_num = int(max(code_t).replace(DB_CODE, ''))+1
        elif DB_TABLE+freq+'_'+str(i).rjust(4,'0') not in list(merge_file['db_table']) and i == 1:
            found = True
            table_num = 1
            code_num = 1
    
    return table_num, code_num

def NEW_KEYS(f, freq, FREQLIST, DB_TABLE, DB_CODE, df_key, DATA_BASE, db_table_t, start_table, start_code, DATA_BASE_new, DB_name_new):
    
    if start_code >= 200:
        if freq == 'W':
            db_table_t = db_table_t.reindex(FREQLIST['W_s'])
        DATA_BASE_new[DB_TABLE+freq+'_'+str(start_table).rjust(4,'0')] = db_table_t
        DB_name_new.append(DB_TABLE+freq+'_'+str(start_table).rjust(4,'0'))
        start_table += 1
        start_code = 1
        db_table_t = pd.DataFrame(index = FREQLIST[freq], columns = [])
    db_table = DB_TABLE+freq+'_'+str(start_table).rjust(4,'0')
    db_code = DB_CODE+str(start_code).rjust(3,'0')
    #db_table_t[db_code] = DATA_BASE[df_key.iloc[f]['db_table']][df_key.iloc[f]['db_code']]
    db_table_t = pd.concat([db_table_t, pd.DataFrame(list(DATA_BASE[df_key.iloc[f]['db_table']][df_key.iloc[f]['db_code']]), index=DATA_BASE[df_key.iloc[f]['db_table']][df_key.iloc[f]['db_code']].index, columns=[db_code])], axis=1)
    df_key.loc[f, 'db_table'] = db_table
    df_key.loc[f, 'db_code'] = db_code
    start_code += 1
    db_table_new = db_table
    db_code_new = db_code
    
    return df_key, DATA_BASE_new, DB_name_new, db_table_t, start_table, start_code, db_table_new, db_code_new

def CONCATE(NAME, suf, data_path, DB_TABLE, DB_CODE, FREQNAME, FREQLIST, tStart, df_key, KEY_DATA_t, DB_dict, DB_name_dict, find_unknown=True, DATA_BASE_t=None):
    if find_unknown == True:
        repeated_standard = 'start'
    else:
        repeated_standard = 'last'
    #print('Reading file: '+NAME+'key'+suf+', Time: ', int(time.time() - tStart),'s'+'\n')
    #KEY_DATA_t = readExcelFile(data_path+NAME+'key'+suf+'.xlsx', header_ = 0, index_col_=0, sheet_name_=NAME+'key')
    if DATA_BASE_t == None:
        try:
            with open(data_path+NAME+'database_num'+suf+'.txt','r',encoding=ENCODING) as f:  #用with一次性完成open、close檔案
                database_num = int(f.read().replace('\n', ''))
            DATA_BASE_t = {}
            for i in range(1,database_num+1):
                logging.info('Reading file: '+NAME+'database_'+str(i)+suf+', Time: '+str(int(time.time() - tStart))+' s'+'\n')
                DB_t = readExcelFile(data_path+NAME+'database_'+str(i)+suf+'.xlsx', header_ = 0, index_col_=0, acceptNoFile=False, sheet_name_=None)
                for d in DB_t.keys():
                    DATA_BASE_t[d] = DB_t[d]
        except:
            logging.info('Reading file: '+NAME+'database'+suf+', Time: '+str(int(time.time() - tStart))+' s'+'\n')
            DATA_BASE_t = readExcelFile(data_path+NAME+'database'+suf+'.xlsx', header_ = 0, index_col_=0)
    if KEY_DATA_t.empty == False and type(DATA_BASE_t) != dict:
        ERROR(NAME+'database'+suf+'.xlsx Not Found.')
    elif type(DATA_BASE_t) != dict:
        DATA_BASE_t = {}
    
    logging.info('Concating file: '+NAME+'key'+suf+', Time: '+str(int(time.time() - tStart))+' s'+'\n')
    KEY_DATA_t = pd.concat([KEY_DATA_t, df_key], ignore_index=True)
    
    logging.info('Concating file: '+NAME+'database'+suf+', Time: '+str(int(time.time() - tStart))+' s'+'\n')
    for f in FREQNAME:
        for d in DB_name_dict[f]:
            sys.stdout.write("\rConcating sheet: "+str(d))
            sys.stdout.flush()
            if d in DATA_BASE_t.keys():
                DATA_BASE_t[d] = DATA_BASE_t[d].join(DB_dict[f][d], how='outer')
                DATA_BASE_t[d] = DATA_BASE_t[d].sort_index(ascending=False)
            else:
                DATA_BASE_t[d] = DB_dict[f][d]
        sys.stdout.write("\n")

    logging.info('Time: '+str(int(time.time() - tStart))+' s'+'\n')
    KEY_DATA_t = KEY_DATA_t.sort_values(by=['freq', 'name', 'db_table', 'snl'], ignore_index=True)
    
    repeated = 0
    repeated_index = []
    Repeat = {}
    Repeat['key'] = []
    Repeat[repeated_standard] = []
    for i in range(1, len(KEY_DATA_t)):
        if i in Repeat['key']:
            continue
        Repeat['key'] = []
        Repeat[repeated_standard] = []
        if KEY_DATA_t.iloc[i]['name'] == KEY_DATA_t.iloc[i-1]['name']:
            j = i
            Repeat['key'].append(j-1)
            Repeat[repeated_standard].append(str(KEY_DATA_t.iloc[j-1][repeated_standard]))
            while KEY_DATA_t.iloc[j]['name'] == KEY_DATA_t.iloc[j-1]['name']:
                repeated += 1
                Repeat['key'].append(j)
                Repeat[repeated_standard].append(str(KEY_DATA_t.iloc[j][repeated_standard]))
                j += 1
                if j >= len(KEY_DATA_t):
                    break
            if repeated_standard == 'start':
                keep = Repeat['key'][Repeat[repeated_standard].index(min(Repeat[repeated_standard]))]
            elif repeated_standard == 'last':
                keep = Repeat['key'][Repeat[repeated_standard].index(max(Repeat[repeated_standard]))]
            for k in Repeat['key']:
                if k != keep and ((repeated_standard == 'start' and Repeat[repeated_standard][Repeat['key'].index(k)] == min(Repeat[repeated_standard])) or (repeated_standard == 'last' and Repeat[repeated_standard][Repeat['key'].index(k)] == max(Repeat[repeated_standard]))):
                    if k < keep or Repeat[repeated_standard][Repeat['key'].index(keep)] == 'Nan':
                        repeated_index.append(keep)
                        keep = k
                    else:
                        repeated_index.append(k)
                elif k != keep and ((repeated_standard == 'start' and Repeat[repeated_standard][Repeat['key'].index(k)] > min(Repeat[repeated_standard])) or (repeated_standard == 'last' and Repeat[repeated_standard][Repeat['key'].index(k)] < max(Repeat[repeated_standard]))):
                    if Repeat[repeated_standard][Repeat['key'].index(keep)] == 'Nan':
                        repeated_index.append(keep)
                        keep = k
                    else:
                        repeated_index.append(k)
        sys.stdout.write("\r"+str(repeated)+" repeated data key(s) found ("+str(round((i+1)*100/len(KEY_DATA_t), 1))+"%)*")
        sys.stdout.flush()
    sys.stdout.write("\n")
    for target in repeated_index:
        sys.stdout.write("\rDropping repeated database column(s)...("+str(round((repeated_index.index(target)+1)*100/len(repeated_index), 1))+"%)*")
        sys.stdout.flush()
        DATA_BASE_t[KEY_DATA_t.iloc[target]['db_table']] = DATA_BASE_t[KEY_DATA_t.iloc[target]['db_table']].drop(columns = KEY_DATA_t.iloc[target]['db_code'])
    sys.stdout.write("\n")
    KEY_DATA_t = KEY_DATA_t.drop(repeated_index)
    KEY_DATA_t.reset_index(drop=True, inplace=True)
    #print(KEY_DATA_t)
    print('Time: ', int(time.time() - tStart),'s'+'\n')
    for s in range(KEY_DATA_t.shape[0]):
        sys.stdout.write("\rSetting new snls: "+str(s+1))
        sys.stdout.flush()
        KEY_DATA_t.loc[s, 'snl'] = s+1
    sys.stdout.write("\n")
    logging.info('Setting new files, Time: '+str(int(time.time() - tStart))+' s'+'\n')
    
    start_table_dict = {}
    start_code_dict = {}
    DB_new_dict = {}
    db_table_t_dict = {}
    DB_name_new_dict = {}
    for f in FREQNAME:
        start_table_dict[f] = 1
        start_code_dict[f] = 1
        DB_new_dict[f] = {}
        db_table_t_dict[f] = pd.DataFrame(index = FREQLIST[f], columns = [])
        DB_name_new_dict[f] = []
    db_table_new = 0
    db_code_new = 0
    for f in range(KEY_DATA_t.shape[0]):
        sys.stdout.write("\rSetting new keys: "+str(db_table_new)+" "+str(db_code_new))
        sys.stdout.flush()
        freq = KEY_DATA_t.iloc[f]['freq']
        if not DB_name_dict[freq]:
            db_table_new = KEY_DATA_t.iloc[f]['db_table']
            db_code_new = KEY_DATA_t.iloc[f]['db_code']
            if db_table_new not in DB_dict[freq].keys():
                DB_dict[freq][db_table_new] = DATA_BASE_t[db_table_new]
            continue
        df_key, DB_new_dict[freq], DB_name_new_dict[freq], db_table_t_dict[freq], start_table_dict[freq], start_code_dict[freq], db_table_new, db_code_new = \
            NEW_KEYS(f, freq, FREQLIST, DB_TABLE, DB_CODE, KEY_DATA_t, DATA_BASE_t, db_table_t_dict[freq], start_table_dict[freq], start_code_dict[freq], DB_new_dict[freq], DB_name_new_dict[freq])
    sys.stdout.write("\n")
    for f in FREQNAME:
        if db_table_t_dict[f].empty == False:
            DB_new_dict[f][DB_TABLE+f+'_'+str(start_table_dict[f]).rjust(4,'0')] = db_table_t_dict[f]
            DB_name_new_dict[f].append(DB_TABLE+f+'_'+str(start_table_dict[f]).rjust(4,'0'))
        if not not DB_name_new_dict[f]:
            DB_dict[f] = DB_new_dict[f]
            DB_name_dict[f] = DB_name_new_dict[f]

    logging.info('Concating new files: '+NAME+'database, Time: '+str(int(time.time() - tStart))+' s'+'\n')
    DATA_BASE_dict = {}
    for f in FREQNAME:
        if not DB_name_dict[f]:
            for key in DB_dict[f]:
                sys.stdout.write("\rConcating sheet: "+str(key))
                sys.stdout.flush()
                DATA_BASE_dict[key] = DB_dict[f][key]
            sys.stdout.write("\n")
            continue
        #DATA_BASE_dict[f] = {}
        for d in DB_name_dict[f]:
            sys.stdout.write("\rConcating sheet: "+str(d))
            sys.stdout.flush()
            #DATA_BASE_dict[f][d] = DB_dict[f][d]
            DATA_BASE_dict[d] = DB_dict[f][d]
        sys.stdout.write("\n")
    
    #print(KEY_DATA_t)
    print('Time: ', int(time.time() - tStart),'s'+'\n')

    return KEY_DATA_t, DATA_BASE_dict

def UPDATE(original_file, updated_file, key_list, NAME, data_path, orig_suf, up_suf, FREQLIST=None, original_database=None, updated_database=None):
    updated = 0
    tStart = time.time()
    logging.info('Updating file: '+str(int(time.time() - tStart))+' s'+'\n')
    if original_database == None:
        try:
            with open(data_path+NAME+'database_num'+orig_suf+'.txt','r',encoding=ENCODING) as f:  #用with一次性完成open、close檔案
                database_num = int(f.read().replace('\n', ''))
            original_database = {}
            for i in range(1,database_num+1):
                logging.info('Reading original database: '+NAME+'database_'+str(i)+orig_suf+', Time: '+str(int(time.time() - tStart))+' s'+'\n')
                DB_t = readExcelFile(data_path+NAME+'database_'+str(i)+orig_suf+'.xlsx', header_ = 0, index_col_=0, acceptNoFile=False, sheet_name_=None)
                for d in DB_t.keys():
                    original_database[d] = DB_t[d]
        except:
            logging.info('Reading original database: '+NAME+'database'+orig_suf+'.xlsx, Time: '+str(int(time.time() - tStart))+' s'+'\n')
            original_database = readExcelFile(data_path+NAME+'database'+orig_suf+'.xlsx', header_ = 0, index_col_=0, acceptNoFile=False)
    if updated_database == None:
        try:
            with open(data_path+NAME+'database_num'+up_suf+'.txt','r',encoding=ENCODING) as f:  #用with一次性完成open、close檔案
                database_num = int(f.read().replace('\n', ''))
            updated_database = {}
            for i in range(1,database_num+1):
                logging.info('Reading updated database: '+NAME+'database_'+str(i)+up_suf+', Time: '+str(int(time.time() - tStart))+' s'+'\n')
                DB_t = readExcelFile(data_path+NAME+'database_'+str(i)+up_suf+'.xlsx', header_ = 0, index_col_=0, acceptNoFile=False, sheet_name_=None)
                for d in DB_t.keys():
                    updated_database[d] = DB_t[d]
        except:
            logging.info('Reading updated database: '+NAME+'database'+up_suf+'.xlsx, Time: '+str(int(time.time() - tStart))+' s'+'\n')
            updated_database = readExcelFile(data_path+NAME+'database'+up_suf+'.xlsx', header_ = 0, index_col_=0, acceptNoFile=False)
    CAT = ['desc_e', 'old_name', 'form_e', 'form_c']
    
    original_file = original_file.set_index('name')
    updated_file = updated_file.set_index('name')
    for ind in updated_file.index:
        sys.stdout.write("\rUpdating latest data time ("+str(round((list(updated_file.index).index(ind)+1)*100/len(updated_file.index), 2))+"%): "+ind+" ")
        sys.stdout.flush()

        if ind in original_file.index:
            for c in CAT:
                original_file.loc[ind, c] = updated_file.loc[ind, c]
            if updated_file.loc[ind, 'last'] == 'Nan':
                continue
            elif ((original_file.loc[ind, 'last'] == 'Nan' or str(original_file.loc[ind, 'last']) == 'None') and updated_file.loc[ind, 'last'] != 'Nan') or updated_file.loc[ind, 'last'] > original_file.loc[ind, 'last']:
                updated+=1
            if updated_file.loc[ind, 'last'] != 'Nan':
                original_file.loc[ind, 'last'] = updated_file.loc[ind, 'last']
            if updated_file.loc[ind, 'start'] != 'Nan' and (original_file.loc[ind, 'start'] == 'Nan' or str(original_file.loc[ind, 'start']) == 'None' or updated_file.loc[ind, 'start'] < original_file.loc[ind, 'start']):
                original_file.loc[ind, 'start'] = updated_file.loc[ind, 'start']
            for period in updated_database[updated_file.loc[ind, 'db_table']].index:
                if updated_file.loc[ind, 'db_table'][3] == 'W' and type(period) != str:
                    period = period.strftime('%Y-%m-%d')
                if str(updated_database[updated_file.loc[ind, 'db_table']].loc[period, updated_file.loc[ind, 'db_code']]) != 'nan':
                    original_database[original_file.loc[ind, 'db_table']].loc[period, original_file.loc[ind, 'db_code']] = updated_database[updated_file.loc[ind, 'db_table']].loc[period, updated_file.loc[ind, 'db_code']]
                elif period >= updated_file.loc[ind, 'start'] and str(updated_database[updated_file.loc[ind, 'db_table']].loc[period, updated_file.loc[ind, 'db_code']]) == 'nan':
                    original_database[original_file.loc[ind, 'db_table']].loc[period, original_file.loc[ind, 'db_code']] = ''
        else:
            ERROR('Updated file index does not belongs to the original file index list: '+ind)
    sys.stdout.write("\n\n")
    for key in original_database.keys():
        original_database[key] = original_database[key].sort_index(axis=0, ascending=False)
        if FREQLIST != None:
            original_database[key] = original_database[key].reindex(FREQLIST[key[3]]) #key[3]==D
    original_file = original_file.reset_index()
    original_file = original_file.reindex(key_list, axis='columns')
    logging.info('updated: '+str(updated)+'\n')
    logging.info('Time: '+str(int(time.time() - tStart))+' s'+'\n')

    return original_file, original_database
