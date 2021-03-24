import os
from os import listdir
from os.path import isfile, join
from pathlib import Path
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import shutil
import calendar
import traceback

from urllib3.util.retry import Retry
from azure.storage.blob import BlobClient

from modules import utils
from modules import cryptoUtil
from modules import common
from modules.resource import conf


def setConfig(app_profile='dev'):

    # temp path 
    conf.temp_dir = str(Path.home().as_posix()) + "/data/collect_temp"

    # config setting
    conf.db_config = conf.conf_info[app_profile]['db_config']

    # config setting from db info
    code_data = common.getCode({'code_grp_id_telegram':'TELEGRAM', 'code_grp_id_blob':'BLOB_STORAGE'})
    conf.telegram_config = utils.row2dict([x for x in code_data if x['code_grp_id'] == 'TELEGRAM'], 'code', 'code_nm')
    conf.blob_config = utils.row2dict([x for x in code_data if x['code_grp_id'] == 'BLOB_STORAGE'], 'code', 'code_nm')

# list를 dictionary 로 변경
def row2dict(arr, key, val):
    dict = {}
    # print(arr)
    for v in arr:
        dict[v[key]] = v[val]
        # print(v[key], v[val])
    return dict


# dictionary 값 검색
def findDictByKey(obj, key):
    if key in obj: return obj[key]
    for k, v in obj.items():
        if isinstance(v,dict):
            return findDictByKey(v, key)
        elif type(v) == type({}):
            return findDictByKey(v, key)

# object 전체 검색
def findDeep(dictionary, key):
    if type(dictionary) == type({}):
        for k, v in dictionary.items():
            if k == key:
                yield v
            elif isinstance(v, dict):
                for result in findDeep(v, key):
                    yield result
            elif isinstance(v, list):
                for d in v:
                    for result in findDeep(d, key):
                        yield result

# 하위 디렉토리 전체 검색
def findDeepChildFiles(path):
    if os.path.exists(path):
        path_list = listdir(path)

        for p in path_list:
            f_path = join(path, p)
            if isfile(f_path):
                yield f_path
            else:
                yield from findDeepChildFiles(f_path)

# 하위파일 조회
# def getChildFiles(root_dir):
#     if os.path.exists(root_dir):
#         files = os.listdir(root_dir)
#         for file in files:
#             path = os.path.join(root_dir, file)

#             if os.path.isdir(path):
#                 yield from getChildFiles(path)
#             else:
#                 yield path

# list 객체 값 가져오기
def getListValue(row, cond_key, cond_val, get_key):
    for v in row:
        if v[cond_key] == cond_val:
            return v[get_key]


# list 객체 값 셋팅하기
def setListValue(row, cond_key, cond_val, set_key, set_val):
    for v in row:
        if v[cond_key] == cond_val:
            v[set_key] = set_val


# 마지막 array 삭제
def popLastArray(arr):
    # download 완료후 마지막 path는 비워준다.
    del arr[-1]

# 기간별 평일 date list 가져오기
def getWeekDateRange(params, weekday_only=False):

    date_list=[]
    if 'rg_st_dt' in params and 'rg_ed_dt' in params:
        from datetime import datetime, timedelta
        format_str = '%Y%m%d'
        st_dt = datetime.strptime(params['rg_st_dt'], format_str)
        ed_dt = datetime.strptime(params['rg_ed_dt'], format_str)
        for x in range((ed_dt - st_dt).days+1):
            day = st_dt + timedelta(days=x)
            # 평일 필터
            if weekday_only and day.weekday() < 5:
                date_list.append(day.strftime(format_str))
            elif not weekday_only:
                date_list.append(day.strftime(format_str))

    return date_list

def getLastByMonth(ym):
    y = ym[0:4]
    m = ym[4:6]
    return f'{y}{m}{calendar.monthrange(int(y),int(m))[1]}'

def getLastByYear(y):
    m = '12'
    return f'{y}{m}{calendar.monthrange(int(y),int(m))[1]}'

def trimDictionary(values):
    temp = {}
    for v in values:
        if values[v] == '' or values[v] is None:
            pass
        else:
            temp[v] = values[v]
    return temp

# blob 파일 업로드
def uploadBlob(container_name='outdata-col',filepath='', data=bytes, params={}):
    
    try:
        conn = BlobClient.from_connection_string(blob_name=filepath, 
                                                conn_str=cryptoUtil.decrypt(conf.blob_config['CONN_STR']), 
                                                container_name=container_name)
        conn.upload_blob(data, overwrite=True)
        conn.close()
    except Exception as e:
        common.setErrMsg(err_yn='Y', err_cnt=f'blob error {traceback.format_exc()}', params=params)

# blob 파일 업로드
def deleteBlob(container_name='outdata-col',filepath='', data=bytes, params={}):
    from azure.core.exceptions import ResourceNotFoundError
    try:
        conn = BlobClient.from_connection_string(blob_name=filepath, 
                                                conn_str=cryptoUtil.decrypt(conf.blob_config['CONN_STR']), 
                                                container_name=container_name)
        if conn.exists():
            conn.delete_blob()
        conn.close()
    except ResourceNotFoundError as e:
        pass
    except Exception as e:
        common.setErrMsg(err_yn='N', err_cnt=f'blob error {traceback.format_exc()}', params=params)

# blob 파일 업로드
def getBlob(container_name='outdata-col',filepath='', params={}):
    
    try:
        conn = BlobClient.from_connection_string(blob_name=filepath, 
                                                conn_str=cryptoUtil.decrypt(conf.blob_config['CONN_STR']), 
                                                container_name=container_name)
        return conn.download_blob().readall()
        
    except Exception as e:
        common.setErrMsg(err_yn='Y', err_cnt=f'blob error {traceback.format_exc()}', params=params)

def isNull(val):
    if val is None or val == '' or val == 'null':
        return True
    else:
        return False

def isNotNull(val):
    if not utils.isNull(val):
        return True
    else:
        return False

def nvl(v, v2):
    return v2 if utils.isNull(v) else v

def getDateTime(format_str='%Y-%m-%d %H:%M:%S', cur_timezone='Asia/Seoul'):
    from datetime import datetime
    from pytz import timezone

    return datetime.now(timezone(cur_timezone)).strftime(format_str)


# 스크래핑 유틸
def requestContent(url, headers, min_sec=0, max_sec=0, params={}):
    from random import randint
    import time

    try:
        time.sleep(randint(min_sec, max_sec))
        session = utils.requestsRetry()
        return session.get(url, headers=headers).content
    except Exception as e:
        common.setErrMsg('N', traceback.format_exc(), params)
        return b''

# 스크래핑 유틸
def requestDownload(url, file_path, headers, min_sec=0, max_sec=0, params={}):
    from random import randint
    import time

    try:
        byte_file = []
        time.sleep(randint(min_sec, max_sec))
        session = utils.requestsRetry()
        resp = session.get(url, headers=headers)

        if resp.status_code == 200:
            for chunk in resp.iter_content(chunk_size=8*1024):
                if chunk:
                    byte_file.append(chunk)

            with open(file_path, 'wb') as f:
                f.write(b''.join(byte_file))
        else:
            common.setErrMsg('N', resp.status_code, params)

    except Exception as e:
        common.setErrMsg('N', traceback.format_exc(), params)

# request retry
def requestsRetry(session=None):

    retries = 3 # retry 횟수
    # {backoff_factor} * (2 ^ ({number of total retries} - 1))
    backoff_factor = 1 # sleep time backoff_factor*1,*2,*3
    status_forcelist = (500, 502, 504)

    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def getFileSize(file_path):
    try:
        return os.path.getsize(file_path)
    except:
        return 0
            
# 디렉토리 생성
def initDirectory(path):
    if not (os.path.isdir(path)):
        os.makedirs(os.path.join(path))
    # else:
    #     try:
    #         shutil.rmtree(path)
    #     except:
    #         pass

# 파일 생성
def makeFile(file_path, byte_file, type):
    with open(file_path, type) as f:
        f.write(byte_file)
        f.close()
        
# 스크래핑 유틸
