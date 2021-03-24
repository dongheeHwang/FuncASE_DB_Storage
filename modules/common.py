import logging
import datetime
import os

from modules import database as db
from modules import utils
from modules import common
from modules import telegramUtil
from modules.resource import query
from modules.resource import conf

DELIMITER_FLAG = "\u001F"

# 공통코드 그룹별
def getCode(params):
    with db.Database() as cur:
        return cur.selectAll(query.select_ot_code, params)

# 공통 코드 상세
def getCodeDetail(params):
    with db.Database() as cur:
        return cur.selectAll(query.select_ot_code_detail, params)

# 수집 룰
def getColRule(params):
    with db.Database() as cur:
        return cur.selectOne(query.select_ot_col_rule, params)

# 수집 룰 상세
def getColRuleDtl(params):
    with db.Database() as cur:
        return cur.selectAll(query.select_ot_col_rule_dtl, params)

# 파싱 룰
def getColPas(params):
    with db.Database() as cur:
        return cur.selectAll(query.select_ot_col_pas, params)

# 파일목록
def getFile(params):
    with db.Database() as cur:
        return cur.selectAll(query.select_ot_file, params)

# FTP 인증정보
def getColAuth(params):
    with db.Database() as cur:
        return cur.selectOne(query.select_ot_col_auth, params)
# 인증 상세
def getColAuthDtl(params):
    with db.Database() as cur:
        return cur.selectAll(query.select_ot_col_auth_dtl, params)

# 코드그룹
def getCodeGrp(params):
    with db.Database() as cur:
        return cur.selectAll(query.select_ot_cd, params)

# 작업기록 시작 등록
def updateColWorkStart(params):
    with db.Database() as cur:
        cur.execute(query.update_ot_col_work_start, params)
        cur.commit()

# 작업기록 완료 등록
def updateColWorkEnd(params):
    with db.Database() as cur:
        cur.execute(query.update_ot_col_work_end, params)
        cur.commit()
        
# RULE 테이블 작업기록
def updateColLast(params):
    with db.Database() as cur:
        cur.execute(query.update_ot_col_rule_last, params)
        cur.commit()

# 텔레그램 메시지 등록
def insertOtMsg(params):
    with db.Database() as cur:
        cur.execute(query.insert_ot_msg, params)
        cur.commit()

def logInfo(msg, params):
    # history 설정
    common.setHistory(msg, params)
    # 시스템 로그
    logging.info(msg)
    
def logDebug(msg, params):
    # history 설정
    common.setHistory(msg, params)
    # 시스템 로그
    logging.debug(msg)
    
# array 메시지 셋팅
def setErrMsg(err_yn='', err_cnt='', params={}):
    # history 설정
    common.setHistory(err_cnt, params)

    # 텔레그램 메시지
    telegramUtil.send_message(err_cnt)

    # if params['err_yn'] == 'N':# or err_yn == 'Y':
    params['err_yn'] = err_yn
    params['err_cnt'] = err_cnt


# array 메시지 가져오기 
def getErrMsg(params):
    return params['err_yn'], params['err_cnt']


# 히스토리 로그 
def setHistory(msg, params):
    nowDate = utils.getDateTime('%Y-%m-%d %H:%M:%S')
    if 'ex_log' not in params:
        params['ex_log'] = []
    params['ex_log'].append(f'{nowDate} {msg}')
