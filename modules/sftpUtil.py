import logging
import paramiko
import pathlib
import os
import io
import re
from datetime import datetime
import time
from ftplib import FTP, FTP_TLS
import traceback
import ssl
import stat
from threading import Thread

from modules import utils
from modules import cryptoUtil
from modules import common


class Ftp:
    def __init__(self, params={}, col_rule_file=[], col_auth={}):
        self.params = params
        self.col_rule_file = col_rule_file
        self.col_auth = col_auth
        self.ssh = None
        self.sftp = None
    
    # 접속끊김 현상으로 인해 주기적 호출 신호 발생
    def doSendMsg(self):
        while(True):
            time.sleep(1)
            if self._isSftp():
                if self.ssh:
                    time.sleep(1 * 60)
                    self.ssh.exec_command('pwd')
                else:
                    break
            elif not self._isSftp():
                if self.sftp:
                    time.sleep(1 * 60)
                    self.sftp.getwelcome()
                    self.sftp.sendcmd('STAT')
                else:
                    break
            else:
                break

    def connection(self):
        
        if self.sftp is None:
            try:
                # sftp
                if self._isSftp():
                    self.ssh = paramiko.SSHClient()
                    # blob data 다운로드
                    blob_data = utils.getBlob(container_name=self.col_auth['container'], filepath=self.col_auth['key_nm'], params=self.params)
                    str_key = blob_data.decode('utf-8')
                    
                    # bytes 를 키로 인식
                    pkey = paramiko.RSAKey.from_private_key(io.StringIO(str_key))

                    conn_config = {'hostname': self.col_auth['hostname'],
                                'port': self.col_auth['port'],
                                'username': self.col_auth['username']}

                    if utils.isNotNull(self.col_auth['password']):
                        conn_config['password'] = cryptoUtil.decrypt(self.col_auth['password'])
                    conn_config['pkey'] = pkey

                    # 빈 dictionary 정리
                    conn_config = utils.trimDictionary(conn_config)
                    # ssh connect
                    self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                    self.ssh.connect(**conn_config)
                    self.sftp = self.ssh.open_sftp()
                    
                # sftp
                elif not self._isSftp():
                    self.sftp = FTP_TLS(host=self.col_auth['hostname'], context=ssl._create_stdlib_context(ssl.PROTOCOL_TLSv1_2))
                    self.sftp.login(self.col_auth['username'], cryptoUtil.decrypt(self.col_auth['password']))
                    self.sftp.set_pasv(True)

                    try:
                        # 보안 통신
                        self.sftp.prot_p()
                    except:
                        # 일반 통신
                        self.sftp.prot_c()

                else:
                    common.setErrMsg(err_yn='Y', err_cnt=f'{self.params["col_id"]} sftp_type{self.params["sftp_type"]}이 입력되지 않았습니다.', params=self.params)

            except Exception as e:
                common.setErrMsg(err_yn='Y', err_cnt=f'{self.params["col_id"]} {traceback.format_exc()}', params=self.params)


    # sftp 최초 요청
    def start(self):
        
        # ftp 연결
        self.connection()
        
        # 정상처리 후 파일이 있는경우
        err_yn, err_cnt = common.getErrMsg(self.params)

        # error 아닌경우에 실행
        if err_yn != 'Y':
            # 작업 수행
            th1 = Thread(target=self.doSendMsg)
            th2 = Thread(target=self.searchByDepth)

            th1.start()
            th2.start()
            th1.join()
            th2.join()
            

    def searchByDepth(self):
        paths = [p for p in self.col_rule_file if p['dtl_val'] and p['dtl_val'].find('/') > -1]
        for pa in paths:
            
            # depth 파이프라인으로 분리 
            split_path = pa['dtl_val'].split('/')

            # depth별 검색 ( |downlowd|20201202|shinhan.csv )
            self.searchDeep(0, [], split_path[1:])
        
        if self.ssh: self.ssh = None
        if self.sftp:self.sftp = None

    # 재귀호출 하여 하위 디렉토리 검색
    def searchDeep(self, idx=0, append_path=[], regex_path=[], flag='d'):

        time.sleep(.1)
        cur_dir = f"/{'/'.join(append_path)}"

        # path에 확장자(suffix)가 있다면 파일 or 디렉토리
        is_file = True if flag != 'd' else False
        is_same_depth = len(append_path) == len(regex_path)

        # 파일의 다운로드
        if is_file and is_same_depth:
            self._downloadFile(append_path)
        # 디렉토리 파일 다운로드 (directory이면서, 요청경로와 현재경로가 같으면 download)
        elif is_file is False and is_same_depth and len(append_path) > 0:
            # 하위디렉토리 파일 다운로드
            self._downloadFilesByDirectory(append_path)
        else:
            err_msg = ''
            
            # sftp 경로 검색
            file_list = [] if is_file else self._getListFtp(cur_dir)
            date_list = utils.getWeekDateRange(self.params)
            
            for fi in file_list:

                filename = fi['filename']
                flag = fi['flag']

                # date 구분기호
                st_exp = '{{'
                ed_exp = '}}'
                format_ymd = '%Y%m%d'

                str_regex = regex_path[idx]
                
                format_str = str_regex[str_regex.find(st_exp)+len(st_exp):str_regex.find(ed_exp)] if str_regex.find(st_exp)>-1 and str_regex.find(ed_exp)>-1 else ''
                if utils.isNotNull(format_str) and self.params['rg_opt'] == 'Y':
                    
                    # str_regex = str_regex.replace(st_exp,'').replace(ed_exp,'')
                    for date in date_list:
                        
                        format_date = datetime.strptime(date, format_ymd).strftime(format_str)
                        str_reg = str_regex.replace(f'{st_exp}{format_str}{ed_exp}', format_date)

                        if re.search(str_reg, filename):
                            self.search(idx, append_path, filename, regex_path, flag)
                    
                # 정규식 검색
                elif re.search(str_regex, filename):
                    # 현재 경로 추가
                    self.search(idx, append_path, filename, regex_path, flag)

                # (범위여부) & (일자포함) 일 경우 검색
                elif str_regex.find(st_exp)>-1 and str_regex.find(ed_exp)>-1:

                    try:
                        # 포멧 데이터 추출
                        tmp_date = datetime.now().strftime(format_str)

                        # 문자열에서 일자만 추출 yyyymmdd, yymm
                        cur_dates = re.findall('[0-9]{'+str(len(tmp_date))+'}', filename)

                        if cur_dates:
                            # format_str = '%Y%m%d' if len(cur_dates[0]) == 8 else '%Y%m' 
                            # 일자인지 확인을 위해 추출된 date에서 형변환 시도
                            datetype = datetime.strptime(cur_dates[0], format_str)

                            str_regex_file = datetype.strftime(str_regex.replace(st_exp,'').replace(ed_exp,''))
                            rg_st_dt = int(datetime.strptime(self.params['rg_st_dt'], format_ymd).strftime(format_str))
                            rg_ed_dt = int(datetime.strptime(self.params['rg_ed_dt'], format_ymd).strftime(format_str))

                            # 범위 사용여부
                            if self.params['rg_opt'] == 'Y':
                                cur_date = int(cur_dates[0])

                                # 범위 지정
                                if rg_st_dt <= cur_date <= rg_ed_dt and str_regex_file == filename:
                                    # 현재 경로 추가
                                    self.search(idx, append_path, str_regex_file, regex_path, flag)
                            elif self.params['rg_opt'] == 'N':
                                if str_regex_file == filename:
                                    # 현재 경로 추가
                                    self.search(idx, append_path, str_regex_file, regex_path, flag)

                    except ValueError as e:
                        common.setErrMsg(err_yn='Y', err_cnt=f'ValueError : {traceback.format_exc()}', params=self.params)
                    except Exception as e:
                        common.setErrMsg(err_yn='Y', err_cnt=f'Exception : {traceback.format_exc()}', params=self.params)
                else:
                    pass

            if err_msg != '':
                common.setErrMsg(err_yn='Y', err_cnt=err_msg, params=self.params)

    def search(self, idx, append_path, str_regex_file, regex_path, flag):
        # 현재 경로 추가
        append_path.append(str_regex_file)
        # 재귀 호출
        self.searchDeep(idx + 1, append_path, regex_path, flag)
        # 마지막 array 삭제
        utils.popLastArray(append_path)   

    # 디렉토리 하위 파일 전체 다운로드
    def _downloadFilesByDirectory(self, append_path):
        cur_dir = f"/{'/'.join(append_path)}"
        parent_dir = "/".join(append_path)  # append_path[len(append_path)-1]

        # sftp 경로 검색
        file_list = self._getListFtp(cur_dir)
        for fi in file_list:
            filename = fi['filename']
            
            append_path = append_path[:]
            append_path.append(filename)

            # download File
            self._downloadFile(append_path)
            utils.popLastArray(append_path)

    # 해당 파일 다운로드
    def _downloadFile(self, append_path):
        work_seq_path = self.params['work_seq_path']
        filepath = f"/{'/'.join(append_path)}"
        parent_dir = f"/{'/'.join(append_path[:-1]) }"

        # 서버 temp 디렉토리 생성
        temp_dir_path = work_seq_path + parent_dir
        if not os.path.isdir(temp_dir_path) and not os.path.exists(temp_dir_path):
            os.makedirs(os.path.join(temp_dir_path))

        # 파일이 없을경우 생성
        temp_file_path = work_seq_path + filepath
        if not os.path.exists(temp_file_path):
            common.logInfo(filepath, self.params)
            
            # sftp 경우
            if self._isSftp():
                self.sftp.get(filepath, temp_file_path)
            # ftp
            elif self._isSftp() == False:
                s_idx = filepath.rfind('/')
                self.sftp.cwd(filepath[:s_idx])
                self.sftp.retrbinary(f'RETR {filepath[s_idx+1:]}', open(temp_file_path, 'wb').write)

    # sftp, FTP 구분
    def _isSftp(self):
        if self.params['sftp_type'] == 'S':
            return True
        elif self.params['sftp_type'] == 'F':
            return False
        else:
            return None

    # ftp, sftp directory 가져오기
    def _getListFtp(self, cwd, files = []):
        file_list = []

        try:
            # sftp
            if self._isSftp():
                for fi in self.sftp.listdir_attr(cwd):
                    flag = 'd' if stat.S_ISDIR(fi.st_mode) else '-'
                    file_list.append({'filename':fi.filename, 'flag':flag})
            # ftp
            else:
                data = []
                self.sftp.cwd(cwd)
                self.sftp.dir(data.append)

                for item in data:
                    pos = item.rfind(' ')
                    name = item[pos+1:]
                    file_list.append({'filename':name, 'flag':item[0]})
        except Exception as e:
            common.setErrMsg(err_yn='Y', err_cnt=f'{self.params["col_id"]} {cwd} 해당 경로가 존재하지 않습니다. {traceback.format_exc()}', params=self.params)

        return file_list
