import traceback

from modules import utils
from modules import common

# 스크래핑 요청
def requestScraping(params, rule_data={}, file_data=[]):

    for idx, file_info in enumerate(file_data):
        # blob data 다운로드
        blob_data = utils.getBlob(container_name=file_info['container'], filepath=file_info['key_nm'], params=params)
        blob_str = blob_data.decode('utf-8')
        params['file_seq'] = str(idx+1)
        try:
            exec(blob_str)
        except Exception as e:
            common.setErrMsg(err_yn='Y', err_cnt=f'스크래핑 \n {traceback.format_exc()}', params=params)
        