import json
import math
import time
import traceback
import string

from urllib.parse import urlencode, quote_plus
import xmltodict

from modules import common
from modules import utils

class API:
    is_error = False

    def __init__(self, params={}, rule_data={}, dtl_header=[], dtl_param=[], pas_data=[]):
        self.params = params
        self.rule_data = rule_data
        self.dtl_header = dtl_header
        self.dtl_param = dtl_param
        self.pas_data = pas_data

    # request
    def start(self):

        # 리턴 타입 체크
        if utils.isNull(self.rule_data['res_type']):
            raise Exception('ot_col_rul [res_type] 이 입력되지 않았습니다.')

        if self.params['rg_case'] == '3' and (utils.isNull(self.params['rg_st_dt']) or utils.isNull(self.params['rg_ed_dt'])):
            raise Exception(f'{self.rule_data["col_id"]} rg_case(3)에 (rg_st_dt, rg_ed_dt)가 입력되지 않았습니다.')

        if self.rule_data['pag_yn'] == 'Y' and utils.isNull(self.rule_data['tot_count_nm']):
            raise Exception(f'{self.rule_data["col_id"]} tot_count_nm 에 입력된 값이 없습니다.')
            
        # 코드그룹 사용여부
        self.params['code_group_yn'] = 'Y' if '5' == utils.getListValue(self.dtl_param, 'rg_pm_opt', '5', 'rg_pm_opt') else 'N'
        self.params['code_grp_id'] = utils.getListValue(self.dtl_param, cond_key='rg_pm_opt', cond_val='5', get_key='dtl_val')

        # 기본 case
        if self.params['rg_case'] in ['1','2']:
            self.requestByCode(d_index=0)
        # 지정일 case
        elif self.params['rg_case'] == '3':
            
            # 평일 date Range 가져오기
            date_list = utils.getWeekDateRange(self.params, weekday_only=False)
            
            # date 별 실행
            for idx, date in enumerate(date_list):
                # rg_pm_opt=4 의 기준일 date 값 셋팅
                self.params['allocate_dt'] = date
                utils.setListValue(self.dtl_param, cond_key='rg_pm_opt', cond_val='4', set_key='dtl_val', set_val=date)
                
                if self.is_error:
                    break
                self.requestByCode(d_index=idx)

    # code group 별 실행
    def requestByCode(self, d_index):
        
        # 코드 그룹 미사용
        if utils.isNull(self.params['code_grp_id']):
            self.requetAPI(d_index=d_index, c_index=0)

        # 코드 그룹 사용
        else:
            code_grp = common.getCodeGrp({'code_grp_id': self.params['code_grp_id']})
            if len(code_grp) == 0:
                raise Exception(f'ot_cd 에 등록된 코드({self.params["code_grp_id"]})가 없습니다.')

            for idx, code in enumerate(code_grp):
                self.params['code_value'] = code['code']
                # rg_pm_opt=5 의 코드 그룹 데이터 셋팅
                utils.setListValue(self.dtl_param, cond_key='rg_pm_opt', cond_val='5', set_key='dtl_val', set_val=code['code'])

                if self.is_error:
                    break
                
                # requestAPI
                self.requetAPI(d_index=d_index, c_index=idx)

    # requestAPI
    def requetAPI(self, d_index, c_index, page_no=1):
        
        url = self.rule_data['url_info'].rstrip('/')
        method = self.rule_data['method']
        res_type = self.rule_data['res_type']  # 리턴 타입
        
        # ** 디렉토리 초기 생성 **
        # 오리지널 디렉토리
        self._setResponsePath(d_index, c_index, page_no)
        utils.initDirectory(self.params['filepath_org'])
        # 파싱 디렉토리
        for pas in self.pas_data:
            self._setParsePath(pas['seq'], d_index, c_index, page_no)
            utils.initDirectory(self.params['filepath_pas'])
        # ** 디렉토리 초기 생성 **

        self.params.update({'page_no': str(page_no)})
        str_response = ''

        while(True):
            try:
                # retry 세션 가져오기
                session = utils.requestsRetry()
                
                # page_no dtl에 있으면 dtl에서 가져오기
                try:
                    page_no = int(utils.getListValue(self.dtl_param, cond_key='rg_pm_opt', cond_val='3', get_key='dtl_val'))
                except:
                    page_no = int(self.params['page_no'])
                
                # url 요청시 delay
                time.sleep(utils.nvl(self.rule_data['sleep_opt'], 0))
                
                # request 시작
                response = ''
                dicHeader = utils.row2dict(self.dtl_header, 'dtl_id', 'dtl_val')
                dicParam = utils.row2dict(self.dtl_param, 'dtl_id', 'dtl_val')
                if method == 'GET':
                    # tmp_dtl_param = [x for x in dtl_param if x['rg_pm_opt'] not in ['4', '5']]
                    for v in dicParam:
                        dicParam[quote_plus(v)] = dicParam[v]
                    response = session.get(f'{url}?{urlencode(dicParam)}', headers=dicHeader)

                elif method == 'REST':
                    # dtl에 증가변수 6이 있으면 해당 값에 pag_no를 더해준다.
                    if page_no > 1 and '6' == utils.getListValue(self.dtl_param, 'rg_pm_opt', '6', 'rg_pm_opt'):
                        for x in self.dtl_param:
                            if x['rg_pm_opt'] == '6':
                                x['dtl_val'] = int(x['dtl_val']) + int(self.rule_data['pag_no'])

                    restUrl = url + quote_plus(f"/{'/'.join([str(x['dtl_val']) for x in self.dtl_param])}", safe="/")
                    response = session.get(restUrl, headers=dicHeader)

                elif method == 'POST':
                    # template 사용시
                    if self.rule_data['tp_yn'] == 'Y':
                        template = string.Template(self.rule_data['template'])
                        dicParam = template.substitute(dicParam)
                        for i in ['\r','\n','\t']:
                            dicParam = dicParam.replace(i,'')
                        dicParam = dicParam.encode('utf-8')

                    response = session.post(url, headers=dicHeader, data=dicParam)
                # close
                session.close()
                # del dicHeader, dicParam
                # request 종료

                # response
                if response.status_code != 200:
                    str_response = self._getContentStr(response)
                    raise Exception(f'status_code : {response.status_code}\n{str_response}')
                    break
                else:
                    str_response = self._getContentStr(response)

                    # 파일 사이즈 체크해서 파일명 변경
                    limit_size = self._getLimitSize(limit_mega=self.params['response_file_limit']) # limit
                    if limit_size <= utils.getFileSize(self.params['filepath_org_full']):
                        self._setResponsePath(d_index, c_index, page_no)

                    open(self.params['filepath_org_full'],'a', encoding='utf-8').write(f'{str_response}\n')

                    # 파싱룰 (JSON, XML, HTML)
                    if res_type == 'J':
                        pas_response = json.loads(str_response)
                    elif res_type == 'X':
                        dict_type = xmltodict.parse(str_response)
                        pas_response = json.loads(json.dumps(dict_type))
                    
                    # 파싱
                    self.parseResponse(pas_response, d_index, c_index, page_no)

                    # 페이징 사용여부
                    if self.rule_data['pag_yn'] != 'Y':
                        break
                
                    # total_count value 가져오기
                    arr_tot_count = list(utils.findDeep(pas_response, self.rule_data['tot_count_nm']))

                    if len(arr_tot_count) < 1:
                        raise Exception(f'{self.rule_data["col_id"]} tot_count_nm({self.rule_data["tot_count_nm"]}) 객체를 찾을 수 없습니다. ')
                    tot_count = int(arr_tot_count[0])
                    
                    # 페이징 제한 
                    max_page_no = math.ceil(int(tot_count)/int(self.rule_data['pag_no']))
                    if page_no >= max_page_no:
                        break
                    else:
                        # page_no 값 셋팅
                        utils.setListValue(self.dtl_param, cond_key='rg_pm_opt', cond_val='3',
                                            set_key='dtl_val', set_val=str(page_no + 1))
                        self.params['page_no'] = str(page_no + 1)

            except json.JSONDecodeError as ex:
                raise Exception(f'{self.rule_data["col_id"]} requestAPI : {traceback.format_exc()}, \n response : {str_response[:1000]}')
            except Exception as ex:
                raise Exception(f'{self.rule_data["col_id"]} requestAPI : {traceback.format_exc()}, \n response : {str_response[:1000]}')
            finally:
                if session: session.close()


    # 룰데이터 파싱
    def parseResponse(self, pas_response, d_index, c_index, page_no):

        # 파싱룰 데이터
        for pas in self.pas_data:
            
            top_nd_nm = pas['top_nd']
            seq = pas['seq']
            self._setParsePath(seq, d_index, c_index, page_no)

            # dictionary
            top_data = utils.findDeep(pas_response, top_nd_nm)

            for top in top_data:
                if not top:
                    continue
                if type(top) == type([]):
                    for t in top:
                        self.parseResponse2File(pas, d_index, c_index, page_no, t)
                else:
                    self.parseResponse2File(pas, d_index, c_index, page_no, top)

    def parseResponse2File(self, pas, d_index, c_index, page_no, top):
        iter_nm = pas['iter']
        nd = pas['nd']
        seq = pas['seq']
        iter_row = utils.findDeep(top, iter_nm)
        # iter_data = top[iter_nm]
        for iter_data in iter_row:
            if not iter_data:
                continue
            # iter data array
            iter_row = self._getIterData(iter_data, nd)

            limit_size = self._getLimitSize(limit_mega=self.params['response_file_limit'])  # limit
            if limit_size <= utils.getFileSize(self.params['filepath_pas_full']):
                self._setParsePath(seq, d_index, c_index, page_no)

            open(self.params['filepath_pas_full'], 'a', encoding='utf-8').write("\n".join(iter_row))

    def _getContentStr(self, str_response):

        try:
            return str_response.content.decode('utf-8').replace('\n', '').replace('\r', '')
        except:
            try:
                return str_response.content.decode('euc-kr').replace('\n', '').replace('\r', '')
            except:
                raise Exception('response의 encode type이 확인되지 않습니다.')

    def _getIterData(self, iter_data, nd):
        # iter data array
        if type(iter_data) == type([]):
            for obj in iter_data:

                # ND 컬럼 순서 미설정시
                if utils.isNull(nd):
                    # 전체 파일 작성
                    row = self._getRowByInfo(obj, obj)
                else:
                    row = self._getRowByInfo(obj, nd.split(','))
                if len(row) > 0:
                    yield f"{common.DELIMITER_FLAG}".join(row)
        elif type(iter_data) == type({}):
            row = self._getRowByInfo(iter_data, nd.split(','))
            if len(row) > 0:
                yield f"{common.DELIMITER_FLAG}".join(row)

    def _getRowByInfo(self, obj, iter):
        row = []
        # iter type check ( dict or list )
        if type(iter) == type({}) or type(iter) == type([]):
            # 파일 row별 정보 입력 지정일|코드그룹|work_dt
            if self.params['rg_case'] == '3' and self.params['code_group_yn'] == 'Y':
                row.append(self.params['allocate_dt'])
                row.append(self.params['code_value'])
            elif self.params['rg_case'] == '3':
                row.append(self.params['allocate_dt'])
            elif self.params['code_group_yn'] == 'Y':
                row.append(self.params['work_dt'])
                row.append(self.params['code_value'])
            else:
                row.append(self.params['work_dt'])
            # 파일 해더 정보
            for v in iter:
                if v in obj:
                    row.append(str(obj[v]).replace('\n','').replace('\r',''))
            return row
        else:
            return row

    # 파싱 파일명 변경
    def _setParsePath(self, seq, d_index, c_index, page_no):
        self.params['filepath_pas'] = f"{self.params['filepath']}/parse/{seq}"
        self.params['filepath_pas_full'] = f"{self.params['filepath_pas']}/{d_index:03}-{c_index:03}-{page_no:07}"

    # 오리지널 파일명 변경
    def _setResponsePath(self, d_index, c_index, page_no):
        self.params['filepath_org'] = f"{self.params['filepath']}/original"
        self.params['filepath_org_full'] = f"{self.params['filepath_org']}/{d_index:03}-{c_index:03}-{page_no:07}"

    # 100 M
    def _getLimitSize(self, limit_mega):
        return int(limit_mega) * 1024 * 1024
