select_ot_code = """
select code_grp_id
     , code
     , code_nm
  from ot_cd
 where code_grp_id in ( %(code_grp_id_telegram)s, %(code_grp_id_blob)s )
"""

select_ot_code_detail = """
select code_grp_id
     , code
     , code_nm
  from ot_cd
 where code_grp_id = %(code_grp_id)s
"""

# 수집 룰 조회
select_ot_col_rule = """
select col_id
     , auth_id
     , use_at
     , col_type
     , sftp_type 
     , url_info   # url
     , res_type   # return type (json, xml, html)
     , method     # 통신방식
     , pag_yn     # 범위설정
     , tot_count_nm
     , pag_no
     , sftp_type
     , cd_file_id
     , sleep_opt
     , template
     , tp_yn
     , rg_opt
     , col_co
  from ot_col_rule a
 where col_id = %(col_id)s
"""

select_ot_file = """
select fl_mask_nm   as key_nm
     , up_path_key  as container
     , seq
  from ot_col_rule a
  left outer join ot_file b on a.cd_file_id = b.fl_id and b.del_yn='N'
 where a.col_id = %(col_id)s
"""

# 수집 룰 상세 조회
select_ot_col_rule_dtl = """
select dtl_id, upper_dtl_id, dtl_val, rg_pm_opt
  from ot_col_rule_dtl
 where col_id = %(col_id)s
 order by dtl_seq
"""

# 파싱룰 조회
select_ot_col_pas = """
select seq, top_nd, iter, nd
  from ot_col_pas
 where col_id = %(col_id)s
"""

# 인증파일 정보 조회
select_ot_col_auth = """
select a.col_id,
       b.auth_tgt     as hostname,
       b.auth_port    as port,
       b.id           as username,
       b.pw           as `password`,
       c.fl_mask_nm   as key_nm,
       c.up_path_key  as container
  from ot_col_rule a 
 inner join ot_col_auth b on a.auth_id = b.auth_id
  left outer join ot_file c on b.auth_file = c.fl_id and c.del_yn='N'
 where a.col_id = %(col_id)s
"""

# 인증 정보 조회
select_ot_col_auth_dtl = """
select b.auth_key  as auth_nm,
       b.auth_val as auth_key,
       b.seq          as auth_seq
  from ot_col_rule a 
 inner join ot_col_auth_dtl b on a.auth_id = b.auth_id
 where a.col_id = %(col_id)s
"""

# 작업기록관리 조회
select_ot_col_work_next_seq = """
select nvl(max(seq), 0)+1 as next_seq
  from ot_col_work
 where col_id = %(col_id)s
   and work_dt = %(work_dt)s
"""

select_ot_cd = """
select code, code_nm
  from ot_cd
 where code_grp_id = %(code_grp_id)s
   and use_at = 'Y'
"""

# 작업기록관리
insert_ot_col_work = """
insert into ot_col_work (
  col_id, work_dt, seq, ex_id, st_dt
) values (
  %(col_id)s, %(work_dt)s, %(seq)s, %(ex_id)s, now()
)
"""

# 작업기록 시작
update_ot_col_work_start = """
update ot_col_work
   set st_dt = now()
     , end_dt = null
     , err_yn = '-'
     , ex_log = %(ex_log)s
 where col_id = %(col_id)s
   and work_dt = %(work_dt)s
   and seq = %(work_seq)s
"""

# 작업기록 종료
update_ot_col_work_end = """
update ot_col_work
   set end_dt = now()
     , err_yn = %(err_yn)s
     , err_cnt = %(err_cnt)s
     , ex_log = %(ex_log)s
     , fl_size = %(fl_size)s
 where col_id = %(col_id)s
   and work_dt = %(work_dt)s
   and seq = %(work_seq)s
"""

update_ot_col_rule_last = """
update ot_col_rule
   set last_col_dt = now()
     , col_co = %(col_co)s
 where col_id = %(col_id)s
"""

# 텔레그램 메시지 등록
insert_ot_msg = """
insert into ot_msg(
  msg_id, msg_cnt, msg_dt, suc_yn
) values (
  (select nvl(max(msg_id),0)+1 from ot_msg as seq_id), %(msg_cnt)s, sysdate(), %(suc_yn)s
 )
"""
