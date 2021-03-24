set REQ_TXT=req.txt
set DOWN_PATH=./.packages

rem directory 삭제
rmdir /S/Q %DOWN_PATH%

rem package 목록 추출
pip freeze > %REQ_TXT%

rem package 다운로드
pip download -d %DOWN_PATH% -r %REQ_TXT%

rem del %REQ_TXT%