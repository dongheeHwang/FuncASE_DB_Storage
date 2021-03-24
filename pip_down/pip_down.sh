REQ_TXT=req.txt
DOWN_PATH=./.packages

# directory 정리
rm -rf $DOWN_PATH

# package 목록 추출
pip freeze > $REQ_TXT

# package 다운로드
pip download -d $DOWN_PATH -r $REQ_TXT

# rm $REQ_TXT