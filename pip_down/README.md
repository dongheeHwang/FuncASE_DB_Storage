# package 목록 추출
pip freeze > req.txt

# package 다운로드
pip download -d ./python_packages -r req.txt

# wheel 파일 전체 설치
pip install --no-index --find-links=./.packages -r req.txt

# 디렉토리 정리
find . -type d -empty -delete