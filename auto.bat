@echo off
chcp 65001 > nul
cd /d "%~dp0"
set PYTHONIOENCODING=utf-8
echo. >> data\log.txt
echo =========================================== >> data\log.txt
echo [%date% %time%] run >> data\log.txt
python collect.py >> data\log.txt 2>&1
python fundamentals.py >> data\log.txt 2>&1
python report_html.py --no-open >> data\log.txt 2>&1
echo [%date% %time%] done >> data\log.txt
