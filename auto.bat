@echo off
chcp 65001 1>NUL
cd /d "%~dp0"
set PYTHONIOENCODING=utf-8
echo. >> data\log.txt
echo =========================================== >> data\log.txt
echo [%date% %time%] run >> data\log.txt
python collect.py >> data\log.txt 2>&1
python fundamentals.py >> data\log.txt 2>&1
python report_html.py --no-open >> data\log.txt 2>&1

echo [%date% %time%] git push attempt >> data\log.txt
git add -A >> data\log.txt 2>&1
git diff --cached --quiet
if %errorlevel% neq 0 (
    git commit -m "daily %date%" >> data\log.txt 2>&1
    git push origin main >> data\log.txt 2>&1
    echo [%date% %time%] git push done >> data\log.txt
) else (
    echo [%date% %time%] no changes - skip push >> data\log.txt
)

echo [%date% %time%] finished >> data\log.txt
