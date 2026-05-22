@echo off
chcp 65001 1>NUL
cd /d "%~dp0"
set PYTHONIOENCODING=utf-8
echo. >> data\log.txt
echo =========================================== >> data\log.txt
echo [%date% %time%] analysis_daily start >> data\log.txt
python build_data.py >> data\log.txt 2>&1
python listing_dates.py >> data\log.txt 2>&1
python categories.py >> data\log.txt 2>&1
python recommend.py >> data\log.txt 2>&1
python prospects.py >> data\log.txt 2>&1
python buy_timing.py >> data\log.txt 2>&1
python valuation.py >> data\log.txt 2>&1
python earnings_surprise.py >> data\log.txt 2>&1
python report_html.py --no-open >> data\log.txt 2>&1
echo [%date% %time%] gh-pages deploy >> data\log.txt
python deploy.py >> data\log.txt 2>&1
echo [%date% %time%] analysis_daily done >> data\log.txt