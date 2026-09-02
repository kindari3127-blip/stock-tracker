@echo off
chcp 65001 1>NUL
cd /d "%~dp0"
set PYTHONIOENCODING=utf-8
echo. >> data\intraday_log.txt
echo =========================================== >> data\intraday_log.txt
echo [%date% %time%] intraday start >> data\intraday_log.txt
rem --- FAST intraday refresh only (NO Claude API calls) ---
rem  Heavy discovery steps (recommend/prospects/valuation) fetch 1500+ tickers
rem  one-by-one from FDR (10-20 min) and are NOT suitable for a 15-min cycle.
rem  Those are computed by the morning/evening full runs (auto.bat / analysis_daily.bat).
rem  Intraday only refreshes prices, indices, flows, sector strength -> rebuild report -> deploy.
python collect.py >> data\intraday_log.txt 2>&1
python collect_extra.py >> data\intraday_log.txt 2>&1
python flows.py >> data\intraday_log.txt 2>&1
python macro.py >> data\intraday_log.txt 2>&1
python build_data.py >> data\intraday_log.txt 2>&1
python report_html.py --no-open >> data\intraday_log.txt 2>&1
python deploy.py >> data\intraday_log.txt 2>&1
echo [%date% %time%] intraday done >> data\intraday_log.txt
