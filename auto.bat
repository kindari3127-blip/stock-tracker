@echo off
chcp 65001 1>NUL
cd /d "%~dp0"
set PYTHONIOENCODING=utf-8
echo. >> data\log.txt
echo =========================================== >> data\log.txt
echo [%date% %time%] run >> data\log.txt
python collect.py >> data\log.txt 2>&1
python collect_extra.py >> data\log.txt 2>&1
python fundamentals.py >> data\log.txt 2>&1
python fundamentals_extra.py >> data\log.txt 2>&1
rem business_scrape.py: 사업개요 변동 적어 일요일에만 전체 갱신 (캐시 자동 재사용)
powershell -NoProfile -Command "if ((Get-Date).DayOfWeek -eq 'Sunday') { python business_scrape.py }" >> data\log.txt 2>&1
python build_data.py >> data\log.txt 2>&1
python listing_dates.py >> data\log.txt 2>&1
python calendar_events.py >> data\log.txt 2>&1
python earnings_season.py >> data\log.txt 2>&1
python event_stocks.py >> data\log.txt 2>&1
python categories.py >> data\log.txt 2>&1
python recommend.py >> data\log.txt 2>&1
python valuation.py >> data\log.txt 2>&1
rem chart_5y.py: 5년 시계열은 변화 적어 주1회(월요일)만 갱신
powershell -NoProfile -Command "if ((Get-Date).DayOfWeek -eq 'Monday') { python chart_5y.py }" >> data\log.txt 2>&1
python news_analysis.py >> data\log.txt 2>&1
python market_overview.py >> data\log.txt 2>&1
python daily_reports.py >> data\log.txt 2>&1
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
