@echo off
chcp 65001 > nul
cd /d %~dp0
echo ============================================
echo   주식추적기 - 당일 종가 수집
echo ============================================
python collect.py
python collect_extra.py
python fundamentals.py
python fundamentals_extra.py
rem business_scrape.py: 사업개요는 변동 적어 신규 종목 추가시·주 1회만 권장
rem python business_scrape.py
python report_html.py
echo.
echo --- 최근 5일 섹터별 종가 요약 ---
python view.py
echo.
pause
