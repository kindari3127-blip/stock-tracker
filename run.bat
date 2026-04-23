@echo off
chcp 65001 > nul
cd /d %~dp0
echo ============================================
echo   주식추적기 - 당일 종가 수집
echo ============================================
python collect.py
python fundamentals.py
python report_html.py
echo.
echo --- 최근 5일 섹터별 종가 요약 ---
python view.py
echo.
pause
