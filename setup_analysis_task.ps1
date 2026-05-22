# setup_analysis_task.ps1
# 작업 스케줄러 2개 등록 (관리자 권한 불필요):
#   1) 주식추적기         - 전체 파이프라인 (월~금 05:00)
#   2) 주식추적기_분석    - 빠른 분석 갱신  (월~금 17:30, 장마감 후)
# 실행: powershell -ExecutionPolicy Bypass -File setup_analysis_task.ps1

$dir = $PSScriptRoot

schtasks /Create /TN "주식추적기" `
    /TR "cmd.exe /c `"$dir\auto.bat`"" `
    /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 05:00 /F

schtasks /Create /TN "주식추적기_분석" `
    /TR "cmd.exe /c `"$dir\analysis_daily.bat`"" `
    /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 17:30 /F

Write-Host ""
schtasks /Query /TN "주식추적기" /FO LIST | Select-String "TaskName|Status|Next Run"
schtasks /Query /TN "주식추적기_분석" /FO LIST | Select-String "TaskName|Status|Next Run"
