# WIP — 주식추적기

**목표:** 한국 주식시장 63개 세분화 섹터 × 대표주 3종(189종목) 종가 일별 누적
**경로:** `C:\Users\kinda\OneDrive\바탕 화면\주식추적기\`
**GitHub:** https://github.com/kindari3127-blip/stock-tracker
**Pages:** https://kindari3127-blip.github.io/stock-tracker/report.html (PWA, 휴대폰 홈)
**메모리:** project_stock_tracker.md

---

## 운영 상태 ✅

- 매일 월~금 18:00 Windows 작업 스케줄러 자동 실행 (등록 완료 2026-04-23)
- 작업명: `주식추적기` / 실행: `cmd.exe /c "...auto.bat"`
- auto.bat: collect.py + report_html.py --no-open + git push
- 변경 없으면 commit 건너뜀
- **AI 분석 3종(news_analysis / market_overview / daily_reports) 모두 Max 플랜 CLI 래퍼 사용** (2026-05-04 전환). Anthropic SDK 직접 호출 제거 → API 과금 0원.
- **작업 스케줄러 재활성화 완료** (2026-05-04, State=Ready, 매일 05:00). market_overview.py end-to-end 검증 통과 후 활성화.
- **PWA 새로고침 시 fresh data 보장** (2026-05-04). sw.js 의 fetch 에 `cache: 'no-cache'` 추가 → HTTP·CDN 재검증 강제. report.html 의 SW 등록부에 `reg.update()` + `skipWaiting` + `controllerchange→reload` 플로우 추가, 새 SW 발견 시 PWA 재시작 없이 즉시 갱신. chart_data/chart_5y JSON 페치는 `?v=BUILD_TS` 로 cache-bust.

## 다음 행동

기본: 자동 운영. 손댈 일 없음.

이벤트성 작업:
- [ ] 상장폐지/티커 변경 발견 시 `sectors.py` 업데이트 (예: HD현대건설기계 → HD건설기계 267270)
- [ ] 주 1회 `industry_index.py` 수동 실행 권장 (네이버 79개 업종 매핑 갱신)
- [ ] 신규 관심 종목 → `explore.py <키워드> [N]`로 조사 후 `sectors.py` 추가

## 절대 주의 (실수 잦은 부분)

- **`pykrx` 사용 금지** — 2026년 KRX 변경으로 OHLCV JSON 파싱 전부 실패. **FinanceDataReader만**
- 네이버 금융 크롤링: `BeautifulSoup(r.content, 'lxml')` (바이트 전달). `r.encoding='euc-kr'` 강제 시 전부 깨짐
- log.txt는 UTF-8 → PowerShell `Get-Content -Encoding UTF8` 필수
- `.bat` 작성 시: (1) BOM 없이 (Write 도구는 BOM 추가 → wb 모드로 직접 쓰기), (2) `.bat`에 영문 명령만 (cp949), (3) `>nul` 자동 변환되면 `1>NUL`로
- **Anthropic SDK 직접 호출 금지** — 모든 Claude 호출은 `_claude_cli.py` 의 `call_claude_cli()` 만 사용 (Max 플랜 OAuth). 새 AI 스크립트 추가 시 동일 패턴 따를 것

## 자주 쓰는 명령

```
# 수동 실행 (collect+fundamentals+report+view+pause)
run.bat

# 섹터 외 종목 탐색
python explore.py 우주항공 5

# 업종 매핑 재구축
python industry_index.py
```

## 끊김 후 재개

1. 이 WIP.md 읽기
2. `data/log.txt` 확인 (UTF-8 인코딩 필수) — 자동 실행 성공 여부
3. 종목 추가/변경 요청이면 `sectors.py`만 수정 후 다음 18:00 자동 실행 대기 (또는 수동 `run.bat`)
