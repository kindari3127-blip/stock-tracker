# WIP — 주식추적기

**목표:** 한국 주식시장 68개 세분화 섹터 × 대표주 3종(204쌍, 고유 203종목) 종가 일별 누적
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
- **PFR(=시총÷FCF) 추가** (2026-05-05). DART OpenAPI 로 영업CF·CAPEX(유형+무형 취득) 수집 → FCF 계산. `cashflow.py` 신설(corp_code 매핑 7일 캐시), `data/cashflow.csv` 저장. **대상은 KRX 전체 보통주 시총 ≥ 1,000억 + DART 매핑 = 1,564종목 → 1,551 저장 (878 유효)**. 종목 모달·검색결과 PFR 표시 + 신규 「PFR 저평가 Top 200」 단일 패널. FY 2025 기준 저평가 Top1 = 상상인(0.31), Top10 대부분 보험·금융업(OCF가 보험료 수입이라 비대). 색상: 녹(<10) → 연녹(<20) → 황(<40) → 빨(≥40). cashflow.py 풀 실행 ≈ 12분, auto.bat 매일 새벽 5시 실행에 포함됨.
- **PFR Top200 추적 외 종목도 모달·차트 통합** (2026-05-05). `recommend.py` 의 chart_pool 에 PFR Top 200 union 추가 → chart_data.json 에 가격 시계열 포함(현재 689종목). `_load_fundamentals` 가 추적 외 PFR Top 200 메타까지 fund 에 포함. `openStock` 폴백 분기로 추적 외 종목도 모달 표시 (헤더에 "추적외" 뱃지, 추적 종목 전용 UI는 `isTracked` 가드로 생략).

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
- **DART API 키**는 `.env` 의 `DART_API_KEY` (gitignore 됨). 분당 1000 / 일 10000 호출 제한, 우리는 종목당 1회/일 ≈ 204회 → 여유. corp_code 매핑은 7일 캐시(`data/corp_code_map.json`). 신규 종목 추가 시 캐시 자동 갱신됨

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
