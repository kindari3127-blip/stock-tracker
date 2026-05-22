# WIP — 주식추적기

**목표:** 한국 주식시장 68개 세분화 섹터 × 대표주 3종(204쌍, 고유 203종목) 종가 일별 누적
**경로:** `C:\Users\kinda\OneDrive\바탕 화면\주식추적기\`
**GitHub:** https://github.com/kindari3127-blip/stock-tracker
**Pages:** https://kindari3127-blip.github.io/stock-tracker/report.html (PWA, 휴대폰 홈)
**메모리:** project_stock_tracker.md

---

## 운영 상태 ✅

- **시장 전망 + 유망주 발굴 전면 개편 (2026-05-21~22, Phase 1~3 워머업 + Phase 4 UI)**
  - **macro.py** 신설: USD/KRW · US10Y(`^TNX`) · US2Y(`^IRX`) · WTI · GOLD · DXY · VIX · S&P500 · BTC · KOSPI · KOSDAQ 11지표 일별 + D1/D5/D20 변화율 → `data/macro.json`. FDR 단일 호출 ~3초.
  - **flows.py** 신설: 시장 전체 일별 수급(네이버 `investorDealTrendDay`, 단위 백만원, KOSPI 기준 시장 전체 — 네이버가 KOSPI/KOSDAQ 분리 응답 안 줘 1종 통합) + 종목별 외인·기관 일별(네이버 `frgn.naver` table#3, 20일치) → `data/flows_market.json`, `data/flows_stock.json`. 종목별 374종 100% 수집 (~2분).
  - **prospects.py** 신설: 멀티팩터 유망주 발굴. 풀 = chart_data 686종 (시총 상위 500 + PFR Top 200 + 추적). 점수 = 펀더(0.30) + 기술(0.20) + 모멘텀(0.20) + 수급(0.20) + 섹터(0.10). 출력 5뷰 — 종합 TOP 30 / 60일 신고가 근접(≥97%) / 외인 매수 강도 / 모멘텀+펀더 동시 강세 / 거래량 spike(3일 평균이 20일 평균 2.5배+).
  - **recommend.py 패치**: `chart_data.json`에 `volumes` 추가(거래량 시계열). prospects.py의 vol_spike 시그널이 다음 빌드부터 활성화.
  - **cashflow.py**: 시총 하한 1,000억 → **500억**으로 확장(2026-05-21).
  - **valuation.py 보강**: `fcf_yield` = FCF / 시총 × 100 (cashflow.csv 의 fcf 활용). 추적 200종 100% 채움. EV/EBITDA는 EBITDA 컬럼 부재로 보류(Phase 3 작업).
  - **market_overview.py 입력 확장**: 매크로 11지표 + 시장 수급 5일 + 종목별 외인 TOP/BOT 5종을 AI 시황 프롬프트에 추가. 시스템 프롬프트에 매크로↔국내 시장 연결성, 수급 방향 분석 가이드 추가.
  - **sector_rotation.py** 신설(Phase 3 v1): SECTORS 79개 × 종목별 1년 시계열 → 1주·1개월·3개월·1년 시총가중 평균 등락률 + momentum_score(0.4·0.3·0.2·0.1). 시계열 ~4분 소요라 월요일에만 실행. `data/sector_rotation.json`.
  - **earnings_surprise.py** 신설(Phase 3 v0): fundamentals.csv 의 `operating_profit_q` vs `operating_profit_q_est` 갭. 157종 산출. 발표 시점 매칭/발표후 주가반응은 v1 작업.
  - **auto.bat 편입**: macro → (build_data) → flows → recommend → prospects → buy_timing → valuation → earnings_surprise → (월요일만) sector_rotation → news_analysis → market_overview.
  - **analysis_daily.bat 편입**: recommend → prospects + valuation → earnings_surprise (오후 분석에 발굴·갭 갱신).
  - **report.html UI 통합** (2,062 KB, +1,000 KB): 신설 섹션 5개 — 매크로 카드 그리드(D1/D5/D20 색상 표시) / 시장 수급 5일 표(개인·외인·기관 + 누적) / 유망주 발굴(5탭) / 섹터 로테이션 4타임프레임 표 / 어닝 서프라이즈 TOP. window.NEW 단일 변수로 던지고 자체 IIFE 렌더러 — 기존 코드 무회귀.
- **deploy 경로 OneDrive 밖으로 분리 (2026-05-21)**: `.deploy/` → `C:\Users\kinda\.stock-tracker-deploy\`. OneDrive Files On-Demand 의 ReparsePoint + ReadOnly 잠금으로 `shutil.rmtree` 가 PermissionError(WinError 5) 를 던져 2026-05-21 05:00 / 17:30 자동 실행 두 번 모두 gh-pages push 실패한 상태였음. `deploy.py` 의 `DEPLOY` 상수 변경 + `clean_deploy()` 에 `onerror` 핸들러(`stat.S_IWRITE` 로 read-only 해제 후 재시도) 추가. 수동 2회 실행으로 init → 재배포 정상 확인. (옛 `.deploy/` 폴더는 OneDrive 잠금으로 수동 삭제 불가 — 그대로 두면 무해, 정리하려면 OneDrive 일시정지 후 `attrib -r /s /d` 필요)
- **작업 스케줄러 2개 체계 운영 (2026-05-16 확정)**
  - `주식추적기` (월~금 05:00): 전체 파이프라인 (collect → fundamentals → cashflow → 분석 → AI → 배포)
  - `주식추적기_분석` (월~금 17:30): 빠른 분석 갱신 (build_data → recommend → buy_timing → 배포). **강세섹터·강세종목·추천종목·매수타이밍** 장마감 당일 데이터 반영.
  - 재등록 필요 시: `powershell -ExecutionPolicy Bypass -File setup_analysis_task.ps1`
- **구 문제 해결**: 스케줄러가 "매주 월요일 05:00"으로 잘못 설정되어 있던 것을 "월~금 05:00"으로 수정 (2026-05-14 LastResult=0xC000013A Ctrl+C 강제종료 확인)
- auto.bat: collect.py + report_html.py --no-open + git push
- 변경 없으면 commit 건너뜀
- **AI 분석 3종(news_analysis / market_overview / daily_reports) 모두 Max 플랜 CLI 래퍼 사용** (2026-05-04 전환). Anthropic SDK 직접 호출 제거 → API 과금 0원.
- **PWA 새로고침 시 fresh data 보장** (2026-05-04). sw.js 의 fetch 에 `cache: 'no-cache'` 추가 → HTTP·CDN 재검증 강제. report.html 의 SW 등록부에 `reg.update()` + `skipWaiting` + `controllerchange→reload` 플로우 추가, 새 SW 발견 시 PWA 재시작 없이 즉시 갱신. chart_data/chart_5y JSON 페치는 `?v=BUILD_TS` 로 cache-bust.
- **PFR(=시총÷FCF) 추가** (2026-05-05). DART OpenAPI 로 영업CF·CAPEX(유형+무형 취득) 수집 → FCF 계산. `cashflow.py` 신설(corp_code 매핑 7일 캐시), `data/cashflow.csv` 저장. **대상은 KRX 전체 보통주 시총 ≥ 1,000억 + DART 매핑 = 1,564종목 → 1,551 저장 (878 유효)**. 종목 모달·검색결과 PFR 표시 + 신규 「PFR 저평가 Top 200」 단일 패널. FY 2025 기준 저평가 Top1 = 상상인(0.31), Top10 대부분 보험·금융업(OCF가 보험료 수입이라 비대). 색상: 녹(<10) → 연녹(<20) → 황(<40) → 빨(≥40). cashflow.py 풀 실행 ≈ 12분, auto.bat 매일 새벽 5시 실행에 포함됨.
- **PFR Top200 추적 외 종목도 모달·차트 통합** (2026-05-05). `recommend.py` 의 chart_pool 에 PFR Top 200 union 추가 → chart_data.json 에 가격 시계열 포함(현재 689종목). `_load_fundamentals` 가 추적 외 PFR Top 200 메타까지 fund 에 포함. `openStock` 폴백 분기로 추적 외 종목도 모달 표시 (헤더에 "추적외" 뱃지, 추적 종목 전용 UI는 `isTracked` 가드로 생략).

## 다음 행동

기본: 자동 운영. 손댈 일 없음.
- `주식추적기` (05:00) + `주식추적기_분석` (17:30) 두 스케줄 모두 Ready 상태 확인 완료.

이벤트성 작업:
- [ ] 상장폐지/티커 변경 발견 시 `sectors.py` 업데이트 (예: HD현대건설기계 → HD건설기계 267270)
- [ ] 주 1회 `industry_index.py` 수동 실행 권장 (네이버 79개 업종 매핑 갱신)
- [ ] 신규 관심 종목 → `explore.py <키워드> [N]`로 조사 후 `sectors.py` 추가

2026-05-22 전면 개편 완성 (Phase 1~4 모두):
- [x] **earnings_surprise v1 완성**: DART 정기공시 발표일 매칭 + 발표일 -1 거래일 vs +5 거래일 주가반응 계산. 159종 100% 매칭. `top_positive`(갭) + `top_reaction`(주가반응 — "시장이 인정한 어닝 서프라이즈") 두 뷰. 예: 엘앤에프 갭 +36% + 주가반응 +19%, LG이노텍 갭 +109% + 주가반응 +11%.
- [x] **EV/EBITDA 추가** (cashflow.py에 감가상각비 ACC_DEPR account_nm 키워드 매칭으로 수집 추가 → valuation.py에서 EBITDA = 영업이익(억원 단위) + 감가상각비, EV = 시총 + 부채(자본총계 × debt_ratio) 근사). **현재 채움률 45/200 (대기업 005930·000660 등은 CF 표에 감가상각 명시 안 함 → 주석 API로 정밀 추출 v2 필요)**. 합리적 범위(0~200) 클램프, median 15.4 (한국 시장 정상치).
- [x] **유망주 발굴 풀 KRX 전체로 확장**: `recommend.py` CHART_POOL_TOP_N 500 → 1500. chart_data 1,567종 (KRX 시총 상위 1,500 + PFR Top 200 + 추적 union). prospects 발굴 풀 686 → **1,567종**. recommend 시계열 수집 ~26분.
- [x] **cashflow 풀 확장**: 시총 ≥ 500억 효과로 1,564 → **1,997종**, PFR 유효 1,120종. 저평가 Top5 = 상상인(0.3), 상상인증권(0.3), 흥국화재(0.3), 동양생명(0.5), 웅진(0.8).
- [x] **report.html 3-탭 재구성**: 헤더에 `<nav class="tab-bar">` 추가 (시황 / 발굴 / 추적). 클래스 기반 JS 토글로 mainPanel `.section` 들을 탭 그룹별 show/hide. nav-menu 앵커 링크도 자동 탭 전환. 마크업 손대지 않고 IIFE에서 sectionTab() 매핑.
- [x] **PWA 폰 업데이트 UX 강화 (2026-05-22)**: (1) 헤더에 `vYYYYMMDDHHMMSS` 빌드 태그 표시, (2) `⟳` 강제 새로고침 버튼 — 캐시 전체 삭제 + SW unregister + `?_=ts` 쿼리 하드 리로드, (3) 빌드 ts localStorage 비교 → 변경 시 "새 데이터 반영됨" 토스트 3.5초, (4) `visibilitychange` 시 `reg.update()` 강제 호출 (앱 복귀 시 즉시 최신 확인). 기존 controllerchange→reload 자동 메커니즘과 결합되어 폰에서 별도 조작 없이 갱신.
- [x] **sector_rotation v2 풀 확장 (2026-05-22 13:30)**: SECTORS 약 250종 → industry_map.json + search_index.json 결합, 시총 ≥ 500억 KRX 전체 풀 2,033종 / 77 업종. 평균 = 시총가중. chart_data(60일) + chart_5y(~1년) 우선 사용, 부족분만 fdr 폴백 (492회). 종목 단위 ±150% 클리핑(IPO·테마주 노이즈 제거). 모멘텀 TOP: 생명보험·전자장비·복합기업·반도체·전자제품 순.
- [x] **EV/EBITDA 금융업 제외 (2026-05-22 13:30)**: 은행·보험·증권·신용서비스·다각화된금융 업종은 EBITDA 개념 의미 없어 제외 (KB금융·신한지주 등 더 이상 미친 EV/EBITDA 노출 안 됨). 합리적 범위 클램프 0~200 → 0~100 으로 좁힘. 단위 코멘트 "백만원" → "억원" 으로 정정 (네이버 표 기준). 200종 중 37종 채워짐 — cashflow.csv depr 행 비율(405/1997) 한계, 다음 풀 재수집 시 늘어남.

Phase 5 다음 세션 (정밀 보완):
- [ ] **EV/EBITDA 정밀화**: 대기업 감가상각 추출 — DART 주석(footnote) API 또는 사업보고서 PDF 파싱. 현재 채움률 22% → 80%+ 목표.
- [ ] **PWA 매크로·수급 위젯**: 홈 화면 카드에 매크로 핵심 5지표 + 외인 누적 mini-chart.
- [ ] **fundamentals 단위 컬럼 메타**: operating_profit / revenue / net_profit 단위(억원)를 명시적 메타 컬럼으로 (현재는 valuation.py 주석에만 명시).
- [ ] **sector_rotation v3 (선택)**: 80일 시계열 chart_data 확장 → 3개월 정확 계산. 현재는 60일이라 3M = 60일 차이 근사.

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
