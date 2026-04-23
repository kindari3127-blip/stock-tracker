# 주식추적기

> 매일 평일 18:00 자동 수집 · 리포트 재생성 · GitHub Pages 자동 배포 · PWA로 휴대폰 홈 화면에 추가 가능

📈 **[리포트 바로 보기](./report.html)**

---


대한민국 주식시장을 **64개 세분화 섹터**로 나누고, 각 섹터의 **대표주 3종**(시총·주도주·실적 기준)의
**일별 종가를 누적 기록**하는 프로그램입니다.

## 구성

| 파일 | 역할 |
|---|---|
| `sectors.py` | 섹터 → 대표주 3종 매핑 (총 192종목) |
| `collect.py` | 당일 종가 수집 후 `data/prices.csv`에 누적 저장 |
| `view.py` | 섹터별 최근 N일 종가 피벗 출력 |
| `run.bat` | 더블클릭 실행 (수집 + 요약 출력) |
| `data/prices.csv` | 누적 데이터 (date, sector, ticker, name, close, change_pct) |

## 사용법

```
# 오늘 종가 수집 (비영업일이면 직전 영업일)
python collect.py

# 특정 일자
python collect.py 2026-04-22

# 누적 결과 조회
python view.py          # 최근 5일
python view.py 10       # 최근 10일
python view.py 5 반도체 # '반도체' 포함 섹터만
```

혹은 `run.bat` 더블클릭.

## 매일 자동 실행 (Windows 작업 스케줄러)

1. `작업 스케줄러` → `기본 작업 만들기`
2. 트리거: 매일, 장 마감 후 (예: 18:00)
3. 동작: 프로그램 시작 → `C:\Users\kinda\OneDrive\바탕 화면\주식추적기\run.bat`

## 데이터 컬럼

- `date`: YYYYMMDD
- `sector`: 섹터명 (예: `반도체_메모리`)
- `ticker`: 종목코드 6자리
- `name`: 종목명
- `close`: 종가 (원)
- `change_pct`: 전일 대비 등락률 (%)

## 섹터 보완/수정

`sectors.py` 의 `SECTORS` 딕셔너리를 편집하면 즉시 반영됩니다.
종목코드(ticker)는 6자리 문자열, 앞자리 0 유지 필수.

## 의존성

```
pip install -r requirements.txt
```

데이터 출처: FinanceDataReader (네이버 금융 기반). 한 번 호출로 전 종목 시세 조회 가능하여 빠르고 안정적.
