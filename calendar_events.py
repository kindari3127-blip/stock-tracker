# -*- coding: utf-8 -*-
"""
주식 모멘텀 일정 캘린더 (하드코딩 + 패턴 계산).
출력: data/calendar.json — 향후 60일 이벤트 리스트
"""
import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

HERE = Path(__file__).parent
OUT = HERE / "data" / "calendar.json"
WINDOW_DAYS = 60

# 2026년 한국 금통위 일정 (출처: 한국은행 공식)
BOK_MEETINGS_2026 = [
    "2026-01-14", "2026-02-26", "2026-04-09", "2026-05-28",
    "2026-07-09", "2026-08-27", "2026-10-15", "2026-11-26",
]

# 2026년 미국 FOMC 일정 (FRB 공식 발표)
FOMC_MEETINGS_2026 = [
    "2026-01-28", "2026-03-18", "2026-04-29", "2026-06-17",
    "2026-07-29", "2026-09-16", "2026-10-28", "2026-12-09",
]

# 한국 거래소 2026년 휴장일 (KRX 공식)
KRX_HOLIDAYS_2026 = [
    ("2026-01-01", "신정"),
    ("2026-02-16", "설날 연휴"), ("2026-02-17", "설날"), ("2026-02-18", "설날 연휴"),
    ("2026-03-01", "3·1절"), ("2026-03-02", "3·1절 대체"),
    ("2026-05-05", "어린이날"), ("2026-05-25", "부처님오신날"),
    ("2026-06-03", "지방선거일"), ("2026-06-06", "현충일"),
    ("2026-08-15", "광복절"),
    ("2026-09-24", "추석 연휴"), ("2026-09-25", "추석"), ("2026-09-26", "추석 연휴"),
    ("2026-10-03", "개천절"), ("2026-10-09", "한글날"),
    ("2026-12-25", "성탄절"), ("2026-12-31", "연말 휴장"),
]

QUARTER_SEASONS = [
    ("01-25", "03-15", "전년 4Q·연간 실적 시즌"),
    ("04-15", "05-15", "1Q 실적 시즌"),
    ("07-15", "08-15", "2Q 실적 시즌"),
    ("10-15", "11-15", "3Q 실적 시즌"),
]

# 글로벌 주요 학회·쇼·포럼 (모멘텀 영향 큰 행사 위주)
GLOBAL_EVENTS = [
    ("2026-05-29", "2026-06-02", "ASCO 2026 (미국임상종양학회)", "학회", "high", "제약·바이오 임상 발표"),
    ("2026-06-09", "2026-06-13", "WWDC 2026 (Apple 개발자 컨퍼런스)", "쇼", "high", "애플 신제품·SW 발표"),
    ("2026-06-16", "2026-06-20", "BIO International 2026", "학회", "mid", "바이오 산업 컨퍼런스"),
    ("2026-09-04", "2026-09-08", "IFA 2026 베를린 (유럽 가전 박람회)", "쇼", "high", "가전·디스플레이"),
    ("2026-09-09", "2026-09-12", "Apple iPhone Event 2026", "쇼", "high", "신형 아이폰 공개 추정"),
    ("2026-10-15", "2026-10-18", "K-디스플레이 2026", "쇼", "mid", "디스플레이 업종"),
    ("2026-11-07", "2026-11-11", "AHA 2026 (미국심장학회)", "학회", "high", "심혈관·임상 결과"),
    ("2027-01-06", "2027-01-09", "CES 2027 라스베이거스", "쇼", "high", "전자·자동차·AI 신제품"),
    ("2027-01-11", "2027-01-14", "JPM Healthcare 2027 (J.P.모건 헬스케어)", "포럼", "high", "글로벌 제약·바이오 IR"),
    ("2027-01-19", "2027-01-23", "다보스 포럼 2027 (WEF 연차총회)", "포럼", "high", "글로벌 거시·정책"),
    ("2027-03-01", "2027-03-04", "MWC 2027 바르셀로나 (모바일월드콩그레스)", "쇼", "high", "통신·5G·반도체"),
    ("2027-03-19", "2027-03-23", "AAD 2027 (미국피부과학회)", "학회", "mid", "피부 의약품·미용"),
]


def _option_expiry(year: int, month: int) -> date:
    """매월 둘째 목요일 = KOSPI200 옵션 만기."""
    d = date(year, month, 1)
    while d.weekday() != 3:
        d += timedelta(days=1)
    return d + timedelta(days=7)


def _futures_expiry(year: int, month: int) -> date:
    """3·6·9·12월 둘째 목요일 = KOSPI200 선물 만기."""
    return _option_expiry(year, month)


def build() -> None:
    today = date.today()
    end = today + timedelta(days=WINDOW_DAYS)
    events: list[dict] = []

    for d, name in KRX_HOLIDAYS_2026:
        dt = date.fromisoformat(d)
        if today <= dt <= end:
            events.append({"date": d, "type": "휴장", "title": f"한국 휴장 — {name}", "impact": "low"})

    for d in BOK_MEETINGS_2026:
        dt = date.fromisoformat(d)
        if today <= dt <= end:
            events.append({"date": d, "type": "통화정책", "title": "한국 금통위 (기준금리 결정)", "impact": "high"})

    for d in FOMC_MEETINGS_2026:
        dt = date.fromisoformat(d)
        if today <= dt <= end:
            events.append({"date": d, "type": "통화정책", "title": "미국 FOMC (기준금리 결정)", "impact": "high"})

    for y in (today.year, today.year + 1):
        for m in range(1, 13):
            d = _option_expiry(y, m)
            if today <= d <= end:
                title = "KOSPI200 선물·옵션 동시만기" if m in (3, 6, 9, 12) else "KOSPI200 옵션 만기"
                impact = "high" if m in (3, 6, 9, 12) else "mid"
                events.append({"date": d.isoformat(), "type": "파생만기", "title": title, "impact": impact})

    for d_start, d_end, title, etype, impact, desc in GLOBAL_EVENTS:
        s = date.fromisoformat(d_start)
        e = date.fromisoformat(d_end)
        if today <= s <= end or today <= e <= end or (s <= today <= e):
            events.append({
                "date": d_start, "date_end": d_end,
                "type": etype, "title": title,
                "impact": impact, "desc": desc,
            })

    for start_md, end_md, label in QUARTER_SEASONS:
        for y in (today.year, today.year + 1):
            try:
                s = date.fromisoformat(f"{y}-{start_md}")
                e = date.fromisoformat(f"{y}-{end_md}")
            except ValueError:
                continue
            if today <= s <= end or today <= e <= end or (s <= today <= e):
                events.append({
                    "date": s.isoformat(), "date_end": e.isoformat(),
                    "type": "실적시즌", "title": label, "impact": "high",
                })

    events.sort(key=lambda x: x["date"])

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps({
        "generated": datetime.now().isoformat(),
        "window_days": WINDOW_DAYS,
        "events": events,
    }, ensure_ascii=False), encoding="utf-8")
    print(f"이벤트 저장: {OUT} ({len(events)}건)")
    for e in events[:8]:
        d2 = (' ~ ' + e['date_end']) if e.get('date_end') else ''
        print(f"  {e['date']}{d2}  [{e['impact']}] {e['title']}")


if __name__ == "__main__":
    build()
