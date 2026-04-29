# -*- coding: utf-8 -*-
"""
분기 실적 시즌 동안 시총 상위 종목들을 캘린더에 "발표 예정"으로 추가.
정확한 일자는 DART API 필요 — 본 스크립트는 시즌 통합 표시.
"""
import json
import sys
from datetime import date, timedelta
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

HERE = Path(__file__).parent
SEARCH = HERE / "data" / "search_index.json"
CAL = HERE / "data" / "calendar.json"
TOP_N = 30

SEASON_DEFS = [
    (1, 25, 3, 15, "전년 4Q·연간 실적 시즌 (개별 종목 발표)"),
    (4, 15, 5, 15, "1Q 실적 시즌 (개별 종목 발표)"),
    (7, 15, 8, 15, "2Q 실적 시즌 (개별 종목 발표)"),
    (10, 15, 11, 15, "3Q 실적 시즌 (개별 종목 발표)"),
]


def _next_season(today: date) -> tuple[date, date, str] | None:
    for sm, sd, em, ed, lab in SEASON_DEFS:
        s = date(today.year, sm, sd)
        e = date(today.year, em, ed)
        if today > e:
            continue
        return s, e, lab
    sm, sd, em, ed, lab = SEASON_DEFS[0]
    return date(today.year + 1, sm, sd), date(today.year + 1, em, ed), lab


def main() -> None:
    today = date.today()
    season = _next_season(today)
    if not season:
        return
    s, e, lab = season

    search = json.loads(SEARCH.read_text(encoding="utf-8"))
    top = sorted(search.get("stocks", []), key=lambda x: x.get("m", 0) or 0, reverse=True)[:TOP_N]

    cal = json.loads(CAL.read_text(encoding="utf-8")) if CAL.exists() else {"events": []}
    cal["events"] = [ev for ev in cal["events"] if ev.get("type") != "실적공시예정"]

    cal["events"].append({
        "date": s.isoformat(),
        "date_end": e.isoformat(),
        "type": "실적공시예정",
        "title": f"{lab} — 시총 상위 {TOP_N}개 발표 예정",
        "impact": "high",
        "desc": "정확한 일자는 종목별 상이 (분기 종료 후 30~45일). 정확한 일정은 DART API 필요.",
        "stocks": [{"t": x["t"], "n": x["n"], "i": x.get("i", "")} for x in top],
    })
    cal["events"].sort(key=lambda x: x["date"])

    CAL.write_text(json.dumps(cal, ensure_ascii=False), encoding="utf-8")
    print(f"실적 시즌 추가: {lab} ({s} ~ {e}) / 종목 {TOP_N}개")


if __name__ == "__main__":
    main()
