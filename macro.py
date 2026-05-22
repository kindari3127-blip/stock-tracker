# -*- coding: utf-8 -*-
"""
macro.py — 거시지표 일별 수집
USD/KRW · US10Y(^TNX) · WTI · VIX · DXY · S&P500 · KOSPI · KOSDAQ · GOLD · BTC
최근 60거래일 시계열 + 핵심 변화율 → data/macro.json
"""
import sys
import json
from datetime import datetime, timedelta
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

import FinanceDataReader as fdr

HERE = Path(__file__).parent
DATA = HERE / "data"
DATA.mkdir(exist_ok=True)
OUT = DATA / "macro.json"

# 표시명 → FDR 심볼 + 단위 + 카테고리
SYMBOLS = [
    # 환율·금리·원자재 (시장 컨텍스트 핵심)
    ("USD/KRW",  "USD/KRW",  "원",   "환율"),
    ("US10Y",    "^TNX",     "%",    "금리"),
    ("US2Y",     "^IRX",     "%",    "금리"),
    ("WTI",      "CL=F",     "$",    "원자재"),
    ("GOLD",     "GC=F",     "$",    "원자재"),
    # 위험선호 지표
    ("DXY",      "DX-Y.NYB", "",     "위험"),
    ("VIX",      "VIX",      "",     "위험"),
    # 글로벌 지수
    ("S&P500",   "US500",    "p",    "글로벌"),
    ("BTC",      "BTC/USD",  "$",    "글로벌"),
    # 국내 지수
    ("KOSPI",    "KS11",     "p",    "국내"),
    ("KOSDAQ",   "KQ11",     "p",    "국내"),
]


def _pct(cur, prev):
    if prev is None or prev == 0:
        return None
    return round((cur - prev) / abs(prev) * 100, 2)


def fetch_one(name: str, sym: str) -> dict | None:
    try:
        start = (datetime.now() - timedelta(days=120)).strftime("%Y-%m-%d")
        df = fdr.DataReader(sym, start)
        if df is None or len(df) == 0:
            return None
        df = df.tail(60).copy()
        closes = [round(float(x), 4) for x in df["Close"].tolist()]
        dates = [d.strftime("%Y-%m-%d") for d in df.index]
        cur = closes[-1]
        return {
            "name": name,
            "symbol": sym,
            "current": cur,
            "d1": _pct(cur, closes[-2] if len(closes) > 1 else None),
            "d5": _pct(cur, closes[-6] if len(closes) > 5 else None),
            "d20": _pct(cur, closes[-21] if len(closes) > 20 else None),
            "d60": _pct(cur, closes[0] if len(closes) > 1 else None),
            "high60": round(max(closes), 4),
            "low60": round(min(closes), 4),
            "series": [{"d": d, "c": c} for d, c in zip(dates, closes)],
        }
    except Exception as e:
        print(f"  FAIL {name:10s} ({sym}): {e}")
        return None


def main():
    print("매크로 수집 시작")
    out = {
        "updated": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "items": [],
    }
    ok = 0
    for name, sym, unit, cat in SYMBOLS:
        rec = fetch_one(name, sym)
        if rec is None:
            print(f"  -    {name}")
            continue
        rec["unit"] = unit
        rec["category"] = cat
        out["items"].append(rec)
        d1 = rec["d1"]
        d20 = rec["d20"]
        print(f"  OK   {name:10s} {rec['current']:>12.2f}  D1={d1:>6}%  D20={d20:>6}%" if d1 is not None and d20 is not None
              else f"  OK   {name:10s} {rec['current']:>12.2f}")
        ok += 1

    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n저장: {OUT}  ({ok}/{len(SYMBOLS)} 지표)")


if __name__ == "__main__":
    main()
