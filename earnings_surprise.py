# -*- coding: utf-8 -*-
"""
earnings_surprise.py — 어닝 서프라이즈 (v1)

v0 → v1 진화:
  v0: fundamentals.csv 의 op_q(실적) vs op_q_est(추정) 단순 갭만
  v1: + DART 공시검색으로 실제 발표일 매칭
      + 발표일 -1 거래일 vs 발표일 +5 거래일 주가 변화율
      → "어닝 서프라이즈 + 시장 반응" 결합

데이터 소스:
  fundamentals.csv     — 분기 실적·추정 (q_period)
  data/corp_code_map.json — stock_code → corp_code (cashflow.py 가 캐싱)
  DART OpenAPI list.json — 정기공시 발표일
  FDR DataReader        — 발표 전후 주가

출력: data/earnings_surprise.json
  - top_positive[]      : 갭 + 발표후 주가반응 상위
  - top_reaction[]      : 발표후 주가반응 상위 (서프라이즈 + 시장 인정)
"""
import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
import FinanceDataReader as fdr

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

HERE = Path(__file__).parent
DATA = HERE / "data"
FUND = DATA / "fundamentals.csv"
CORP_MAP = DATA / "corp_code_map.json"
OUT = DATA / "earnings_surprise.json"

DART_BASE = "https://opendart.fss.or.kr/api"


def _api_key() -> str:
    env = HERE / ".env"
    if env.exists():
        for line in env.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line.startswith("DART_API_KEY="):
                return line.split("=", 1)[1].strip()
    return os.environ.get("DART_API_KEY", "").strip()


def _qperiod_to_dates(q: str) -> tuple[str, str] | None:
    """'2026.03' → (분기 종료일+1, +90일). 발표일 검색 범위."""
    try:
        y, m = q.split(".")
        y, m = int(y), int(m)
    except Exception:
        return None
    end_of_q = datetime(y, m, 1)
    # 분기말 = 해당 월 말일. 3월=4월 1일 / 6월=7월 1일 ...
    # 간단히 m+1 1일 → +90일
    if m == 12:
        bgn = datetime(y + 1, 1, 1)
    else:
        bgn = datetime(y, m + 1, 1)
    end = bgn + timedelta(days=90)
    return bgn.strftime("%Y%m%d"), end.strftime("%Y%m%d")


def fetch_announcement_date(key: str, corp_code: str, q_period: str) -> str | None:
    """해당 분기 정기공시(분기/반기/사업보고서)의 첫 발표일 (YYYY-MM-DD)."""
    rng = _qperiod_to_dates(q_period)
    if not rng:
        return None
    bgn, end = rng
    params = {
        "crtfc_key": key,
        "corp_code": corp_code,
        "bgn_de": bgn,
        "end_de": end,
        "pblntf_ty": "A",  # 정기공시
        "page_count": 10,
    }
    try:
        r = requests.get(f"{DART_BASE}/list.json", params=params, timeout=10)
        d = r.json()
    except Exception:
        return None
    if d.get("status") != "000":
        return None
    items = d.get("list", [])
    # 분기·반기·사업보고서 중 가장 빠른 발표일
    for it in items:
        rcept_dt = it.get("rcept_dt", "")
        if len(rcept_dt) == 8:
            return f"{rcept_dt[:4]}-{rcept_dt[4:6]}-{rcept_dt[6:8]}"
    return None


def fetch_price_reaction(ticker: str, announce_date: str, lookback: int = 1, forward: int = 5) -> float | None:
    """발표일 -lookback 거래일 종가 vs +forward 거래일 종가 변화율(%)."""
    try:
        d0 = datetime.strptime(announce_date, "%Y-%m-%d")
    except Exception:
        return None
    start = (d0 - timedelta(days=10)).strftime("%Y-%m-%d")
    end = (d0 + timedelta(days=15)).strftime("%Y-%m-%d")
    try:
        df = fdr.DataReader(ticker, start, end)
    except Exception:
        return None
    if df is None or len(df) < 3:
        return None
    df = df.sort_index()
    closes = df["Close"].astype(float).tolist()
    dates = list(df.index)
    # announce_date 직후 첫 거래일 찾기
    after = [i for i, d in enumerate(dates) if d.strftime("%Y-%m-%d") >= announce_date]
    if not after:
        return None
    i_announce = after[0]
    i_before = max(0, i_announce - lookback)
    i_after = min(len(closes) - 1, i_announce + forward)
    if i_after <= i_before:
        return None
    return round((closes[i_after] / closes[i_before] - 1) * 100, 2)


def main() -> None:
    if not FUND.exists():
        print("fundamentals.csv 없음 — 스킵")
        return
    if not CORP_MAP.exists():
        print("corp_code_map.json 없음 (cashflow.py 먼저 실행 필요) — v0 모드로 폴백")

    key = _api_key()
    corp_map = json.loads(CORP_MAP.read_text(encoding="utf-8")) if CORP_MAP.exists() else {}

    fund = pd.read_csv(FUND, dtype={"ticker": str})
    fund["ticker"] = fund["ticker"].str.zfill(6)

    rows = []
    success = 0
    for _, r in fund.iterrows():
        t = r["ticker"]
        op_q = pd.to_numeric(r.get("operating_profit_q"), errors="coerce")
        op_q_est = pd.to_numeric(r.get("operating_profit_q_est"), errors="coerce")
        if pd.isna(op_q) or pd.isna(op_q_est) or op_q_est == 0:
            continue
        surprise = round((op_q - op_q_est) / abs(op_q_est) * 100, 1)

        # v1: 실제 발표일 + 주가반응
        q_period = str(r.get("q_period") or "")
        announce_date = None
        price_reaction = None
        if key and t in corp_map and q_period:
            announce_date = fetch_announcement_date(key, corp_map[t], q_period)
            time.sleep(0.05)
            if announce_date:
                price_reaction = fetch_price_reaction(t, announce_date)
                success += 1

        rows.append({
            "t": t,
            "n": str(r.get("name") or ""),
            "q_period": q_period,
            "op_q": float(op_q),
            "op_q_est": float(op_q_est),
            "surprise_pct": surprise,
            "announce_date": announce_date,
            "price_reaction_pct": price_reaction,  # 발표일 -1 vs +5거래일
        })

    # 정렬 두 가지
    top_positive = sorted(rows, key=lambda x: x["surprise_pct"], reverse=True)[:25]
    top_reaction = sorted(
        [r for r in rows if r["price_reaction_pct"] is not None and r["surprise_pct"] > 0],
        key=lambda x: x["price_reaction_pct"],
        reverse=True,
    )[:25]

    out = {
        "updated": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "count": len(rows),
        "with_date": success,
        "top_positive": top_positive,
        "top_reaction": top_reaction,
    }
    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"저장: {OUT}  총 {len(rows)}종 · 발표일 매칭 {success}종")

    if top_positive:
        print("\n[어닝 서프라이즈 TOP 5 (실적 갭)]")
        for r in top_positive[:5]:
            d = r["announce_date"] or "-"
            pr = f"{r['price_reaction_pct']:+.1f}%" if r["price_reaction_pct"] is not None else "-"
            print(f"  {r['n']:14} 갭 +{r['surprise_pct']:>6.1f}%  발표 {d}  주가반응 {pr}")
    if top_reaction:
        print("\n[발표후 주가반응 TOP 5 (서프라이즈 종목 중)]")
        for r in top_reaction[:5]:
            print(f"  {r['n']:14} 갭 +{r['surprise_pct']:>6.1f}%  주가반응 {r['price_reaction_pct']:+.1f}%  발표 {r['announce_date']}")


if __name__ == "__main__":
    main()
