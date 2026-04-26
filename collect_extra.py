# -*- coding: utf-8 -*-
"""
확장패널/대시보드용 SECTORS 외 종목의 종가 수집.

- 백필: python collect_extra.py 2025-04-23 2026-04-24
- 일일: python collect_extra.py             # 오늘 종가 추가

대상 종목:
  business_scrape._collect_targets() 와 동일 — SECTORS ∪ 산업맵 상위 30 ∪ 테마 상위 30
  (단 SECTORS 등록은 prices.csv에서 처리하므로 SECTORS 외 종목만 prices_extra.csv에 저장)

데이터 형식: date, ticker, name, close, change_pct
"""
import sys
import time
from pathlib import Path

import pandas as pd
import FinanceDataReader as fdr
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeRemainingColumn

from sectors import SECTORS
from business_scrape import _collect_targets

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

HERE = Path(__file__).parent
OUT = HERE / "data" / "prices_extra.csv"


def _extra_targets() -> list[tuple[str, str]]:
    in_sectors = {t for items in SECTORS.values() for t, _ in items}
    return [(t, n) for t, n in _collect_targets() if t not in in_sectors]


def collect_today() -> list[dict]:
    listing = fdr.StockListing("KRX").drop_duplicates(subset=["Code"]).set_index("Code")
    # 최근 영업일
    ref = fdr.DataReader("005930").tail(1).index[-1].strftime("%Y%m%d")
    rows = []
    for ticker, name in _extra_targets():
        if ticker not in listing.index:
            continue
        r = listing.loc[ticker]
        if pd.isna(r["Close"]) or r["Close"] <= 0:
            continue
        rows.append({
            "date": ref, "ticker": ticker, "name": name,
            "close": int(r["Close"]),
            "change_pct": round(float(r["ChagesRatio"]), 2),
        })
    return rows


def collect_range(start: str, end: str) -> list[dict]:
    start_iso = f"{start[:4]}-{start[4:6]}-{start[6:]}"
    end_iso = f"{end[:4]}-{end[4:6]}-{end[6:]}"
    tasks = _extra_targets()
    rows = []
    miss = 0
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold]{task.description}[/bold]"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeRemainingColumn(),
        transient=False,
    ) as p:
        job = p.add_task(f"확장 백필 {start}~{end}", total=len(tasks))
        for ticker, name in tasks:
            try:
                df = fdr.DataReader(ticker, start_iso, end_iso)
            except Exception:
                df = None
            if df is None or len(df) == 0:
                miss += 1
            else:
                for dt, r in df.iterrows():
                    rows.append({
                        "date": dt.strftime("%Y%m%d"),
                        "ticker": ticker, "name": name,
                        "close": int(r["Close"]),
                        "change_pct": round(float(r["Change"]) * 100, 2),
                    })
            p.advance(job)
    print(f"누락: {miss}/{len(tasks)}")
    return rows


def main(args: list[str]) -> None:
    if len(args) >= 2:
        start = args[0].replace("-", "")
        end = args[1].replace("-", "")
        rows = collect_range(start, end)
    else:
        rows = collect_today()

    if not rows:
        print("수집 결과 없음.")
        return

    new_df = pd.DataFrame(rows)
    print(f"수집: {len(new_df):,}건")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    if OUT.exists():
        old = pd.read_csv(OUT, dtype={"ticker": str, "date": str})
        combined = pd.concat([old, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["date", "ticker"], keep="last")
    else:
        combined = new_df
    combined = combined.sort_values(["date", "ticker"]).reset_index(drop=True)
    combined.to_csv(OUT, index=False, encoding="utf-8-sig")
    print(f"저장: {OUT}")
    print(f"누적 행수: {len(combined):,} / 종목 수: {combined['ticker'].nunique():,} / 날짜 수: {combined['date'].nunique()}")


if __name__ == "__main__":
    main(sys.argv[1:])
