# -*- coding: utf-8 -*-
"""
시총 상위 200 종목 × 5년 주별 종가 시계열.
출력: data/chart_5y.json — {ticker: {dates: [...], closes: [...]}}
"""
import json
import sys
import time
from pathlib import Path

import FinanceDataReader as fdr
import pandas as pd
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeRemainingColumn

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

HERE = Path(__file__).parent
SEARCH = HERE / "data" / "search_index.json"
OUT = HERE / "data" / "chart_5y.json"
TOP_N = 200
YEARS = 5


def build() -> None:
    search = json.loads(SEARCH.read_text(encoding="utf-8"))
    stocks = sorted(search.get("stocks", []), key=lambda s: s.get("m", 0) or 0, reverse=True)[:TOP_N]

    end = pd.Timestamp.today().normalize()
    recent_start = end - pd.DateOffset(years=YEARS)
    fetch_start = end - pd.Timedelta(days=1500)

    ma_specs = [(20, "ma20"), (60, "ma60"), (120, "ma120"), (1000, "ma1000")]

    out: dict[str, dict] = {}
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold]{task.description}[/bold]"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeRemainingColumn(),
        transient=True,
    ) as p:
        job = p.add_task(f"5년 시계열 + MA (TOP {TOP_N})", total=len(stocks))
        for s in stocks:
            t = s["t"]
            try:
                df = fdr.DataReader(t, fetch_start, end)
            except Exception:
                p.advance(job)
                continue
            if df is None or len(df) < 30:
                p.advance(job)
                continue
            closes_d = df["Close"].astype(float)
            ma_d = {key: closes_d.rolling(period).mean() for period, key in ma_specs}

            recent = closes_d[closes_d.index >= recent_start]
            weekly = recent.resample("W-FRI").last().dropna()
            if len(weekly) < 10:
                p.advance(job)
                continue
            row = {
                "dates": [d.strftime("%Y%m%d") for d in weekly.index],
                "closes": [int(c) for c in weekly],
            }
            for _, key in ma_specs:
                vals = ma_d[key].reindex(weekly.index, method="ffill")
                row[key] = [int(v) if pd.notna(v) else None for v in vals]
            out[t] = row
            p.advance(job)
            time.sleep(0.05)

    OUT.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
    size_kb = OUT.stat().st_size / 1024
    print(f"5년 차트 저장: {OUT} ({len(out)}종목, {size_kb:,.0f} KB)")


if __name__ == "__main__":
    build()
