# -*- coding: utf-8 -*-
"""
섹터별 대표주 종가 수집 → data/prices.csv 에 누적 저장
사용법:
    python collect.py                # 오늘 (또는 직전 영업일)
    python collect.py 2026-04-22     # 특정 날짜
"""
import sys
from pathlib import Path

import pandas as pd
import FinanceDataReader as fdr
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeRemainingColumn

from sectors import SECTORS

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

HERE = Path(__file__).parent
DATA = HERE / "data" / "prices.csv"


def _ref_date() -> str:
    """KRX 최근 영업일 (삼성전자 최신 봉의 일자)."""
    df = fdr.DataReader("005930").tail(1)
    return df.index[-1].strftime("%Y%m%d")


def collect_latest() -> tuple[list[dict], list[tuple]]:
    listing = fdr.StockListing("KRX").drop_duplicates(subset=["Code"]).set_index("Code")
    date = _ref_date()
    rows, missing = [], []
    for sector, items in SECTORS.items():
        for ticker, name in items:
            if ticker not in listing.index:
                missing.append((sector, ticker, name))
                continue
            r = listing.loc[ticker]
            close = r["Close"]
            if pd.isna(close) or close <= 0:
                missing.append((sector, ticker, name))
                continue
            rows.append({
                "date": date,
                "sector": sector,
                "ticker": ticker,
                "name": name,
                "close": int(close),
                "change_pct": round(float(r["ChagesRatio"]), 2),
            })
    return rows, missing


def collect_by_date(date: str) -> tuple[list[dict], list[tuple]]:
    return collect_range(date, date, label=f"종목별 수집 ({date})")


def collect_range(start: str, end: str, label: str = "범위 수집") -> tuple[list[dict], list[tuple]]:
    start_iso = f"{start[:4]}-{start[4:6]}-{start[6:]}"
    end_iso = f"{end[:4]}-{end[4:6]}-{end[6:]}"
    tasks = [(s, t, n) for s, items in SECTORS.items() for t, n in items]
    rows, missing = [], []
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold]{task.description}[/bold]"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeRemainingColumn(),
        transient=True,
    ) as p:
        job = p.add_task(label, total=len(tasks))
        for sector, ticker, name in tasks:
            try:
                df = fdr.DataReader(ticker, start_iso, end_iso)
            except Exception:
                df = None
            if df is None or len(df) == 0:
                missing.append((sector, ticker, name))
            else:
                for dt, r in df.iterrows():
                    rows.append({
                        "date": dt.strftime("%Y%m%d"),
                        "sector": sector,
                        "ticker": ticker,
                        "name": name,
                        "close": int(r["Close"]),
                        "change_pct": round(float(r["Change"]) * 100, 2),
                    })
            p.advance(job)
    return rows, missing


def main(args: list[str]) -> None:
    if len(args) >= 2:
        start = args[0].replace("-", "")
        end = args[1].replace("-", "")
        print(f"범위 수집: {start} ~ {end}")
        rows, missing = collect_range(start, end)
    elif len(args) == 1:
        date = args[0].replace("-", "")
        print(f"지정일 수집: {date}")
        rows, missing = collect_by_date(date)
    else:
        rows, missing = collect_latest()
        date = rows[0]["date"] if rows else _ref_date()
        print(f"수집 대상일: {date}  (KRX 최근 영업일)")

    new_df = pd.DataFrame(rows)
    print(f"수집 완료: {len(new_df)}건 / 누락: {len(missing)}건")
    for s, t, n in missing[:15]:
        print(f"  [누락] {s} / {t} {n}")

    DATA.parent.mkdir(parents=True, exist_ok=True)
    if DATA.exists():
        old = pd.read_csv(DATA, dtype={"ticker": str, "date": str})
        combined = pd.concat([old, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["date", "sector", "ticker"], keep="last")
    else:
        combined = new_df

    combined = combined.sort_values(["date", "sector", "ticker"]).reset_index(drop=True)
    combined.to_csv(DATA, index=False, encoding="utf-8-sig")
    print(f"저장: {DATA}")
    print(f"누적 행수: {len(combined):,} / 기록된 날짜 수: {combined['date'].nunique()}")


if __name__ == "__main__":
    main(sys.argv[1:])
