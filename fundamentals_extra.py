# -*- coding: utf-8 -*-
"""
SECTORS 외 확장패널/대시보드 종목의 PER/PBR/ROE/실적 수집.
fundamentals.py 의 scrape() 를 재사용.

출력:
  data/financials_extra.csv  (long format)
  data/fundamentals_extra.csv (wide 요약)
"""
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeRemainingColumn

from sectors import SECTORS
from fundamentals import scrape
from business_scrape import _collect_targets

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

HERE = Path(__file__).parent
FIN_OUT = HERE / "data" / "financials_extra.csv"
FUND_OUT = HERE / "data" / "fundamentals_extra.csv"


def _extra_targets() -> list[tuple[str, str]]:
    in_sectors = {t for items in SECTORS.values() for t, _ in items}
    return [(t, n) for t, n in _collect_targets() if t not in in_sectors]


def main() -> None:
    tasks = _extra_targets()
    print(f"확장 fundamentals 수집 대상: {len(tasks)}종")

    all_rows: list[dict] = []
    failed: list[tuple[str, str]] = []

    with Progress(
        SpinnerColumn(), TextColumn("[bold]{task.description}[/bold]"),
        BarColumn(), TextColumn("{task.completed}/{task.total}"),
        TimeRemainingColumn(),
    ) as p:
        job = p.add_task("확장 지표", total=len(tasks))
        for ticker, name in tasks:
            try:
                rows = scrape(ticker)
                if rows:
                    for r in rows:
                        r["name"] = name
                    all_rows.extend(rows)
                else:
                    failed.append((ticker, name))
            except Exception:
                failed.append((ticker, name))
            p.advance(job)
            time.sleep(0.08)
            # 중간 저장 (200개마다)
            if (p.tasks[0].completed % 200) == 0 and all_rows:
                pd.DataFrame(all_rows).to_csv(FIN_OUT, index=False, encoding="utf-8-sig")

    df = pd.DataFrame(all_rows)
    FIN_OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(FIN_OUT, index=False, encoding="utf-8-sig")
    print(f"저장: {FIN_OUT}  행수: {len(df):,}")
    if failed:
        print(f"실패 {len(failed)}개")

    if df.empty:
        return

    # wide 요약 (fundamentals.py 와 동일 구조, sector 칼럼 없음)
    def _wide(sub: pd.DataFrame, suffix: str) -> pd.DataFrame:
        if sub.empty:
            return pd.DataFrame(columns=["ticker"])
        w = (sub.groupby(["ticker", "metric"], as_index=False).last()
             .pivot(index="ticker", columns="metric", values="value"))
        w.columns = [f"{c}{suffix}" for c in w.columns]
        return w.reset_index()

    def _period(sub: pd.DataFrame, col: str) -> pd.DataFrame:
        if sub.empty:
            return pd.DataFrame(columns=["ticker", col])
        return sub.groupby("ticker")["period"].last().reset_index().rename(columns={"period": col})

    annual = df[df["period_type"] == "annual"].copy()
    if not annual.empty:
        annual["pkey"] = pd.to_numeric(
            annual["period"].astype(str).str.replace(".", "", regex=False),
            errors="coerce",
        )
        annual = annual.dropna(subset=["pkey"]).sort_values("pkey")
    a_act = annual[annual["is_estimate"] == 0] if not annual.empty else annual
    a_est = annual[annual["is_estimate"] == 1] if not annual.empty else annual

    quarter = df[df["period_type"] == "quarterly"].copy()
    if not quarter.empty:
        quarter["pkey"] = pd.to_numeric(
            quarter["period"].astype(str).str.replace(".", "", regex=False),
            errors="coerce",
        )
        quarter = quarter.dropna(subset=["pkey"]).sort_values("pkey")
    q_act = quarter[quarter["is_estimate"] == 0] if not quarter.empty else quarter
    q_est = quarter[quarter["is_estimate"] == 1] if not quarter.empty else quarter

    meta = df[["ticker", "name"]].drop_duplicates("ticker")
    wide = (meta.merge(_wide(a_act, ""), on="ticker", how="left")
                .merge(_wide(a_est, "_est"), on="ticker", how="left")
                .merge(_wide(q_act, "_q"), on="ticker", how="left")
                .merge(_wide(q_est, "_q_est"), on="ticker", how="left")
                .merge(_period(a_act, "actual_period"), on="ticker", how="left")
                .merge(_period(a_est, "est_period"), on="ticker", how="left")
                .merge(_period(q_act, "q_period"), on="ticker", how="left")
                .merge(_period(q_est, "q_est_period"), on="ticker", how="left"))
    wide["updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")

    base_metrics = ["per", "pbr", "eps", "bps", "roe", "op_margin", "net_margin",
                    "dividend_yield", "payout_ratio", "debt_ratio",
                    "revenue", "operating_profit", "net_profit"]
    cols_annual = [c for c in base_metrics if c in wide.columns]
    cols_annual_est = [f"{c}_est" for c in base_metrics if f"{c}_est" in wide.columns]
    cols_q = [f"{c}_q" for c in base_metrics if f"{c}_q" in wide.columns]
    cols_q_est = [f"{c}_q_est" for c in base_metrics if f"{c}_q_est" in wide.columns]
    cols = (["ticker", "name", "updated",
             "actual_period", "est_period", "q_period", "q_est_period"]
            + cols_annual + cols_annual_est + cols_q + cols_q_est)
    wide = wide[cols]
    wide.to_csv(FUND_OUT, index=False, encoding="utf-8-sig")
    print(f"저장: {FUND_OUT}  행수: {len(wide):,}")


if __name__ == "__main__":
    main()
