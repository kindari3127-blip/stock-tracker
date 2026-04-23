# -*- coding: utf-8 -*-
"""
네이버 금융 종목 페이지에서 기업실적·투자지표 수집.
출력:
  data/financials.csv  (long format — 기간별 모든 지표)
  data/fundamentals.csv (각 종목 최신 지표 요약 wide)
"""
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeRemainingColumn

from sectors import SECTORS

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

HERE = Path(__file__).parent
FIN_OUT = HERE / "data" / "financials.csv"
FUND_OUT = HERE / "data" / "fundamentals.csv"

# 네이버 페이지 표의 지표명 → 내부 키
METRICS = {
    "매출액": "revenue",
    "영업이익": "operating_profit",
    "당기순이익": "net_profit",
    "영업이익률": "op_margin",
    "순이익률": "net_margin",
    "ROE(지배주주)": "roe",
    "부채비율": "debt_ratio",
    "당좌비율": "quick_ratio",
    "유보율": "retention",
    "EPS(원)": "eps",
    "PER(배)": "per",
    "BPS(원)": "bps",
    "PBR(배)": "pbr",
    "주당배당금(원)": "dps",
    "시가배당률(%)": "dividend_yield",
    "배당성향(%)": "payout_ratio",
}


def _num(s: str) -> float | None:
    s = (s or "").strip().replace(",", "").replace("%", "")
    if not s or s in ("-", "N/A"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def scrape(ticker: str) -> list[dict]:
    url = f"https://finance.naver.com/item/main.naver?code={ticker}"
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
    soup = BeautifulSoup(r.content, "lxml")  # 바이트 전달 → 파서가 meta charset 자동 감지
    sec = soup.select_one("div.section.cop_analysis")
    if not sec:
        return []
    tbl = sec.select_one("table")
    if not tbl:
        return []
    thead = tbl.select("thead tr")
    if len(thead) < 2:
        return []

    first = thead[0].find_all(["th", "td"])
    spans = [int(e.get("colspan") or 1) for e in first]
    if len(spans) < 3:
        return []
    annual_n = spans[1]
    periods = [th.get_text(strip=True) for th in thead[1].find_all(["th", "td"])]
    annual_periods = periods[:annual_n]
    quarter_periods = periods[annual_n:]

    out = []
    for tr in tbl.select("tbody tr"):
        th = tr.select_one("th")
        if not th:
            continue
        # 숨김 span (주석용) 제거
        for s in th.select("span.txt_acd"):
            s.decompose()
        label = th.get_text(strip=True)
        key = METRICS.get(label)
        if not key:
            continue
        tds = tr.select("td")
        vals = [_num(td.get_text(strip=True)) for td in tds]
        for i, p in enumerate(annual_periods):
            if i < len(vals):
                out.append({
                    "ticker": ticker,
                    "period": p.replace("(E)", ""),
                    "period_type": "annual",
                    "is_estimate": 1 if "(E)" in p else 0,
                    "metric": key,
                    "value": vals[i],
                })
        for i, p in enumerate(quarter_periods):
            idx = annual_n + i
            if idx < len(vals):
                out.append({
                    "ticker": ticker,
                    "period": p.replace("(E)", ""),
                    "period_type": "quarterly",
                    "is_estimate": 1 if "(E)" in p else 0,
                    "metric": key,
                    "value": vals[idx],
                })
    return out


def main() -> None:
    tasks = [(s, t, n) for s, items in SECTORS.items() for t, n in items]
    all_rows = []
    failed: list[tuple[str, str]] = []
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold]{task.description}[/bold]"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeRemainingColumn(),
        transient=True,
    ) as p:
        job = p.add_task("지표 수집", total=len(tasks))
        for sector, ticker, name in tasks:
            try:
                rows = scrape(ticker)
                if rows:
                    for r in rows:
                        r["name"] = name
                        r["sector"] = sector
                    all_rows.extend(rows)
                else:
                    failed.append((ticker, name))
            except Exception:
                failed.append((ticker, name))
            p.advance(job)
            time.sleep(0.08)  # rate limit 예방

    df = pd.DataFrame(all_rows)
    FIN_OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(FIN_OUT, index=False, encoding="utf-8-sig")
    print(f"저장: {FIN_OUT}  행수: {len(df):,}")
    if failed:
        print(f"실패 {len(failed)}개: {', '.join(f'{t}/{n}' for t, n in failed[:10])}" + ("..." if len(failed) > 10 else ""))

    if df.empty:
        return

    # 요약 fundamentals — 연간(trailing/forward) + 분기(trailing/forward)
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
    annual["pkey"] = annual["period"].str.replace(".", "", regex=False).astype(int)
    annual = annual.sort_values("pkey")
    a_act, a_est = annual[annual["is_estimate"] == 0], annual[annual["is_estimate"] == 1]

    quarter = df[df["period_type"] == "quarterly"].copy()
    quarter["pkey"] = quarter["period"].str.replace(".", "", regex=False).astype(int)
    quarter = quarter.sort_values("pkey")
    q_act, q_est = quarter[quarter["is_estimate"] == 0], quarter[quarter["is_estimate"] == 1]

    meta = df[["ticker", "name", "sector"]].drop_duplicates("ticker")
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
    cols = (["ticker", "name", "sector", "updated",
             "actual_period", "est_period", "q_period", "q_est_period"]
            + cols_annual + cols_annual_est + cols_q + cols_q_est)
    wide = wide[cols]
    wide.to_csv(FUND_OUT, index=False, encoding="utf-8-sig")
    print(f"저장: {FUND_OUT}  행수: {len(wide):,}  (열 {len(cols)})")


if __name__ == "__main__":
    main()
