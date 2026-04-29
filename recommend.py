# -*- coding: utf-8 -*-
"""
188 fundamentals 풀 → 60일 시계열 → 4개 점수 가중합 → TOP 20 추천.
출력:
  data/recommend.json — 추천 종목 + 점수 분해
  data/chart_data.json — 종목별 60일 종가 (차트용)
"""
import json
import sys
import time
from pathlib import Path

import FinanceDataReader as fdr
import numpy as np
import pandas as pd
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeRemainingColumn

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

HERE = Path(__file__).parent
FUND = HERE / "data" / "fundamentals.csv"
SEARCH = HERE / "data" / "search_index.json"
STRENGTH = HERE / "data" / "strength.json"
OUT_RECO = HERE / "data" / "recommend.json"
OUT_CHART = HERE / "data" / "chart_data.json"

DAYS = 60
MIN_BARS = 30
CHART_POOL_TOP_N = 500


def _z(series: pd.Series) -> pd.Series:
    s = series.replace([np.inf, -np.inf], np.nan)
    mu = s.mean()
    sigma = s.std()
    if sigma == 0 or pd.isna(sigma):
        return pd.Series([0.0] * len(s), index=s.index)
    return (s - mu) / sigma


def build() -> None:
    fund = pd.read_csv(FUND, dtype={"ticker": str})
    fund["ticker"] = fund["ticker"].str.zfill(6)
    search = json.loads(SEARCH.read_text(encoding="utf-8"))
    strength = json.loads(STRENGTH.read_text(encoding="utf-8"))

    px_map = {s["t"]: s for s in search.get("stocks", [])}
    sector_score = {s["name"]: s["ret"] for s in strength.get("sectors_top", [])}
    sector_score.update({s["name"]: s["ret"] for s in strength.get("sectors_bottom", [])})

    end = pd.Timestamp.today().normalize()
    start = end - pd.Timedelta(days=DAYS * 1.6)

    rows: list[dict] = []
    chart: dict[str, dict] = {}

    fund_tickers = set(fund["ticker"].tolist())
    top_by_mcap = sorted(search.get("stocks", []), key=lambda s: s.get("m", 0) or 0, reverse=True)[:CHART_POOL_TOP_N]
    chart_pool = {s["t"] for s in top_by_mcap}
    tickers = sorted(fund_tickers | chart_pool)
    print(f"풀: 펀더멘털 {len(fund_tickers)} ∪ 시총상위 {len(chart_pool)} = {len(tickers)}종목")

    with Progress(
        SpinnerColumn(),
        TextColumn("[bold]{task.description}[/bold]"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeRemainingColumn(),
        transient=True,
    ) as p:
        job = p.add_task("시계열 수집", total=len(tickers))
        for t in tickers:
            try:
                df = fdr.DataReader(t, start, end)
            except Exception:
                df = None
            if df is None or len(df) < MIN_BARS:
                p.advance(job)
                continue
            df = df.tail(DAYS)
            closes = df["Close"].astype(float).tolist()
            volumes = df["Volume"].astype(float).tolist()
            if t in chart_pool or t in fund_tickers:
                chart[t] = {
                    "dates": [d.strftime("%Y%m%d") for d in df.index],
                    "closes": [int(c) for c in closes],
                }

            if t not in fund_tickers:
                p.advance(job)
                time.sleep(0.05)
                continue

            close_now = closes[-1]
            close_5d = closes[-6] if len(closes) >= 6 else closes[0]
            close_20d = closes[-21] if len(closes) >= 21 else closes[0]
            ret_5d = (close_now / close_5d - 1) * 100
            ret_20d = (close_now / close_20d - 1) * 100

            ma20 = float(np.mean(closes[-20:]))
            ma60 = float(np.mean(closes[-60:])) if len(closes) >= 60 else float(np.mean(closes))
            high_60 = max(closes[-60:]) if len(closes) >= 60 else max(closes)
            low_60 = min(closes[-60:]) if len(closes) >= 60 else min(closes)
            pos_in_range = (close_now - low_60) / (high_60 - low_60 + 1e-9) * 100
            above_ma20 = (close_now / ma20 - 1) * 100
            above_ma60 = (close_now / ma60 - 1) * 100

            recent_vol = float(np.mean(volumes[-5:])) if len(volumes) >= 5 else float(np.mean(volumes))
            base_vol = float(np.mean(volumes[:-5])) if len(volumes) >= 10 else recent_vol
            vol_ratio = recent_vol / (base_vol + 1) if base_vol > 0 else 1.0

            rows.append({
                "t": t,
                "ret_5d": ret_5d,
                "ret_20d": ret_20d,
                "above_ma20": above_ma20,
                "above_ma60": above_ma60,
                "pos_in_range": pos_in_range,
                "vol_ratio": vol_ratio,
            })
            p.advance(job)
            time.sleep(0.05)

    if not rows:
        raise SystemExit("시계열 수집 실패: 0건.")

    df_p = pd.DataFrame(rows).set_index("t")
    df_p["mom_score"] = _z(df_p["ret_5d"]) * 0.4 + _z(df_p["ret_20d"]) * 0.4 + _z(df_p["vol_ratio"]) * 0.2
    df_p["chart_score"] = _z(df_p["above_ma20"]) * 0.35 + _z(df_p["above_ma60"]) * 0.35 + _z(df_p["pos_in_range"]) * 0.3

    fund_idx = fund.set_index("ticker")
    eps = pd.to_numeric(fund_idx.get("eps"), errors="coerce")
    eps_est = pd.to_numeric(fund_idx.get("eps_est"), errors="coerce")
    roe = pd.to_numeric(fund_idx.get("roe"), errors="coerce")
    eps_g = (eps_est / eps - 1) * 100
    eps_g[eps <= 0] = np.nan
    df_p["eps_g"] = df_p.index.map(eps_g)
    df_p["roe_v"] = df_p.index.map(roe)
    df_p["fund_score"] = _z(df_p["roe_v"].fillna(df_p["roe_v"].median())) * 0.5 \
                       + _z(df_p["eps_g"].fillna(df_p["eps_g"].median())) * 0.5

    sec_for_ticker = fund_idx["sector"].to_dict() if "sector" in fund_idx.columns else {}

    def _rotation(t: str) -> float:
        live = px_map.get(t, {})
        ind = live.get("i") or sec_for_ticker.get(t) or ""
        return sector_score.get(ind, 0.0)

    df_p["rot_raw"] = [_rotation(t) for t in df_p.index]
    df_p["rot_score"] = _z(df_p["rot_raw"])

    df_p["total"] = (
        df_p["mom_score"] * 0.30
        + df_p["fund_score"] * 0.25
        + df_p["chart_score"] * 0.25
        + df_p["rot_score"] * 0.20
    )
    df_p = df_p.sort_values("total", ascending=False)

    top: list[dict] = []
    name_map = {str(r["ticker"]).zfill(6): str(r["name"]) for _, r in fund.iterrows()}
    for t, r in df_p.head(20).iterrows():
        live = px_map.get(t, {})
        top.append({
            "t": t,
            "n": name_map.get(t, ""),
            "i": live.get("i", ""),
            "c": live.get("c"),
            "r": live.get("r"),
            "m": live.get("m"),
            "total": round(float(r["total"]), 2),
            "scores": {
                "momentum": round(float(r["mom_score"]), 2),
                "fundamental": round(float(r["fund_score"]), 2),
                "chart": round(float(r["chart_score"]), 2),
                "rotation": round(float(r["rot_score"]), 2),
            },
            "metrics": {
                "ret_5d": round(float(r["ret_5d"]), 2),
                "ret_20d": round(float(r["ret_20d"]), 2),
                "above_ma20": round(float(r["above_ma20"]), 2),
                "above_ma60": round(float(r["above_ma60"]), 2),
                "pos_in_range": round(float(r["pos_in_range"]), 1),
                "vol_ratio": round(float(r["vol_ratio"]), 2),
                "roe": None if pd.isna(r["roe_v"]) else round(float(r["roe_v"]), 2),
                "eps_g": None if pd.isna(r["eps_g"]) else round(float(r["eps_g"]), 2),
            },
        })

    OUT_RECO.write_text(json.dumps({
        "ref_date": strength.get("ref_date"),
        "ref_label": strength.get("ref_label"),
        "weights": {"momentum": 0.30, "fundamental": 0.25, "chart": 0.25, "rotation": 0.20},
        "pool_size": int(len(df_p)),
        "top": top,
    }, ensure_ascii=False), encoding="utf-8")
    OUT_CHART.write_text(json.dumps(chart, ensure_ascii=False), encoding="utf-8")
    print(f"추천 저장: {OUT_RECO} (TOP 20 / 풀 {len(df_p)})")
    print(f"차트 저장: {OUT_CHART} ({len(chart)}종목)")
    print(f"TOP 5:")
    for r in top[:5]:
        s = r["scores"]
        print(f"  {r['n']:14} 종합 {r['total']:+.2f} (모멘텀 {s['momentum']:+.2f} / 실적 {s['fundamental']:+.2f} / 차트 {s['chart']:+.2f} / 순환 {s['rotation']:+.2f})")


if __name__ == "__main__":
    build()
