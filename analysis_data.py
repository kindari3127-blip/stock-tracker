# -*- coding: utf-8 -*-
"""
리포트 클릭 분석 카드용 데이터 빌더.

prices.csv + prices_extra.csv + fundamentals[_extra].csv +
financials[_extra].csv + business.json 을 종목별로 통합.

출력 dict 형식 (JS embed용 — 키 짧게):
  {
    "005930": {
      "n": "삼성전자",
      "h": [[20250423, 53700], [20250424, 54000], ...],   # date_yyyymmdd, close
      "ka": {"pa":"2025.12","pe":"2026.12","per":33.4,"per_est":5.55,...},  # 연간 KPI
      "kq": {"pa":"2025.12","pe":"2026.03",...},  # 분기 KPI
      "fa": {"p":["2022.12",...,"2026.12"], "e":[0,0,0,0,1], "r":[...], "op":[...], "np":[...]},
      "fq": {"p":[...],"e":[...],"r":[...],"op":[...],"np":[...]},
      "b":  {"h":"AI 수요...","d":"2026/04/10","p":["...","..."]}
    }
  }
"""
from __future__ import annotations

import math
import pandas as pd


KPI_FIELDS = ["per", "pbr", "eps", "roe", "dividend_yield"]


def _safe(v):
    if v is None:
        return None
    try:
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return None
        return float(v) if isinstance(v, (int, float)) else v
    except (TypeError, ValueError):
        return None


def _kpi_dict(row: pd.Series, mode: str) -> dict:
    """row에서 mode(annual/quarter) KPI 추출."""
    if mode == "annual":
        pa_col, pe_col = "actual_period", "est_period"
        suf, suf_est = "", "_est"
    else:
        pa_col, pe_col = "q_period", "q_est_period"
        suf, suf_est = "_q", "_q_est"

    out = {}
    pa = row.get(pa_col)
    pe = row.get(pe_col)
    if pa is not None and pd.notna(pa):
        out["pa"] = str(pa)
    if pe is not None and pd.notna(pe):
        out["pe"] = str(pe)
    for k in KPI_FIELDS:
        v = row.get(f"{k}{suf}")
        ve = row.get(f"{k}{suf_est}")
        if v is not None and pd.notna(v):
            out[k] = round(float(v), 4)
        if ve is not None and pd.notna(ve):
            out[f"{k}_est"] = round(float(ve), 4)
    return out


def _fin_block(fin_sub: pd.DataFrame, period_type: str) -> dict | None:
    """financials long format → {p, e, r, op, np} dict."""
    sub = fin_sub[fin_sub["period_type"] == period_type]
    if sub.empty:
        return None

    metrics = ["revenue", "operating_profit", "net_profit"]
    sub = sub[sub["metric"].isin(metrics)]
    if sub.empty:
        return None

    # 정렬 키: period 'YYYY.MM' 또는 'YYYY.QQ' → 숫자 변환
    sub = sub.copy()
    sub["pkey"] = sub["period"].astype(str).str.replace(".", "", regex=False)
    sub["pkey"] = pd.to_numeric(sub["pkey"], errors="coerce")
    sub = sub.dropna(subset=["pkey"]).sort_values("pkey")

    periods = sub["period"].drop_duplicates().tolist()
    est_map = sub.groupby("period")["is_estimate"].max().to_dict()

    def _series(metric_key):
        m = sub[sub["metric"] == metric_key].set_index("period")["value"].to_dict()
        return [(_safe(m.get(p)) if m.get(p) is not None else None) for p in periods]

    return {
        "p": [str(p) for p in periods],
        "e": [int(est_map.get(p, 0)) for p in periods],
        "r": _series("revenue"),
        "op": _series("operating_profit"),
        "np": _series("net_profit"),
    }


def build_stock_data(
    prices_df: pd.DataFrame,
    prices_extra_df: pd.DataFrame,
    fund_df: pd.DataFrame,
    fund_extra_df: pd.DataFrame,
    fin_df: pd.DataFrame,
    fin_extra_df: pd.DataFrame,
    biz_data: dict,
    target_tickers: set[str] | None = None,
) -> dict:
    """target_tickers: 빌드할 ticker 화이트리스트 (None이면 전체)."""
    # 가격: SECTORS과 extra 통합 → 종목별 (date, close) 시퀀스
    price_pieces = []
    if prices_df is not None and not prices_df.empty:
        price_pieces.append(prices_df[["date", "ticker", "name", "close"]])
    if prices_extra_df is not None and not prices_extra_df.empty:
        price_pieces.append(prices_extra_df[["date", "ticker", "name", "close"]])
    all_prices = (pd.concat(price_pieces, ignore_index=True)
                  if price_pieces else pd.DataFrame(columns=["date","ticker","name","close"]))

    # SECTORS 등록 ticker는 여러 sector에 중복 등록될 수 있어 (date,ticker)로 고유화
    if not all_prices.empty:
        all_prices = (all_prices.sort_values(["ticker", "date"])
                                .drop_duplicates(subset=["date", "ticker"], keep="last"))

    # fundamentals 통합
    fund_pieces = []
    if fund_df is not None and not fund_df.empty:
        fund_pieces.append(fund_df)
    if fund_extra_df is not None and not fund_extra_df.empty:
        fund_pieces.append(fund_extra_df)
    all_fund = (pd.concat(fund_pieces, ignore_index=True)
                if fund_pieces else pd.DataFrame())
    if not all_fund.empty:
        all_fund = all_fund.drop_duplicates(subset=["ticker"], keep="first")
        all_fund = all_fund.set_index("ticker")

    # financials 통합 (long format)
    fin_pieces = []
    if fin_df is not None and not fin_df.empty:
        fin_pieces.append(fin_df)
    if fin_extra_df is not None and not fin_extra_df.empty:
        fin_pieces.append(fin_extra_df)
    all_fin = (pd.concat(fin_pieces, ignore_index=True)
               if fin_pieces else pd.DataFrame())

    # 출력 빌드
    sd: dict = {}

    if all_prices.empty:
        prices_iter = iter([])
    else:
        prices_iter = all_prices.groupby("ticker")

    for ticker, sub in prices_iter:
        if target_tickers is not None and ticker not in target_tickers:
            continue
        sub = sub.sort_values("date")
        if len(sub) < 2:
            continue
        history = list(zip(sub["date"].astype(int).tolist(),
                           sub["close"].astype(int).tolist()))
        sd[ticker] = {
            "n": str(sub["name"].iloc[-1]),
            "h": history,
        }

    # 가격은 없지만 fundamentals/biz 있을 수 있는 ticker
    extra_tickers = set()
    if not all_fund.empty:
        extra_tickers.update(all_fund.index.tolist())
    if biz_data:
        extra_tickers.update(biz_data.keys())
    for t in extra_tickers:
        if target_tickers is not None and t not in target_tickers:
            continue
        if t not in sd:
            biz = biz_data.get(t) if biz_data else None
            name = (biz.get("name") if biz else None) or ""
            sd[t] = {"n": name, "h": []}

    # KPI/financials/biz 병합
    for ticker, entry in sd.items():
        if not all_fund.empty and ticker in all_fund.index:
            row = all_fund.loc[ticker]
            ka = _kpi_dict(row, "annual")
            kq = _kpi_dict(row, "quarter")
            if ka:
                entry["ka"] = ka
            if kq:
                entry["kq"] = kq

        if not all_fin.empty:
            fsub = all_fin[all_fin["ticker"] == ticker]
            if not fsub.empty:
                fa = _fin_block(fsub, "annual")
                fq = _fin_block(fsub, "quarterly")
                if fa:
                    entry["fa"] = fa
                if fq:
                    entry["fq"] = fq

        biz = biz_data.get(ticker) if biz_data else None
        if biz and (biz.get("header") or biz.get("points")
                    or biz.get("products") or biz.get("subsidiaries")):
            b_entry = {
                "h": biz.get("header", ""),
                "d": biz.get("date", ""),
                "p": biz.get("points") or [],
            }
            if biz.get("products"):
                b_entry["pr"] = biz["products"]  # [{name, ratio}]
            if biz.get("rnd_pct") is not None:
                b_entry["rd"] = biz["rnd_pct"]
                b_entry["rdy"] = biz.get("rnd_year", "")
            if biz.get("subsidiaries"):
                b_entry["sub"] = biz["subsidiaries"]  # [{name, biz, founded}]
            entry["b"] = b_entry

    return sd


def stats(sd: dict) -> str:
    n = len(sd)
    have_h = sum(1 for v in sd.values() if v.get("h"))
    have_fund = sum(1 for v in sd.values() if v.get("ka") or v.get("kq"))
    have_fin = sum(1 for v in sd.values() if v.get("fa") or v.get("fq"))
    have_biz = sum(1 for v in sd.values() if v.get("b"))
    return (f"종목 {n}개 / 가격이력 {have_h} / 지표 {have_fund} / "
            f"실적 {have_fin} / 주력사업 {have_biz}")
