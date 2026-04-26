# -*- coding: utf-8 -*-
"""
누적 prices.csv 기반의 시황 통계 계산.

- sector_today_changes(df, latest)        : 오늘 섹터별 평균 등락률
- sector_period_returns(df, days)         : 최근 N일 누적 수익률 (섹터별)
- sector_correlation(df, days)            : 최근 N일 일별 수익률의 섹터간 상관계수 행렬
- top_correlations(corr, sector, n, sign) : 한 섹터 기준 + / - 상위 N개 섹터
- new_sector_candidates(indmap, sectors, listing, n)
                                          : sectors.py에 없는 큰 업종 후보 추출
"""
from __future__ import annotations

from collections import Counter

import pandas as pd


def sector_today_changes(df: pd.DataFrame, latest: str) -> pd.Series:
    """오늘 섹터별 대표주 평균 등락률 (내림차순)."""
    today = df[df["date"] == latest]
    if today.empty:
        return pd.Series(dtype=float)
    s = today.groupby("sector")["change_pct"].mean()
    return s.sort_values(ascending=False)


def _wide_close_by_sector(df: pd.DataFrame) -> pd.DataFrame:
    """date × sector 매트릭스. 섹터 일별 종가 = 대표 3종목 평균(노멀라이즈된 인덱스)."""
    # 섹터별로 3종목의 첫날=100 노멀라이즈 후 평균 → 가중치 비슷한 sector index
    pieces = []
    for (sector, ticker), g in df.sort_values("date").groupby(["sector", "ticker"]):
        if len(g) < 2:
            continue
        base = g["close"].iloc[0]
        if base <= 0:
            continue
        idx = g.set_index("date")["close"].astype(float) / base * 100
        idx.name = (sector, ticker)
        pieces.append(idx)
    if not pieces:
        return pd.DataFrame()
    wide = pd.concat(pieces, axis=1).sort_index()
    wide.columns = pd.MultiIndex.from_tuples(wide.columns, names=["sector", "ticker"])
    sector_idx = wide.T.groupby(level="sector").mean().T  # 섹터당 종목들 평균
    return sector_idx


def sector_period_returns(df: pd.DataFrame, days: int) -> pd.Series:
    """최근 days 거래일 누적 수익률 (오름차순)."""
    wide = _wide_close_by_sector(df)
    if wide.empty or len(wide) < 2:
        return pd.Series(dtype=float)
    n = min(days, len(wide) - 1)
    end = wide.iloc[-1]
    start = wide.iloc[-1 - n]
    ret = (end / start - 1.0) * 100.0
    return ret.sort_values(ascending=True)


def sector_correlation(df: pd.DataFrame, days: int = 60) -> pd.DataFrame:
    """최근 days 거래일 일별 수익률의 섹터간 Pearson 상관계수."""
    wide = _wide_close_by_sector(df)
    if wide.empty or len(wide) < 5:
        return pd.DataFrame()
    wide = wide.tail(days + 1)
    rets = wide.pct_change().dropna(how="all")
    if rets.empty or len(rets) < 5:
        return pd.DataFrame()
    return rets.corr()


def top_correlations(corr: pd.DataFrame, sector: str, n: int = 5,
                     sign: str = "pos") -> list[tuple[str, float]]:
    """sector와 양/음의 상관관계 TOP n. sign: 'pos' or 'neg'."""
    if corr.empty or sector not in corr.index:
        return []
    s = corr[sector].drop(sector)
    s = s.dropna()
    if s.empty:
        return []
    if sign == "pos":
        s = s.sort_values(ascending=False)
    else:
        s = s.sort_values(ascending=True)
    return [(idx, float(v)) for idx, v in s.head(n).items()]


def _combine_prices(prices_df: pd.DataFrame, prices_extra_df: pd.DataFrame) -> pd.DataFrame:
    """SECTORS + extra 종가 통합 (date, ticker, name, close) — 중복 제거."""
    pieces = []
    if prices_df is not None and not prices_df.empty:
        pieces.append(prices_df[["date", "ticker", "name", "close"]])
    if prices_extra_df is not None and not prices_extra_df.empty:
        pieces.append(prices_extra_df[["date", "ticker", "name", "close"]])
    if not pieces:
        return pd.DataFrame(columns=["date", "ticker", "name", "close"])
    out = pd.concat(pieces, ignore_index=True)
    out = (out.sort_values(["ticker", "date"])
              .drop_duplicates(subset=["date", "ticker"], keep="last"))
    return out


def stock_returns_combined(prices_df: pd.DataFrame, prices_extra_df: pd.DataFrame,
                           days: int) -> pd.DataFrame:
    """모든 종목의 N거래일 누적 수익률 (return_pct, close, name)."""
    combined = _combine_prices(prices_df, prices_extra_df)
    if combined.empty:
        return pd.DataFrame(columns=["ticker", "name", "close", "return_pct"])
    rows = []
    for ticker, sub in combined.groupby("ticker"):
        sub = sub.sort_values("date")
        if len(sub) < 2:
            continue
        n = min(days, len(sub) - 1)
        end = float(sub["close"].iloc[-1])
        start = float(sub["close"].iloc[-1 - n])
        if start <= 0:
            continue
        rows.append({
            "ticker": ticker,
            "name": sub["name"].iloc[-1],
            "close": int(end),
            "return_pct": (end / start - 1.0) * 100.0,
        })
    return pd.DataFrame(rows)


def stock_52w_levels(prices_df: pd.DataFrame, prices_extra_df: pd.DataFrame) -> pd.DataFrame:
    """종목별 52주(252일) 고가/저가/저점근접도(0=저점,1=고점)."""
    combined = _combine_prices(prices_df, prices_extra_df)
    if combined.empty:
        return pd.DataFrame()
    rows = []
    for ticker, sub in combined.groupby("ticker"):
        sub = sub.sort_values("date").tail(252)
        if len(sub) < 30:
            continue
        cur = float(sub["close"].iloc[-1])
        hi = float(sub["close"].max())
        lo = float(sub["close"].min())
        if hi <= lo:
            continue
        ratio = (cur - lo) / (hi - lo)
        rows.append({
            "ticker": ticker,
            "name": sub["name"].iloc[-1],
            "close": int(cur),
            "high_52w": int(hi),
            "low_52w": int(lo),
            "low_proximity": ratio,  # 0=저점, 1=고점
            "from_high_pct": (cur / hi - 1.0) * 100.0,
        })
    return pd.DataFrame(rows)


# ─────────────────────────── 투자 인사이트 4종 스크린 ───────────────────────────
def quality_value_composite(fund_all: pd.DataFrame, n: int = 15) -> pd.DataFrame:
    """우량+가치 composite. PER<12 + PBR<1.5 + ROE>10 + EPS>0.
       composite = ROE / (PER * PBR) — 높을수록 좋음."""
    if fund_all is None or fund_all.empty:
        return pd.DataFrame()
    cols = ["per", "pbr", "roe", "eps"]
    if not all(c in fund_all.columns for c in cols):
        return pd.DataFrame()
    df = fund_all.dropna(subset=cols).copy()
    df = df[(df["per"] > 0) & (df["per"] < 12)
            & (df["pbr"] > 0) & (df["pbr"] < 1.5)
            & (df["roe"] > 10) & (df["eps"] > 0)]
    if df.empty:
        return df
    df["score"] = df["roe"] / (df["per"] * df["pbr"])
    return df.sort_values("score", ascending=False).head(n)


def pressed_quality(fund_all: pd.DataFrame,
                    prices_df: pd.DataFrame, prices_extra_df: pd.DataFrame,
                    days: int = 20, threshold_pct: float = -5.0,
                    n: int = 15) -> pd.DataFrame:
    """눌림 우량주: ROE>10 + EPS>0 + 최근 N일 누적 < threshold_pct (기본 -5%)."""
    rets = stock_returns_combined(prices_df, prices_extra_df, days)
    if rets.empty or fund_all is None or fund_all.empty:
        return pd.DataFrame()
    if "roe" not in fund_all.columns or "eps" not in fund_all.columns:
        return pd.DataFrame()
    fund_use = fund_all.dropna(subset=["roe", "eps"])
    df = fund_use.merge(rets[["ticker", "return_pct"]], on="ticker", how="inner")
    df = df[(df["roe"] > 10) & (df["eps"] > 0) & (df["return_pct"] < threshold_pct)]
    if df.empty:
        return df
    return df.sort_values("return_pct", ascending=True).head(n)


def low_52w_quality(fund_all: pd.DataFrame,
                    prices_df: pd.DataFrame, prices_extra_df: pd.DataFrame,
                    proximity_max: float = 0.15, n: int = 15) -> pd.DataFrame:
    """52주 저점 근접 우량주: low_proximity ≤ 0.15 (저점에서 ±15% 이내) + ROE>10 + EPS>0."""
    levels = stock_52w_levels(prices_df, prices_extra_df)
    if levels.empty or fund_all is None or fund_all.empty:
        return pd.DataFrame()
    if "roe" not in fund_all.columns or "eps" not in fund_all.columns:
        return pd.DataFrame()
    fund_use = fund_all.dropna(subset=["roe", "eps"])
    df = fund_use.merge(
        levels[["ticker", "close", "high_52w", "low_52w", "low_proximity", "from_high_pct"]],
        on="ticker", how="inner",
    )
    df = df[(df["roe"] > 10) & (df["eps"] > 0) & (df["low_proximity"] <= proximity_max)]
    if df.empty:
        return df
    return df.sort_values("low_proximity", ascending=True).head(n)


def dividend_quality(fund_all: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    """고배당 우량주: dividend_yield>4% + ROE>8% + EPS>0."""
    if fund_all is None or fund_all.empty:
        return pd.DataFrame()
    cols = ["dividend_yield", "roe", "eps"]
    if not all(c in fund_all.columns for c in cols):
        return pd.DataFrame()
    df = fund_all.dropna(subset=cols).copy()
    df = df[(df["dividend_yield"] > 4) & (df["roe"] > 8) & (df["eps"] > 0)]
    if df.empty:
        return df
    return df.sort_values("dividend_yield", ascending=False).head(n)


def stock_period_returns(df: pd.DataFrame, days: int) -> pd.DataFrame:
    """개별 종목의 최근 N 거래일 누적 수익률.
    반환: ['ticker','name','sector','close','return_pct'] (오름차순=많이 빠진 종목 먼저)
    """
    if df.empty:
        return pd.DataFrame(columns=["ticker", "name", "sector", "close", "return_pct"])
    g = df.sort_values("date").groupby(["sector", "ticker"])
    rows = []
    for (sector, ticker), sub in g:
        if len(sub) < 2:
            continue
        n = min(days, len(sub) - 1)
        end_close = float(sub["close"].iloc[-1])
        start_close = float(sub["close"].iloc[-1 - n])
        if start_close <= 0:
            continue
        ret = (end_close / start_close - 1.0) * 100.0
        rows.append({
            "ticker": ticker, "name": sub["name"].iloc[-1],
            "sector": sector, "close": int(end_close), "return_pct": ret,
        })
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values("return_pct", ascending=True).reset_index(drop=True)


def new_sector_candidates(indmap: dict, sectors: dict[str, list], listing: pd.DataFrame,
                          top_n: int = 15) -> list[dict]:
    """
    sectors.py(SECTORS)에 거의 등록되지 않은 큰 업종 후보 추출.

    rule:
      - 각 industry_map 업종별 대표 3종(시총 상위) 골라서
      - 그 종목들이 SECTORS의 어떤 섹터에도 ≥1개 등록되어 있으면 후보에서 제외
      - "내 섹터 커버리지가 0/3"인 업종만 후보로 남김
      - 시총 합 기준 상위 top_n
    """
    if not indmap or not indmap.get("industries"):
        return []

    registered = set()
    for items in sectors.values():
        for t, _ in items:
            registered.add(t)

    # 시총·종가 룩업
    cap_map: dict[str, float] = {}
    if listing is not None and not listing.empty and "Code" in listing.columns:
        try:
            cap_map = dict(zip(listing["Code"].astype(str), listing["Marcap"]))
        except Exception:
            cap_map = {}

    candidates: list[dict] = []
    for no, info in indmap["industries"].items():
        name = info.get("name", "")
        stocks = info.get("stocks", [])
        if not stocks:
            continue
        # 시총 정렬 후 상위 5
        ranked = sorted(
            stocks,
            key=lambda s: cap_map.get(s.get("ticker", ""), 0) or 0,
            reverse=True,
        )
        top5 = ranked[:5]
        if not top5:
            continue
        # 커버리지 체크: 상위 5중 SECTORS에 들어간 종목 수
        covered = sum(1 for s in top5 if s.get("ticker") in registered)
        if covered >= 1:
            continue  # 이미 일부 커버
        cap_total = sum(cap_map.get(s.get("ticker", ""), 0) or 0 for s in top5)
        if cap_total <= 0:
            continue
        candidates.append({
            "industry_no": no,
            "industry_name": name,
            "top_stocks": [
                {"ticker": s.get("ticker"), "name": s.get("name"),
                 "marcap": cap_map.get(s.get("ticker", ""), 0)}
                for s in top5
            ],
            "marcap_total": cap_total,
        })

    candidates.sort(key=lambda c: c["marcap_total"], reverse=True)
    return candidates[:top_n]
