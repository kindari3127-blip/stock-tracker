# -*- coding: utf-8 -*-
"""
prospects.py — 멀티팩터 유망주 발굴

입력:
  data/chart_data.json     — 종목별 60일 종가 (recommend.py 산출)
  data/search_index.json   — 종목 메타 (시총·업종·등락률·거래량)
  data/fundamentals.csv    — 추적 풀 펀더멘털
  data/fundamentals_extra.csv — 확장 풀 펀더멘털
  data/flows_stock.json    — 종목별 외인·기관 20일 순매수
  data/cashflow.csv        — PFR/FCF (있을 때)
  data/strength.json       — 섹터 강세 점수

출력:
  data/prospects.json
    - top_30                : 멀티팩터 종합 TOP 30
    - new_high              : 60일 신고가 근접/돌파 (close ≥ 0.97 × max60d)
    - foreign_buy_top       : 20일 외인 순매수 강도 TOP 20
    - momentum_with_fund    : 모멘텀+펀더 동시 강세
    - weights/pool 정보
"""
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

HERE = Path(__file__).parent
DATA = HERE / "data"
OUT = DATA / "prospects.json"


def _z(s: pd.Series) -> pd.Series:
    s = s.replace([np.inf, -np.inf], np.nan)
    mu = s.mean()
    sd = s.std()
    if sd == 0 or pd.isna(sd):
        return pd.Series([0.0] * len(s), index=s.index)
    return (s - mu) / sd


def _safe(v, default=np.nan):
    try:
        f = float(v)
        if np.isnan(f) or np.isinf(f):
            return default
        return f
    except Exception:
        return default


def build() -> None:
    chart = json.loads((DATA / "chart_data.json").read_text(encoding="utf-8"))
    search = json.loads((DATA / "search_index.json").read_text(encoding="utf-8"))
    flows = json.loads((DATA / "flows_stock.json").read_text(encoding="utf-8")) if (DATA / "flows_stock.json").exists() else {"stocks": {}}
    strength = json.loads((DATA / "strength.json").read_text(encoding="utf-8")) if (DATA / "strength.json").exists() else {}

    meta = {s["t"]: s for s in search.get("stocks", [])}

    # 펀더멘털: 추적 + 확장 머지
    fund = pd.read_csv(DATA / "fundamentals.csv", dtype={"ticker": str})
    extra_path = DATA / "fundamentals_extra.csv"
    if extra_path.exists():
        fund_x = pd.read_csv(extra_path, dtype={"ticker": str})
        fund = pd.concat([fund, fund_x], ignore_index=True)
    fund["ticker"] = fund["ticker"].str.zfill(6)
    fund = fund.drop_duplicates(subset=["ticker"], keep="first").set_index("ticker")

    # PFR / FCF
    cf_path = DATA / "cashflow.csv"
    pfr_map: dict[str, float] = {}
    if cf_path.exists():
        cf = pd.read_csv(cf_path, dtype={"ticker": str})
        cf["ticker"] = cf["ticker"].str.zfill(6)
        cf = cf.drop_duplicates(subset=["ticker"], keep="first")
        cf = cf[cf["pfr"].notna() & (cf["pfr"] > 0)]
        pfr_map = dict(zip(cf["ticker"], cf["pfr"].astype(float)))

    # 섹터 강세
    sector_score: dict[str, float] = {}
    for s in strength.get("sectors_top", []) + strength.get("sectors_bottom", []):
        sector_score[s["name"]] = s["ret"]

    # 풀: chart_data 보유 종목 (시총 ≥ 500억 자동 만족, search_index 기반이라)
    tickers = sorted(chart.keys())
    rows: list[dict] = []

    for t in tickers:
        c = chart[t]
        closes = c.get("closes") or []
        volumes = c.get("volumes") or []  # recommend.py 패치 이후 채워짐
        if len(closes) < 30:
            continue
        closes = [float(x) for x in closes]
        cur = closes[-1]
        c5 = closes[-6] if len(closes) >= 6 else closes[0]
        c20 = closes[-21] if len(closes) >= 21 else closes[0]
        ma20 = float(np.mean(closes[-20:]))
        ma60 = float(np.mean(closes[-60:])) if len(closes) >= 60 else float(np.mean(closes))
        high60 = max(closes[-60:]) if len(closes) >= 60 else max(closes)
        low60 = min(closes[-60:]) if len(closes) >= 60 else min(closes)
        rng_pos = (cur - low60) / (high60 - low60 + 1e-9) * 100
        ret_5d = (cur / c5 - 1) * 100
        ret_20d = (cur / c20 - 1) * 100
        above_ma20 = (cur / ma20 - 1) * 100
        above_ma60 = (cur / ma60 - 1) * 100
        prox_high = cur / high60 * 100  # 100%면 신고가

        # 거래량 spike: 최근 3일 평균 / 직전 20일 평균
        if len(volumes) >= 10:
            recent_v = float(np.mean(volumes[-3:]))
            base_v = float(np.mean(volumes[:-3])) if len(volumes) > 3 else recent_v
            vol_spike = recent_v / (base_v + 1)
        else:
            vol_spike = 1.0

        # 펀더
        roe = _safe(fund.loc[t, "roe"]) if t in fund.index else np.nan
        eps = _safe(fund.loc[t, "eps"]) if t in fund.index else np.nan
        eps_est = _safe(fund.loc[t, "eps_est"]) if t in fund.index else np.nan
        eps_g = (eps_est / eps - 1) * 100 if eps and eps > 0 else np.nan
        per = _safe(fund.loc[t, "per"]) if t in fund.index else np.nan
        pfr = pfr_map.get(t, np.nan)

        # 수급
        fs = flows.get("stocks", {}).get(t, {})
        f20 = fs.get("foreign_20d") or 0
        f5 = fs.get("foreign_5d") or 0
        i20 = fs.get("inst_20d") or 0
        m = meta.get(t, {})
        mcap = m.get("m") or 0  # 시총(원)
        # 시총 대비 외인 매수 강도 (주수 → 금액 환산은 cur 곱)
        f20_amt = f20 * cur if f20 else 0
        f_strength = (f20_amt / mcap) * 1e4 if mcap > 0 else 0  # %p 단위로 스케일

        # 섹터 점수
        sec_name = m.get("i") or fund.loc[t, "sector"] if t in fund.index else ""
        sec_score = sector_score.get(sec_name or "", 0.0)

        rows.append({
            "t": t,
            "n": m.get("n") or (fund.loc[t, "name"] if t in fund.index else ""),
            "i": m.get("i") or "",
            "c": cur,
            "m": mcap,
            "ret_5d": ret_5d,
            "ret_20d": ret_20d,
            "above_ma20": above_ma20,
            "above_ma60": above_ma60,
            "rng_pos": rng_pos,
            "prox_high": prox_high,
            "roe": roe,
            "eps_g": eps_g,
            "per": per,
            "pfr": pfr,
            "f20": f20,
            "f5": f5,
            "i20": i20,
            "f_strength": f_strength,
            "sec_score": sec_score,
            "vol_spike": vol_spike,
        })

    if not rows:
        raise SystemExit("prospects: 풀 0건")

    df = pd.DataFrame(rows).set_index("t")
    print(f"발굴 풀: {len(df)}종목")

    # PFR 역수(저평가 가점) — None 은 median 대체 후 음수 z (낮을수록 +)
    pfr_filled = df["pfr"].fillna(df["pfr"].median() if df["pfr"].notna().any() else 30.0)
    eps_g_filled = df["eps_g"].fillna(0)
    roe_filled = df["roe"].fillna(df["roe"].median() if df["roe"].notna().any() else 0)

    # 5개 점수
    df["s_fund"] = _z(roe_filled) * 0.4 + _z(eps_g_filled) * 0.4 + (-_z(pfr_filled)) * 0.2
    df["s_tech"] = _z(df["above_ma20"]) * 0.4 + _z(df["above_ma60"]) * 0.3 + _z(df["rng_pos"]) * 0.3
    df["s_mom"]  = _z(df["ret_5d"]) * 0.5 + _z(df["ret_20d"]) * 0.5
    df["s_flow"] = _z(df["f_strength"])
    df["s_sec"]  = _z(df["sec_score"])

    df["total"] = (
        df["s_fund"] * 0.30
        + df["s_tech"] * 0.20
        + df["s_mom"]  * 0.20
        + df["s_flow"] * 0.20
        + df["s_sec"]  * 0.10
    )

    df = df.sort_values("total", ascending=False)

    def _serialize(idx, r) -> dict:
        return {
            "t": idx,
            "n": r["n"],
            "i": r["i"],
            "c": r["c"],
            "m": int(r["m"]) if r["m"] else 0,
            "total": round(float(r["total"]), 2),
            "scores": {
                "fund": round(float(r["s_fund"]), 2),
                "tech": round(float(r["s_tech"]), 2),
                "mom":  round(float(r["s_mom"]), 2),
                "flow": round(float(r["s_flow"]), 2),
                "sec":  round(float(r["s_sec"]), 2),
            },
            "metrics": {
                "ret_5d": round(r["ret_5d"], 2),
                "ret_20d": round(r["ret_20d"], 2),
                "rng_pos": round(r["rng_pos"], 1),
                "prox_high": round(r["prox_high"], 1),
                "roe": None if pd.isna(r["roe"]) else round(r["roe"], 2),
                "eps_g": None if pd.isna(r["eps_g"]) else round(r["eps_g"], 1),
                "per": None if pd.isna(r["per"]) else round(r["per"], 2),
                "pfr": None if pd.isna(r["pfr"]) else round(r["pfr"], 2),
                "f20": int(r["f20"]),
                "f5": int(r["f5"]),
                "i20": int(r["i20"]),
            },
        }

    top_30 = [_serialize(i, r) for i, r in df.head(30).iterrows()]

    # 부가 리스트
    new_high = [_serialize(i, r) for i, r in df[df["prox_high"] >= 97].sort_values("prox_high", ascending=False).head(20).iterrows()]
    foreign_buy = [_serialize(i, r) for i, r in df.sort_values("f_strength", ascending=False).head(20).iterrows()]
    mom_fund = [_serialize(i, r) for i, r in df[(df["s_mom"] > 0.5) & (df["s_fund"] > 0.5)].sort_values("total", ascending=False).head(20).iterrows()]
    # 거래량 spike: 3일 평균이 20일 평균의 2.5배 이상 + 5일 수익률 양수
    vol_alert = [_serialize(i, r) for i, r in df[(df["vol_spike"] >= 2.5) & (df["ret_5d"] > 0)].sort_values("vol_spike", ascending=False).head(20).iterrows()]

    out = {
        "updated": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M"),
        "ref_date": strength.get("ref_date"),
        "weights": {"fund": 0.30, "tech": 0.20, "mom": 0.20, "flow": 0.20, "sec": 0.10},
        "pool_size": int(len(df)),
        "top_30": top_30,
        "new_high": new_high,
        "foreign_buy_top": foreign_buy,
        "momentum_with_fund": mom_fund,
        "volume_spike": vol_alert,
    }

    OUT.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
    print(f"저장: {OUT}")
    print(f"\n[유망주 TOP 5]")
    for r in top_30[:5]:
        s = r["scores"]
        print(f"  {r['n']:14} 종합 {r['total']:+.2f}  펀더 {s['fund']:+.2f} 기술 {s['tech']:+.2f} 모멘텀 {s['mom']:+.2f} 수급 {s['flow']:+.2f} 섹터 {s['sec']:+.2f}")
    print(f"\n[60일 신고가 근접 {len(new_high)}종]  외인매수 TOP {len(foreign_buy)}종  모멘텀+펀더 강세 {len(mom_fund)}종  거래량 spike {len(vol_alert)}종")


if __name__ == "__main__":
    build()
