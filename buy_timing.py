# -*- coding: utf-8 -*-
"""
오늘 매수 타이밍 추천 — 기존 추천(모멘텀 강함)과 반대 컨셉.
"실적은 좋은데 단기 조정 받은 종목" = 저점 매수 기회.

매수 점수 = (실적 z-score) + (역모멘텀) + (RSI 과매도) + (업종 강세 보조)
TOP 10 출력. data/buy_timing.json
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
RECO = HERE / "data" / "recommend.json"
CHART = HERE / "data" / "chart_data.json"
SEARCH = HERE / "data" / "search_index.json"
STRENGTH = HERE / "data" / "strength.json"
OUT = HERE / "data" / "buy_timing.json"


def _rsi(closes: list[float], period: int = 14) -> float | None:
    if len(closes) <= period:
        return None
    gain = loss = 0.0
    for i in range(1, period + 1):
        d = closes[i] - closes[i - 1]
        if d > 0:
            gain += d
        else:
            loss -= d
    ag, al = gain / period, loss / period
    for i in range(period + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        g = d if d > 0 else 0
        l = -d if d < 0 else 0
        ag = (ag * (period - 1) + g) / period
        al = (al * (period - 1) + l) / period
    if al == 0:
        return 100.0
    rs = ag / al
    return 100 - 100 / (1 + rs)


def _z(s: pd.Series) -> pd.Series:
    s = s.replace([np.inf, -np.inf], np.nan)
    mu, sd = s.mean(), s.std()
    if sd == 0 or pd.isna(sd):
        return pd.Series([0.0] * len(s), index=s.index)
    return (s - mu) / sd


def build() -> None:
    reco = json.loads(RECO.read_text(encoding="utf-8"))
    search = json.loads(SEARCH.read_text(encoding="utf-8"))
    strength = json.loads(STRENGTH.read_text(encoding="utf-8"))
    chart = json.loads(CHART.read_text(encoding="utf-8")) if CHART.exists() else {}

    px_map = {s["t"]: s for s in search.get("stocks", [])}
    sector_score = {s["name"]: s["ret"] for s in strength.get("sectors_top", [])}
    sector_score.update({s["name"]: s["ret"] for s in strength.get("sectors_bottom", [])})

    pool = reco.get("all") or reco.get("top", [])
    rows: list[dict] = []
    for r in pool:
        t = r["t"]
        m = r.get("metrics", {})
        sc = r.get("scores", {})
        live = px_map.get(t, {})
        c_data = chart.get(t)
        rsi = None
        if c_data and c_data.get("closes"):
            rsi = _rsi(c_data["closes"])

        rows.append({
            "t": t, "n": r.get("n", live.get("n", "")), "i": r.get("i", live.get("i", "")),
            "c": live.get("c"), "r": live.get("r"), "m": live.get("m"),
            "fund_score": sc.get("fundamental", 0),
            "mom_score": sc.get("momentum", 0),
            "chart_score": sc.get("chart", 0),
            "rot_score": sc.get("rotation", 0),
            "ret_5d": m.get("ret_5d"),
            "ret_20d": m.get("ret_20d"),
            "above_ma20": m.get("above_ma20"),
            "rsi": round(rsi, 1) if rsi is not None else None,
        })

    if not rows:
        OUT.write_text(json.dumps({"top": []}, ensure_ascii=False), encoding="utf-8")
        print("매수 타이밍: 후보 0")
        return

    df = pd.DataFrame(rows)
    df["mom_neg"] = -df["mom_score"].astype(float)
    df["chart_neg"] = -df["chart_score"].astype(float).clip(upper=0)
    df["rsi_v"] = df["rsi"].fillna(50)
    df["rsi_z"] = -_z(df["rsi_v"])

    df["timing"] = (
        df["fund_score"].astype(float) * 0.30
        + _z(df["mom_neg"]) * 0.25
        + _z(df["chart_neg"]) * 0.20
        + df["rsi_z"] * 0.20
        + df["rot_score"].astype(float) * 0.05
    )
    df = df[df["fund_score"] > -0.3]
    df = df[df["rsi_v"].fillna(50) < 60]
    df = df[df["ret_5d"].fillna(0) < 3]
    df = df.sort_values("timing", ascending=False).head(10)

    top: list[dict] = []
    for _, r in df.iterrows():
        live = px_map.get(r["t"], {})
        rsi_lab = "과매도" if r["rsi"] is not None and r["rsi"] < 35 else \
                  "과매수" if r["rsi"] is not None and r["rsi"] > 65 else "중립"
        top.append({
            "t": r["t"],
            "n": r["n"],
            "i": r["i"],
            "c": live.get("c", r["c"]),
            "r": live.get("r", r["r"]),
            "m": live.get("m", r["m"]),
            "timing": round(float(r["timing"]), 2),
            "fund_score": round(float(r["fund_score"]), 2),
            "rsi": r["rsi"],
            "rsi_label": rsi_lab,
            "ret_5d": r["ret_5d"],
            "ret_20d": r["ret_20d"],
            "above_ma20": r["above_ma20"],
        })

    OUT.write_text(json.dumps({
        "ref_date": strength.get("ref_date"),
        "ref_label": strength.get("ref_label"),
        "criteria": "실적 양호 + 단기 조정(역모멘텀) + RSI 과매도 + 업종 강세 보조 (저점 매수 후보)",
        "top": top,
    }, ensure_ascii=False), encoding="utf-8")

    print(f"매수 타이밍 저장: {OUT} (TOP {len(top)})")
    for r in top[:5]:
        print(f"  {r['n']:14} 점수 {r['timing']:+.2f} (실적 {r['fund_score']:+.2f}, RSI {r['rsi']} {r['rsi_label']}, 5일 {r['ret_5d']:+.2f}%)")


if __name__ == "__main__":
    build()
