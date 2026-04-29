# -*- coding: utf-8 -*-
"""
fundamentals.csv 기반 카테고리 분류:
  - 성장주 (growth)         ROE↑ + 추정 ROE↑ + PER 부담 적음
  - 배당주 (dividend)       배당수익률 ≥ 3.5% + 부채비율 ≤ 200%
  - 실적우량주 (quality)    ROE ≥ 15 + 영업이익률 ≥ 15 + PBR ≤ 4
출력: data/categories.json
"""
import json
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

HERE = Path(__file__).parent
FUND = HERE / "data" / "fundamentals.csv"
SEARCH = HERE / "data" / "search_index.json"
LISTING = HERE / "data" / "listing_dates.json"
OUT = HERE / "data" / "categories.json"


def _safe(v) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
    except Exception:
        return None
    if pd.isna(f):
        return None
    return f


def build() -> None:
    df = pd.read_csv(FUND, dtype={"ticker": str})
    df["ticker"] = df["ticker"].str.zfill(6)

    search = json.loads(SEARCH.read_text(encoding="utf-8"))
    px = {s["t"]: s for s in search.get("stocks", [])}

    growth: list[dict] = []
    dividend: list[dict] = []
    quality: list[dict] = []
    value: list[dict] = []

    for _, r in df.iterrows():
        t = r["ticker"]
        per = _safe(r.get("per"))
        pbr = _safe(r.get("pbr"))
        roe = _safe(r.get("roe"))
        roe_est = _safe(r.get("roe_est"))
        op_margin = _safe(r.get("op_margin"))
        dy = _safe(r.get("dividend_yield"))
        debt = _safe(r.get("debt_ratio"))
        eps = _safe(r.get("eps"))
        eps_est = _safe(r.get("eps_est"))

        live = px.get(t, {})
        info = {
            "t": t,
            "n": str(r.get("name") or ""),
            "i": live.get("i") or str(r.get("sector") or ""),
            "c": live.get("c"),
            "r": live.get("r"),
            "m": live.get("m"),
            "per": per, "pbr": pbr, "roe": roe,
            "roe_est": roe_est, "op_margin": op_margin,
            "dividend_yield": dy, "debt_ratio": debt,
        }

        if roe is not None and roe >= 12 and roe_est is not None and roe_est >= roe * 0.95:
            if eps and eps_est and eps > 0 and eps_est > eps * 1.05:
                score = (eps_est / eps - 1) * 100
                growth.append({**info, "score": round(score, 1), "score_label": f"EPS +{score:.1f}%"})

        if dy is not None and dy >= 3.5 and (debt is None or debt <= 200):
            score = dy
            dividend.append({**info, "score": round(score, 2), "score_label": f"배당 {dy:.2f}%"})

        if (roe is not None and roe >= 15 and
            op_margin is not None and op_margin >= 15 and
            pbr is not None and pbr <= 4):
            score = roe + op_margin
            quality.append({**info, "score": round(score, 1), "score_label": f"ROE {roe:.1f} · OM {op_margin:.1f}"})

        if (per is not None and per > 0 and per <= 12 and
            pbr is not None and pbr > 0 and pbr <= 1.5 and
            roe is not None and roe >= 10):
            score = roe / per * 10
            value.append({**info, "score": round(score, 2),
                          "score_label": f"PER {per:.1f} · PBR {pbr:.2f} · ROE {roe:.1f}"})

    growth.sort(key=lambda x: x["score"], reverse=True)
    dividend.sort(key=lambda x: x["score"], reverse=True)
    quality.sort(key=lambda x: x["score"], reverse=True)
    value.sort(key=lambda x: x["score"], reverse=True)

    new_listing: list[dict] = []
    if LISTING.exists():
        ld = json.loads(LISTING.read_text(encoding="utf-8"))
        threshold = (date.today() - timedelta(days=365)).strftime("%Y%m%d")
        for s in search.get("stocks", []):
            t = s["t"]
            fd = ld.get(t)
            if not fd or fd < threshold:
                continue
            new_listing.append({
                "t": t, "n": s.get("n", ""), "i": s.get("i", ""),
                "c": s.get("c"), "r": s.get("r"), "m": s.get("m"),
                "score": int(fd),
                "score_label": f"상장 {fd[:4]}-{fd[4:6]}-{fd[6:]}",
            })
        new_listing.sort(key=lambda x: x["score"], reverse=True)

    out = {
        "ref_date": search.get("ref_date"),
        "growth": growth[:30],
        "dividend": dividend[:30],
        "quality": quality[:30],
        "value": value[:30],
        "new_listing": new_listing[:30],
        "criteria": {
            "growth": "ROE ≥ 12 · 추정 ROE ≥ 현재의 95% · 추정 EPS > 현재 EPS×1.05",
            "dividend": "배당수익률 ≥ 3.5% · 부채비율 ≤ 200%",
            "quality": "ROE ≥ 15 · 영업이익률 ≥ 15 · PBR ≤ 4",
            "value": "PER ≤ 12 · PBR ≤ 1.5 · ROE ≥ 10 (저평가 우량주)",
            "new_listing": "최근 1년 이내 상장 (시총 상위 500 풀)",
        },
    }
    OUT.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
    print(f"성장주: {len(growth)} / 배당주: {len(dividend)} / 실적우량주: {len(quality)} / 저평가: {len(value)} / 신규상장: {len(new_listing)}")
    print(f"저장: {OUT}")


if __name__ == "__main__":
    build()
