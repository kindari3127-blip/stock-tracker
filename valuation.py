# -*- coding: utf-8 -*-
"""
가치판단 모듈:
  - PSR = 시총 / 매출
  - PEG = PER / EPS 성장률(%)
  - 동종업계 중앙값 대비 PER/PBR/PSR 비교 → 저평가/고평가 z-score
  - 종합 가치 점수 (높을수록 저평가)
출력: data/valuation.json
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
FUND = HERE / "data" / "fundamentals.csv"
SEARCH = HERE / "data" / "search_index.json"
OUT = HERE / "data" / "valuation.json"


def _safe(v):
    if v is None or pd.isna(v):
        return None
    try:
        return float(v)
    except Exception:
        return None


def build() -> None:
    fund = pd.read_csv(FUND, dtype={"ticker": str})
    fund["ticker"] = fund["ticker"].str.zfill(6)
    search = json.loads(SEARCH.read_text(encoding="utf-8"))
    px_map = {s["t"]: s for s in search.get("stocks", [])}

    rows: list[dict] = []
    for _, r in fund.iterrows():
        t = r["ticker"]
        live = px_map.get(t, {})
        per = _safe(r.get("per"))
        pbr = _safe(r.get("pbr"))
        roe = _safe(r.get("roe"))
        eps = _safe(r.get("eps"))
        eps_est = _safe(r.get("eps_est"))
        revenue = _safe(r.get("revenue"))
        op_margin = _safe(r.get("op_margin"))
        mcap = live.get("m") or 0

        psr = None
        if revenue and revenue > 0 and mcap > 0:
            psr = round(mcap / (revenue * 1_000_000), 2)

        eps_growth = None
        if eps is not None and eps > 0 and eps_est is not None:
            eps_growth = round((eps_est / eps - 1) * 100, 2)
        peg = None
        if per is not None and per > 0 and eps_growth is not None and eps_growth > 0:
            peg = round(per / eps_growth, 2)

        rows.append({
            "t": t,
            "n": str(r.get("name") or ""),
            "industry": live.get("i") or str(r.get("sector") or ""),
            "mcap": int(mcap) if mcap else None,
            "per": per, "pbr": pbr, "roe": roe,
            "psr": psr, "peg": peg, "op_margin": op_margin,
            "eps": eps, "eps_est": eps_est, "eps_growth": eps_growth,
            "revenue": revenue,
        })

    df = pd.DataFrame(rows)

    industry_stats: dict[str, dict] = {}
    for ind, sub in df.groupby("industry"):
        if not ind or len(sub) < 2:
            continue
        industry_stats[ind] = {
            "per_med": float(sub["per"].dropna().median()) if sub["per"].notna().any() else None,
            "pbr_med": float(sub["pbr"].dropna().median()) if sub["pbr"].notna().any() else None,
            "psr_med": float(sub["psr"].dropna().median()) if sub["psr"].notna().any() else None,
            "roe_med": float(sub["roe"].dropna().median()) if sub["roe"].notna().any() else None,
            "n": int(len(sub)),
        }

    def _level(val, med, lower_better=True):
        if val is None or med is None or med == 0:
            return None, ""
        ratio = val / med
        if lower_better:
            diff_pct = (med - val) / med * 100
        else:
            diff_pct = (val - med) / med * 100
        if abs(diff_pct) < 10:
            label = "업종 평균"
        elif diff_pct > 30:
            label = "현저히 우위"
        elif diff_pct > 10:
            label = "우위"
        elif diff_pct < -30:
            label = "현저히 열위"
        else:
            label = "열위"
        return round(diff_pct, 1), label

    out_items: dict[str, dict] = {}
    for _, r in df.iterrows():
        ind = r["industry"]
        stats = industry_stats.get(ind, {})
        per_diff, per_lab = _level(r["per"], stats.get("per_med"), True)
        pbr_diff, pbr_lab = _level(r["pbr"], stats.get("pbr_med"), True)
        psr_diff, psr_lab = _level(r["psr"], stats.get("psr_med"), True)
        roe_diff, roe_lab = _level(r["roe"], stats.get("roe_med"), False)

        scores = []
        for v in (per_diff, pbr_diff, psr_diff, roe_diff):
            if v is not None:
                scores.append(max(min(v / 30, 1.5), -1.5))
        composite = round(float(np.mean(scores)), 2) if scores else None

        if composite is None:
            verdict = "데이터 부족"
        elif composite >= 0.7:
            verdict = "현저히 저평가"
        elif composite >= 0.3:
            verdict = "저평가"
        elif composite >= -0.3:
            verdict = "적정"
        elif composite >= -0.7:
            verdict = "고평가"
        else:
            verdict = "현저히 고평가"

        out_items[r["t"]] = {
            "industry": ind,
            "industry_n": stats.get("n"),
            "metrics": {
                "per": r["per"], "pbr": r["pbr"], "psr": r["psr"],
                "peg": r["peg"], "roe": r["roe"], "op_margin": r["op_margin"],
                "eps_growth": r["eps_growth"],
            },
            "industry_med": {
                "per": round(stats["per_med"], 2) if stats.get("per_med") else None,
                "pbr": round(stats["pbr_med"], 2) if stats.get("pbr_med") else None,
                "psr": round(stats["psr_med"], 2) if stats.get("psr_med") else None,
                "roe": round(stats["roe_med"], 2) if stats.get("roe_med") else None,
            },
            "compare": {
                "per": {"diff_pct": per_diff, "label": per_lab},
                "pbr": {"diff_pct": pbr_diff, "label": pbr_lab},
                "psr": {"diff_pct": psr_diff, "label": psr_lab},
                "roe": {"diff_pct": roe_diff, "label": roe_lab},
            },
            "composite": composite,
            "verdict": verdict,
        }

    OUT.write_text(json.dumps({
        "ref_date": search.get("ref_date"),
        "items": out_items,
        "industry_stats": {k: {kk: round(vv, 2) if isinstance(vv, float) else vv for kk, vv in v.items()} for k, v in industry_stats.items()},
    }, ensure_ascii=False), encoding="utf-8")
    print(f"가치판단 저장: {OUT} ({len(out_items)}종목 / {len(industry_stats)}업종)")


if __name__ == "__main__":
    build()
