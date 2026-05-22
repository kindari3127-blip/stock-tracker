# -*- coding: utf-8 -*-
"""
sector_rotation.py — 섹터 로테이션 히트맵 데이터 (v2)

v1 → v2:
  - 풀: SECTORS dict(약 250종) → industry_map.json + search_index.json (KRX 시총 ≥ 500억)
  - 평균: 단순 평균 → 시총가중 평균 (대형주 비중 큰)
  - 시계열: fdr 풀 호출 → chart_data.json(60일) + chart_5y.json(~1년) 우선, 부족분만 fdr 폴백

산출: data/sector_rotation.json
  - sectors[].name        — 업종명 (KRX 79업종)
  - sectors[].n_stocks    — 시총 ≥ 500억 + 시계열 보유 종목 수
  - sectors[].ret_1w / ret_1m / ret_3m / ret_1y  — 시총가중 평균 등락률(%)
  - sectors[].momentum_score  — 0.4·1w + 0.3·1m + 0.2·3m + 0.1·1y
  - top_5_now / top_5_quarter / top_5_year
"""
import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import FinanceDataReader as fdr
import numpy as np

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

HERE = Path(__file__).parent
DATA = HERE / "data"
OUT = DATA / "sector_rotation.json"
INDUSTRY_MAP = DATA / "industry_map.json"
SEARCH = DATA / "search_index.json"
CHART = DATA / "chart_data.json"
CHART_5Y = DATA / "chart_5y.json"

MARCAP_MIN = 500 * 1e8  # 500억


_RET_CLIP = 150.0  # ±150% 초과는 IPO·테마주 노이즈 — 분모에서 제외


def _ret(closes: list[float], days: int) -> float | None:
    if not closes or len(closes) < days + 1:
        return None
    base = closes[-(days + 1)]
    if base == 0:
        return None
    r = (closes[-1] / base - 1) * 100
    if abs(r) > _RET_CLIP:
        return None  # 이상치 — 시총가중 평균에서 제외
    return round(r, 2)


def _wavg(pairs: list[tuple[float, float]]) -> float | None:
    """시총가중 평균. pairs = [(value, weight), ...]. value None 은 제외."""
    valid = [(v, w) for v, w in pairs if v is not None and w > 0]
    if not valid:
        return None
    tot_w = sum(w for _, w in valid)
    return round(sum(v * w for v, w in valid) / tot_w, 2)


def main() -> None:
    if not INDUSTRY_MAP.exists():
        print("industry_map.json 없음 — industry_index.py 먼저 실행 필요")
        sys.exit(1)
    if not SEARCH.exists():
        print("search_index.json 없음 — recommend.py 먼저 실행")
        sys.exit(1)

    im = json.loads(INDUSTRY_MAP.read_text(encoding="utf-8"))
    ticker_industry = {t: v["industry"] for t, v in im["ticker_map"].items()}

    search = json.loads(SEARCH.read_text(encoding="utf-8"))
    meta = {s["t"]: s for s in search.get("stocks", [])}

    chart = json.loads(CHART.read_text(encoding="utf-8")) if CHART.exists() else {}
    chart_5y = json.loads(CHART_5Y.read_text(encoding="utf-8")) if CHART_5Y.exists() else {}

    # 시총 ≥ 500억 + industry 보유 풀
    pool: list[tuple[str, str, float]] = []  # (ticker, industry, mcap)
    for t, ind in ticker_industry.items():
        m = meta.get(t)
        if not m:
            continue
        mcap = m.get("m") or 0
        if mcap < MARCAP_MIN:
            continue
        pool.append((t, ind, float(mcap)))

    print(f"풀: {len(pool)}종 (시총 ≥ 500억 + industry 매핑)")

    # 업종별 종목 그룹
    by_industry: dict[str, list[tuple[str, float]]] = {}
    for t, ind, mcap in pool:
        by_industry.setdefault(ind, []).append((t, mcap))

    # 종목별 시계열 → ret 산출
    fdr_calls = 0
    fdr_end = datetime.now()
    fdr_start_1y = fdr_end - timedelta(days=380)
    rets_by_ticker: dict[str, dict] = {}

    for t, _, _ in pool:
        closes_60: list[float] | None = None
        closes_1y: list[float] | None = None

        # 60일 (chart_data 우선)
        if t in chart and chart[t].get("closes"):
            closes_60 = [float(x) for x in chart[t]["closes"]]

        # 1년 (chart_5y 우선)
        if t in chart_5y and chart_5y[t].get("closes"):
            closes_1y = [float(x) for x in chart_5y[t]["closes"]]

        # 둘 다 없으면 fdr 폴백 (1년치)
        if closes_60 is None and closes_1y is None:
            try:
                df = fdr.DataReader(t, fdr_start_1y, fdr_end)
                if df is not None and len(df) >= 5:
                    closes_1y = df["Close"].astype(float).tolist()
                    closes_60 = closes_1y[-60:] if len(closes_1y) >= 5 else None
                fdr_calls += 1
                time.sleep(0.02)
            except Exception:
                pass

        if closes_60 is None and closes_1y is None:
            continue

        # 1주/1개월/3개월: 60일 우선, 부족하면 1년에서
        src_short = closes_60 if closes_60 and len(closes_60) >= 21 else closes_1y
        src_long = closes_1y if closes_1y and len(closes_1y) >= 60 else None

        rets_by_ticker[t] = {
            "r1w": _ret(src_short, 5) if src_short else None,
            "r1m": _ret(src_short, 20) if src_short else None,
            "r3m": _ret(src_long, 60) if src_long else _ret(src_short, 59) if src_short and len(src_short) >= 60 else None,
            "r1y": _ret(src_long, 240) if src_long and len(src_long) >= 241 else None,
        }

    print(f"시계열 보유: {len(rets_by_ticker)}종 / fdr 폴백 {fdr_calls}회")

    sector_rows = []
    for ind, items in by_industry.items():
        rows = [(t, mcap, rets_by_ticker.get(t)) for t, mcap in items if t in rets_by_ticker]
        if not rows:
            continue

        r1w = _wavg([(r["r1w"], mcap) for _, mcap, r in rows])
        r1m = _wavg([(r["r1m"], mcap) for _, mcap, r in rows])
        r3m = _wavg([(r["r3m"], mcap) for _, mcap, r in rows])
        r1y = _wavg([(r["r1y"], mcap) for _, mcap, r in rows])

        comps = [(r1w, 0.4), (r1m, 0.3), (r3m, 0.2), (r1y, 0.1)]
        used = [(v, w) for v, w in comps if v is not None]
        if used:
            tot_w = sum(w for _, w in used)
            mom = round(sum(v * w for v, w in used) / tot_w, 2)
        else:
            mom = None

        n_with_1y = sum(1 for _, _, r in rows if r["r1y"] is not None)

        sector_rows.append({
            "name": ind,
            "n_stocks": len(rows),
            "n_with_1y": n_with_1y,
            "ret_1w": r1w,
            "ret_1m": r1m,
            "ret_3m": r3m,
            "ret_1y": r1y,
            "momentum_score": mom,
        })

    sector_rows.sort(key=lambda x: (x["momentum_score"] if x["momentum_score"] is not None else -999), reverse=True)

    out = {
        "updated": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "pool_size": len(pool),
        "sectors": sector_rows,
        "top_5_now": [s["name"] for s in sorted(sector_rows, key=lambda x: x["ret_1w"] or -999, reverse=True)[:5]],
        "top_5_quarter": [s["name"] for s in sorted(sector_rows, key=lambda x: x["ret_3m"] or -999, reverse=True)[:5]],
        "top_5_year": [s["name"] for s in sorted(sector_rows, key=lambda x: x["ret_1y"] or -999, reverse=True)[:5]],
    }
    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n저장: {OUT}  ({len(sector_rows)}업종 · 풀 {len(pool)}종)")
    print(f"단기 강세 5: {out['top_5_now']}")
    print(f"분기 강세 5: {out['top_5_quarter']}")
    print(f"연간 강세 5: {out['top_5_year']}")

    print("\n[모멘텀 TOP 10]")
    for s in sector_rows[:10]:
        r1w = f"{s['ret_1w']:+.2f}" if s['ret_1w'] is not None else "  -  "
        r1m = f"{s['ret_1m']:+.2f}" if s['ret_1m'] is not None else "  -  "
        r3m = f"{s['ret_3m']:+.2f}" if s['ret_3m'] is not None else "  -  "
        r1y = f"{s['ret_1y']:+.2f}" if s['ret_1y'] is not None else "  -  "
        mom = f"{s['momentum_score']:+.2f}" if s['momentum_score'] is not None else "  -  "
        print(f"  {s['name']:24s} ({s['n_stocks']:3d}종)  1W {r1w}  1M {r1m}  3M {r3m}  1Y {r1y}  mom {mom}")


if __name__ == "__main__":
    main()
