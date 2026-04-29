# -*- coding: utf-8 -*-
"""
KRX 전체 시세 + 79업종 매핑 → 강세 분석 / 검색 인덱스 JSON 빌드.
출력:
  data/strength.json — 전일 강세 섹터 TOP 10 + 종목 TOP 10
  data/search_index.json — KRX 전체 검색 인덱스
"""
import json
import sys
from pathlib import Path

import FinanceDataReader as fdr
import pandas as pd

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

HERE = Path(__file__).parent
INDMAP = HERE / "data" / "industry_map.json"
STRENGTH = HERE / "data" / "strength.json"
SEARCH = HERE / "data" / "search_index.json"


def _ref_date(df: pd.DataFrame) -> str:
    s = fdr.DataReader("005930").tail(1)
    return s.index[-1].strftime("%Y%m%d")


def _load_industry_map() -> tuple[dict, dict]:
    raw = json.loads(INDMAP.read_text(encoding="utf-8"))
    industries = raw.get("industries", {})
    ticker_to_industry: dict[str, str] = {}
    for no, info in industries.items():
        name = info.get("name") or ""
        for s in info.get("stocks", []):
            t = s.get("ticker")
            if t and t not in ticker_to_industry:
                ticker_to_industry[t] = name
    return industries, ticker_to_industry


def _fmt_date(d: str) -> str:
    return f"{d[:4]}-{d[4:6]}-{d[6:]}"


def build() -> None:
    print("KRX 시세 수집 중...")
    listing = fdr.StockListing("KRX").drop_duplicates(subset=["Code"])
    listing = listing[listing["Close"] > 0].copy()
    listing["Code"] = listing["Code"].astype(str).str.zfill(6)

    ref_date = _ref_date(listing)
    print(f"기준일: {ref_date} / 종목 수: {len(listing):,}")

    industries, t2i = _load_industry_map()
    listing["Industry"] = listing["Code"].map(t2i).fillna("")

    rows = listing[
        ["Code", "Name", "Industry", "Close", "ChagesRatio", "Marcap", "Amount", "Volume", "Market"]
    ].rename(columns={
        "Code": "t", "Name": "n", "Industry": "i",
        "Close": "c", "ChagesRatio": "r", "Marcap": "m",
        "Amount": "a", "Volume": "v", "Market": "mk",
    }).copy()

    rows["c"] = rows["c"].astype(int)
    rows["r"] = rows["r"].round(2)
    rows["m"] = rows["m"].fillna(0).astype("int64")
    rows["a"] = rows["a"].fillna(0).astype("int64")
    rows["v"] = rows["v"].fillna(0).astype("int64")

    sector_rets: list[dict] = []
    for no, info in industries.items():
        name = info.get("name") or ""
        if not name:
            continue
        tickers = [s.get("ticker") for s in info.get("stocks", []) if s.get("ticker")]
        if not tickers:
            continue
        sub = listing[listing["Code"].isin(tickers)]
        sub = sub[sub["Marcap"].fillna(0) > 0]
        if len(sub) < 2:
            continue
        mcap_sum = float(sub["Marcap"].sum())
        if mcap_sum <= 0:
            continue
        weighted = float((sub["ChagesRatio"] * sub["Marcap"]).sum() / mcap_sum)
        leaders = sub.sort_values("Marcap", ascending=False).head(3)
        sector_rets.append({
            "name": name,
            "ret": round(weighted, 2),
            "size": int(len(sub)),
            "mcap": int(mcap_sum),
            "leaders": [
                {"t": str(r["Code"]), "n": str(r["Name"]), "r": round(float(r["ChagesRatio"]), 2)}
                for _, r in leaders.iterrows()
            ],
        })

    sectors_top = sorted(sector_rets, key=lambda x: x["ret"], reverse=True)[:10]
    sectors_bot = sorted(sector_rets, key=lambda x: x["ret"])[:10]

    stock_pool = listing[(listing["Marcap"].fillna(0) >= 1e11) & (listing["Amount"].fillna(0) >= 1e9)]
    stocks_top = stock_pool.sort_values("ChagesRatio", ascending=False).head(10)
    stocks_bot = stock_pool.sort_values("ChagesRatio").head(10)

    def _stock_rows(df: pd.DataFrame) -> list[dict]:
        out = []
        for _, r in df.iterrows():
            out.append({
                "t": str(r["Code"]),
                "n": str(r["Name"]),
                "i": str(r.get("Industry") or ""),
                "c": int(r["Close"]),
                "r": round(float(r["ChagesRatio"]), 2),
                "m": int(r["Marcap"] or 0),
                "a": int(r["Amount"] or 0),
            })
        return out

    strength = {
        "ref_date": ref_date,
        "ref_label": _fmt_date(ref_date),
        "sectors_top": sectors_top,
        "sectors_bottom": sectors_bot,
        "stocks_top": _stock_rows(stocks_top),
        "stocks_bottom": _stock_rows(stocks_bot),
        "filter": {
            "stock_min_mcap": int(1e11),
            "stock_min_amount": int(1e9),
        },
    }
    STRENGTH.parent.mkdir(parents=True, exist_ok=True)
    STRENGTH.write_text(json.dumps(strength, ensure_ascii=False), encoding="utf-8")
    print(f"강세 저장: {STRENGTH}")
    print(f"  섹터 분석: {len(sector_rets)}개 / 종목 풀: {len(stock_pool)}개")
    if sectors_top:
        print(f"  최고 섹터: {sectors_top[0]['name']} {sectors_top[0]['ret']:+.2f}%")

    search = {
        "ref_date": ref_date,
        "ref_label": _fmt_date(ref_date),
        "stocks": rows.to_dict(orient="records"),
        "industries": [
            {"name": s["name"], "ret": s["ret"], "size": s["size"], "mcap": s["mcap"]}
            for s in sorted(sector_rets, key=lambda x: x["mcap"], reverse=True)
        ],
    }
    SEARCH.write_text(json.dumps(search, ensure_ascii=False), encoding="utf-8")
    size_mb = SEARCH.stat().st_size / 1024 / 1024
    print(f"검색 저장: {SEARCH} ({size_mb:.2f} MB / 종목 {len(rows)}개)")


if __name__ == "__main__":
    build()
