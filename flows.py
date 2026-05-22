# -*- coding: utf-8 -*-
"""
flows.py — 수급(외인·기관·개인) 일별 수집
1) 시장 전체: 네이버 investorDealTrendDay (KOSPI / KOSDAQ)  → data/flows_market.json
2) 종목별:  네이버 frgn.naver (table#3, 최근 30일)         → data/flows_stock.json
"""
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

import requests
from bs4 import BeautifulSoup

HERE = Path(__file__).parent
DATA = HERE / "data"
DATA.mkdir(exist_ok=True)
OUT_MARKET = DATA / "flows_market.json"
OUT_STOCK = DATA / "flows_stock.json"

HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://finance.naver.com/"}


def _to_int(s: str) -> int | None:
    if not s:
        return None
    s = s.replace(",", "").replace("+", "").strip()
    try:
        return int(s)
    except ValueError:
        return None


# ────────── 1. 시장 전체 (KOSPI 기준 일별 수급) ──────────
# 네이버 investorDealTrendDay 페이지는 KOSPI/KOSDAQ 분리 응답을 안 줘 시장 전체 1종만 수집.
# 단위: 백만원 (네이버 표 그대로). 컬럼: 개인·외국인·기관계 + 세부(금융투자·보험·투신·은행·기타금융·연기금).
def fetch_market_flow(bizdate: str | None = None) -> list[dict]:
    if bizdate is None:
        bizdate = datetime.now().strftime("%Y%m%d")
    rows: list[dict] = []
    r = requests.get(
        "https://finance.naver.com/sise/investorDealTrendDay.naver",
        params={"bizdate": bizdate},
        headers=HEADERS,
        timeout=10,
    )
    soup = BeautifulSoup(r.content, "lxml")
    table = soup.select_one("table.type_1")
    if not table:
        return rows
    for tr in table.select("tr"):
        cells = [c.get_text(strip=True).replace("\xa0", "") for c in tr.select("td")]
        if len(cells) < 6:
            continue
        d = cells[0]
        if not re.match(r"\d{2}\.\d{2}\.\d{2}", d):
            continue
        # 26.05.21 → 2026-05-21
        yy, mm, dd = d.split(".")
        iso = f"20{yy}-{mm}-{dd}"
        rows.append({
            "date":         iso,
            "individual":   _to_int(cells[1]),
            "foreign":      _to_int(cells[2]),
            "institute":    _to_int(cells[3]),
            "inst_detail":  _to_int(cells[4]),  # 기관 본체
            "etc_corp":     _to_int(cells[5]),  # 기타법인
            "fin_inv":      _to_int(cells[6]) if len(cells) > 6 else None,  # 금융투자
            "pension":      _to_int(cells[10]) if len(cells) > 10 else None,  # 연기금
        })
    return rows


def main_market():
    rows = fetch_market_flow()
    out = {
        "updated": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "unit": "백만원",
        "scope": "KOSPI 기준 시장 전체",
        "rows": rows,
    }
    if rows:
        r0 = rows[0]
        print(f"  {len(rows)}일  최근 {r0['date']}  외인 {r0['foreign']:>+,}  기관 {r0['institute']:>+,}  개인 {r0['individual']:>+,}")
    else:
        print("  0일 (수집 실패)")
    OUT_MARKET.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"저장: {OUT_MARKET}")


# ────────── 2. 종목별 ──────────
def fetch_stock_flow(ticker: str, days: int = 20) -> list[dict]:
    """frgn.naver table#3 파싱. 최근 days 일."""
    url = f"https://finance.naver.com/item/frgn.naver?code={ticker}"
    r = requests.get(url, headers=HEADERS, timeout=10)
    soup = BeautifulSoup(r.content, "lxml")
    # 헤더에 '날짜·종가·기관·외국인' 있는 table 찾기
    table = None
    for t in soup.select("table"):
        ths = [c.get_text(strip=True) for c in t.select("th")]
        if "날짜" in ths and "외국인" in ths and "기관" in ths:
            table = t
            break
    if not table:
        return []
    rows: list[dict] = []
    for tr in table.select("tr"):
        cells = [c.get_text(strip=True).replace("\xa0", "") for c in tr.select("td")]
        if len(cells) < 9:
            continue
        d = cells[0]
        if not re.match(r"\d{4}\.\d{2}\.\d{2}", d):
            continue
        rows.append({
            "date":          d.replace(".", "-"),
            "close":         _to_int(cells[1]),
            "vol":           _to_int(cells[4]),
            "inst":          _to_int(cells[5]),  # 기관 순매매
            "foreign":       _to_int(cells[6]),  # 외국인 순매매
            "foreign_hold":  _to_int(cells[7]),  # 외국인 보유주수
        })
        if len(rows) >= days:
            break
    return rows


def _load_universe() -> list[tuple[str, str]]:
    """sectors + (있다면) cashflow 상위 종목 union → 추적·확장 풀."""
    sys.path.insert(0, str(HERE))
    from sectors import SECTORS  # type: ignore
    uniq: dict[str, str] = {}
    for items in SECTORS.values():
        for t, n in items:
            uniq[t] = n
    # cashflow.csv 가 있으면 시총 상위 ≥ 1,000억 추가
    cf = DATA / "cashflow.csv"
    if cf.exists():
        try:
            import pandas as pd
            df = pd.read_csv(cf, dtype={"ticker": str})
            for _, row in df.head(300).iterrows():
                t = str(row.get("ticker", "")).zfill(6)
                n = str(row.get("name", ""))
                if t and t not in uniq:
                    uniq[t] = n
        except Exception:
            pass
    return list(uniq.items())


def _cumulate(rows: list[dict], key: str) -> int:
    return sum((r.get(key) or 0) for r in rows)


def main_stock(max_workers: int = 8):
    universe = _load_universe()
    print(f"종목별 수급 수집: {len(universe)}종목")

    results: dict[str, dict] = {}
    failed: list[str] = []

    def _job(ticker: str, name: str):
        try:
            rows = fetch_stock_flow(ticker, days=20)
            return ticker, name, rows, None
        except Exception as e:
            return ticker, name, [], str(e)

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(_job, t, n) for t, n in universe]
        for i, f in enumerate(as_completed(futs), 1):
            t, n, rows, err = f.result()
            if not rows:
                failed.append(t)
                continue
            results[t] = {
                "name": n,
                "rows": rows,
                "foreign_20d": _cumulate(rows, "foreign"),
                "inst_20d":    _cumulate(rows, "inst"),
                "foreign_5d":  _cumulate(rows[:5], "foreign"),
                "inst_5d":     _cumulate(rows[:5], "inst"),
            }
            if i % 50 == 0:
                print(f"  진행: {i}/{len(universe)}")
            time.sleep(0.02)

    out = {
        "updated": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "count": len(results),
        "failed": len(failed),
        "stocks": results,
    }
    OUT_STOCK.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
    print(f"저장: {OUT_STOCK}  성공 {len(results)} / 실패 {len(failed)}")

    # 외인 누적 매수 TOP/BOT 표시
    if results:
        ranked = sorted(results.items(), key=lambda kv: kv[1]["foreign_20d"], reverse=True)
        print("\n[20일 외인 순매수 TOP 5]")
        for t, r in ranked[:5]:
            print(f"  {r['name']:12s} {r['foreign_20d']:>14,}주")
        print("[20일 외인 순매도 TOP 5]")
        for t, r in ranked[-5:]:
            print(f"  {r['name']:12s} {r['foreign_20d']:>14,}주")


def main():
    print("=== 시장 전체 수급 ===")
    main_market()
    print("\n=== 종목별 수급 ===")
    main_stock()


if __name__ == "__main__":
    main()
