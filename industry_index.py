# -*- coding: utf-8 -*-
"""
네이버 업종 전체 인덱스 캐시 생성.
    python industry_index.py
→ data/industry_map.json 에 저장. 리포트 확장 패널 생성에 사용.
주 1회 정도 수동 실행하거나 auto.bat에 추가.
"""
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeRemainingColumn

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

HERE = Path(__file__).parent
CACHE = HERE / "data" / "industry_map.json"
THEME_CACHE = HERE / "data" / "theme_map.json"
LIST_URL = "https://finance.naver.com/sise/sise_group.naver?type=upjong"
DETAIL_URL = "https://finance.naver.com/sise/sise_group_detail.naver?type=upjong&no={no}"
THEME_LIST_URL = "https://finance.naver.com/sise/theme.naver?&page={page}"
THEME_DETAIL_URL = "https://finance.naver.com/sise/sise_group_detail.naver?type=theme&no={no}"
UA = {"User-Agent": "Mozilla/5.0"}


def _industries() -> dict[str, str]:
    r = requests.get(LIST_URL, headers=UA, timeout=10)
    soup = BeautifulSoup(r.content, "lxml")
    out: dict[str, str] = {}
    for a in soup.select("a[href*='sise_group_detail']"):
        m = re.search(r"no=(\d+)", a.get("href") or "")
        if not m:
            continue
        name = a.get_text(strip=True)
        if name and name not in out:
            out[name] = m.group(1)
    return out


def _themes() -> dict[str, str]:
    """네이버 테마 전체 (여러 페이지) 수집."""
    out: dict[str, str] = {}
    for page in range(1, 15):
        try:
            r = requests.get(THEME_LIST_URL.format(page=page), headers=UA, timeout=10)
        except Exception:
            break
        soup = BeautifulSoup(r.content, "lxml")
        new_count = 0
        for a in soup.select("a[href*='sise_group_detail']"):
            href = a.get("href") or ""
            if "type=theme" not in href:
                continue
            m = re.search(r"no=(\d+)", href)
            if not m:
                continue
            name = a.get_text(strip=True)
            if name and name not in out:
                out[name] = m.group(1)
                new_count += 1
        if new_count == 0:
            break
        time.sleep(0.05)
    return out


def _theme_stocks(no: str) -> list[dict]:
    r = requests.get(THEME_DETAIL_URL.format(no=no), headers=UA, timeout=10)
    soup = BeautifulSoup(r.content, "lxml")
    rows = []
    for tbl in soup.select("table"):
        ths = [th.get_text(strip=True) for th in tbl.select("thead th")]
        if not ths or "종목명" not in ths:
            continue
        for tr in tbl.select("tbody tr"):
            link = tr.select_one("a[href*='code=']")
            if not link:
                continue
            m = re.search(r"code=(\d+)", link.get("href") or "")
            if not m:
                continue
            rows.append({"ticker": m.group(1), "name": link.get_text(strip=True)})
        break
    return rows


def _stocks_of(no: str) -> list[dict]:
    r = requests.get(DETAIL_URL.format(no=no), headers=UA, timeout=10)
    soup = BeautifulSoup(r.content, "lxml")
    rows = []
    for tbl in soup.select("table"):
        ths = [th.get_text(strip=True) for th in tbl.select("thead th")]
        if not ths or "종목명" not in ths:
            continue
        for tr in tbl.select("tbody tr"):
            link = tr.select_one("a[href*='code=']")
            if not link:
                continue
            m = re.search(r"code=(\d+)", link.get("href") or "")
            if not m:
                continue
            code = m.group(1)
            name = link.get_text(strip=True)
            rows.append({"ticker": code, "name": name})
        break
    return rows


def _build(list_fn, stock_fn, label: str, cache_path: Path) -> None:
    items = list_fn()
    ticker_map: dict[str, list] = {}
    groups: dict[str, dict] = {}
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold]{task.description}[/bold]"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeRemainingColumn(),
        transient=True,
    ) as p:
        job = p.add_task(label, total=len(items))
        for name, no in items.items():
            try:
                stocks = stock_fn(no)
            except Exception:
                stocks = []
            groups[no] = {"name": name, "stocks": stocks}
            for s in stocks:
                ticker_map.setdefault(s["ticker"], []).append({"no": no, "group": name, "name": s["name"]})
            p.advance(job)
            time.sleep(0.05)

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(
        json.dumps({
            "generated": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "ticker_map": ticker_map,
            "groups": groups,
        }, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    total = sum(len(v["stocks"]) for v in groups.values())
    print(f"저장: {cache_path}")
    print(f"{label}: {len(groups)}개, 종목 슬롯 {total:,}, 매핑 {len(ticker_map):,}")


def main() -> None:
    target = sys.argv[1] if len(sys.argv) > 1 else "all"
    if target in ("all", "upjong", "industry"):
        # 업종 캐시는 기존 ticker_map 형태(단일 dict 값) 유지 — 리포트 하위호환
        inds = _industries()
        ticker_map: dict[str, dict] = {}
        industries: dict[str, dict] = {}
        with Progress(
            SpinnerColumn(), TextColumn("[bold]{task.description}[/bold]"),
            BarColumn(), TextColumn("{task.completed}/{task.total}"), TimeRemainingColumn(),
            transient=True,
        ) as p:
            job = p.add_task("업종 인덱스", total=len(inds))
            for name, no in inds.items():
                try:
                    stocks = _stocks_of(no)
                except Exception:
                    stocks = []
                industries[no] = {"name": name, "stocks": stocks}
                for s in stocks:
                    ticker_map.setdefault(s["ticker"], {"no": no, "industry": name, "name": s["name"]})
                p.advance(job)
                time.sleep(0.05)
        CACHE.parent.mkdir(parents=True, exist_ok=True)
        CACHE.write_text(json.dumps({
            "generated": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "ticker_map": ticker_map,
            "industries": industries,
        }, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"저장: {CACHE}  업종 {len(industries)}개, 매핑 {len(ticker_map):,}")

    if target in ("all", "theme"):
        _build(_themes, _theme_stocks, "테마 인덱스", THEME_CACHE)


if __name__ == "__main__":
    main()
