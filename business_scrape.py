# -*- coding: utf-8 -*-
"""
fnguide(comp.fnguide.com) 종목 Snapshot에서 기업개요 스크랩.
- bizSummaryHeader: 한 줄 헤드라인 (예: "AI 수요 확대로 메모리 실적 개선")
- bizSummaryContent: 상세 사업개요 (불릿 리스트)
- bizSummaryDate: 작성일자

출력: data/business.json
  { "005930": {"date":"2026/04/10","header":"...","points":["...","..."]}, ... }

사용:
  python business_scrape.py            # 전체 종목 (캐시 있으면 건너뜀)
  python business_scrape.py --force    # 캐시 무시 전체 재수집
  python business_scrape.py 005930     # 특정 종목만
"""
import json
import sys
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeRemainingColumn

from sectors import SECTORS

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

HERE = Path(__file__).parent
OUT = HERE / "data" / "business.json"

URL = (
    "https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp"
    "?pGB=1&gicode=A{ticker}&cID=&MenuYn=Y&ReportGB=&NewMenuID=11&stkGb=701"
)
URL_NAVER = "https://navercomp.wisereport.co.kr/v2/company/c1020001.aspx?cmp_cd={ticker}"


def scrape_naver_detail(ticker: str) -> dict:
    """네이버 종목분석에서 매출구성·R&D·자회사 추출."""
    out = {"products": [], "subsidiaries": []}
    try:
        r = requests.get(URL_NAVER.format(ticker=ticker),
                         headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
    except Exception:
        return out
    if r.status_code != 200:
        return out

    soup = BeautifulSoup(r.content, "lxml")

    # 매출구성: #cTB203
    t = soup.select_one("#cTB203")
    if t:
        for row in t.select("tbody tr"):
            cells = row.find_all(["th", "td"])
            if len(cells) >= 2:
                name = cells[0].get_text(strip=True)
                pct_txt = cells[-1].get_text(strip=True).replace(",", "")
                try:
                    pct = float(pct_txt)
                    if name and name != "제품명":
                        out["products"].append({"name": name[:80], "ratio": pct})
                except ValueError:
                    pass

    # R&D: #cTB205_1
    t = soup.select_one("#cTB205_1")
    if t:
        rows = t.select("tbody tr") or t.find_all("tr")
        for tr in rows:
            cells = [c.get_text(strip=True) for c in tr.find_all(["th", "td"])]
            if cells and "/" in cells[0]:
                out["rnd_year"] = cells[0]
                if len(cells) > 2:
                    try:
                        out["rnd_pct"] = float(cells[2].replace(",", ""))
                    except ValueError:
                        pass
                break

    # 자회사: #cTB212 (상위 8개)
    t = soup.select_one("#cTB212")
    if t:
        for row in t.select("tbody tr"):
            cells = [c.get_text(strip=True) for c in row.find_all(["th", "td"])]
            if len(cells) >= 3:
                out["subsidiaries"].append({
                    "name": cells[0][:60],
                    "biz": cells[1][:40],
                    "founded": cells[2][:10],
                })
        out["subsidiaries"] = out["subsidiaries"][:8]

    return out


def scrape(ticker: str) -> dict | None:
    url = URL.format(ticker=ticker)
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
    except Exception as e:
        return {"error": str(e)}
    if r.status_code != 200:
        return {"error": f"HTTP {r.status_code}"}

    soup = BeautifulSoup(r.content, "lxml")
    header_el = soup.select_one("#bizSummaryHeader")
    content_el = soup.select_one("#bizSummaryContent")
    date_el = soup.select_one("#bizSummaryDate")
    if not header_el and not content_el:
        return None

    header = header_el.get_text(strip=True) if header_el else ""
    date = date_el.get_text(strip=True).strip("[] ") if date_el else ""

    points: list[str] = []
    if content_el:
        for li in content_el.find_all("li"):
            t = li.get_text(strip=True, separator=" ")
            if t:
                points.append(t)
        if not points:
            t = content_el.get_text(strip=True, separator=" ")
            if t:
                points = [t]

    return {"date": date, "header": header, "points": points}


def load_cache() -> dict:
    if OUT.exists():
        try:
            return json.loads(OUT.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def save_cache(cache: dict) -> None:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


def _collect_targets() -> list[tuple[str, str]]:
    """리포트에 등장 가능한 모든 종목 = SECTORS ∪ industry_map 상위 ∪ theme_map 상위.

    EXPAND_TOP_N(=30)와 동일 기준으로 각 그룹 시총 상위 30개 + SECTORS 등록.
    """
    import json
    EXPAND_TOP_N = 30

    tickers: list[tuple[str, str]] = []
    seen: set[str] = set()

    # 1) SECTORS 등록 (대표주)
    for items in SECTORS.values():
        for t, n in items:
            if t not in seen:
                tickers.append((t, n))
                seen.add(t)

    # 2) industry_map 각 업종 상위 30 + theme_map 각 테마 상위 30
    for path in [HERE / "data" / "industry_map.json", HERE / "data" / "theme_map.json"]:
        if not path.exists():
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        groups = data.get("industries") or data.get("groups") or {}
        # 시총 정렬을 위해 KRX 시세 필요 — 여기선 stocks 순서 그대로 상위 N (대부분 시총순으로 저장돼있음)
        for _, info in groups.items():
            for s in (info.get("stocks") or [])[:EXPAND_TOP_N]:
                t = s.get("ticker"); n = s.get("name", "")
                if t and t not in seen:
                    tickers.append((t, n))
                    seen.add(t)
    return tickers


def collect_all(force: bool = False) -> None:
    tickers = _collect_targets()

    cache = {} if force else load_cache()
    # 신규 필드(products/subsidiaries)가 없으면 재수집
    def _needs(t):
        c = cache.get(t)
        if not c: return True
        if "header" not in c: return True
        if "products" not in c and "subsidiaries" not in c: return True
        return False
    todo = [(t, n) for t, n in tickers if _needs(t)]
    print(f"전체 {len(tickers)}종목, 수집 대상 {len(todo)}종목 (캐시 {len(tickers)-len(todo)}종목 재사용)")

    if not todo:
        print("수집할 종목 없음.")
        return

    with Progress(
        SpinnerColumn(), TextColumn("[bold]{task.description}"), BarColumn(),
        TextColumn("{task.completed}/{task.total}"), TimeRemainingColumn(),
    ) as pg:
        tid = pg.add_task("기업개요 수집", total=len(todo))
        for t, n in todo:
            base = scrape(t) or {}
            extra = scrape_naver_detail(t)
            merged = {"name": n,
                      "header": base.get("header", ""),
                      "date": base.get("date", ""),
                      "points": base.get("points", []),
                      **extra}
            cache[t] = merged
            pg.advance(tid)
            time.sleep(0.25)
            # 50개마다 중간 저장
            if pg.tasks[0].completed % 50 == 0:
                save_cache(cache)

    save_cache(cache)
    have = sum(1 for v in cache.values() if v.get("header") or v.get("points"))
    print(f"저장: {OUT}  ({have}/{len(cache)} 정보 확보)")


def main() -> None:
    args = [a for a in sys.argv[1:] if a]
    force = "--force" in args
    args = [a for a in args if not a.startswith("--")]

    if args:
        for t in args:
            res = scrape(t)
            print(t, "→", json.dumps(res, ensure_ascii=False, indent=2))
        return

    collect_all(force=force)


if __name__ == "__main__":
    main()
