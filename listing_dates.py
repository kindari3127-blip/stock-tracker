# -*- coding: utf-8 -*-
"""
시총 상위 500종목의 상장일 캐시 (FDR 시계열 첫 날짜로 추정).
처음 1회: 5분 / 이후: 캐시 활용, 신규(없는 ticker)만 추가 호출.
출력: data/listing_dates.json — {ticker: 'YYYYMMDD' or null}
"""
import json
import sys
import time
from pathlib import Path

import FinanceDataReader as fdr
import pandas as pd
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeRemainingColumn

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

HERE = Path(__file__).parent
SEARCH = HERE / "data" / "search_index.json"
OUT = HERE / "data" / "listing_dates.json"
TOP_N = 500


def build() -> None:
    search = json.loads(SEARCH.read_text(encoding="utf-8"))
    stocks = sorted(search.get("stocks", []), key=lambda s: s.get("m", 0) or 0, reverse=True)[:TOP_N]

    cache: dict[str, str | None] = {}
    if OUT.exists():
        try:
            cache = json.loads(OUT.read_text(encoding="utf-8"))
        except Exception:
            cache = {}

    end = pd.Timestamp.today().normalize()
    start = end - pd.Timedelta(days=400)

    todo = [s for s in stocks if s["t"] not in cache]
    if not todo:
        print(f"상장일 캐시 모두 보유 ({len(cache)}종목). 호출 없음.")
        return

    print(f"신규 호출 대상: {len(todo)}종목")
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold]{task.description}[/bold]"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeRemainingColumn(),
        transient=True,
    ) as p:
        job = p.add_task("상장일 수집", total=len(todo))
        for s in todo:
            t = s["t"]
            try:
                df = fdr.DataReader(t, start, end)
                if df is None or len(df) == 0:
                    cache[t] = None
                else:
                    cache[t] = df.index[0].strftime("%Y%m%d")
            except Exception:
                cache[t] = None
            p.advance(job)
            time.sleep(0.05)

    OUT.write_text(json.dumps(cache, ensure_ascii=False), encoding="utf-8")
    have = sum(1 for v in cache.values() if v)
    print(f"저장: {OUT} (총 {len(cache)}, 첫거래일 {have})")


if __name__ == "__main__":
    build()
