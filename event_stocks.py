# -*- coding: utf-8 -*-
"""
글로벌 학회·쇼·포럼 이벤트마다 관련 한국 종목 매핑 (키워드 → 79업종 매칭).
출력: data/calendar.json 의 각 학회/쇼 이벤트에 stocks 필드 추가.
"""
import json
import sys
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

HERE = Path(__file__).parent
CAL = HERE / "data" / "calendar.json"
SEARCH = HERE / "data" / "search_index.json"

EVENT_KEYWORDS = {
    "ASCO": ["제약", "바이오", "헬스케어"],
    "AHA": ["제약", "바이오", "헬스케어"],
    "AAD": ["제약", "바이오", "화장품"],
    "BIO International": ["제약", "바이오", "헬스케어"],
    "JPM Healthcare": ["제약", "바이오", "헬스케어"],
    "WWDC": ["반도체", "전자", "디스플레이", "소프트웨어"],
    "Apple iPhone": ["반도체", "전자", "디스플레이", "카메라"],
    "IFA": ["전자", "가전", "디스플레이"],
    "K-디스플레이": ["디스플레이", "반도체"],
    "CES": ["반도체", "전자", "자동차", "디스플레이", "AI"],
    "MWC": ["통신", "반도체", "전자"],
    "다보스": [],
}

TOP_PER_INDUSTRY = 8


def _match_keys(title: str) -> list[str]:
    out = []
    for k, inds in EVENT_KEYWORDS.items():
        if k in title:
            out.extend(inds)
    return list(dict.fromkeys(out))


def main() -> None:
    cal = json.loads(CAL.read_text(encoding="utf-8"))
    search = json.loads(SEARCH.read_text(encoding="utf-8"))
    stocks = search.get("stocks", [])

    matched_count = 0
    for ev in cal["events"]:
        if ev.get("type") not in ("학회", "쇼", "포럼"):
            continue
        kws = _match_keys(ev.get("title", ""))
        if not kws:
            continue
        related: list[dict] = []
        for kw in kws:
            ind_stocks = [s for s in stocks if kw in (s.get("i") or "")]
            ind_stocks.sort(key=lambda x: x.get("m", 0) or 0, reverse=True)
            for s in ind_stocks[:TOP_PER_INDUSTRY]:
                if any(r["t"] == s["t"] for r in related):
                    continue
                related.append({"t": s["t"], "n": s["n"], "i": s.get("i", ""), "m": s.get("m", 0)})
        related.sort(key=lambda x: x.get("m", 0), reverse=True)
        ev["stocks"] = related[:20]
        matched_count += 1

    CAL.write_text(json.dumps(cal, ensure_ascii=False), encoding="utf-8")
    print(f"이벤트 종목 매핑: {matched_count}건 갱신")


if __name__ == "__main__":
    main()
