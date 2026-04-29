# -*- coding: utf-8 -*-
"""
일일 기업 리포트 5개 (Claude Haiku).
선정: 추천 TOP 20 중 가치판단 점수 상위 5종목 (중복 제거).
입력: 펀더멘털 + 가치판단 + 추천 점수 + 최근 뉴스 + 시장 총평
출력: data/daily_reports.json
"""
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

import anthropic

HERE = Path(__file__).parent
RECO = HERE / "data" / "recommend.json"
VAL = HERE / "data" / "valuation.json"
CATS = HERE / "data" / "categories.json"
SEARCH = HERE / "data" / "search_index.json"
WATCHLIST = HERE / "data" / "watchlist.json"
QUEUE = HERE / "data" / "report_queue.json"
NEWS = HERE / "data" / "news_analysis.json"
OVERVIEW = HERE / "data" / "market_overview.json"
OUT = HERE / "data" / "daily_reports.json"
MODEL = "claude-haiku-4-5-20251001"
CACHE_DAYS = 7

SYSTEM_PROMPT = """당신은 증권사 리서치 애널리스트입니다.
주어진 펀더멘털·가치판단·추천 점수·최근 뉴스·시장 컨텍스트만을 근거로 일일 기업 리포트를 작성합니다.

작성 규칙:
- 데이터에 없는 사실 추가 금지. 추측·환각 절대 금지.
- 매수·매도 추천 금지. 사실 기반 분석만.
- 5개 섹션을 각 2~3 문장으로.
- 출력은 정확히 다음 JSON 형식만 (다른 텍스트 금지):
{"headline":"20자 이내 한줄 헤드라인","sections":{"value":"…","earnings":"…","products":"…","momentum":"…","outlook":"…"}}

섹션 의미:
- value: 가치관/투자방향 (현재 밸류에이션 수준, 동종업계 대비 위치)
- earnings: 이익창출능력 (ROE, 영업이익률, EPS 추세)
- products: 주력상품/매출 동향 (매출 규모, 성장률, 뉴스에 언급된 사업)
- momentum: 모멘텀/시장 위치 (등락률, 추천 점수 분해, 강세 이유)
- outlook: 향후 전망 (펀더멘털·시장 컨텍스트 기반 3~6개월)"""


def _make_item(ticker: str, name: str, val_items: dict, search_items: dict, reco_map: dict, source: str) -> dict:
    live = search_items.get(ticker, {})
    v = val_items.get(ticker) or {}
    r = reco_map.get(ticker) or {
        "t": ticker, "n": name or live.get("n", ""), "i": live.get("i", ""),
        "c": live.get("c"), "r": live.get("r"),
    }
    return {"reco": r, "val": v, "source": source}


def _select() -> list[dict]:
    reco = json.loads(RECO.read_text(encoding="utf-8"))
    val = json.loads(VAL.read_text(encoding="utf-8"))
    val_items = val.get("items", {})
    cats = json.loads(CATS.read_text(encoding="utf-8")) if CATS.exists() else {}
    search = json.loads(SEARCH.read_text(encoding="utf-8")) if SEARCH.exists() else {}
    search_items = {s["t"]: s for s in search.get("stocks", [])}

    reco_top = reco.get("top", [])
    reco_map = {r["t"]: r for r in reco_top}

    pool: list[dict] = []
    seen: set[str] = set()

    for r in reco_top[:10]:
        t = r["t"]
        if t in seen:
            continue
        seen.add(t)
        v = val_items.get(t) or {}
        pool.append({"reco": r, "val": v, "source": "추천 TOP"})

    for cat_key, src_label in [("value", "저평가"), ("quality", "실적우량")]:
        for s in (cats.get(cat_key) or [])[:5]:
            t = s["t"]
            if t in seen:
                continue
            seen.add(t)
            pool.append(_make_item(t, s.get("n", ""), val_items, search_items, reco_map, src_label))

    if WATCHLIST.exists():
        try:
            wl = json.loads(WATCHLIST.read_text(encoding="utf-8"))
            for t in wl:
                t = str(t).zfill(6)
                if t in seen:
                    continue
                seen.add(t)
                pool.append(_make_item(t, "", val_items, search_items, reco_map, "워치리스트"))
        except Exception:
            pass

    if QUEUE.exists():
        try:
            q = json.loads(QUEUE.read_text(encoding="utf-8"))
            for t in q:
                t = str(t).zfill(6)
                if t in seen:
                    continue
                seen.add(t)
                pool.append(_make_item(t, "", val_items, search_items, reco_map, "사용자 요청"))
        except Exception:
            pass

    return pool


def _load_cache() -> dict:
    if not OUT.exists():
        return {}
    try:
        old = json.loads(OUT.read_text(encoding="utf-8"))
        items: dict = {}
        for r in old.get("reports", []):
            if r.get("t") and r.get("headline"):
                items[r["t"]] = r
        return items
    except Exception:
        return {}


def _is_fresh(rep: dict) -> bool:
    ts = rep.get("generated_at")
    if not ts:
        return False
    try:
        dt = datetime.fromisoformat(ts)
    except Exception:
        return False
    return (datetime.now() - dt).days < CACHE_DAYS


def _user_msg(item: dict, overview: dict, news_items: dict) -> str:
    r = item["reco"]
    v = item["val"]
    t = r["t"]
    parts = [
        f"=== 종목: {r['n']} ({t}) ===",
        f"업종: {r.get('i', '-')}",
        f"종가: {r.get('c', '-')}원 / 등락률: {r.get('r', '-')}%",
        "",
        "[펀더멘털 / 가치판단]",
    ]
    m = v.get("metrics", {})
    cmp_ = v.get("compare", {})
    med = v.get("industry_med", {})
    parts.append(f"  PER {m.get('per')} (업종 중앙 {med.get('per')}, {cmp_.get('per', {}).get('label', '')})")
    parts.append(f"  PBR {m.get('pbr')} (업종 중앙 {med.get('pbr')}, {cmp_.get('pbr', {}).get('label', '')})")
    parts.append(f"  PSR {m.get('psr')} (업종 중앙 {med.get('psr')}, {cmp_.get('psr', {}).get('label', '')})")
    parts.append(f"  ROE {m.get('roe')}% (업종 중앙 {med.get('roe')}, {cmp_.get('roe', {}).get('label', '')})")
    parts.append(f"  PEG {m.get('peg')} / EPS 성장 {m.get('eps_growth')}% / 영업이익률 {m.get('op_margin')}%")
    parts.append(f"  종합 가치점수: {v.get('composite')} ({v.get('verdict')})")
    parts.append("")

    parts.append("[추천 점수 분해]")
    sc = r.get("scores", {})
    me = r.get("metrics", {})
    parts.append(f"  종합 {r.get('total')} = 모멘텀 {sc.get('momentum')} / 실적 {sc.get('fundamental')} / 차트 {sc.get('chart')} / 순환 {sc.get('rotation')}")
    parts.append(f"  5일 {me.get('ret_5d')}% / 20일 {me.get('ret_20d')}% / 60일 고가대비 {me.get('pos_in_range')}% / 거래량비 {me.get('vol_ratio')}")
    parts.append("")

    n = (news_items or {}).get(t)
    if n and n.get("news"):
        parts.append("[최근 뉴스 헤드라인]")
        for x in n["news"][:5]:
            parts.append(f"  - [{x.get('date', '')}] {x.get('title', '')}")
        if n.get("summary"):
            parts.append(f"  요약: {n['summary']}")

    if overview:
        parts.append("")
        parts.append("[시장 컨텍스트]")
        parts.append(f"  {overview.get('headline', '')}")

    return "\n".join(parts)


def main() -> None:
    if not os.getenv("ANTHROPIC_API_KEY"):
        raise SystemExit("ANTHROPIC_API_KEY 없음.")
    if not RECO.exists() or not VAL.exists():
        raise SystemExit("recommend.json 또는 valuation.json 없음. 먼저 빌드.")

    overview = json.loads(OVERVIEW.read_text(encoding="utf-8")) if OVERVIEW.exists() else {}
    news = json.loads(NEWS.read_text(encoding="utf-8")) if NEWS.exists() else {}
    news_items = news.get("items", {})

    selected = _select()
    cache = _load_cache()
    print(f"풀 {len(selected)}종목 / 캐시 보유 {len(cache)}종목")

    todo: list[dict] = []
    reuse: list[dict] = []
    for item in selected:
        t = item["reco"]["t"]
        cached = cache.get(t)
        if cached and _is_fresh(cached):
            cached_copy = dict(cached)
            cached_copy["source"] = item.get("source") or cached.get("source")
            reuse.append(cached_copy)
        else:
            todo.append(item)
    print(f"신규 호출: {len(todo)} / 캐시 재사용: {len(reuse)}")

    client = anthropic.Anthropic() if todo else None
    new_reports: list[dict] = []
    for i, item in enumerate(todo, 1):
        r = item["reco"]
        try:
            user_msg = _user_msg(item, overview, news_items)
            resp = client.messages.create(
                model=MODEL,
                max_tokens=1500,
                system=[{"type": "text", "text": SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}],
                messages=[
                    {"role": "user", "content": user_msg},
                    {"role": "assistant", "content": "{"},
                ],
            )
            raw = "{" + resp.content[0].text
            end = raw.rfind("}")
            if end > 0:
                raw = raw[: end + 1]
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                m = re.search(r"\{.*\}", raw, re.S)
                parsed = json.loads(m.group(0)) if m else {}
            new_reports.append({
                "t": r["t"], "n": r["n"], "i": r.get("i", ""),
                "c": r.get("c"), "r": r.get("r"),
                "composite": item["val"].get("composite"),
                "verdict": item["val"].get("verdict"),
                "headline": parsed.get("headline", ""),
                "sections": parsed.get("sections", {}),
                "source": item.get("source"),
                "generated_at": datetime.now().isoformat(),
            })
            print(f"  [{i}/{len(todo)}] {r['n']:14} {parsed.get('headline', '')}")
        except Exception as e:
            new_reports.append({"t": r["t"], "n": r["n"], "error": str(e)[:200], "source": item.get("source")})
            print(f"  [{i}/{len(todo)}] {r['n']:14} 실패: {str(e)[:80]}")

    reports = reuse + new_reports
    reports.sort(key=lambda x: (0 if "추천" in (x.get("source") or "") else 1, x.get("t", "")))

    OUT.write_text(json.dumps({
        "ref_date": json.loads(RECO.read_text(encoding="utf-8")).get("ref_date"),
        "cache_days": CACHE_DAYS,
        "reports": reports,
    }, ensure_ascii=False), encoding="utf-8")
    ok = sum(1 for r in reports if r.get("headline"))
    print(f"저장: {OUT} (성공 {ok}/{len(reports)} · 신규 {len(new_reports)} · 재사용 {len(reuse)})")

    if QUEUE.exists():
        try:
            QUEUE.write_text("[]\n", encoding="utf-8")
        except Exception:
            pass


if __name__ == "__main__":
    main()
