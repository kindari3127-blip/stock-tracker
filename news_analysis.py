# -*- coding: utf-8 -*-
"""
강세/약세 TOP 10 종목에 대해 네이버 종목 뉴스 → Claude Haiku 4.5 분석.
규칙(메모리): 비용 발생 작업은 1회 시도. 실패 시 해당 종목만 비우고 진행.
출력: data/news_analysis.json
"""
import json
import os
import re
import sys
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

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
STRENGTH = HERE / "data" / "strength.json"
OUT = HERE / "data" / "news_analysis.json"
UA = {"User-Agent": "Mozilla/5.0", "Referer": "https://finance.naver.com/"}

MODEL = "claude-haiku-4-5-20251001"
NEWS_URL = "https://finance.naver.com/item/news_news.naver?code={t}&page=1"
NEWS_PER_TICKER = 5

SYSTEM_PROMPT = """당신은 한국 주식 분석가입니다. 종목의 등락률과 최근 뉴스 헤드라인을 보고 강세/약세 이유를 추정합니다.

규칙:
- 뉴스 헤드라인에 명확히 근거가 있는 것만 원인으로 적습니다. 추측이나 일반론 금지.
- 근거가 약하면 reasons는 비웁니다 (빈 배열). 절대 지어내지 마세요.
- 출력은 정확히 다음 JSON 형식만 (다른 텍스트 절대 금지):
{"summary":"15자 이내 한줄 요약","reasons":[{"text":"구체 원인 1문장","source":"근거 뉴스 제목"}]}
- reasons는 0~4개. 모든 값은 한국어."""


def fetch_news(ticker: str) -> list[dict]:
    """네이버 종목 뉴스 page 1 → 최근 5건 헤드라인."""
    r = requests.get(NEWS_URL.format(t=ticker), headers=UA, timeout=10)
    soup = BeautifulSoup(r.content, "lxml")
    rows: list[dict] = []
    for tr in soup.select("tr"):
        a = tr.select_one("td.title a")
        date_td = tr.select_one("td.date")
        info_td = tr.select_one("td.info")
        if not a or not date_td:
            continue
        title = a.get_text(strip=True)
        if not title:
            continue
        rows.append({
            "title": title,
            "date": date_td.get_text(strip=True),
            "press": info_td.get_text(strip=True) if info_td else "",
        })
        if len(rows) >= NEWS_PER_TICKER:
            break
    return rows


def call_claude(client, name: str, ticker: str, ret: float, news: list[dict]) -> dict:
    user_msg = (
        f"종목: {name} ({ticker})\n"
        f"등락률: {ret:+.2f}%\n"
        f"최근 뉴스:\n"
        + "\n".join(f"- [{n['date']}] {n['title']} ({n['press']})" for n in news)
    )
    resp = client.messages.create(
        model=MODEL,
        max_tokens=500,
        system=[{"type": "text", "text": SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}],
        messages=[
            {"role": "user", "content": user_msg},
            {"role": "assistant", "content": "{"},
        ],
    )
    raw = "{" + resp.content[0].text
    raw = raw.strip()
    end = raw.rfind("}")
    if end > 0:
        raw = raw[: end + 1]
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}", raw, re.S)
        if m:
            return json.loads(m.group(0))
        raise


def main() -> None:
    if not STRENGTH.exists():
        raise SystemExit("strength.json 없음. build_data.py 먼저 실행.")
    if not os.getenv("ANTHROPIC_API_KEY"):
        raise SystemExit("ANTHROPIC_API_KEY 환경변수 없음. .env에 키 추가 필요.")

    s = json.loads(STRENGTH.read_text(encoding="utf-8"))
    targets = (s.get("stocks_top") or []) + (s.get("stocks_bottom") or [])
    uniq: list[dict] = []
    seen: set[str] = set()
    for x in targets:
        t = x["t"]
        if t in seen:
            continue
        seen.add(t)
        uniq.append(x)

    print(f"분석 대상: {len(uniq)}종목")
    client = anthropic.Anthropic()
    items: dict[str, dict] = {}

    for i, st in enumerate(uniq, 1):
        t, n, r = st["t"], st["n"], st["r"]
        record: dict = {"name": n, "ret": r, "news": []}
        try:
            news = fetch_news(t)
            record["news"] = news
            time.sleep(0.25)
            if not news:
                record["summary"] = "관련 뉴스 없음"
                record["reasons"] = []
                items[t] = record
                print(f"  [{i:2}/{len(uniq)}] {n:12} 뉴스 없음")
                continue
            parsed = call_claude(client, n, t, r, news)
            record["summary"] = parsed.get("summary", "")
            record["reasons"] = parsed.get("reasons", [])
            items[t] = record
            print(f"  [{i:2}/{len(uniq)}] {n:12} {record['summary']}")
        except Exception as e:
            record["error"] = str(e)[:200]
            items[t] = record
            print(f"  [{i:2}/{len(uniq)}] {n:12} 실패: {str(e)[:80]}")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(
        json.dumps({"ref_date": s.get("ref_date"), "items": items}, ensure_ascii=False),
        encoding="utf-8",
    )
    ok = sum(1 for v in items.values() if "summary" in v and "error" not in v)
    print(f"저장: {OUT} (성공 {ok}/{len(uniq)})")


if __name__ == "__main__":
    main()
