# -*- coding: utf-8 -*-
"""
어제 한국 주식시장 총평 → Claude Haiku 4.5 1회 호출.
호출은 Max 플랜 CLI 래퍼(`_claude_cli.py`)만 사용. Anthropic SDK / API 키 사용 안 함.
입력: strength.json + KOSPI/KOSDAQ 지수 등락
출력: data/market_overview.json
"""
import json
import sys
from pathlib import Path

import FinanceDataReader as fdr

from _claude_cli import call_claude_cli, ClaudeCLIError, extract_json_object

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

HERE = Path(__file__).parent
STRENGTH = HERE / "data" / "strength.json"
OUT = HERE / "data" / "market_overview.json"
MODEL = "claude-haiku-4-5"

SYSTEM_PROMPT = """당신은 한국 주식시장 일일 총평 작성자입니다.
주어진 통계만 근거로 어제 한국 시장의 흐름과 향후 전망을 작성합니다.

규칙:
- 주어진 데이터에 없는 사실 추가 금지. 추측·환각 금지.
- 강세/약세 섹터·종목의 패턴(테마, 업종 묶음)을 짚어주면 좋음.
- 객관적 톤. 매수·매도 직접 추천 금지.
- outlook(향후 전망)은 데이터에서 보이는 패턴(섹터 로테이션, 모멘텀 강도, 지수 방향)을 근거로 신중하게.
- 출력은 정확히 다음 JSON 형식. 다른 텍스트 금지. 코드블록(```) 사용 금지. `{` 로 시작하고 `}` 로 끝나야 합니다.
{"headline":"15자 이내 한줄 헤드라인","body":"어제 흐름 5~7문장","outlook":{"short_term":"1주일 단기 전망 3~4문장","mid_term":"1개월 중기 전망 3~4문장","watch_sectors":["주목할 섹터/테마 3~5개"],"risks":["주요 리스크 요인 2~3개"]}}"""


def _index(symbol: str) -> dict:
    df = fdr.DataReader(symbol).tail(2)
    if len(df) < 2:
        return {}
    today = df.iloc[-1]
    prev = df.iloc[-2]
    chg = (today["Close"] - prev["Close"]) / prev["Close"] * 100
    return {"close": float(today["Close"]), "change_pct": round(float(chg), 2)}


def main() -> None:
    s = json.loads(STRENGTH.read_text(encoding="utf-8"))
    kospi = _index("KS11")
    kosdaq = _index("KQ11")

    sectors_top = s.get("sectors_top", [])[:5]
    sectors_bot = s.get("sectors_bottom", [])[:5]
    stocks_top = s.get("stocks_top", [])[:5]
    stocks_bot = s.get("stocks_bottom", [])[:5]

    user_msg = (
        f"기준일: {s.get('ref_label')}\n"
        f"KOSPI {kospi.get('close', '-')}p ({kospi.get('change_pct', 0):+.2f}%)\n"
        f"KOSDAQ {kosdaq.get('close', '-')}p ({kosdaq.get('change_pct', 0):+.2f}%)\n\n"
        f"강세 업종 TOP 5 (시총가중 등락률):\n"
        + "\n".join(f"- {x['name']} {x['ret']:+.2f}% / 대표주: {', '.join(l['n'] for l in x.get('leaders', []))}" for x in sectors_top)
        + f"\n\n약세 업종 TOP 5:\n"
        + "\n".join(f"- {x['name']} {x['ret']:+.2f}%" for x in sectors_bot)
        + f"\n\n강세 종목 TOP 5:\n"
        + "\n".join(f"- {x['n']} {x['r']:+.2f}% ({x.get('i') or '-'})" for x in stocks_top)
        + f"\n\n약세 종목 TOP 5:\n"
        + "\n".join(f"- {x['n']} {x['r']:+.2f}% ({x.get('i') or '-'})" for x in stocks_bot)
    )

    try:
        raw = call_claude_cli(user_msg, system=SYSTEM_PROMPT, model=MODEL, timeout=300)
        parsed = extract_json_object(raw)
    except ClaudeCLIError as e:
        raise SystemExit(f"CLI 호출 실패: {e}")

    out = {
        "ref_date": s.get("ref_date"),
        "ref_label": s.get("ref_label"),
        "kospi": kospi,
        "kosdaq": kosdaq,
        **parsed,
    }
    OUT.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
    print(f"헤드라인: {parsed.get('headline', '')}")
    print(f"본문: {parsed.get('body', '')[:120]}...")
    print(f"저장: {OUT}")


if __name__ == "__main__":
    main()
