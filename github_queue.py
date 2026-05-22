# -*- coding: utf-8 -*-
"""
GitHub Issues에서 [리포트요청] open issue를 읽어 ticker 추출.
처리 후 issue close. data/report_queue.json에 합류.
"""
import json
import os
import re
import sys
from pathlib import Path

import requests

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

HERE = Path(__file__).parent
QUEUE = HERE / "data" / "report_queue.json"
REPO = "kindari3127-blip/stock-tracker"


def _token() -> str | None:
    return os.getenv("GITHUB_TOKEN") or os.getenv("GH_TOKEN")


def _headers(pat: str) -> dict:
    return {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {pat}",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def fetch_open() -> list[tuple[str, int]]:
    pat = _token()
    if not pat:
        return []
    try:
        r = requests.get(
            f"https://api.github.com/repos/{REPO}/issues",
            params={"state": "open", "labels": "report-request", "per_page": 50},
            headers=_headers(pat),
            timeout=15,
        )
    except Exception as e:
        print(f"[github] fetch failed: {e}")
        return []
    if not r.ok:
        print(f"[github] {r.status_code} {r.text[:200]}")
        return []
    out: list[tuple[str, int]] = []
    for iss in r.json():
        title = iss.get("title", "")
        if not title.startswith("[리포트요청]"):
            continue
        m = re.search(r"\b(\d{6})\b", title)
        if m:
            out.append((m.group(1), iss["number"]))
    return out


def close_issue(num: int) -> bool:
    pat = _token()
    if not pat:
        return False
    try:
        r = requests.patch(
            f"https://api.github.com/repos/{REPO}/issues/{num}",
            json={"state": "closed", "state_reason": "completed"},
            headers=_headers(pat),
            timeout=15,
        )
        return r.ok
    except Exception:
        return False


def main() -> None:
    if not _token():
        print("GITHUB_TOKEN/GH_TOKEN 없음 — 건너뜀")
        return
    pairs = fetch_open()
    if not pairs:
        print("open report-request issues 없음")
        return

    existing: list[str] = []
    if QUEUE.exists():
        try:
            existing = json.loads(QUEUE.read_text(encoding="utf-8"))
        except Exception:
            existing = []

    added = 0
    for t, _num in pairs:
        if t not in existing:
            existing.append(t)
            added += 1
    QUEUE.write_text(json.dumps(existing, ensure_ascii=False), encoding="utf-8")
    print(f"GitHub queue 합류: {added}건 (전체 큐 {len(existing)})")

    closed = 0
    for _t, num in pairs:
        if close_issue(num):
            closed += 1
    print(f"Issue close: {closed}/{len(pairs)}")


if __name__ == "__main__":
    main()
