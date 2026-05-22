# -*- coding: utf-8 -*-
"""
DART OpenAPI에서 영업활동현금흐름·CAPEX 수집 → FCF·PFR 산출.
대상: KRX 전체 보통주 중 시총 ≥ MARCAP_MIN 인 종목 (DART corp_code 매핑된 것만).
출력:
  data/cashflow.csv  (ticker,name,sector,fy,ocf,capex,fcf,marcap,pfr)
  data/corp_code_map.json (캐시; stock_code → corp_code)

PFR = 시가총액 / FCF
FCF = OCF - 유형자산취득 - 무형자산취득

sector 컬럼: SECTORS dict 에 등록된 종목은 해당 섹터, 외 종목은 "기타"
"""
import io
import json
import os
import sys
import time
import zipfile
from datetime import datetime
from pathlib import Path
from xml.etree import ElementTree as ET

import FinanceDataReader as fdr
import pandas as pd
import requests
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeRemainingColumn

from sectors import SECTORS

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

HERE = Path(__file__).parent
DATA = HERE / "data"
CF_OUT = DATA / "cashflow.csv"
CORP_MAP = DATA / "corp_code_map.json"

DART_BASE = "https://opendart.fss.or.kr/api"
MARCAP_MIN = 500 * 1e8  # 500억 이상 (2026-05-21 확장: 1,000억 → 500억)

# DART 표준 IFRS 계정 ID
ACC_OCF = "ifrs-full_CashFlowsFromUsedInOperatingActivities"
ACC_CAPEX_PPE = "ifrs-full_PurchaseOfPropertyPlantAndEquipmentClassifiedAsInvestingActivities"
ACC_CAPEX_INT = "ifrs-full_PurchaseOfIntangibleAssetsClassifiedAsInvestingActivities"


def _api_key() -> str:
    # .env 로드 (간단 파서 — 외부 의존 없음)
    env = HERE / ".env"
    if env.exists():
        for line in env.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line.startswith("DART_API_KEY="):
                return line.split("=", 1)[1].strip()
    key = os.environ.get("DART_API_KEY", "").strip()
    if not key:
        raise RuntimeError("DART_API_KEY 가 .env 또는 환경변수에 없습니다.")
    return key


def _latest_fy() -> int:
    """현재 시점 기준 가장 최근 공시된 사업보고서 연도."""
    # 사업보고서는 사업연도 종료 후 90일 이내 → 12월 결산기준 4월 이후 가능
    today = datetime.now()
    return today.year - 1 if today.month >= 4 else today.year - 2


def load_corp_map(key: str, force: bool = False) -> dict[str, str]:
    """stock_code(6자리) → corp_code(8자리). 캐시 7일."""
    if not force and CORP_MAP.exists():
        age = time.time() - CORP_MAP.stat().st_mtime
        if age < 7 * 86400:
            return json.loads(CORP_MAP.read_text(encoding="utf-8"))

    print("DART corp code map 다운로드 중...")
    r = requests.get(f"{DART_BASE}/corpCode.xml?crtfc_key={key}", timeout=30)
    r.raise_for_status()
    z = zipfile.ZipFile(io.BytesIO(r.content))
    xml_bytes = z.read(z.namelist()[0])
    root = ET.fromstring(xml_bytes)
    mapping: dict[str, str] = {}
    for e in root.iter("list"):
        sc = (e.findtext("stock_code") or "").strip()
        cc = (e.findtext("corp_code") or "").strip()
        if sc and cc and len(sc) == 6:
            # 동일 stock_code 중복 시 첫 번째 유지
            mapping.setdefault(sc, cc)
    DATA.mkdir(parents=True, exist_ok=True)
    CORP_MAP.write_text(json.dumps(mapping, ensure_ascii=False), encoding="utf-8")
    print(f"  매핑 {len(mapping):,}건 저장: {CORP_MAP}")
    return mapping


def fetch_cf(key: str, corp_code: str, year: int) -> tuple[float | None, float | None, float | None, float | None]:
    """(ocf, capex_ppe, capex_int, depr_amort). fs_div CFS 우선, 없으면 OFS 폴백.
    depr_amort: CF 표의 감가상각비·무형자산상각비 합계 (account_nm 키워드 매칭, 중복 제거)."""
    for fs in ("CFS", "OFS"):
        url = f"{DART_BASE}/fnlttSinglAcntAll.json"
        params = {
            "crtfc_key": key,
            "corp_code": corp_code,
            "bsns_year": str(year),
            "reprt_code": "11011",  # 사업보고서
            "fs_div": fs,
        }
        try:
            r = requests.get(url, params=params, timeout=15)
            d = r.json()
        except Exception:
            return None, None, None, None
        if d.get("status") != "000":
            continue
        items = d.get("list", [])
        if not items:
            continue
        ocf = capex_ppe = capex_int = None
        depr_vals: list[float] = []
        depr_seen_nm: set[str] = set()
        for it in items:
            if it.get("sj_div") != "CF":
                continue
            aid = it.get("account_id", "")
            nm = (it.get("account_nm") or "").strip()
            amt = it.get("thstrm_amount", "").replace(",", "").strip()
            if not amt or amt in ("-", ""):
                continue
            try:
                val = float(amt)
            except ValueError:
                continue
            if aid == ACC_OCF:
                ocf = val
            elif aid == ACC_CAPEX_PPE:
                capex_ppe = val
            elif aid == ACC_CAPEX_INT:
                capex_int = val
            else:
                # 감가상각 / 상각비 항목 (CF 표의 가산 항목): account_nm 키워드 매칭
                nm_l = nm.lower()
                if (("감가상각" in nm or "상각비" in nm or "depreciat" in nm_l or "amortis" in nm_l)
                        and nm not in depr_seen_nm):
                    depr_vals.append(abs(val))  # CF 표 가산 항목은 양수
                    depr_seen_nm.add(nm)
        if ocf is not None:
            depr = sum(depr_vals) if depr_vals else None
            return ocf, capex_ppe, capex_int, depr
    return None, None, None, None


def main() -> None:
    key = _api_key()
    fy = _latest_fy()
    print(f"기준 사업연도: {fy}")

    corp_map = load_corp_map(key)
    listing = fdr.StockListing("KRX").drop_duplicates(subset=["Code"]).set_index("Code")

    # 추적 종목(SECTORS)은 sector 라벨 부여, 외 종목은 "기타"
    sector_of: dict[str, str] = {}
    for s, items in SECTORS.items():
        for t, _ in items:
            sector_of.setdefault(t, s)

    # 대상 풀: DART 매핑 + 시총 ≥ MARCAP_MIN
    pool: list[tuple[str, str, str, float]] = []  # (ticker, name, sector, marcap)
    for ticker, row in listing.iterrows():
        if ticker not in corp_map:
            continue
        m = row.get("Marcap")
        if not pd.notna(m) or m < MARCAP_MIN:
            continue
        # 추적 중이면 SECTORS 의 이름·섹터 유지, 외 종목은 listing 의 Name·"기타"
        if ticker in sector_of:
            # SECTORS dict 에서 이름 찾기
            nm = next((n for s_items in SECTORS.values() for t, n in s_items if t == ticker), row.get("Name", ticker))
            pool.append((ticker, nm, sector_of[ticker], float(m)))
        else:
            nm = row.get("Name", ticker)
            pool.append((ticker, nm, "기타", float(m)))

    print(f"대상 풀: {len(pool)}종목 (시총 ≥ {int(MARCAP_MIN/1e8)}억, DART 매핑 있음)")

    rows: list[dict] = []
    failed: list[tuple[str, str, str]] = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[bold]{task.description}[/bold]"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeRemainingColumn(),
        transient=True,
    ) as p:
        job = p.add_task("DART 현금흐름 수집", total=len(pool))
        for ticker, name, sector, marcap in pool:
            corp_code = corp_map.get(ticker)
            ocf, ppe, intang, depr = fetch_cf(key, corp_code, fy)
            if ocf is None:
                failed.append((sector, ticker, name))
                p.advance(job)
                time.sleep(0.05)
                continue
            capex = (ppe or 0) + (intang or 0)
            fcf = ocf - capex
            pfr = (marcap / fcf) if (marcap and fcf and fcf > 0) else None

            rows.append({
                "ticker": ticker,
                "name": name,
                "sector": sector,
                "fy": fy,
                "ocf": ocf,
                "capex_ppe": ppe,
                "capex_intangible": intang,
                "capex": capex,
                "fcf": fcf,
                "depr": depr,  # 감가상각비 (CF 표 가산 합계)
                "marcap": marcap,
                "pfr": round(pfr, 2) if pfr else None,
            })
            p.advance(job)
            time.sleep(0.05)  # rate limit (분당 1000)

    df = pd.DataFrame(rows)
    DATA.mkdir(parents=True, exist_ok=True)
    df.to_csv(CF_OUT, index=False, encoding="utf-8-sig")
    print(f"저장: {CF_OUT}  행수: {len(df):,}")
    if failed:
        print(f"실패 {len(failed)}개 (대표): {', '.join(f'{t}/{n}' for _, t, n in failed[:10])}"
              + ("..." if len(failed) > 10 else ""))

    # 요약: PFR 분포
    valid = df[df["pfr"].notna()]
    if len(valid):
        top5 = valid.nsmallest(5, "pfr")[["name", "pfr"]]
        top5_str = ", ".join(f"{r['name']}({r['pfr']:.1f})" for _, r in top5.iterrows())
        print(f"PFR 유효 {len(valid)}/{len(df)}, 중앙값 {valid['pfr'].median():.1f}, 저평가 Top5: {top5_str}")


if __name__ == "__main__":
    main()
