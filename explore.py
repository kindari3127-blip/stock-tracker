# -*- coding: utf-8 -*-
"""
네이버 업종 기반 종목 탐색기.

사용법:
    python explore.py 반도체             # '반도체' 포함 업종 상위 20개
    python explore.py 반도체 30          # 상위 30개
    python explore.py                    # 전체 업종 목록만 출력
"""
import os
import re
import sys
import time
import webbrowser
from datetime import datetime
from pathlib import Path

import FinanceDataReader as fdr
import pandas as pd
import requests
from bs4 import BeautifulSoup
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

from fundamentals import scrape as scrape_fundamentals

HERE = Path(__file__).parent
OUT = HERE / "explore.html"
console = Console()

LIST_URL = "https://finance.naver.com/sise/sise_group.naver?type=upjong"
DETAIL_URL = "https://finance.naver.com/sise/sise_group_detail.naver?type=upjong&no={no}"
UA = {"User-Agent": "Mozilla/5.0"}


def fetch_industries() -> dict[str, str]:
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


def fetch_industry_stocks(no: str) -> pd.DataFrame:
    r = requests.get(DETAIL_URL.format(no=no), headers=UA, timeout=10)
    soup = BeautifulSoup(r.content, "lxml")
    rows = []
    for tbl in soup.select("table"):
        ths = [th.get_text(strip=True) for th in tbl.select("thead th")]
        if not ths or "종목명" not in ths:
            continue
        for tr in tbl.select("tbody tr"):
            tds = tr.select("td")
            if len(tds) < 4:
                continue
            link = tr.select_one("a[href*='code=']")
            if not link:
                continue
            m = re.search(r"code=(\d+)", link.get("href") or "")
            if not m:
                continue
            code = m.group(1)
            name = link.get_text(strip=True)
            close_s = tds[1].get_text(strip=True).replace(",", "")
            chg_s = tds[3].get_text(strip=True).replace("%", "").replace("+", "")
            try:
                close = int(close_s) if close_s else None
            except ValueError:
                close = None
            try:
                chg = float(chg_s)
            except ValueError:
                chg = None
            rows.append({"ticker": code, "name": name, "close": close, "change_pct": chg})
        break
    return pd.DataFrame(rows)


def _latest_per(fund_rows: list[dict]) -> dict[str, float | None]:
    """fundamentals.scrape 결과에서 대표 지표를 뽑음: 연간 Trailing PER/PBR/EPS/ROE + Forward PER."""
    out = {"per": None, "pbr": None, "eps": None, "roe": None, "per_est": None, "eps_est": None}
    if not fund_rows:
        return out
    df = pd.DataFrame(fund_rows)
    df["pkey"] = df["period"].astype(str).str.replace(".", "", regex=False).astype(int)
    annual = df[df["period_type"] == "annual"].sort_values("pkey")
    actual = annual[annual["is_estimate"] == 0]
    est = annual[annual["is_estimate"] == 1]
    def last(sub, metric):
        s = sub[sub["metric"] == metric]
        return float(s.iloc[-1]["value"]) if len(s) and pd.notna(s.iloc[-1]["value"]) else None
    out["per"] = last(actual, "per")
    out["pbr"] = last(actual, "pbr")
    out["eps"] = last(actual, "eps")
    out["roe"] = last(actual, "roe")
    out["per_est"] = last(est, "per")
    out["eps_est"] = last(est, "eps")
    return out


def _fmt_n(v) -> str:
    return f"{int(v):,}" if v is not None and pd.notna(v) else "-"


def _fmt_r(v) -> str:
    return f"{v:.2f}" if v is not None and pd.notna(v) else "-"


def _fmt_pct(v) -> str:
    return f"{v:+.2f}%" if v is not None and pd.notna(v) else "-"


def _fmt_cap(v) -> str:
    if v is None or pd.isna(v):
        return "-"
    v = float(v)
    if v >= 1e12:
        return f"{v/1e12:,.1f}조"
    if v >= 1e8:
        return f"{v/1e8:,.0f}억"
    return f"{v:,.0f}"


def pick_industry(kw: str, ind: dict[str, str]) -> tuple[str, str] | None:
    # 정확 일치 최우선
    if kw in ind:
        return kw, ind[kw]
    matches = [(k, v) for k, v in ind.items() if kw in k]
    if not matches:
        return None
    if len(matches) == 1:
        return matches[0]
    console.print(f"[yellow]'{kw}' 매칭 업종 {len(matches)}개:[/]")
    for i, (k, _) in enumerate(matches, 1):
        console.print(f"  {i}) {k}")
    console.print(f"[dim]더 구체적인 키워드로 재실행하세요.[/]")
    return None


def render_console(industry: str, df: pd.DataFrame) -> None:
    up = int((df["change_pct"] > 0).sum())
    dn = int((df["change_pct"] < 0).sum())
    header = Text()
    header.append(f"업종: {industry}  ", style="bold")
    header.append(f"{len(df)}종목   ", style="dim")
    header.append(f"▲ {up}  ", style="red")
    header.append(f"▼ {dn}", style="blue")
    console.print(Panel(header, box=box.DOUBLE, border_style="bright_white"))

    t = Table(box=box.ROUNDED, show_header=True, header_style="bold dim", expand=False)
    t.add_column("#", justify="right", style="dim")
    t.add_column("종목", justify="left", no_wrap=True)
    t.add_column("코드", justify="center", style="dim")
    t.add_column("종가", justify="right")
    t.add_column("등락률", justify="right")
    t.add_column("시총", justify="right")
    t.add_column("PER", justify="right")
    t.add_column("Fwd PER", justify="right")
    t.add_column("PBR", justify="right")
    t.add_column("ROE%", justify="right")
    for i, r in enumerate(df.itertuples(index=False), 1):
        chg = r.change_pct
        if chg is None or pd.isna(chg):
            chg_str = "-"
        elif chg > 0:
            chg_str = f"[red]▲ +{chg:.2f}%[/red]"
        elif chg < 0:
            chg_str = f"[blue]▼ {chg:.2f}%[/blue]"
        else:
            chg_str = "·"
        t.add_row(
            str(i),
            r.name,
            r.ticker,
            _fmt_n(r.close),
            chg_str,
            _fmt_cap(r.marcap),
            _fmt_r(r.per),
            f"[magenta]{_fmt_r(r.per_est)}[/magenta]" if r.per_est is not None else "-",
            _fmt_r(r.pbr),
            _fmt_r(r.roe),
        )
    console.print(t)


def render_html(industry: str, df: pd.DataFrame) -> Path:
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    up = int((df["change_pct"] > 0).sum())
    dn = int((df["change_pct"] < 0).sum())
    rows = []
    for i, r in enumerate(df.itertuples(index=False), 1):
        chg = r.change_pct
        chg_cls = "flat"
        chg_str = "-"
        if chg is not None and pd.notna(chg):
            if chg > 0: chg_cls, chg_str = "up", f"▲ +{chg:.2f}%"
            elif chg < 0: chg_cls, chg_str = "down", f"▼ {chg:.2f}%"
            else: chg_cls, chg_str = "flat", "· 0.00%"
        per_est_html = (f'<span class="est">{_fmt_r(r.per_est)}E</span>'
                        if r.per_est is not None and pd.notna(r.per_est) else "-")
        eps_est_html = (f'<span class="est">{_fmt_n(r.eps_est)}E</span>'
                        if r.eps_est is not None and pd.notna(r.eps_est) else "-")
        rows.append(
            f'<tr><td class="idx">{i}</td>'
            f'<td class="name">{r.name}</td>'
            f'<td class="code">{r.ticker}</td>'
            f'<td class="n">{_fmt_n(r.close)}</td>'
            f'<td class="chg {chg_cls}">{chg_str}</td>'
            f'<td class="n">{_fmt_cap(r.marcap)}</td>'
            f'<td class="n">{_fmt_r(r.per)}</td>'
            f'<td class="n">{per_est_html}</td>'
            f'<td class="n">{_fmt_r(r.pbr)}</td>'
            f'<td class="n">{_fmt_n(r.eps)}</td>'
            f'<td class="n">{eps_est_html}</td>'
            f'<td class="n">{_fmt_r(r.roe)}</td></tr>'
        )
    html = f"""<!DOCTYPE html><html lang="ko"><head><meta charset="utf-8">
<title>업종 탐색 · {industry}</title>
<style>
body{{font-family:system-ui,-apple-system,Segoe UI,"Malgun Gothic",sans-serif;margin:0;background:#eef1f5;color:#1a202c}}
header{{background:linear-gradient(135deg,#1a202c,#2d3748);color:#f7fafc;padding:14px 28px;display:flex;gap:20px;align-items:center;flex-wrap:wrap}}
header h1{{margin:0;font-size:18px}}
header .meta{{opacity:.75;font-size:13px}}
header .stat{{margin-left:auto;display:flex;gap:12px;font-weight:600}}
header .up{{color:#fc8181}} header .down{{color:#63b3ed}}
main{{padding:20px;max-width:1400px;margin:0 auto}}
.card{{background:white;border-radius:12px;padding:20px;box-shadow:0 1px 3px rgba(0,0,0,.08)}}
table{{width:100%;border-collapse:collapse;font-size:13px}}
th{{text-align:left;background:#f7fafc;color:#4a5568;font-size:11px;padding:8px;letter-spacing:.05em;text-transform:uppercase;border-bottom:1px solid #e2e8f0}}
td{{padding:10px 8px;border-bottom:1px solid #f1f5f9}}
td.idx{{color:#a0aec0;text-align:right;width:34px}}
td.name{{font-weight:600}}
td.code{{font-family:ui-monospace,Consolas,monospace;color:#a0aec0;font-size:12px}}
td.n{{text-align:right;font-variant-numeric:tabular-nums}}
td.chg{{text-align:right;font-weight:600;font-variant-numeric:tabular-nums;white-space:nowrap}}
td.chg.up{{color:#e53e3e}} td.chg.down{{color:#3182ce}} td.chg.flat{{color:#a0aec0}}
.est{{color:#805ad5;font-weight:600}}
footer{{padding:20px;text-align:center;color:#a0aec0;font-size:12px}}
</style></head>
<body>
<header>
  <h1>업종 탐색</h1>
  <span class="meta">{industry} · {len(df)}종목</span>
  <span class="stat"><span class="up">▲ {up}</span><span class="down">▼ {dn}</span></span>
</header>
<main><div class="card"><table>
<thead><tr><th>#</th><th>종목</th><th>코드</th><th>종가</th><th>등락률</th><th>시총</th>
<th>PER</th><th>Fwd PER</th><th>PBR</th><th>EPS</th><th>Fwd EPS</th><th>ROE%</th></tr></thead>
<tbody>{''.join(rows)}</tbody></table></div></main>
<footer>생성 {now} · 업종/시세: 네이버 금융 · 시총: FinanceDataReader</footer>
</body></html>"""
    OUT.write_text(html, encoding="utf-8")
    return OUT


def main() -> None:
    args = sys.argv[1:]
    if not args:
        ind = fetch_industries()
        console.print(f"[bold]네이버 업종 목록 ({len(ind)}개)[/]")
        for k in ind:
            console.print(f"  · {k}")
        console.print("\n사용: [cyan]python explore.py <키워드> [개수][/]")
        return

    keyword = args[0]
    top_n = int(args[1]) if len(args) > 1 else 20

    ind = fetch_industries()
    picked = pick_industry(keyword, ind)
    if not picked:
        if keyword not in ind:
            console.print(f"[yellow]'{keyword}' 매칭 업종 없음.[/] 전체 목록을 확인하려면 인자 없이 실행.")
        return
    industry, no = picked

    console.print(f"[bold]업종 조회:[/] {industry}")
    stocks = fetch_industry_stocks(no)
    if stocks.empty:
        console.print("[yellow]종목 데이터 비어있음[/]")
        return

    # 시총 조인 (KRX StockListing 캐시)
    listing = fdr.StockListing("KRX")[["Code", "Marcap"]].rename(columns={"Code": "ticker", "Marcap": "marcap"})
    listing["ticker"] = listing["ticker"].astype(str)
    stocks["ticker"] = stocks["ticker"].astype(str)
    stocks = stocks.merge(listing, on="ticker", how="left")
    stocks = stocks.sort_values("marcap", ascending=False, na_position="last").head(top_n).reset_index(drop=True)

    # 상위 N개에 대해 지표 크롤링
    console.print(f"[dim]상위 {len(stocks)}개 지표 크롤링...[/]")
    pers, per_ests, pbrs, epss, eps_ests, roes = [], [], [], [], [], []
    for t in stocks["ticker"]:
        try:
            rows = scrape_fundamentals(t)
            kpi = _latest_per(rows)
        except Exception:
            kpi = {"per": None, "pbr": None, "eps": None, "roe": None, "per_est": None, "eps_est": None}
        pers.append(kpi["per"]); per_ests.append(kpi["per_est"])
        pbrs.append(kpi["pbr"]); epss.append(kpi["eps"]); eps_ests.append(kpi["eps_est"])
        roes.append(kpi["roe"])
        time.sleep(0.08)
    stocks["per"] = pers
    stocks["per_est"] = per_ests
    stocks["pbr"] = pbrs
    stocks["eps"] = epss
    stocks["eps_est"] = eps_ests
    stocks["roe"] = roes

    render_console(industry, stocks)
    path = render_html(industry, stocks)
    console.print(f"\n[dim]HTML:[/] {path}")
    try:
        os.startfile(str(path))
    except Exception:
        webbrowser.open(path.as_uri())


if __name__ == "__main__":
    main()
