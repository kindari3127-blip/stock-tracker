# -*- coding: utf-8 -*-
"""
누적 종가(data/prices.csv) + 기업실적(data/financials.csv) + 지표(data/fundamentals.csv)
→ 단일 HTML 리포트.

섹터 카드:
  - 스냅샷 표 (종목 · 코드 · 종가 · 등락률 · PER · PBR · ROE)
  - 시계열 차트 (3종목 노멀라이즈, 1M/6M/1Y/ALL)
  - 종목별 연간/분기 매출·영업이익·순이익 비교 표
"""
import json
import os
import sys
import webbrowser
from collections import Counter
from datetime import datetime
from pathlib import Path

import FinanceDataReader as fdr
import pandas as pd
import plotly.graph_objects as go

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

from sector_theme import SECTOR_TO_THEME

HERE = Path(__file__).parent
PRICES = HERE / "data" / "prices.csv"
FUND = HERE / "data" / "fundamentals.csv"
FIN = HERE / "data" / "financials.csv"
INDMAP = HERE / "data" / "industry_map.json"
THEMEMAP = HERE / "data" / "theme_map.json"
OUT = HERE / "report.html"

EXPAND_TOP_N = 30  # 섹터 확장 패널에 표시할 상위 종목 수


def _n(v) -> str:
    return f"{int(v):,}" if pd.notna(v) else "-"


def _pct(v, decimals: int = 2) -> str:
    return f"{v:.{decimals}f}%" if pd.notna(v) else "-"


def _ratio(v) -> str:
    return f"{v:.2f}" if pd.notna(v) else "-"


def _fmt_cap(v) -> str:
    if v is None or pd.isna(v):
        return "-"
    v = float(v)
    if v >= 1e12:
        return f"{v/1e12:,.1f}조"
    if v >= 1e8:
        return f"{v/1e8:,.0f}억"
    return f"{v:,.0f}"


def _expand_block(sector_name: str, sector_tickers: list[str],
                  indmap: dict, thememap: dict, listing: pd.DataFrame) -> str:
    """섹터에 맞는 테마(우선) 또는 업종(fallback) 종목 상위 N개를 <details>로."""
    industry_name = ""
    stocks: list[dict] = []
    source_label = ""

    # 1) 테마 매핑 우선
    theme_name = SECTOR_TO_THEME.get(sector_name)
    if theme_name and thememap:
        tg = thememap.get("groups", {})
        for no, info in tg.items():
            if info.get("name") == theme_name:
                stocks = info.get("stocks", [])
                industry_name = info.get("name", theme_name)
                source_label = "테마"
                break

    # 2) 업종 fallback
    if not stocks and indmap:
        ticker_map = indmap.get("ticker_map", {})
        industries = indmap.get("industries", {})
        nos = [ticker_map.get(t, {}).get("no") for t in sector_tickers]
        nos = [n for n in nos if n]
        if nos:
            no = Counter(nos).most_common(1)[0][0]
            info = industries.get(no, {})
            industry_name = info.get("name", "업종")
            stocks = info.get("stocks", [])
            source_label = "업종"

    if not stocks:
        return ""
    df = pd.DataFrame(stocks)
    if listing is not None and not listing.empty:
        df = df.merge(
            listing.rename(columns={
                "Code": "ticker", "Close": "close",
                "ChagesRatio": "change_pct", "Marcap": "marcap",
            })[["ticker", "close", "change_pct", "marcap"]],
            on="ticker", how="left"
        )
    else:
        df["close"] = pd.NA; df["change_pct"] = pd.NA; df["marcap"] = pd.NA
    df = df.sort_values("marcap", ascending=False, na_position="last").head(EXPAND_TOP_N)

    highlight = set(sector_tickers)
    rows = []
    for i, r in enumerate(df.itertuples(index=False), 1):
        chg = r.change_pct
        chg_cls, chg_str = "flat", "-"
        if pd.notna(chg):
            if chg > 0: chg_cls, chg_str = "up", f"▲ +{chg:.2f}%"
            elif chg < 0: chg_cls, chg_str = "down", f"▼ {chg:.2f}%"
            else: chg_cls, chg_str = "flat", "· 0.00%"
        close_str = f"{int(r.close):,}" if pd.notna(r.close) else "-"
        star = '<span class="star" title="이 섹터의 대표주">★</span>' if r.ticker in highlight else ""
        rows.append(
            f'<tr><td class="idx">{i}</td>'
            f'<td class="name">{r.name} {star}</td>'
            f'<td class="code">{r.ticker}</td>'
            f'<td class="n">{close_str}</td>'
            f'<td class="chg {chg_cls}">{chg_str}</td>'
            f'<td class="n">{_fmt_cap(r.marcap)}</td></tr>'
        )

    summary = (f'<summary>▾ 같은 {source_label} 보기 · <b>{industry_name}</b> '
               f'<span class="dim">({len(stocks)}종목 중 상위 {len(df)})</span></summary>')
    return (
        f'<details class="expand">{summary}'
        f'<table class="industry">'
        f'<thead><tr><th>#</th><th>종목</th><th>코드</th><th>종가</th><th>등락률</th><th>시총</th></tr></thead>'
        f'<tbody>{"".join(rows)}</tbody></table></details>'
    )


def _eok(v) -> str:
    """억 원 단위 값을 보기 좋게. 1조(=10000억) 이상이면 조 단위."""
    if pd.isna(v):
        return "-"
    v = float(v)
    if abs(v) >= 10000:
        return f"{v/10000:,.1f}조"
    if abs(v) >= 100:
        return f"{v:,.0f}억"
    return f"{v:,.1f}억"


def _chg_cell(chg: float) -> str:
    if pd.isna(chg):
        return '<td class="chg flat">-</td>'
    if chg > 0:
        return f'<td class="chg up">▲ +{chg:.2f}%</td>'
    if chg < 0:
        return f'<td class="chg down">▼ {chg:.2f}%</td>'
    return '<td class="chg flat">· 0.00%</td>'


def _chart(sector: str, sec_df: pd.DataFrame) -> str:
    fig = go.Figure()
    for name, sub in sec_df.sort_values("date").groupby("name", sort=False):
        base = sub.iloc[0]["close"]
        if base <= 0:
            continue
        x = pd.to_datetime(sub["date"], format="%Y%m%d")
        y = sub["close"] / base * 100
        fig.add_trace(go.Scatter(
            x=x, y=y, mode="lines", name=name,
            line=dict(width=1.8),
            customdata=sub["close"].values,
            hovertemplate="<b>%{fullData.name}</b><br>%{x|%Y-%m-%d}<br>종가 %{customdata:,}원 (지수 %{y:.1f})<extra></extra>",
        ))
    fig.update_layout(
        height=320,
        margin=dict(l=50, r=20, t=40, b=30),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0, font=dict(size=11)),
        hovermode="x unified",
        xaxis=dict(
            rangeselector=dict(
                buttons=[
                    dict(count=1, label="1M", step="month", stepmode="backward"),
                    dict(count=6, label="6M", step="month", stepmode="backward"),
                    dict(count=1, label="1Y", step="year", stepmode="backward"),
                    dict(step="all", label="ALL"),
                ],
                bgcolor="#f0f4f8", activecolor="#2b6cb0", x=0.5, xanchor="center", y=1.18,
                font=dict(size=11),
            ),
            rangeslider=dict(visible=False),
            type="date",
            showgrid=False,
        ),
        yaxis=dict(title="지수 (1년전=100)", gridcolor="#edf2f7", zeroline=False),
        paper_bgcolor="white",
        plot_bgcolor="#fafbfc",
        font=dict(family="system-ui,-apple-system,Segoe UI,sans-serif", size=12),
    )
    safe = sector.replace("/", "_").replace(" ", "_")
    return fig.to_html(include_plotlyjs=False, full_html=False, div_id=f"chart_{safe}",
                       config={"displayModeBar": False, "responsive": True})


def _fin_table(title: str, pivot_df: pd.DataFrame, est_periods: set[str]) -> str:
    if pivot_df.empty:
        return ""
    cols = list(pivot_df.columns)
    header_cells = "".join(
        f'<th class="{"est" if p in est_periods else ""}">{p}{" (E)" if p in est_periods else ""}</th>'
        for p in cols
    )
    row_defs = [("revenue", "매출"), ("operating_profit", "영업"), ("net_profit", "순이익")]
    rows_html = []
    for key, label in row_defs:
        if key not in pivot_df.index:
            continue
        cells = []
        for p in cols:
            v = pivot_df.at[key, p]
            cls = "est" if p in est_periods else ""
            cells.append(f'<td class="{cls}">{_eok(v)}</td>')
        rows_html.append(f'<tr><td class="rlabel">{label}</td>{"".join(cells)}</tr>')
    return (
        f'<div class="fin-tbl-wrap"><div class="fin-tbl-title">{title}</div>'
        f'<table class="fin"><thead><tr><th></th>{header_cells}</tr></thead>'
        f'<tbody>{"".join(rows_html)}</tbody></table></div>'
    )


def _kpi_line(fund_row: pd.Series, mode: str) -> str:
    """mode: 'annual' 또는 'quarter'. 해당 기준의 period-tag + 지표 뱃지 HTML."""
    if mode == "annual":
        pa_col, pe_col = "actual_period", "est_period"
        suf, suf_est = "", "_est"
        label = "연간"
    else:
        pa_col, pe_col = "q_period", "q_est_period"
        suf, suf_est = "_q", "_q_est"
        label = "분기"

    pa = fund_row.get(pa_col) if pa_col in fund_row.index else None
    pe = fund_row.get(pe_col) if pe_col in fund_row.index else None

    fields = [
        ("per", "PER", _ratio),
        ("pbr", "PBR", _ratio),
        ("eps", "EPS", _n),
        ("roe", "ROE", lambda v: _pct(v)),
        ("dividend_yield", "배당률", lambda v: _pct(v)),
    ]
    badges = []
    for base, display, fmt in fields:
        k = f"{base}{suf}"
        ke = f"{base}{suf_est}"
        v = fund_row.get(k) if k in fund_row.index else None
        ve = fund_row.get(ke) if ke in fund_row.index else None
        has_v = v is not None and pd.notna(v)
        has_e = ve is not None and pd.notna(ve)
        if not has_v and not has_e:
            continue
        if has_v and has_e:
            body = f'{fmt(v)} <span class="est">→ {fmt(ve)}E</span>'
        elif has_v:
            body = fmt(v)
        else:
            body = f'<span class="est">{fmt(ve)}E</span>'
        badges.append(f'<span class="kpi"><b>{display}</b> {body}</span>')

    if not badges and not (pd.notna(pa) or pd.notna(pe)):
        return ""

    pa_s = str(pa) if pd.notna(pa) else "-"
    pe_s = str(pe) if pd.notna(pe) else "-"
    tag_cls = "period-tag" if mode == "annual" else "period-tag q"
    tag = (f'<span class="{tag_cls}"><span class="tag-label">{label}</span>'
           f'{pa_s} → {pe_s}<span class="est-mark">E</span></span>')
    return f'<div class="kpi-line">{tag}{"".join(badges)}</div>'


def _fin_block(ticker: str, name: str, fund_row: pd.Series | None, fin_sub: pd.DataFrame) -> str:
    if fund_row is not None:
        kpi_html = _kpi_line(fund_row, "annual") + _kpi_line(fund_row, "quarter")
        if not kpi_html:
            kpi_html = '<span class="kpi dim">지표 미수집</span>'
    else:
        kpi_html = '<span class="kpi dim">지표 미수집</span>'

    # 연간/분기 pivot
    est_annual, est_quarter = set(), set()
    annual_piv = pd.DataFrame()
    quarter_piv = pd.DataFrame()
    if not fin_sub.empty:
        metrics = ["revenue", "operating_profit", "net_profit"]
        a = fin_sub[(fin_sub["period_type"] == "annual") & (fin_sub["metric"].isin(metrics))]
        q = fin_sub[(fin_sub["period_type"] == "quarterly") & (fin_sub["metric"].isin(metrics))]
        if not a.empty:
            annual_piv = a.pivot_table(index="metric", columns="period", values="value", aggfunc="last")
            est_annual = set(a[a["is_estimate"] == 1]["period"])
        if not q.empty:
            quarter_piv = q.pivot_table(index="metric", columns="period", values="value", aggfunc="last")
            est_quarter = set(q[q["is_estimate"] == 1]["period"])

    tables_html = (_fin_table("연간", annual_piv, est_annual) +
                   _fin_table("분기", quarter_piv, est_quarter))

    return (
        f'<div class="fin-stock">'
        f'<div class="fin-head"><span class="fin-name">{name}</span>'
        f'<span class="fin-code">{ticker}</span>'
        f'<span class="fin-kpis">{kpi_html}</span></div>'
        f'<div class="fin-tables">{tables_html}</div>'
        f'</div>'
    )


CSS = """
* { box-sizing: border-box; }
body { font-family: system-ui,-apple-system,Segoe UI,"Malgun Gothic",sans-serif; margin: 0; background: #eef1f5; color: #1a202c; }
header { position: sticky; top: 0; z-index: 100; background: linear-gradient(135deg,#1a202c,#2d3748); color: #f7fafc; padding: 14px 28px; display: flex; align-items: center; gap: 20px; box-shadow: 0 2px 12px rgba(0,0,0,.18); flex-wrap: wrap; }
header h1 { margin: 0; font-size: 18px; font-weight: 700; letter-spacing: .01em; }
header .meta { font-size: 13px; opacity: .75; }
header .stat { margin-left: auto; display: flex; gap: 14px; font-size: 14px; font-weight: 600; }
header .up { color: #fc8181; } header .down { color: #63b3ed; } header .flat { color: #a0aec0; }
nav.sector-nav { background:#fff; padding:10px 20px; border-bottom:1px solid #e2e8f0; overflow-x:auto; white-space:nowrap; font-size:12px; position:sticky; top:56px; z-index:90; }
nav.sector-nav a { display:inline-block; padding:4px 10px; margin-right:4px; border-radius:999px; background:#edf2f7; color:#4a5568; text-decoration:none; transition:all .15s; }
nav.sector-nav a:hover { background:#3182ce; color:white; }
main { padding: 20px; max-width: 1400px; margin: 0 auto; padding-bottom: env(safe-area-inset-bottom, 20px); }
@media (max-width: 640px) {
  header { padding: 12px 16px; gap: 10px; }
  header h1 { font-size: 16px; }
  header .meta { font-size: 11px; }
  header .stat { font-size: 12px; gap: 8px; }
  main { padding: 12px; }
  .card { padding: 14px; border-radius: 10px; }
  .card h2 { font-size: 14px; }
  nav.sector-nav { padding: 8px 12px; font-size: 11px; }
  nav.sector-nav a { padding: 3px 8px; }
  details.expand summary { font-size: 12px; padding: 8px 12px; }
  .fin-kpis { max-width: 100%; align-items: flex-start; }
  .fin-kpis .kpi { font-size: 10px; padding: 2px 8px; }
  table.snap td, table.industry td, table.fin td { padding: 6px 4px; font-size: 11px; }
  table.snap th, table.industry th, table.fin th { padding: 6px 4px; font-size: 10px; }
}
.grid-cards { display: grid; grid-template-columns: 1fr; gap: 16px; }
.card { background: white; border-radius: 12px; padding: 20px; box-shadow: 0 1px 3px rgba(0,0,0,.06), 0 1px 2px rgba(0,0,0,.04); scroll-margin-top: 110px; }
.card h2 { margin: 0 0 14px 0; font-size: 16px; color: #2d3748; border-left: 3px solid #3182ce; padding-left: 10px; }
.grid { display: grid; grid-template-columns: minmax(280px, 360px) 1fr; gap: 24px; align-items: start; }
@media (max-width: 860px) { .grid { grid-template-columns: 1fr; } }

table.snap { width: 100%; border-collapse: collapse; font-size: 13px; }
table.snap th { text-align: left; font-weight: 600; color: #718096; padding: 6px 8px; border-bottom: 1px solid #e2e8f0; font-size: 11px; letter-spacing: .03em; text-transform: uppercase; }
table.snap td { padding: 9px 8px; border-bottom: 1px solid #f1f5f9; }
table.snap tr:last-child td { border-bottom: none; }
table.snap td.name { font-weight: 600; }
table.snap td.code { font-family: ui-monospace,Consolas,monospace; color: #a0aec0; font-size: 12px; }
table.snap td.num { text-align: right; font-variant-numeric: tabular-nums; }
table.snap td.close { text-align: right; font-variant-numeric: tabular-nums; font-weight: 500; }
table.snap td.chg { text-align: right; font-variant-numeric: tabular-nums; font-weight: 600; white-space: nowrap; }
table.snap td.chg.up { color: #e53e3e; } table.snap td.chg.down { color: #3182ce; } table.snap td.chg.flat { color: #a0aec0; }
.chart-wrap { min-width: 0; }

.fin-block { margin-top: 18px; padding-top: 16px; border-top: 1px dashed #cbd5e0; display: flex; flex-direction: column; gap: 16px; }
.fin-stock { background: #f8fafc; border-radius: 8px; padding: 12px 14px; }
.fin-head { display: flex; align-items: center; gap: 8px; margin-bottom: 8px; flex-wrap: wrap; }
.fin-name { font-weight: 700; font-size: 14px; color: #2d3748; }
.fin-code { font-family: ui-monospace,Consolas,monospace; color: #a0aec0; font-size: 11px; }
.fin-kpis { margin-left: auto; display: flex; flex-direction: column; gap: 4px; align-items: flex-end; max-width: 70%; }
.kpi-line { display: flex; flex-wrap: wrap; gap: 6px; justify-content: flex-end; align-items: center; }
.kpi { background: white; border: 1px solid #e2e8f0; border-radius: 999px; padding: 2px 10px; font-size: 11px; color: #4a5568; white-space: nowrap; }
.kpi b { color: #2d3748; margin-right: 3px; }
.kpi.dim { color: #a0aec0; font-style: italic; }
.kpi .est { color: #805ad5; font-weight: 600; }
.period-tag { background: #ebf4ff; color: #2c5282; border-radius: 999px; padding: 2px 10px; font-size: 11px; font-weight: 500; white-space: nowrap; }
.period-tag.q { background: #f0fff4; color: #276749; }
.period-tag .tag-label { font-weight: 700; margin-right: 6px; }
.period-tag .est-mark { color: #805ad5; margin-left: 2px; font-weight: 700; }
@media (max-width: 860px) { .fin-kpis { max-width: 100%; align-items: flex-start; } .kpi-line { justify-content: flex-start; } }
.fin-tables { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
@media (max-width: 860px) { .fin-tables { grid-template-columns: 1fr; } }
.fin-tbl-wrap { min-width: 0; overflow-x: auto; }
.fin-tbl-title { font-size: 11px; font-weight: 600; color: #718096; text-transform: uppercase; letter-spacing: .05em; margin-bottom: 4px; padding-left: 4px; }
table.fin { width: 100%; border-collapse: collapse; font-size: 12px; font-variant-numeric: tabular-nums; }
table.fin th, table.fin td { padding: 4px 8px; text-align: right; border-bottom: 1px solid #edf2f7; }
table.fin th { background: #edf2f7; font-weight: 600; color: #4a5568; font-size: 11px; white-space: nowrap; }
table.fin th.est { color: #a0aec0; font-style: italic; }
table.fin td.rlabel { text-align: left; color: #4a5568; font-weight: 500; }
table.fin td.est { color: #a0aec0; }

details.expand { margin-top: 18px; padding-top: 14px; border-top: 1px dashed #cbd5e0; }
details.expand summary { cursor: pointer; font-size: 13px; font-weight: 600; padding: 8px 14px; background: #ebf4ff; color: #2b6cb0; border-radius: 10px; display: inline-block; user-select: none; list-style: none; transition: background .15s; }
details.expand summary::-webkit-details-marker { display: none; }
details.expand summary:hover { background: #bee3f8; }
details.expand[open] summary { background: #2b6cb0; color: white; margin-bottom: 12px; }
details.expand summary b { margin: 0 2px; }
details.expand summary .dim { font-weight: 400; opacity: .7; margin-left: 6px; }
table.industry { width: 100%; border-collapse: collapse; font-size: 13px; margin-top: 6px; }
table.industry th { background: #f7fafc; color: #4a5568; font-size: 11px; padding: 8px; letter-spacing: .05em; text-transform: uppercase; border-bottom: 1px solid #e2e8f0; text-align: left; }
table.industry td { padding: 8px; border-bottom: 1px solid #f1f5f9; }
table.industry td.idx { color: #a0aec0; text-align: right; width: 38px; }
table.industry td.name { font-weight: 600; }
table.industry td.code { font-family: ui-monospace,Consolas,monospace; color: #a0aec0; font-size: 12px; }
table.industry td.n { text-align: right; font-variant-numeric: tabular-nums; }
table.industry td.chg { text-align: right; font-weight: 600; font-variant-numeric: tabular-nums; white-space: nowrap; }
table.industry td.chg.up { color: #e53e3e; } table.industry td.chg.down { color: #3182ce; } table.industry td.chg.flat { color: #a0aec0; }
table.industry .star { color: #ecc94b; margin-left: 4px; }

footer { padding: 24px; text-align: center; color: #a0aec0; font-size: 12px; }
"""


def main(auto_open: bool = True) -> None:
    if not PRICES.exists():
        print("가격 데이터 없음. python collect.py 먼저 실행.")
        return

    df = pd.read_csv(PRICES, dtype={"ticker": str, "date": str})
    df["close"] = df["close"].astype(int)
    latest = df["date"].max()
    today = df[df["date"] == latest].copy()

    # fundamentals / financials 로드
    fund_df = pd.read_csv(FUND, dtype={"ticker": str}) if FUND.exists() else pd.DataFrame()
    fin_df = pd.read_csv(FIN, dtype={"ticker": str}) if FIN.exists() else pd.DataFrame()
    fund_lookup: dict[str, pd.Series] = (
        {row["ticker"]: row for _, row in fund_df.iterrows()} if not fund_df.empty else {}
    )

    # 확장 패널 데이터: 업종/테마 인덱스 + KRX 시세 스냅샷
    indmap, thememap = {}, {}
    if INDMAP.exists():
        try:
            indmap = json.loads(INDMAP.read_text(encoding="utf-8"))
        except Exception:
            indmap = {}
    if THEMEMAP.exists():
        try:
            thememap = json.loads(THEMEMAP.read_text(encoding="utf-8"))
        except Exception:
            thememap = {}
    try:
        listing = fdr.StockListing("KRX")
        listing["Code"] = listing["Code"].astype(str)
    except Exception:
        listing = pd.DataFrame()

    up = int((today["change_pct"] > 0).sum())
    dn = int((today["change_pct"] < 0).sum())
    fl = int((today["change_pct"] == 0).sum())
    d_iso = f"{latest[:4]}-{latest[4:6]}-{latest[6:]}"
    n_days = df["date"].nunique()
    first_date = df["date"].min()
    first_iso = f"{first_date[:4]}-{first_date[4:6]}-{first_date[6:]}"

    sectors_sorted = sorted(today["sector"].unique())
    nav_html = "".join(f'<a href="#sec-{i}">{s}</a>' for i, s in enumerate(sectors_sorted))

    blocks = []
    for i, sector in enumerate(sectors_sorted):
        snap = today[today["sector"] == sector].sort_values("close", ascending=False)

        # 스냅샷 표 (PER/PBR/ROE 컬럼 포함)
        rows = []
        for _, r in snap.iterrows():
            fund = fund_lookup.get(r["ticker"])
            per = _ratio(fund["per"]) if fund is not None and "per" in fund.index else "-"
            pbr = _ratio(fund["pbr"]) if fund is not None and "pbr" in fund.index else "-"
            roe = _pct(fund["roe"]) if fund is not None and "roe" in fund.index else "-"
            rows.append(
                f'<tr><td class="name">{r["name"]}</td>'
                f'<td class="code">{r["ticker"]}</td>'
                f'<td class="close">{_n(r["close"])}</td>'
                f'{_chg_cell(r["change_pct"])}'
                f'<td class="num">{per}</td>'
                f'<td class="num">{pbr}</td>'
                f'<td class="num">{roe}</td>'
                f'</tr>'
            )
        tbl = (
            '<table class="snap"><thead><tr>'
            '<th>종목</th><th>코드</th><th>종가</th><th>등락률</th>'
            '<th>PER</th><th>PBR</th><th>ROE</th>'
            '</tr></thead><tbody>' + "".join(rows) + '</tbody></table>'
        )

        chart = _chart(sector, df[df["sector"] == sector])

        # 실적 블록 (3종목)
        fin_blocks = []
        for _, r in snap.iterrows():
            fund = fund_lookup.get(r["ticker"])
            sub = fin_df[fin_df["ticker"] == r["ticker"]] if not fin_df.empty else pd.DataFrame()
            fin_blocks.append(_fin_block(r["ticker"], r["name"], fund, sub))

        expand = _expand_block(sector, snap["ticker"].tolist(), indmap, thememap, listing)
        blocks.append(
            f'<section class="card" id="sec-{i}">'
            f'<h2>{sector}</h2>'
            f'<div class="grid">'
            f'<div class="table-wrap">{tbl}</div>'
            f'<div class="chart-wrap">{chart}</div>'
            f'</div>'
            f'<div class="fin-block">{"".join(fin_blocks)}</div>'
            f'{expand}'
            f'</section>'
        )

    generated = datetime.now().strftime("%Y-%m-%d %H:%M")
    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<title>주식추적기 · {d_iso}</title>
<link rel="manifest" href="./manifest.json">
<meta name="theme-color" content="#1a202c">
<link rel="icon" type="image/svg+xml" href="./icon.svg">
<link rel="apple-touch-icon" href="./icon.svg">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<meta name="apple-mobile-web-app-title" content="주식추적기">
<script>
if ('serviceWorker' in navigator) {{
  window.addEventListener('load', () => navigator.serviceWorker.register('./sw.js').catch(()=>{{}}));
}}
</script>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>{CSS}</style>
</head>
<body>
<header>
  <h1>주식추적기</h1>
  <span class="meta">{d_iso} · {first_iso}부터 {n_days}일 누적 · {len(sectors_sorted)}개 섹터 / {len(today)}종목</span>
  <span class="stat">
    <span class="up">▲ {up}</span>
    <span class="down">▼ {dn}</span>
    <span class="flat">· {fl}</span>
  </span>
</header>
<nav class="sector-nav">{nav_html}</nav>
<main><div class="grid-cards">{''.join(blocks)}</div></main>
<footer>생성: {generated} · 가격: FinanceDataReader · 실적: 네이버 금융 · 차트: Plotly · 단위: 매출/영업/순이익 = 억원(1조=10000억)</footer>
</body>
</html>
"""
    OUT.write_text(html, encoding="utf-8")
    size_kb = OUT.stat().st_size // 1024
    print(f"저장: {OUT}  ({size_kb:,}KB)")
    if auto_open:
        try:
            os.startfile(str(OUT))
        except Exception:
            webbrowser.open(OUT.as_uri())


if __name__ == "__main__":
    main(auto_open="--no-open" not in sys.argv)
