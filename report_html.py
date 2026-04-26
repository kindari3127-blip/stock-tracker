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
from sectors import SECTORS
from market_stats import (
    sector_today_changes,
    sector_period_returns,
    sector_correlation,
    top_correlations,
    new_sector_candidates,
    stock_period_returns,
    quality_value_composite,
    pressed_quality,
    low_52w_quality,
    dividend_quality,
)
from analysis_data import build_stock_data, stats as _sd_stats

HERE = Path(__file__).parent
PRICES = HERE / "data" / "prices.csv"
FUND = HERE / "data" / "fundamentals.csv"
FIN = HERE / "data" / "financials.csv"
INDMAP = HERE / "data" / "industry_map.json"
THEMEMAP = HERE / "data" / "theme_map.json"
BIZ = HERE / "data" / "business.json"
LEADERS = HERE / "data" / "global_leaders.json"
FUND_EXTRA = HERE / "data" / "fundamentals_extra.csv"
FIN_EXTRA = HERE / "data" / "financials_extra.csv"
PRICES_EXTRA = HERE / "data" / "prices_extra.csv"
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
            f'<tr class="row-clickable" data-ticker="{r.ticker}" '
            f'onclick="showStockAnalysis(\'{r.ticker}\', this)">'
            f'<td class="idx">{i}</td>'
            f'<td class="name">{r.name} {star} <span class="row-arrow">▾</span></td>'
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


def _ins_link(ticker_to_sector: dict, sectors_sorted: list, sector: str) -> str:
    if sector and sector in sectors_sorted:
        return f"#sec-{sectors_sorted.index(sector)}"
    return "#"


def _ins_row_open(ticker: str) -> str:
    """row-clickable 시작 태그 — 클릭 시 분석 카드 표시."""
    return (f'<tr class="row-clickable" data-ticker="{ticker}" '
            f'onclick="showStockAnalysis(\'{ticker}\', this)">')


def _ins_namecell(name: str, sector: str, ticker: str, ticker_to_sector: dict,
                  sectors_sorted: list) -> str:
    """이름 + 섹터 chip(클릭 시 섹터로 점프, 행 클릭 분리) + 티커."""
    if sector:
        link = _ins_link(ticker_to_sector, sectors_sorted, sector)
        sec_tag = (f'<a class="stk-sec stk-sec-link" href="{link}" '
                   f'onclick="event.stopPropagation();">{sector}</a>')
    else:
        sec_tag = ""
    return (f'<td class="ins-name">'
            f'<span class="ins-name-text">{name}</span> {sec_tag}'
            f'<span class="ins-tk">{ticker}</span></td>')


def _quality_value_html(df: pd.DataFrame, ticker_to_sector: dict,
                        sectors_sorted: list) -> str:
    if df.empty:
        return "<p class='dim'>조건 만족 종목 없음 (조건: PER&lt;12 · PBR&lt;1.5 · ROE&gt;10% · EPS&gt;0)</p>"
    rows = []
    for _, r in df.iterrows():
        t = r["ticker"]
        sector = ticker_to_sector.get(t, "")
        rows.append(
            f"{_ins_row_open(t)}{_ins_namecell(r['name'], sector, t, ticker_to_sector, sectors_sorted)}"
            f"<td class='n'>{r['per']:.1f}</td>"
            f"<td class='n'>{r['pbr']:.2f}</td>"
            f"<td class='n ins-good'>{r['roe']:.1f}%</td>"
            f"<td class='n ins-score'>{r['score']:.2f}</td></tr>"
        )
    return (
        "<table class='ins-tbl'>"
        "<thead><tr><th>종목</th><th>PER</th><th>PBR</th><th>ROE</th>"
        "<th title='ROE / (PER × PBR)'>점수</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table>"
    )


def _pressed_quality_html(df: pd.DataFrame, ticker_to_sector: dict,
                          sectors_sorted: list, days: int) -> str:
    if df.empty:
        return f"<p class='dim'>조건 만족 종목 없음 (조건: ROE&gt;10% · EPS&gt;0 · 최근 {days}일 -5% 이하)</p>"
    rows = []
    for _, r in df.iterrows():
        t = r["ticker"]
        sector = ticker_to_sector.get(t, "")
        per = r.get("per")
        per_s = f"{per:.1f}" if pd.notna(per) and per is not None else "-"
        rows.append(
            f"{_ins_row_open(t)}{_ins_namecell(r['name'], sector, t, ticker_to_sector, sectors_sorted)}"
            f"<td class='n ins-good'>{r['roe']:.1f}%</td>"
            f"<td class='n'>{per_s}</td>"
            f"<td class='n ins-bad'>{r['return_pct']:+.2f}%</td></tr>"
        )
    return (
        "<table class='ins-tbl'>"
        "<thead><tr><th>종목</th><th>ROE</th><th>PER</th>"
        f"<th>{days}일 수익률</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table>"
    )


def _low52w_html(df: pd.DataFrame, ticker_to_sector: dict, sectors_sorted: list) -> str:
    if df.empty:
        return ("<p class='dim'>조건 만족 종목 없음 "
                "(조건: 52주 저점 ±15% + ROE&gt;10% + EPS&gt;0)</p>")
    rows = []
    for _, r in df.iterrows():
        t = r["ticker"]
        sector = ticker_to_sector.get(t, "")
        prox = r["low_proximity"] * 100  # 0%=저점, 100%=고점
        from_hi = r["from_high_pct"]
        rows.append(
            f"{_ins_row_open(t)}{_ins_namecell(r['name'], sector, t, ticker_to_sector, sectors_sorted)}"
            f"<td class='n ins-good'>{r['roe']:.1f}%</td>"
            f"<td class='n'>{int(r['close']):,}</td>"
            f"<td class='n ins-bad'>{from_hi:+.1f}%</td>"
            f"<td class='n'>{prox:.1f}%</td></tr>"
        )
    return (
        "<table class='ins-tbl'>"
        "<thead><tr><th>종목</th><th>ROE</th><th>현재가</th>"
        "<th title='52주 고점 대비'>고점대비</th>"
        "<th title='0%=저점, 100%=고점'>저점근접</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table>"
    )


def _dividend_html(df: pd.DataFrame, ticker_to_sector: dict, sectors_sorted: list) -> str:
    if df.empty:
        return "<p class='dim'>조건 만족 종목 없음 (조건: 배당수익률&gt;4% · ROE&gt;8% · EPS&gt;0)</p>"
    rows = []
    for _, r in df.iterrows():
        t = r["ticker"]
        sector = ticker_to_sector.get(t, "")
        rows.append(
            f"{_ins_row_open(t)}{_ins_namecell(r['name'], sector, t, ticker_to_sector, sectors_sorted)}"
            f"<td class='n ins-yield'>{r['dividend_yield']:.2f}%</td>"
            f"<td class='n ins-good'>{r['roe']:.1f}%</td>"
            f"<td class='n'>{int(r['eps']):,}</td></tr>"
        )
    return (
        "<table class='ins-tbl'>"
        "<thead><tr><th>종목</th><th>배당수익률</th><th>ROE</th><th>EPS</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table>"
    )


def _insight_section(fund_all: pd.DataFrame,
                     prices_df: pd.DataFrame, prices_extra_df: pd.DataFrame,
                     ticker_to_sector: dict, sectors_sorted: list) -> str:
    """4가지 투자 인사이트 패널을 묶은 HTML 섹션."""
    qv = quality_value_composite(fund_all, n=15)
    pq = pressed_quality(fund_all, prices_df, prices_extra_df, days=20,
                         threshold_pct=-5.0, n=15)
    lq = low_52w_quality(fund_all, prices_df, prices_extra_df,
                         proximity_max=0.15, n=15)
    dq = dividend_quality(fund_all, n=10)

    return f"""
      <h3 class="dash-section-h">💡 투자 인사이트 — 좋은 주식을 쌀 때 사기</h3>
      <p class="dash-section-note">매일 자동 갱신 · 클릭하면 해당 섹터로 이동 · 조건은 하단 패널 헤더에 표시</p>
      <div class="ins-grid">
        <div class="dash-block ins-block">
          <h3 class="ins-h">💎 우량 + 가치 종합 TOP15</h3>
          <p class="ins-cond">PER &lt; 12 · PBR &lt; 1.5 · ROE &gt; 10% · EPS &gt; 0 → 점수 = ROE / (PER × PBR)</p>
          {_quality_value_html(qv, ticker_to_sector, sectors_sorted)}
        </div>
        <div class="dash-block ins-block">
          <h3 class="ins-h">🩸 눌림 우량주 TOP15 <span class="ins-h-sub">(20일)</span></h3>
          <p class="ins-cond">ROE &gt; 10% · EPS &gt; 0 + 최근 20일 -5% 이하 — 실적 견조한데 일시 빠진 종목</p>
          {_pressed_quality_html(pq, ticker_to_sector, sectors_sorted, 20)}
        </div>
        <div class="dash-block ins-block">
          <h3 class="ins-h">📉 52주 저점 근접 우량주 TOP15</h3>
          <p class="ins-cond">52주 저점 ±15% 이내 + ROE &gt; 10% + EPS &gt; 0 — 1년 저점 근처의 흑자 우량주</p>
          {_low52w_html(lq, ticker_to_sector, sectors_sorted)}
        </div>
        <div class="dash-block ins-block">
          <h3 class="ins-h">💰 고배당 우량주 TOP10</h3>
          <p class="ins-cond">배당수익률 &gt; 4% · ROE &gt; 8% · EPS &gt; 0 — 배당+안정 수익성 조합</p>
          {_dividend_html(dq, ticker_to_sector, sectors_sorted)}
        </div>
      </div>
    """


def _global_leaders(leaders: list[dict], ticker_to_sector: dict,
                    sectors_sorted: list[str]) -> str:
    if not leaders:
        return "<p class='dim'>큐레이션 데이터 없음</p>"
    rows = []
    for L in leaders:
        t = L.get("ticker", "")
        sector = ticker_to_sector.get(t, "")
        link = f"#sec-{sectors_sorted.index(sector)}" if sector in sectors_sorted else "#"
        sec_tag = f'<span class="stk-sec">{sector}</span>' if sector else ''
        rows.append(
            f'<li><a class="leader-row" href="{link}">'
            f'<span class="leader-field">{L.get("field","")}</span>'
            f'<span class="leader-name">{L.get("name","")} {sec_tag}</span>'
            f'<span class="leader-note">{L.get("rank_note","")}</span>'
            f'</a></li>'
        )
    return f"<ul class='leader-list'>{''.join(rows)}</ul>"


def _dashboard_card(df: pd.DataFrame, latest: str,
                    indmap: dict, listing: pd.DataFrame,
                    fund_df: pd.DataFrame, fund_extra_df: pd.DataFrame,
                    leaders: list[dict],
                    prices_extra_df: pd.DataFrame) -> str:
    """오늘의 시황 카드: 급등 섹터 / 소외 섹터 / 새 섹터 후보."""
    today_chg = sector_today_changes(df, latest)
    ret_5d = sector_period_returns(df, days=5)
    ret_20d = sector_period_returns(df, days=20)

    def _sector_link(s: str) -> str:
        sectors_sorted = sorted(df[df["date"] == latest]["sector"].unique())
        if s in sectors_sorted:
            i = sectors_sorted.index(s)
            return f'<a href="#sec-{i}" class="dash-link">{s}</a>'
        return f'<span>{s}</span>'

    def _list_changes(s: pd.Series, n: int, cls: str) -> str:
        rows = []
        for sector, v in s.head(n).items():
            sign = "+" if v > 0 else ""
            rows.append(
                f'<li><span class="dash-name">{_sector_link(sector)}</span>'
                f'<span class="dash-num {cls}">{sign}{v:.2f}%</span></li>'
            )
        return "<ol class='dash-list'>" + "".join(rows) + "</ol>"

    surge_html = _list_changes(today_chg, 5, "up") if not today_chg.empty else "<p class='dim'>데이터 부족</p>"
    weak_5d = _list_changes(ret_5d, 5, "down") if not ret_5d.empty else "<p class='dim'>데이터 부족</p>"
    weak_20d = _list_changes(ret_20d, 5, "down") if not ret_20d.empty else "<p class='dim'>데이터 부족</p>"

    # 종목 단위 눌림 TOP10 (5/20/60일)
    sectors_sorted = sorted(df[df["date"] == latest]["sector"].unique())

    def _stock_link(sector: str) -> str:
        if sector in sectors_sorted:
            i = sectors_sorted.index(sector)
            return f"#sec-{i}"
        return "#"

    def _stock_panel(period_days: int, n: int = 10) -> str:
        ranked = stock_period_returns(df, days=period_days)
        if ranked.empty:
            return "<p class='dim'>데이터 부족</p>"
        rows = []
        for _, r in ranked.head(n).iterrows():
            v = r["return_pct"]
            rows.append(
                f'<li><a href="{_stock_link(r["sector"])}" class="stock-link">'
                f'<span class="stk-name">{r["name"]}</span>'
                f'<span class="stk-sec">{r["sector"]}</span>'
                f'<span class="dash-num down">{v:+.2f}%</span></a></li>'
            )
        return "<ol class='dash-list stock-list'>" + "".join(rows) + "</ol>"

    weak_stock_5d = _stock_panel(5, 10)
    weak_stock_20d = _stock_panel(20, 10)
    weak_stock_60d = _stock_panel(60, 10)

    # SECTORS + 확장 fundamentals 통합
    fund_all_pieces = []
    if fund_df is not None and not fund_df.empty:
        fund_all_pieces.append(fund_df)
    if fund_extra_df is not None and not fund_extra_df.empty:
        fund_all_pieces.append(fund_extra_df)
    fund_all = pd.concat(fund_all_pieces, ignore_index=True) if fund_all_pieces else pd.DataFrame()

    # ticker → sector 매핑 (SECTORS 기준; 한 ticker가 여러 섹터면 첫 번째)
    ticker_to_sector: dict[str, str] = {}
    for s, items in SECTORS.items():
        for t, _ in items:
            if t not in ticker_to_sector:
                ticker_to_sector[t] = s

    insight_html = _insight_section(fund_all, df, prices_extra_df,
                                     ticker_to_sector, sectors_sorted)
    leaders_html = _global_leaders(leaders, ticker_to_sector, sectors_sorted)

    # 새 섹터 후보
    cands = new_sector_candidates(indmap, SECTORS, listing, top_n=10)
    cand_rows = []
    for c in cands:
        cap_str = _fmt_cap(c["marcap_total"])
        top_names = " · ".join(s["name"] for s in c["top_stocks"][:3])
        cand_rows.append(
            f'<li><span class="cand-name">{c["industry_name"]}</span>'
            f'<span class="cand-cap">{cap_str}</span>'
            f'<span class="cand-top">{top_names}</span></li>'
        )
    cand_html = (
        "<ul class='cand-list'>" + "".join(cand_rows) + "</ul>"
        if cand_rows else "<p class='dim'>후보 없음 (industry_map 확인)</p>"
    )

    return f"""
    <section class="card dashboard">
      <h2>오늘의 시황 <span class="dash-sub">매일 자동 갱신</span></h2>
      <div class="dash-grid">
        <div class="dash-block">
          <h3 class="dash-h">▲ 오늘 급등 섹터 TOP5</h3>
          {surge_html}
        </div>
        <div class="dash-block">
          <h3 class="dash-h">▼ 최근 5일 소외 섹터 TOP5</h3>
          {weak_5d}
        </div>
        <div class="dash-block">
          <h3 class="dash-h">▼ 최근 20일 소외 섹터 TOP5</h3>
          {weak_20d}
        </div>
        <div class="dash-block dash-cands">
          <h3 class="dash-h">＋ 새 섹터 후보 (등록 안된 큰 업종)</h3>
          {cand_html}
        </div>
      </div>

      <h3 class="dash-section-h">📉 지금 가장 눌려있는 종목 (이슈 종결 시 반등 기대 후보)</h3>
      <p class="dash-section-note">매일 자동 갱신 · 클릭하면 해당 섹터로 이동</p>
      <div class="dash-grid">
        <div class="dash-block">
          <h3 class="dash-h">▼ 5일 누적 하락 TOP10</h3>
          {weak_stock_5d}
        </div>
        <div class="dash-block">
          <h3 class="dash-h">▼ 20일 누적 하락 TOP10</h3>
          {weak_stock_20d}
        </div>
        <div class="dash-block">
          <h3 class="dash-h">▼ 60일 누적 하락 TOP10</h3>
          {weak_stock_60d}
        </div>
      </div>

      {insight_html}

      <h3 class="dash-section-h">🌏 분야별 글로벌 1위 (큐레이션)</h3>
      <p class="dash-section-note">시장 점유율·기술 리더십 기준. 변동 가능성이 있어 주기적 재검토 필요.</p>
      <div class="dash-block dash-wide">
        {leaders_html}
      </div>
    </section>
    """


def _correlation_panel(corr: pd.DataFrame, sector: str,
                       sectors_sorted: list[str]) -> str:
    """섹터 카드용 상관계수 details 패널."""
    if corr.empty or sector not in corr.index:
        return ""
    pos = top_correlations(corr, sector, n=5, sign="pos")
    neg = top_correlations(corr, sector, n=5, sign="neg")

    def _link(s: str) -> str:
        if s in sectors_sorted:
            i = sectors_sorted.index(s)
            return f'<a href="#sec-{i}">{s}</a>'
        return s

    def _row(s: str, v: float, kind: str) -> str:
        cls = "corr-pos" if v > 0.3 else ("corr-neg" if v < -0.1 else "corr-mid")
        return (f'<tr><td class="corr-name">{_link(s)}</td>'
                f'<td class="corr-num {cls}">{v:+.3f}</td></tr>')

    pos_rows = "".join(_row(s, v, "pos") for s, v in pos)
    neg_rows = "".join(_row(s, v, "neg") for s, v in neg)

    return f"""
    <details class="corr-panel">
      <summary>▾ 같이/반대로 움직이는 섹터 보기 <span class="dim">(최근 60일 일별 수익률 상관계수)</span></summary>
      <div class="corr-grid">
        <div>
          <div class="corr-h">함께 움직이는 TOP5</div>
          <table class="corr-tbl"><tbody>{pos_rows}</tbody></table>
        </div>
        <div>
          <div class="corr-h">반대로 움직이는 TOP5</div>
          <table class="corr-tbl"><tbody>{neg_rows}</tbody></table>
        </div>
      </div>
      <div class="corr-note">+1에 가까울수록 같이 움직임, -1에 가까울수록 반대로 움직임. 한국 시장은 전반적으로 양의 상관관계가 많아 "반대" 측은 "가장 약하게 따라가는 섹터"로 볼 수도 있음.</div>
    </details>
    """


def _business_details(ticker: str, biz: dict | None) -> str:
    """종목 카드용 주력사업 details 패널."""
    if not biz or not (biz.get("header") or biz.get("points") or biz.get("products")):
        return '<details class="biz-panel"><summary>▾ 주력사업·실적 보기 <span class="dim">(정보 없음)</span></summary></details>'
    header = biz.get("header", "")
    date = biz.get("date", "")
    points = biz.get("points") or []
    products = biz.get("products") or []
    rnd_pct = biz.get("rnd_pct")
    rnd_year = biz.get("rnd_year", "")
    subs = biz.get("subsidiaries") or []

    summary = (
        f'<summary>▾ 주력사업·실적 보기'
        + (f' <b>· {header}</b>' if header else '')
        + (f' <span class="dim">[{date}]</span>' if date else '')
        + '</summary>'
    )

    body_parts = []
    if points:
        points_html = "".join(f'<li>{p}</li>' for p in points)
        body_parts.append(f'<ul class="biz-points">{points_html}</ul>')

    # 매출구성
    if products:
        prod_rows = []
        for p in products[:8]:
            name = p.get("name", "")
            ratio = p.get("ratio")
            try:
                ratio_f = float(ratio) if ratio is not None else None
            except (TypeError, ValueError):
                ratio_f = None
            ratio_str = f"{ratio_f:.2f}%" if ratio_f is not None else "-"
            bar_w = max(0, min(100, ratio_f)) if ratio_f is not None and ratio_f > 0 else 0
            bar = (f'<div class="prod-bar"><div class="prod-bar-fill" '
                   f'style="width:{bar_w}%"></div></div>') if bar_w > 0 else ""
            prod_rows.append(
                f'<tr><td class="prod-name">{name}</td>'
                f'<td class="prod-ratio">{ratio_str}</td>'
                f'<td class="prod-barcell">{bar}</td></tr>'
            )
        body_parts.append(
            '<div class="biz-subblock"><div class="biz-sub-title">📊 매출 구성</div>'
            f'<table class="prod-tbl"><tbody>{"".join(prod_rows)}</tbody></table></div>'
        )

    # R&D 배지
    if rnd_pct is not None:
        try:
            rnd_str = f"{float(rnd_pct):.2f}%"
            yr = f" ({rnd_year})" if rnd_year else ""
            body_parts.append(
                f'<div class="biz-subblock"><span class="rnd-badge">'
                f'<b>R&D 비중{yr}</b> {rnd_str}</span></div>'
            )
        except (TypeError, ValueError):
            pass

    # 자회사 (5개까지)
    if subs:
        sub_rows = "".join(
            f'<tr><td class="sub-name">{s.get("name","")}</td>'
            f'<td class="sub-biz">{s.get("biz","")}</td>'
            f'<td class="sub-founded">{s.get("founded","")}</td></tr>'
            for s in subs[:5]
        )
        body_parts.append(
            f'<details class="biz-subblock-details">'
            f'<summary>▾ 주요 자회사 ({len(subs)}개 중 5개)</summary>'
            f'<table class="sub-tbl"><thead><tr><th>회사명</th><th>주요사업</th><th>설립</th></tr></thead>'
            f'<tbody>{sub_rows}</tbody></table></details>'
        )

    body = "".join(body_parts)
    return f'<details class="biz-panel">{summary}{body}</details>'


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


def _fin_block(ticker: str, name: str, fund_row: pd.Series | None,
               fin_sub: pd.DataFrame, biz: dict | None = None) -> str:
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

    biz_html = _business_details(ticker, biz)

    return (
        f'<div class="fin-stock">'
        f'<div class="fin-head"><span class="fin-name">{name}</span>'
        f'<span class="fin-code">{ticker}</span>'
        f'<span class="fin-kpis">{kpi_html}</span></div>'
        f'{biz_html}'
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

/* ===== 시황 대시보드 ===== */
.dashboard { background: linear-gradient(135deg,#fffbeb,#fef3c7); border-left: 4px solid #d97706; }
.dashboard h2 { border-left: 3px solid #d97706; color: #78350f; }
.dash-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 16px; }
@media (min-width: 1100px) { .dash-grid { grid-template-columns: repeat(4, 1fr); } }
@media (max-width: 640px) { .dash-grid { grid-template-columns: 1fr; gap: 12px; } }
.dash-block { background: white; border-radius: 10px; padding: 14px; box-shadow: 0 1px 2px rgba(0,0,0,.04); }
.dash-h { margin: 0 0 10px 0; font-size: 13px; color: #78350f; font-weight: 700; letter-spacing: .02em; }
.dash-list { list-style: decimal; padding-left: 22px; margin: 0; font-size: 13px; }
.dash-list li { padding: 3px 0; display: list-item; }
.dash-name { color: #2d3748; font-weight: 500; }
.dash-link { color: #2b6cb0; text-decoration: none; font-weight: 600; }
.dash-link:hover { text-decoration: underline; }
.dash-num { float: right; font-variant-numeric: tabular-nums; font-weight: 600; }
.dash-num.up { color: #e53e3e; }
.dash-num.down { color: #3182ce; }
.dash-cands { grid-column: 1 / -1; }
@media (min-width: 1100px) { .dash-cands { grid-column: auto; } }
.cand-list { list-style: none; padding: 0; margin: 0; font-size: 12px; }
.cand-list li { display: grid; grid-template-columns: 1fr auto; gap: 4px 8px; padding: 6px 0; border-bottom: 1px dashed #fde68a; }
.cand-list li:last-child { border-bottom: none; }
.cand-name { font-weight: 600; color: #78350f; }
.cand-cap { color: #92400e; font-variant-numeric: tabular-nums; font-weight: 600; }
.cand-top { grid-column: 1 / -1; color: #78716c; font-size: 11px; }
.dim { color: #a8a29e; font-size: 12px; font-style: italic; }
.dash-sub { font-size: 11px; font-weight: 400; color: #92400e; opacity: .7; margin-left: 8px; }
.dash-section-h { margin: 22px 0 4px 0; font-size: 14px; color: #78350f; font-weight: 700; }
.dash-section-note { margin: 0 0 12px 0; font-size: 11px; color: #92400e; opacity: .75; }
.stock-list { padding-left: 24px; }
.stock-list li { padding: 4px 0; font-size: 12px; }
.stock-link { color: #1a202c; text-decoration: none; display: grid; grid-template-columns: 1fr auto auto; gap: 6px 10px; align-items: baseline; }
.stock-link:hover .stk-name { color: #2b6cb0; }
.stk-name { font-weight: 600; }
.stk-sec { font-size: 10px; color: #92400e; background: #fef3c7; padding: 1px 6px; border-radius: 999px; white-space: nowrap; }

/* 가치주/세계1위 wide block */
.dash-wide { padding: 16px; }
.val-tbl { width: 100%; border-collapse: collapse; font-size: 12px; }
.val-tbl th { background: #f7fafc; color: #4a5568; font-size: 10px; padding: 6px; letter-spacing: .05em; text-transform: uppercase; border-bottom: 1px solid #e2e8f0; text-align: right; }
.val-tbl th:first-child { text-align: left; }
.val-tbl td { padding: 6px; border-bottom: 1px solid #f1f5f9; }
.val-tbl td.n { text-align: right; font-variant-numeric: tabular-nums; }
.val-tbl td.val-roe { color: #d97706; font-weight: 700; }
.val-tbl a.val-name { color: #2d3748; text-decoration: none; font-weight: 600; }
.val-tbl a.val-name:hover { color: #2b6cb0; }
.leader-list { list-style: none; padding: 0; margin: 0; display: grid; grid-template-columns: 1fr; gap: 6px; }
@media (min-width: 700px) { .leader-list { grid-template-columns: 1fr 1fr; } }
.leader-row { display: grid; grid-template-columns: 130px 1fr; gap: 4px 12px; padding: 8px 12px; border-radius: 8px; background: white; text-decoration: none; color: inherit; transition: background .15s; }
.leader-row:hover { background: #fef3c7; }
.leader-field { font-size: 11px; color: #92400e; font-weight: 700; padding: 2px 8px; background: #fef3c7; border-radius: 999px; align-self: start; white-space: nowrap; }
.leader-name { font-weight: 700; color: #1a202c; font-size: 13px; }
.leader-note { grid-column: 1 / -1; color: #4a5568; font-size: 11px; line-height: 1.5; }

/* ===== 투자 인사이트 ===== */
.ins-grid { display: grid; grid-template-columns: 1fr; gap: 14px; }
@media (min-width: 900px) { .ins-grid { grid-template-columns: 1fr 1fr; } }
.ins-block { padding: 14px 16px; }
.ins-h { margin: 0 0 4px 0; font-size: 14px; color: #78350f; font-weight: 700; }
.ins-h-sub { font-size: 11px; font-weight: 400; opacity: .7; margin-left: 4px; }
.ins-cond { margin: 0 0 10px 0; font-size: 11px; color: #92400e; opacity: .8; line-height: 1.5; }
.ins-tbl { width: 100%; border-collapse: collapse; font-size: 12px; }
.ins-tbl th { background: #f7fafc; color: #4a5568; font-size: 10px; padding: 5px 6px; letter-spacing: .04em; text-transform: uppercase; border-bottom: 1px solid #e2e8f0; text-align: right; }
.ins-tbl th:first-child { text-align: left; }
.ins-tbl td { padding: 5px 6px; border-bottom: 1px solid #f1f5f9; }
.ins-tbl tr:last-child td { border-bottom: none; }
.ins-tbl td.n { text-align: right; font-variant-numeric: tabular-nums; }
.ins-tbl td.ins-name { font-weight: 600; max-width: 230px; line-height: 1.3; }
.ins-name-text { color: #1a202c; }
.ins-tbl tr.row-clickable { cursor: pointer; }
.ins-tbl tr.row-clickable:hover { background: #fef3c7; }
.ins-tbl tr.row-clickable:hover .ins-name-text { color: #2b6cb0; }
.ins-tbl tr.row-clickable.row-open { background: #fef3c7; }
.ins-tbl tr.row-clickable.row-open .ins-name-text { color: #d97706; }
.ins-tbl tr.row-clickable.row-open::after { content: '▾'; color: #d97706; }
.stk-sec.stk-sec-link { text-decoration: none; cursor: pointer; }
.stk-sec.stk-sec-link:hover { background: #fde68a; }
.ins-tk { display: block; font-family: ui-monospace,Consolas,monospace; color: #a0aec0; font-size: 10px; font-weight: 400; }
.ins-good { color: #d97706; font-weight: 700; }
.ins-bad { color: #2563eb; font-weight: 600; }
.ins-yield { color: #059669; font-weight: 700; }
.ins-score { color: #7c3aed; font-weight: 700; }

/* ===== 섹터 상관계수 패널 ===== */
.corr-panel { margin-top: 14px; padding-top: 12px; border-top: 1px dashed #cbd5e0; }
.corr-panel summary { cursor: pointer; font-size: 13px; font-weight: 600; padding: 8px 14px; background: #f0f9ff; color: #0369a1; border-radius: 10px; display: inline-block; user-select: none; list-style: none; }
.corr-panel summary::-webkit-details-marker { display: none; }
.corr-panel summary:hover { background: #bae6fd; }
.corr-panel[open] summary { background: #0369a1; color: white; margin-bottom: 12px; }
.corr-panel summary .dim { color: inherit; opacity: .7; font-style: normal; }
.corr-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; margin-top: 8px; }
@media (max-width: 640px) { .corr-grid { grid-template-columns: 1fr; } }
.corr-h { font-size: 11px; font-weight: 700; color: #0369a1; text-transform: uppercase; letter-spacing: .05em; margin-bottom: 6px; }
.corr-tbl { width: 100%; border-collapse: collapse; font-size: 13px; }
.corr-tbl td { padding: 5px 8px; border-bottom: 1px solid #f1f5f9; }
.corr-tbl td.corr-name { font-weight: 500; }
.corr-tbl td.corr-name a { color: #2b6cb0; text-decoration: none; }
.corr-tbl td.corr-name a:hover { text-decoration: underline; }
.corr-tbl td.corr-num { text-align: right; font-variant-numeric: tabular-nums; font-weight: 600; width: 70px; }
.corr-pos { color: #d97706; }
.corr-mid { color: #64748b; }
.corr-neg { color: #2563eb; }
.corr-note { font-size: 11px; color: #64748b; margin-top: 10px; padding: 8px 10px; background: #f8fafc; border-radius: 6px; line-height: 1.5; }

/* ===== 종목 주력사업 details ===== */
.biz-panel { margin: 6px 0 10px 0; }
.biz-panel summary { cursor: pointer; font-size: 12px; padding: 6px 10px; background: #f0fdf4; color: #166534; border-radius: 8px; user-select: none; list-style: none; transition: background .15s; }
.biz-panel summary::-webkit-details-marker { display: none; }
.biz-panel summary:hover { background: #bbf7d0; }
.biz-panel[open] summary { background: #166534; color: white; margin-bottom: 8px; }
.biz-panel summary b { color: inherit; }
.biz-panel summary .dim { color: inherit; opacity: .65; font-style: normal; font-size: 11px; }
.biz-points { margin: 0; padding: 8px 12px 8px 24px; background: #f9fafb; border-radius: 6px; font-size: 12px; line-height: 1.65; color: #334155; }
.biz-points li { margin-bottom: 6px; }
.biz-points li:last-child { margin-bottom: 0; }
.biz-subblock { margin-top: 10px; padding: 10px 12px; background: #f9fafb; border-radius: 6px; }
.biz-sub-title { font-size: 11px; font-weight: 700; color: #166534; text-transform: uppercase; letter-spacing: .05em; margin-bottom: 6px; }
.biz-subblock-details { margin-top: 10px; padding: 10px 12px; background: #f9fafb; border-radius: 6px; }
.biz-subblock-details summary { cursor: pointer; font-size: 11px; font-weight: 700; color: #475569; user-select: none; list-style: none; }
.biz-subblock-details summary::-webkit-details-marker { display: none; }
.biz-subblock-details[open] summary { color: #1a202c; margin-bottom: 8px; }

/* ===== 클릭 분석 카드 (확장패널 row 클릭 시) ===== */
.row-clickable { cursor: pointer; transition: background .15s; }
.row-clickable:hover { background: #fef3c7 !important; }
.row-clickable .row-arrow { font-size: 10px; color: #a0aec0; margin-left: 4px; transition: transform .2s; }
.row-clickable.row-open { background: #fef3c7 !important; }
.row-clickable.row-open .row-arrow { transform: rotate(180deg); color: #d97706; }
tr.analysis-row { background: #fffbeb !important; }
tr.analysis-row > td { padding: 0 !important; border-bottom: 2px solid #fde68a !important; }
.analysis-card { padding: 16px; }
.analysis-head { display: flex; align-items: baseline; gap: 10px; margin-bottom: 10px; flex-wrap: wrap; }
.analysis-head .a-name { font-size: 16px; font-weight: 700; color: #1a202c; }
.analysis-head .a-code { font-family: ui-monospace,Consolas,monospace; color: #a0aec0; font-size: 12px; }
.analysis-head .a-biz-h { color: #166534; font-weight: 600; font-size: 13px; padding: 2px 10px; background: #dcfce7; border-radius: 999px; }
.analysis-chart { width: 100%; min-height: 280px; margin-bottom: 12px; }
.analysis-kpis { display: flex; flex-direction: column; gap: 4px; margin-bottom: 12px; padding: 10px 12px; background: white; border-radius: 8px; }
.analysis-kpi-line { display: flex; flex-wrap: wrap; gap: 6px; align-items: center; }
.a-period-tag { background: #ebf4ff; color: #2c5282; border-radius: 999px; padding: 2px 10px; font-size: 11px; font-weight: 500; }
.a-period-tag.q { background: #f0fff4; color: #276749; }
.a-period-tag .est-mark { color: #805ad5; margin-left: 2px; font-weight: 700; }
.a-kpi { background: #f7fafc; border: 1px solid #e2e8f0; border-radius: 999px; padding: 2px 10px; font-size: 11px; color: #4a5568; }
.a-kpi b { color: #2d3748; margin-right: 3px; }
.a-kpi .est { color: #805ad5; font-weight: 600; }
.analysis-fin { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-bottom: 12px; }
@media (max-width: 700px) { .analysis-fin { grid-template-columns: 1fr; } }
.a-fin-tbl-wrap { background: white; border-radius: 8px; padding: 10px; }
.a-fin-tbl-title { font-size: 11px; font-weight: 700; color: #718096; text-transform: uppercase; letter-spacing: .05em; margin-bottom: 6px; }
.a-fin-tbl { width: 100%; border-collapse: collapse; font-size: 11px; font-variant-numeric: tabular-nums; }
.a-fin-tbl th, .a-fin-tbl td { padding: 3px 6px; text-align: right; border-bottom: 1px solid #f1f5f9; }
.a-fin-tbl th { background: #f7fafc; font-weight: 600; color: #4a5568; font-size: 10px; }
.a-fin-tbl th.est { color: #a0aec0; font-style: italic; }
.a-fin-tbl td.rlabel { text-align: left; color: #4a5568; font-weight: 500; }
.a-fin-tbl td.est { color: #a0aec0; }
.analysis-biz { background: #f9fafb; border-radius: 8px; padding: 10px 12px; }
.analysis-biz .a-biz-title { font-size: 11px; font-weight: 700; color: #166534; text-transform: uppercase; letter-spacing: .05em; margin-bottom: 6px; }
.analysis-biz ul { margin: 0; padding-left: 18px; font-size: 12px; line-height: 1.65; color: #334155; }
.analysis-biz li { margin-bottom: 6px; }
.analysis-empty { padding: 24px; text-align: center; color: #a0aec0; font-size: 12px; font-style: italic; }

/* 매출구성 */
.analysis-products { background: white; border-radius: 8px; padding: 10px 12px; margin-bottom: 12px; }
.prod-tbl { width: 100%; border-collapse: collapse; font-size: 12px; }
.prod-tbl td { padding: 5px 6px; border-bottom: 1px solid #f1f5f9; vertical-align: middle; }
.prod-tbl tr:last-child td { border-bottom: none; }
.prod-name { color: #334155; max-width: 280px; word-break: keep-all; }
.prod-ratio { text-align: right; font-variant-numeric: tabular-nums; font-weight: 700; color: #2b6cb0; width: 70px; white-space: nowrap; }
.prod-barcell { width: 40%; min-width: 80px; padding-left: 8px !important; }
.prod-bar { background: #f1f5f9; border-radius: 4px; height: 8px; overflow: hidden; }
.prod-bar-fill { background: linear-gradient(90deg,#3182ce,#63b3ed); height: 100%; border-radius: 4px; }

/* R&D 배지 */
.analysis-rnd { margin-bottom: 10px; }
.rnd-badge { display: inline-block; padding: 4px 12px; background: #ede9fe; color: #5b21b6; border-radius: 999px; font-size: 12px; }
.rnd-badge b { color: #4c1d95; margin-right: 4px; }

/* 자회사 */
.analysis-subs { background: white; border-radius: 8px; padding: 10px 12px; margin-top: 12px; }
.analysis-subs summary { cursor: pointer; font-size: 12px; font-weight: 600; color: #475569; user-select: none; list-style: none; padding: 4px 0; }
.analysis-subs summary::-webkit-details-marker { display: none; }
.analysis-subs[open] summary { color: #1a202c; margin-bottom: 8px; }
.sub-tbl { width: 100%; border-collapse: collapse; font-size: 11px; }
.sub-tbl th { background: #f7fafc; padding: 6px; text-align: left; color: #4a5568; font-size: 10px; letter-spacing: .05em; text-transform: uppercase; border-bottom: 1px solid #e2e8f0; }
.sub-tbl td { padding: 6px; border-bottom: 1px solid #f1f5f9; }
.sub-tbl td.sub-name { font-weight: 500; color: #334155; }
.sub-tbl td.sub-biz { color: #64748b; }
.sub-tbl td.sub-founded { color: #94a3b8; font-variant-numeric: tabular-nums; white-space: nowrap; width: 70px; }
"""

ANALYSIS_JS = r"""
(function(){
  function fmtEok(v) {
    if (v == null) return '-';
    var n = Number(v);
    if (!isFinite(n)) return '-';
    if (Math.abs(n) >= 10000) return (n/10000).toFixed(1) + '조';
    if (Math.abs(n) >= 100) return Math.round(n).toLocaleString() + '억';
    return n.toFixed(1) + '억';
  }
  function fmtNum(v, d) {
    if (v == null) return '-';
    var n = Number(v);
    if (!isFinite(n)) return '-';
    return n.toFixed(d == null ? 2 : d);
  }
  function fmtPct(v) {
    if (v == null) return '-';
    var n = Number(v);
    if (!isFinite(n)) return '-';
    return n.toFixed(2) + '%';
  }
  function fmtInt(v) {
    if (v == null) return '-';
    var n = Number(v);
    if (!isFinite(n)) return '-';
    return Math.round(n).toLocaleString();
  }
  function dateFromInt(d) {
    var s = String(d);
    return new Date(+s.slice(0,4), +s.slice(4,6) - 1, +s.slice(6,8));
  }

  function kpiLine(k, mode) {
    if (!k) return '';
    var label = mode === 'annual' ? '연간' : '분기';
    var tagCls = mode === 'annual' ? 'a-period-tag' : 'a-period-tag q';
    var pa = k.pa || '-', pe = k.pe || '-';
    var fields = [
      ['per','PER',function(v){return fmtNum(v);}],
      ['pbr','PBR',function(v){return fmtNum(v);}],
      ['eps','EPS',function(v){return fmtInt(v);}],
      ['roe','ROE',function(v){return fmtPct(v);}],
      ['dividend_yield','배당',function(v){return fmtPct(v);}]
    ];
    var badges = [];
    fields.forEach(function(f){
      var v = k[f[0]];
      var ve = k[f[0]+'_est'];
      if (v == null && ve == null) return;
      var body;
      if (v != null && ve != null) body = f[2](v) + ' <span class="est">→ ' + f[2](ve) + 'E</span>';
      else if (v != null) body = f[2](v);
      else body = '<span class="est">' + f[2](ve) + 'E</span>';
      badges.push('<span class="a-kpi"><b>'+f[1]+'</b> '+body+'</span>');
    });
    if (!badges.length && pa === '-' && pe === '-') return '';
    var tag = '<span class="'+tagCls+'"><b>'+label+'</b> '+pa+' → '+pe+'<span class="est-mark">E</span></span>';
    return '<div class="analysis-kpi-line">'+tag+badges.join('')+'</div>';
  }

  function finTable(title, blk) {
    if (!blk || !blk.p || !blk.p.length) return '';
    var heads = blk.p.map(function(p, i){
      var est = blk.e && blk.e[i] ? ' class="est"' : '';
      var suffix = blk.e && blk.e[i] ? ' (E)' : '';
      return '<th'+est+'>'+p+suffix+'</th>';
    }).join('');
    function row(label, arr) {
      if (!arr) return '';
      var cells = arr.map(function(v, i){
        var est = blk.e && blk.e[i] ? ' class="est"' : '';
        return '<td'+est+'>'+fmtEok(v)+'</td>';
      }).join('');
      return '<tr><td class="rlabel">'+label+'</td>'+cells+'</tr>';
    }
    return ('<div class="a-fin-tbl-wrap">'
      + '<div class="a-fin-tbl-title">'+title+'</div>'
      + '<table class="a-fin-tbl"><thead><tr><th></th>'+heads+'</tr></thead>'
      + '<tbody>'+row('매출', blk.r)+row('영업', blk.op)+row('순이익', blk.np)+'</tbody></table></div>');
  }

  function buildCard(ticker, d) {
    var bizHead = (d.b && d.b.h) ? '<span class="a-biz-h">'+d.b.h+'</span>' : '';
    var head = ('<div class="analysis-head">'
      + '<span class="a-name">'+(d.n||ticker)+'</span>'
      + '<span class="a-code">'+ticker+'</span>'
      + bizHead + '</div>');

    var hasHist = d.h && d.h.length > 1;
    var chart = hasHist ? '<div class="analysis-chart" id="achart-'+ticker+'"></div>'
                        : '<div class="analysis-empty">가격 이력 데이터 없음 (확장 백필 진행 중)</div>';

    var kpis = '';
    if (d.ka || d.kq) {
      kpis = '<div class="analysis-kpis">' + kpiLine(d.ka,'annual') + kpiLine(d.kq,'quarter') + '</div>';
    }

    var fin = '';
    if (d.fa || d.fq) {
      fin = '<div class="analysis-fin">' + finTable('연간', d.fa) + finTable('분기', d.fq) + '</div>';
    }

    var biz = '';
    if (d.b && d.b.p && d.b.p.length) {
      var pts = d.b.p.map(function(p){return '<li>'+p+'</li>';}).join('');
      var dateStr = d.b.d ? ' <span style="color:#a0aec0;font-weight:400;">['+d.b.d+']</span>' : '';
      biz = ('<div class="analysis-biz">'
        + '<div class="a-biz-title">주력사업·실적'+dateStr+'</div>'
        + '<ul>'+pts+'</ul></div>');
    }

    // 매출구성
    var products = '';
    if (d.b && d.b.pr && d.b.pr.length) {
      var rows = d.b.pr.map(function(p){
        var ratio = Number(p.ratio);
        var bar = isFinite(ratio) && ratio > 0
          ? '<div class="prod-bar"><div class="prod-bar-fill" style="width:'+Math.min(100, ratio)+'%"></div></div>'
          : '';
        return '<tr><td class="prod-name">'+(p.name||'')+'</td>'
             +'<td class="prod-ratio">'+(isFinite(ratio)?ratio.toFixed(2)+'%':'-')+'</td>'
             +'<td class="prod-barcell">'+bar+'</td></tr>';
      }).join('');
      products = ('<div class="analysis-products">'
        + '<div class="a-biz-title">매출 구성</div>'
        + '<table class="prod-tbl"><tbody>'+rows+'</tbody></table></div>');
    }

    // R&D
    var rnd = '';
    if (d.b && d.b.rd != null) {
      var rdy = d.b.rdy ? ' ('+d.b.rdy+')' : '';
      rnd = '<span class="rnd-badge"><b>R&amp;D 비중'+rdy+'</b> '+Number(d.b.rd).toFixed(2)+'%</span>';
    }

    // 자회사
    var subs = '';
    if (d.b && d.b.sub && d.b.sub.length) {
      var rows = d.b.sub.map(function(s){
        return '<tr><td class="sub-name">'+(s.name||'')+'</td>'
             +'<td class="sub-biz">'+(s.biz||'')+'</td>'
             +'<td class="sub-founded">'+(s.founded||'')+'</td></tr>';
      }).join('');
      subs = ('<details class="analysis-subs"><summary>▾ 주요 자회사 ('+d.b.sub.length+'개)</summary>'
        + '<table class="sub-tbl"><thead><tr><th>회사명</th><th>주요사업</th><th>설립</th></tr></thead>'
        + '<tbody>'+rows+'</tbody></table></details>');
    }

    var rndWrap = rnd ? '<div class="analysis-rnd">'+rnd+'</div>' : '';
    return '<div class="analysis-card">'+head+chart+kpis+rndWrap+products+fin+biz+subs+'</div>';
  }

  function renderChart(ticker, d) {
    if (!d.h || d.h.length < 2 || !window.Plotly) return;
    var dates = d.h.map(function(p){ return dateFromInt(p[0]); });
    var closes = d.h.map(function(p){ return p[1]; });
    var trace = {
      x: dates, y: closes, mode: 'lines', name: d.n || ticker,
      line: {width: 2, color: '#2b6cb0'},
      hovertemplate: '%{x|%Y-%m-%d}<br>종가 %{y:,}원<extra></extra>'
    };
    Plotly.newPlot('achart-'+ticker, [trace], {
      height: 260,
      margin: {l: 50, r: 20, t: 30, b: 30},
      xaxis: {
        type: 'date', showgrid: false,
        rangeselector: {
          buttons: [
            {count:1, label:'1M', step:'month', stepmode:'backward'},
            {count:6, label:'6M', step:'month', stepmode:'backward'},
            {count:1, label:'1Y', step:'year', stepmode:'backward'},
            {step:'all', label:'ALL'}
          ],
          bgcolor:'#f0f4f8', activecolor:'#2b6cb0', x:0.5, xanchor:'center', y:1.15, font:{size:10}
        },
        rangeslider:{visible:false}
      },
      yaxis: {gridcolor: '#edf2f7', zeroline: false, title: '종가 (원)'},
      paper_bgcolor: 'white', plot_bgcolor: '#fafbfc',
      font: {family: 'system-ui,-apple-system,Segoe UI,sans-serif', size: 11}
    }, {displayModeBar: false, responsive: true});
  }

  window.showStockAnalysis = function(ticker, rowEl) {
    if (!rowEl) return;
    // 토글
    var next = rowEl.nextElementSibling;
    if (next && next.classList && next.classList.contains('analysis-row')
        && next.dataset.ticker === ticker) {
      next.remove();
      rowEl.classList.remove('row-open');
      return;
    }
    // 페이지 전체에서 열려 있는 분석 카드 모두 닫기 (중복 차트 ID 방지)
    document.querySelectorAll('tr.analysis-row').forEach(function(el){
      var prev = el.previousElementSibling;
      if (prev && prev.classList) prev.classList.remove('row-open');
      el.remove();
    });
    var d = window.SD && window.SD[ticker];
    if (!d) {
      // 데이터 없는 경우에도 빈 카드 표시
      d = {n: rowEl.querySelector('.name') ? rowEl.querySelector('.name').firstChild.textContent.trim() : ticker, h:[]};
    }
    // 분석 row 삽입
    var cols = rowEl.children.length || 6;
    var tr = document.createElement('tr');
    tr.className = 'analysis-row';
    tr.dataset.ticker = ticker;
    var td = document.createElement('td');
    td.colSpan = cols;
    td.innerHTML = buildCard(ticker, d);
    tr.appendChild(td);
    rowEl.parentNode.insertBefore(tr, rowEl.nextSibling);
    rowEl.classList.add('row-open');

    // Plotly 렌더링은 DOM 추가 후
    setTimeout(function(){ renderChart(ticker, d); }, 0);
  };
})();
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

    biz_data: dict = {}
    if BIZ.exists():
        try:
            biz_data = json.loads(BIZ.read_text(encoding="utf-8"))
        except Exception:
            biz_data = {}

    leaders: list[dict] = []
    if LEADERS.exists():
        try:
            leaders = json.loads(LEADERS.read_text(encoding="utf-8")).get("leaders", [])
        except Exception:
            leaders = []

    fund_extra_df = pd.read_csv(FUND_EXTRA, dtype={"ticker": str}) if FUND_EXTRA.exists() else pd.DataFrame()
    fin_extra_df = pd.read_csv(FIN_EXTRA, dtype={"ticker": str}) if FIN_EXTRA.exists() else pd.DataFrame()
    prices_extra_df = pd.read_csv(PRICES_EXTRA, dtype={"ticker": str, "date": str}) if PRICES_EXTRA.exists() else pd.DataFrame()
    if not prices_extra_df.empty:
        prices_extra_df["close"] = prices_extra_df["close"].astype(int)

    try:
        listing = fdr.StockListing("KRX")
        listing["Code"] = listing["Code"].astype(str)
    except Exception:
        listing = pd.DataFrame()

    # 섹터 상관계수 (1회 계산)
    corr_matrix = sector_correlation(df, days=60)

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
            biz = biz_data.get(r["ticker"])
            fin_blocks.append(_fin_block(r["ticker"], r["name"], fund, sub, biz))

        corr_html = _correlation_panel(corr_matrix, sector, sectors_sorted)
        expand = _expand_block(sector, snap["ticker"].tolist(), indmap, thememap, listing)
        blocks.append(
            f'<section class="card" id="sec-{i}">'
            f'<h2>{sector}</h2>'
            f'<div class="grid">'
            f'<div class="table-wrap">{tbl}</div>'
            f'<div class="chart-wrap">{chart}</div>'
            f'</div>'
            f'<div class="fin-block">{"".join(fin_blocks)}</div>'
            f'{corr_html}'
            f'{expand}'
            f'</section>'
        )

    # 클릭 분석 카드용 데이터 빌드 + JS 임베드
    stock_data = build_stock_data(
        prices_df=df,
        prices_extra_df=prices_extra_df,
        fund_df=fund_df,
        fund_extra_df=fund_extra_df,
        fin_df=fin_df,
        fin_extra_df=fin_extra_df,
        biz_data=biz_data,
    )
    print(f"분석 데이터: {_sd_stats(stock_data)}")
    sd_json = json.dumps(stock_data, ensure_ascii=False, separators=(",", ":"))

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
<main>
<div class="grid-cards">
{_dashboard_card(df, latest, indmap, listing, fund_df, fund_extra_df, leaders, prices_extra_df)}
{''.join(blocks)}
</div>
</main>
<footer>생성: {generated} · 가격: FinanceDataReader · 실적: 네이버 금융 · 차트: Plotly · 단위: 매출/영업/순이익 = 억원(1조=10000억)</footer>
<script>
window.SD = {sd_json};
window.SECTORS_SORTED = {json.dumps(sectors_sorted, ensure_ascii=False)};
</script>
<script>{ANALYSIS_JS}</script>
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
