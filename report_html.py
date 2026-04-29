# -*- coding: utf-8 -*-
"""
data/strength.json + search_index.json + categories.json
+ news_analysis.json + market_overview.json + fundamentals.csv
→ 단일 report.html (PWA, 모바일 친화).
"""
import json
import sys
import webbrowser
from pathlib import Path

import pandas as pd

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

HERE = Path(__file__).parent
STRENGTH = HERE / "data" / "strength.json"
SEARCH = HERE / "data" / "search_index.json"
CATEGORIES = HERE / "data" / "categories.json"
NEWS = HERE / "data" / "news_analysis.json"
OVERVIEW = HERE / "data" / "market_overview.json"
RECOMMEND = HERE / "data" / "recommend.json"
CHART = HERE / "data" / "chart_data.json"
CHART_5Y = HERE / "data" / "chart_5y.json"
CALENDAR = HERE / "data" / "calendar.json"
VALUATION = HERE / "data" / "valuation.json"
DAILY_REPORTS = HERE / "data" / "daily_reports.json"
FUND = HERE / "data" / "fundamentals.csv"
OUT = HERE / "report.html"


def _load_fundamentals() -> dict:
    if not FUND.exists():
        return {}
    df = pd.read_csv(FUND, dtype={"ticker": str})
    cols = ["per", "pbr", "eps", "bps", "roe", "op_margin", "net_margin",
            "dividend_yield", "debt_ratio", "per_est", "roe_est"]
    out = {}
    for _, r in df.iterrows():
        t = str(r.get("ticker") or "").zfill(6)
        if not t:
            continue
        d = {}
        for c in cols:
            v = r.get(c)
            if pd.notna(v):
                try:
                    d[c] = round(float(v), 2)
                except Exception:
                    pass
        if d:
            out[t] = d
    return out


def _load_per_history() -> dict:
    """ticker별 분기 PER 시계열. (period: 'YYYY.MM' → per)"""
    out: dict[str, dict] = {}
    for path in [HERE / "data" / "financials.csv", HERE / "data" / "financials_extra.csv"]:
        if not path.exists():
            continue
        try:
            df = pd.read_csv(path, dtype={"ticker": str, "period": str})
        except Exception:
            continue
        df["ticker"] = df["ticker"].str.zfill(6)
        sub = df[(df["metric"] == "per") & (df["period_type"] == "quarterly") & (df["is_estimate"] == 0)]
        for t, g in sub.groupby("ticker"):
            g = g.sort_values("period")
            pers = []
            for _, r in g.iterrows():
                v = r["value"]
                if pd.notna(v) and float(v) > 0:
                    pers.append({"p": str(r["period"]), "v": round(float(v), 2)})
            if pers:
                out[t] = pers
    return out


def _load_json(p: Path, default):
    if not p.exists():
        return default
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return default


HTML_TEMPLATE = r"""<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<title>주식추적기 · {ref_label}</title>
<link rel="manifest" href="./manifest.json">
<meta name="theme-color" content="#0f172a">
<link rel="icon" href="./icon.svg" type="image/svg+xml">
<style>
:root {{
  --bg:#0f172a; --panel:#1e293b; --panel2:#172033; --line:#334155;
  --text:#e2e8f0; --muted:#94a3b8; --accent:#38bdf8;
  --up:#ef4444; --down:#3b82f6; --flat:#94a3b8;
}}
* {{ box-sizing:border-box; }}
html {{ scroll-behavior:smooth; scroll-padding-top:110px; }}
html,body {{ margin:0; padding:0; background:var(--bg); color:var(--text);
  font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","Apple SD Gothic Neo","Noto Sans KR",sans-serif;
  font-size:15px; line-height:1.5; -webkit-text-size-adjust:100%; }}
header {{ position:sticky; top:0; z-index:10; background:var(--bg); padding:10px 12px 8px; border-bottom:1px solid var(--line); }}
.title {{ display:flex; align-items:baseline; justify-content:space-between; margin-bottom:8px; }}
.title h1 {{ font-size:18px; margin:0; font-weight:700; }}
.title .date {{ font-size:12px; color:var(--muted); }}
.search-wrap {{ position:relative; }}
#searchInput {{ width:100%; padding:10px 36px 10px 12px; background:var(--panel); border:1px solid var(--line);
  border-radius:8px; color:var(--text); font-size:15px; outline:none; }}
#searchInput:focus {{ border-color:var(--accent); }}
#clearBtn {{ position:absolute; right:8px; top:50%; transform:translateY(-50%); background:none; border:none; color:var(--muted);
  font-size:20px; cursor:pointer; padding:4px 8px; display:none; }}
main {{ padding:12px; padding-bottom:40px; }}
.section {{ background:var(--panel); border:1px solid var(--line); border-radius:10px; margin-bottom:12px; overflow:hidden; }}
.section h2 {{ font-size:14px; margin:0; padding:10px 12px; background:var(--panel2); border-bottom:1px solid var(--line); display:flex; align-items:center; gap:8px; }}
.section h2 .badge {{ font-size:11px; color:var(--muted); font-weight:400; }}
.tabs {{ display:flex; gap:4px; padding:8px 8px 0; flex-wrap:wrap; }}
.tab {{ padding:6px 12px; background:transparent; border:1px solid var(--line); border-radius:6px;
  color:var(--muted); cursor:pointer; font-size:13px; }}
.tab.active {{ background:var(--accent); border-color:var(--accent); color:#0f172a; font-weight:600; }}
.list {{ list-style:none; margin:0; padding:0; }}
.list li {{ padding:10px 12px; border-bottom:1px solid var(--line); display:flex; align-items:center; gap:10px; cursor:pointer; }}
.list li:last-child {{ border-bottom:none; }}
.list li:hover {{ background:var(--panel2); }}
.rank {{ width:22px; text-align:center; color:var(--muted); font-size:13px; flex-shrink:0; }}
.name {{ flex:1; min-width:0; }}
.name .n1 {{ font-weight:600; font-size:14px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }}
.name .n2 {{ font-size:11px; color:var(--muted); white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }}
.right {{ text-align:right; flex-shrink:0; }}
.right .price {{ font-size:13px; font-weight:600; }}
.right .ret {{ font-size:13px; font-weight:700; }}
.up {{ color:var(--up); }}
.down {{ color:var(--down); }}
.flat {{ color:var(--flat); }}
.empty {{ padding:20px; color:var(--muted); text-align:center; }}
.leaders {{ margin-top:4px; font-size:11px; color:var(--muted); white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }}
.leaders .ld-up {{ color:#fca5a5; }}
.leaders .ld-down {{ color:#93c5fd; }}
.criteria {{ font-size:11px; color:var(--muted); padding:6px 12px; border-bottom:1px solid var(--line); background:var(--panel2); }}
#resultsPanel {{ display:none; }}
#mainPanel {{ display:block; }}
#resultsPanel.show {{ display:block; }}
#mainPanel.hide {{ display:none; }}

.market-card {{ background:linear-gradient(135deg, #1e293b 0%, #172033 100%); padding:14px; }}
.market-headline {{ font-size:16px; font-weight:700; margin-bottom:6px; color:var(--accent); }}
.market-body {{ font-size:13px; color:var(--text); line-height:1.6; }}
.market-indices {{ display:flex; gap:14px; margin-top:10px; padding-top:10px; border-top:1px solid var(--line); font-size:12px; }}
.market-indices span {{ color:var(--muted); }}

#detail {{ position:fixed; left:0; right:0; bottom:0; background:var(--panel); border-top:2px solid var(--accent); border-radius:12px 12px 0 0;
  padding:14px 14px calc(14px + env(safe-area-inset-bottom)); max-height:80vh; overflow-y:auto; transform:translateY(100%); transition:transform .25s ease; z-index:20; }}
#detail.show {{ transform:translateY(0); }}
#detailClose {{ float:right; background:none; border:none; color:var(--muted); font-size:22px; cursor:pointer; line-height:1; }}
#detail h3 {{ margin:0 0 4px; font-size:18px; }}
#detail .sub {{ font-size:12px; color:var(--muted); margin-bottom:12px; }}
.stat-grid {{ display:grid; grid-template-columns:1fr 1fr; gap:8px; margin-top:8px; }}
.stat {{ background:var(--panel2); padding:8px 10px; border-radius:6px; }}
.stat .lab {{ font-size:11px; color:var(--muted); }}
.stat .val {{ font-size:14px; font-weight:600; margin-top:2px; }}
.analysis {{ background:var(--panel2); padding:10px 12px; border-radius:8px; margin-top:12px; border-left:3px solid var(--accent); }}
.analysis .head {{ font-size:13px; font-weight:700; margin-bottom:6px; color:var(--accent); }}
.analysis .reason {{ font-size:13px; padding:4px 0; border-bottom:1px dashed var(--line); }}
.analysis .reason:last-child {{ border-bottom:none; }}
.analysis .src {{ font-size:11px; color:var(--muted); margin-top:2px; }}
.news-list {{ margin-top:12px; }}
.news-list .item {{ padding:6px 0; border-bottom:1px dashed var(--line); font-size:12px; }}
.news-list .item:last-child {{ border-bottom:none; }}
.news-list .item .meta {{ color:var(--muted); margin-right:6px; }}
#backdrop {{ position:fixed; inset:0; background:rgba(0,0,0,0.5); z-index:15; display:none; }}
#backdrop.show {{ display:block; }}
.search-meta {{ padding:8px 12px; font-size:12px; color:var(--muted); border-bottom:1px solid var(--line); }}
.search-section-h {{ font-size:12px; color:var(--accent); padding:8px 12px 4px; font-weight:600; }}
.score-bars {{ display:flex; gap:4px; margin-top:3px; font-size:10px; color:var(--muted); }}
.score-bars span {{ background:var(--line); padding:1px 5px; border-radius:3px; }}
.score-bars span.s-up {{ background:#7f1d1d; color:#fee2e2; }}
.score-bars span.s-dn {{ background:#1e3a8a; color:#dbeafe; }}
.chart-wrap {{ margin-top:14px; padding:10px; background:var(--panel2); border-radius:8px; }}
.chart-wrap h4 {{ margin:0 0 8px; font-size:12px; color:var(--muted); font-weight:500; display:flex; justify-content:space-between; }}
.chart-wrap svg {{ display:block; width:100%; height:auto; }}
.chart-legend {{ font-size:10px; color:var(--muted); margin-top:6px; display:flex; flex-wrap:wrap; gap:8px; }}
.chart-legend span {{ display:inline-flex; align-items:center; gap:3px; }}
.chart-legend .dot {{ width:10px; height:2px; display:inline-block; }}
.report-card {{ padding:12px; border-bottom:1px solid var(--line); cursor:pointer; }}
.report-card:last-child {{ border-bottom:none; }}
.report-card:hover {{ background:var(--panel2); }}
.report-card .rh {{ display:flex; justify-content:space-between; align-items:baseline; gap:8px; }}
.report-card .rh .name {{ font-weight:700; font-size:14px; }}
.report-card .rh .verdict {{ font-size:11px; padding:2px 7px; border-radius:10px; background:var(--panel2); color:var(--accent); }}
.report-card .head {{ font-size:13px; color:var(--accent); margin-top:4px; font-weight:600; }}
.report-card .meta {{ font-size:11px; color:var(--muted); margin-top:2px; }}
.val-card {{ background:var(--panel2); border-radius:8px; padding:10px 12px; margin-top:12px; border-left:3px solid #fbbf24; }}
.val-card .head {{ font-size:13px; font-weight:700; color:#fbbf24; margin-bottom:6px; display:flex; justify-content:space-between; }}
.val-grid {{ display:grid; grid-template-columns:1fr 1fr; gap:5px; font-size:12px; }}
.val-row {{ display:flex; justify-content:space-between; padding:3px 0; border-bottom:1px dashed var(--line); }}
.val-row:last-child {{ border-bottom:none; }}
.val-row .lbl {{ color:var(--muted); }}
.val-row .v {{ font-weight:600; }}
.val-row .v.good {{ color:#86efac; }}
.val-row .v.bad {{ color:#fca5a5; }}
.history-section .item {{ padding:8px 12px; border-bottom:1px solid var(--line); display:flex; align-items:center; gap:10px; cursor:pointer; }}
.history-section .item:hover {{ background:var(--panel2); }}
.history-section .group-h {{ font-size:11px; color:var(--accent); padding:8px 12px 2px; font-weight:600; background:var(--panel2); }}
.history-section .empty {{ padding:20px; color:var(--muted); text-align:center; font-size:13px; }}
.history-clear {{ font-size:11px; color:var(--muted); cursor:pointer; padding:4px 8px; border:1px solid var(--line); border-radius:4px; background:none; }}
.detail-section {{ font-size:13px; padding:10px 0; border-bottom:1px dashed var(--line); }}
.detail-section:last-child {{ border-bottom:none; }}
.detail-section .label {{ font-size:11px; color:var(--accent); font-weight:600; margin-bottom:3px; }}
.tracker-bar {{ background:var(--panel2); padding:6px 10px; border-radius:6px; font-size:11px; margin-top:8px; color:var(--muted); }}
.tracker-bar b {{ color:var(--text); }}
.ch-tab {{ font-size:10px; padding:2px 8px; margin-left:3px; background:transparent; color:var(--muted); border:1px solid var(--line); border-radius:4px; cursor:pointer; }}
.ch-tab.active {{ background:var(--accent); color:#0f172a; border-color:var(--accent); font-weight:600; }}
.chart-tt {{ position:absolute; background:rgba(15,23,42,0.96); border:1px solid var(--accent); padding:6px 9px; border-radius:5px; font-size:11px; pointer-events:none; display:none; z-index:5; white-space:nowrap; line-height:1.5; box-shadow:0 2px 8px rgba(0,0,0,0.4); }}
.chart-tt b {{ color:var(--accent); }}
.bench-wrap {{ padding:8px 10px; background:var(--panel2); border-radius:6px; margin-top:10px; font-size:12px; display:flex; align-items:center; gap:8px; flex-wrap:wrap; }}
.bench-wrap input[type=date] {{ background:var(--bg); color:var(--text); border:1px solid var(--line); border-radius:4px; padding:3px 6px; font-size:12px; color-scheme:dark; }}
.bench-wrap .label {{ color:var(--muted); }}
.nav-menu {{ display:flex; gap:5px; overflow-x:auto; padding:7px 12px; background:var(--panel2); border-top:1px solid var(--line); scrollbar-width:none; -ms-overflow-style:none; }}
.nav-menu::-webkit-scrollbar {{ display:none; }}
.nav-menu a {{ flex-shrink:0; padding:5px 11px; font-size:12px; color:var(--muted); text-decoration:none; border-radius:14px; background:var(--panel); border:1px solid var(--line); white-space:nowrap; transition:all 0.15s; }}
.nav-menu a:hover, .nav-menu a:active {{ color:#0f172a; background:var(--accent); border-color:var(--accent); font-weight:600; }}
@media (min-width:600px) {{
  body {{ font-size:14px; }}
  main {{ max-width:760px; margin:0 auto; }}
  header {{ padding:12px 16px; }}
}}
</style>
</head>
<body>
<header>
  <div class="title">
    <h1>주식추적기</h1>
    <span class="date">{ref_label}</span>
  </div>
  <div class="search-wrap">
    <input id="searchInput" type="search" placeholder="종목명 / 티커 / 업종 검색" autocomplete="off" inputmode="search">
    <button id="clearBtn" type="button" aria-label="지우기">×</button>
  </div>
  <nav class="nav-menu">
    <a href="#sec-overview">시장 총평</a>
    <a href="#sec-reports">기업 리포트</a>
    <a href="#sec-calendar">일정</a>
    <a href="#sec-sectors">강세 섹터</a>
    <a href="#sec-stocks">강세 종목</a>
    <a href="#sec-reco">추천</a>
    <a href="#sec-history">내가 본 종목</a>
    <a href="#sec-cat">카테고리</a>
  </nav>
</header>

<main>
  <div id="resultsPanel">
    <div class="section">
      <h2>검색 결과 <span class="badge" id="searchMeta"></span></h2>
      <div id="searchResults"></div>
    </div>
  </div>

  <div id="mainPanel">
    <div class="section" id="overviewSection" style="display:none;">
      <a id="sec-overview"></a>
      <h2>어제 시장 총평</h2>
      <div class="market-card">
        <div class="market-headline" id="ovHeadline"></div>
        <div class="market-body" id="ovBody"></div>
        <div id="ovOutlook"></div>
        <div class="market-indices" id="ovIndices"></div>
      </div>
    </div>

    <div class="section" id="reportsSection" style="display:none;">
      <a id="sec-reports"></a>
      <h2>오늘의 기업 리포트 <span class="badge">Claude 분석</span></h2>
      <div id="reportsList"></div>
    </div>

    <div class="section" id="calendarSection" style="display:none;">
      <a id="sec-calendar"></a>
      <h2>다가오는 일정 <span class="badge">60일 / 모멘텀 영향</span></h2>
      <ul class="list" id="calendarList"></ul>
    </div>

    <div class="section">
      <a id="sec-sectors"></a>
      <h2>전일 강세 섹터 <span class="badge">79업종 · 시총가중</span></h2>
      <div class="tabs">
        <button class="tab active" data-target="sectors_top">상승 TOP 10</button>
        <button class="tab" data-target="sectors_bottom">하락 TOP 10</button>
      </div>
      <ul class="list" id="sectorList"></ul>
    </div>

    <div class="section">
      <a id="sec-stocks"></a>
      <h2>전일 강세 종목 <span class="badge">시총·거래대금 필터</span></h2>
      <div class="tabs">
        <button class="tab active" data-target="stocks_top">상승 TOP 10</button>
        <button class="tab" data-target="stocks_bottom">하락 TOP 10</button>
      </div>
      <ul class="list" id="stockList"></ul>
    </div>

    <div class="section" id="recoSection" style="display:none;">
      <a id="sec-reco"></a>
      <h2>추천 종목 <span class="badge" id="recoBadge"></span></h2>
      <div class="criteria">모멘텀 30% · 실적 25% · 차트 25% · 순환매 20% (z-score 가중합)</div>
      <ul class="list" id="recoList"></ul>
    </div>

    <div class="section history-section" id="historySection" style="display:none;">
      <a id="sec-history"></a>
      <h2>내가 본 종목 <span class="badge" id="histBadge"></span>
        <span class="badge" id="queueBadge" style="color:#fbbf24;"></span>
        <button class="history-clear" id="copyQueueBtn" type="button">큐 복사</button>
        <button class="history-clear" id="histClear" type="button">전체 지우기</button>
      </h2>
      <div id="historyList"></div>
    </div>

    <div class="section" id="categorySection" style="display:none;">
      <a id="sec-cat"></a>
      <h2>카테고리 <span class="badge" id="catBadge"></span></h2>
      <div class="tabs">
        <button class="tab active" data-cat="growth">성장주</button>
        <button class="tab" data-cat="dividend">배당주</button>
        <button class="tab" data-cat="quality">실적우량주</button>
        <button class="tab" data-cat="value">저평가</button>
        <button class="tab" data-cat="new_listing">신규상장</button>
      </div>
      <div class="criteria" id="catCriteria"></div>
      <ul class="list" id="catList"></ul>
    </div>
  </div>
</main>

<div id="backdrop"></div>
<div id="detail">
  <button id="detailClose" aria-label="닫기">×</button>
  <div id="detailBody"></div>
</div>

<script>
const STRENGTH = {strength_json};
const SEARCH = {search_json};
const FUND = {fund_json};
const NEWS = {news_json};
const OVERVIEW = {overview_json};
const CATS = {categories_json};
const RECO = {recommend_json};
const CHARTS = {chart_json};
const VAL = {valuation_json};
const REPORTS = {reports_json};
const CHARTS_5Y = {chart_5y_json};
const PER_HIST = {per_hist_json};
const CAL = {calendar_json};
const HISTORY_KEY = 'st_history_v1';
const QUEUE_KEY = 'st_report_queue_v1';

function loadQueue() {{
  try {{ return JSON.parse(localStorage.getItem(QUEUE_KEY) || '[]'); }} catch (e) {{ return []; }}
}}
function saveQueue(arr) {{
  try {{ localStorage.setItem(QUEUE_KEY, JSON.stringify(arr.slice(0, 50))); }} catch (e) {{}}
}}
function requestReport(ticker) {{
  const q = loadQueue();
  if (q.includes(ticker)) {{
    alert('이미 리포트 요청 큐에 있습니다.');
    return;
  }}
  q.push(ticker);
  saveQueue(q);
  alert(`리포트 요청 추가됨 (큐 ${{q.length}}개). 다음 PC 빌드(매일 05:00) 시 합류됩니다.\n\n동기화 안내: 휴대폰에서 요청한 경우, PC에서 "큐 복사" 버튼으로 코드를 받아 data/report_queue.json에 붙여넣으세요.`);
  renderQueueIndicator();
}}
function renderQueueIndicator() {{
  const q = loadQueue();
  const el = $('#queueBadge');
  if (el) el.textContent = q.length ? `리포트 큐 ${{q.length}}` : '';
}}

function getHistPer(ticker, dateYYYYMMDD) {{
  const arr = PER_HIST[ticker];
  if (!arr || !arr.length) return null;
  const ym = dateYYYYMMDD.slice(0,4) + '.' + dateYYYYMMDD.slice(4,6);
  let best = null;
  for (let i = 0; i < arr.length; i++) {{
    if (arr[i].p <= ym) best = arr[i];
    else break;
  }}
  return best;
}}

const $ = (s) => document.querySelector(s);
const fmtN = (v) => v == null ? '-' : Number(v).toLocaleString('ko-KR');
const fmtPct = (v) => v == null ? '-' : (v >= 0 ? '+' : '') + Number(v).toFixed(2) + '%';
const cls = (v) => v > 0 ? 'up' : v < 0 ? 'down' : 'flat';
const fmtMcap = (v) => {{
  if (!v) return '-';
  v = Number(v);
  if (v >= 1e12) return (v/1e12).toFixed(1).replace(/\.0$/,'') + '조';
  if (v >= 1e8) return (v/1e8).toFixed(0) + '억';
  return v.toLocaleString('ko-KR');
}};

if (OVERVIEW && OVERVIEW.headline) {{
  $('#overviewSection').style.display = 'block';
  $('#ovHeadline').textContent = OVERVIEW.headline;
  $('#ovBody').textContent = OVERVIEW.body || '';
  const out = OVERVIEW.outlook || {{}};
  let outHtml = '';
  if (out.short_term || out.mid_term || (out.watch_sectors||[]).length || (out.risks||[]).length) {{
    outHtml = '<div style="margin-top:12px; padding-top:10px; border-top:1px solid var(--line); font-size:13px;">';
    if (out.short_term) outHtml += `<div style="margin-bottom:8px;"><b style="color:var(--accent);">단기 전망 (1주)</b><br><span>${{out.short_term}}</span></div>`;
    if (out.mid_term) outHtml += `<div style="margin-bottom:8px;"><b style="color:var(--accent);">중기 전망 (1개월)</b><br><span>${{out.mid_term}}</span></div>`;
    if ((out.watch_sectors||[]).length) outHtml += `<div style="margin-bottom:6px;"><b style="color:#86efac;">주목 섹터:</b> ${{out.watch_sectors.join(' · ')}}</div>`;
    if ((out.risks||[]).length) outHtml += `<div><b style="color:#fca5a5;">리스크:</b> ${{out.risks.join(' · ')}}</div>`;
    outHtml += '</div>';
  }}
  $('#ovOutlook').innerHTML = outHtml;
  const ksp = OVERVIEW.kospi || {{}};
  const ksq = OVERVIEW.kosdaq || {{}};
  const idx = (lab, d) => d.close ? `<span><b style="color:var(--text);">${{lab}}</b> ${{Number(d.close).toFixed(2)}} <span class="${{cls(d.change_pct)}}">${{fmtPct(d.change_pct)}}</span></span>` : '';
  $('#ovIndices').innerHTML = idx('KOSPI', ksp) + idx('KOSDAQ', ksq);
}}

function renderSectors(key) {{
  const arr = STRENGTH[key] || [];
  const ul = $('#sectorList');
  ul.innerHTML = arr.map((s, i) => {{
    const ldHTML = (s.leaders || []).map(l => {{
      const c = l.r > 0 ? 'ld-up' : l.r < 0 ? 'ld-down' : '';
      return `<span class="${{c}}">${{l.n}} ${{fmtPct(l.r)}}</span>`;
    }}).join(' · ');
    return `<li data-industry="${{s.name}}">
      <span class="rank">${{i+1}}</span>
      <div class="name">
        <div class="n1">${{s.name}}</div>
        <div class="leaders">${{ldHTML}}</div>
      </div>
      <div class="right">
        <div class="ret ${{cls(s.ret)}}">${{fmtPct(s.ret)}}</div>
        <div class="n2" style="color:var(--muted);font-size:11px;">${{s.size}}종목</div>
      </div>
    </li>`;
  }}).join('');
}}

function renderStocks(key) {{
  const arr = STRENGTH[key] || [];
  const ul = $('#stockList');
  ul.innerHTML = arr.map((s, i) => `
    <li data-ticker="${{s.t}}" data-strength="1">
      <span class="rank">${{i+1}}</span>
      <div class="name">
        <div class="n1">${{s.n}}</div>
        <div class="n2">${{s.t}} · ${{s.i || '-'}}</div>
      </div>
      <div class="right">
        <div class="price">${{fmtN(s.c)}}</div>
        <div class="ret ${{cls(s.r)}}">${{fmtPct(s.r)}}</div>
      </div>
    </li>`).join('');
}}

function renderCategory(key) {{
  if (!CATS) return;
  const arr = CATS[key] || [];
  const ul = $('#catList');
  $('#catBadge').textContent = `${{arr.length}}종목 · fundamentals 기반`;
  $('#catCriteria').textContent = '기준: ' + (CATS.criteria ? CATS.criteria[key] : '');
  ul.innerHTML = arr.map((s, i) => `
    <li data-ticker="${{s.t}}">
      <span class="rank">${{i+1}}</span>
      <div class="name">
        <div class="n1">${{s.n}}</div>
        <div class="n2">${{s.t}} · ${{s.i || '-'}} · ${{s.score_label || ''}}</div>
      </div>
      <div class="right">
        <div class="price">${{fmtN(s.c)}}</div>
        <div class="ret ${{cls(s.r)}}">${{s.r != null ? fmtPct(s.r) : '-'}}</div>
      </div>
    </li>`).join('') || '<li class="empty">조건 충족 종목 없음</li>';
}}

document.querySelectorAll('.section .tabs').forEach(tabs => {{
  tabs.addEventListener('click', e => {{
    const btn = e.target.closest('.tab');
    if (!btn) return;
    tabs.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    btn.classList.add('active');
    if (btn.dataset.target) {{
      const target = btn.dataset.target;
      if (target.startsWith('sectors_')) renderSectors(target);
      else renderStocks(target);
    }} else if (btn.dataset.cat) {{
      renderCategory(btn.dataset.cat);
    }}
  }});
}});

renderSectors('sectors_top');
renderStocks('stocks_top');

function renderRecommend() {{
  if (!RECO || !RECO.top) return;
  const ul = $('#recoList');
  $('#recoBadge').textContent = `TOP ${{RECO.top.length}} · 풀 ${{RECO.pool_size || '-'}}`;
  ul.innerHTML = RECO.top.map((s, i) => {{
    const sc = s.scores || {{}};
    const tag = (lab, v) => `<span class="${{v > 0 ? 's-up' : v < 0 ? 's-dn' : ''}}">${{lab}} ${{v > 0 ? '+' : ''}}${{v.toFixed(1)}}</span>`;
    return `<li data-ticker="${{s.t}}">
      <span class="rank">${{i+1}}</span>
      <div class="name">
        <div class="n1">${{s.n}}</div>
        <div class="n2">${{s.t}} · ${{s.i || '-'}}</div>
        <div class="score-bars">
          ${{tag('모멘텀', sc.momentum || 0)}}${{tag('실적', sc.fundamental || 0)}}${{tag('차트', sc.chart || 0)}}${{tag('순환', sc.rotation || 0)}}
        </div>
      </div>
      <div class="right">
        <div class="price">${{fmtN(s.c)}}</div>
        <div class="ret ${{cls(s.r)}}">${{s.r != null ? fmtPct(s.r) : ''}}</div>
        <div class="n2" style="font-size:11px; color:var(--accent); font-weight:600;">${{(s.total >= 0 ? '+' : '') + s.total}}</div>
      </div>
    </li>`;
  }}).join('');
}}

if (RECO && RECO.top && RECO.top.length) {{
  $('#recoSection').style.display = 'block';
  renderRecommend();
}}

if (CAL && CAL.events && CAL.events.length) {{
  $('#calendarSection').style.display = 'block';
  const today = new Date(); today.setHours(0,0,0,0);
  $('#calendarList').innerHTML = CAL.events.map((e, idx) => {{
    const eDate = new Date(e.date);
    const days = Math.round((eDate - today) / 86400000);
    const dayLab = days < 0 ? (e.date_end && new Date(e.date_end) >= today ? '진행중' : 'D+' + (-days)) : days === 0 ? '오늘' : `D-${{days}}`;
    const impactColor = e.impact === 'high' ? '#ef4444' : e.impact === 'mid' ? '#fbbf24' : '#94a3b8';
    const dateRange = e.date_end ? `${{e.date.slice(5)}}~${{e.date_end.slice(5)}}` : e.date.slice(5);
    const desc = e.desc ? `<div style="font-size:11px;color:var(--muted);margin-top:2px;">${{e.desc}}</div>` : '';
    const hasStocks = (e.stocks || []).length;
    const stockToggle = hasStocks ? `<div style="margin-top:6px;"><a href="#" class="cal-toggle" data-i="${{idx}}" style="font-size:11px;color:var(--accent);text-decoration:none;">관련 종목 ${{e.stocks.length}}개 ▼</a><div id="calStocks-${{idx}}" style="display:none; margin-top:6px; padding-left:6px;"></div></div>` : '';
    return `<li style="cursor:default;">
      <span class="rank" style="color:${{impactColor}};font-weight:700;font-size:11px;">${{dayLab}}</span>
      <div class="name">
        <div class="n1">${{e.title}}</div>
        <div class="n2">${{e.type}} · ${{dateRange}}</div>
        ${{desc}}
        ${{stockToggle}}
      </div>
    </li>`;
  }}).join('');

  document.querySelectorAll('.cal-toggle').forEach(a => {{
    a.addEventListener('click', ev => {{
      ev.preventDefault();
      ev.stopPropagation();
      const idx = +a.dataset.i;
      const ev_obj = CAL.events[idx];
      const box = $('#calStocks-' + idx);
      if (box.style.display === 'none') {{
        let html = '';
        (ev_obj.stocks || []).slice(0, 12).forEach(st => {{
          const live = SEARCH.stocks.find(x => x.t === st.t) || st;
          html += `<div data-ticker="${{st.t}}" style="padding:3px 4px; font-size:12px; cursor:pointer; border-bottom:1px dashed var(--line); display:flex; justify-content:space-between;">
            <span><b>${{st.n}}</b> <span style="color:var(--muted);font-size:10px;">${{st.t}} · ${{st.i || ''}}</span></span>
            <span class="${{cls(live.r)}}">${{live.r != null ? fmtPct(live.r) : ''}}</span>
          </div>`;
        }});
        box.innerHTML = html;
        box.style.display = 'block';
        a.textContent = `관련 종목 ${{ev_obj.stocks.length}}개 ▲`;
      }} else {{
        box.style.display = 'none';
        a.textContent = `관련 종목 ${{ev_obj.stocks.length}}개 ▼`;
      }}
    }});
  }});
}}

if (REPORTS && REPORTS.reports && REPORTS.reports.length) {{
  $('#reportsSection').style.display = 'block';
  $('#reportsList').innerHTML = REPORTS.reports.map(rep => {{
    if (!rep.headline) return '';
    const srcTag = rep.source ? `<span style="font-size:10px;padding:1px 6px;background:var(--panel2);color:var(--muted);border-radius:8px;margin-left:6px;">${{rep.source}}</span>` : '';
    return `<div class="report-card" data-ticker="${{rep.t}}">
      <div class="rh">
        <span class="name">${{rep.n}} <span style="color:var(--muted);font-size:11px;font-weight:400;">${{rep.t}}</span>${{srcTag}}</span>
        <span class="verdict">${{rep.verdict || '-'}}</span>
      </div>
      <div class="head">${{rep.headline}}</div>
      <div class="meta">${{rep.i || '-'}} · ${{fmtN(rep.c)}}원 ${{rep.r != null ? '<span class="' + cls(rep.r) + '">' + fmtPct(rep.r) + '</span>' : ''}}</div>
    </div>`;
  }}).join('');
}}

function loadHistory() {{
  try {{ return JSON.parse(localStorage.getItem(HISTORY_KEY) || '[]'); }} catch (e) {{ return []; }}
}}
function saveHistory(arr) {{
  try {{ localStorage.setItem(HISTORY_KEY, JSON.stringify(arr.slice(0, 200))); }} catch (e) {{}}
}}
function recordHistory(ticker) {{
  const s = SEARCH.stocks.find(x => x.t === ticker);
  if (!s) return;
  const hist = loadHistory();
  const existing = hist.find(h => h.t === ticker);
  if (existing) {{
    existing.last_seen = new Date().toISOString();
    existing.last_ref_date = SEARCH.ref_date;
  }} else {{
    hist.unshift({{
      t: ticker,
      n: s.n,
      i: s.i || '',
      first_seen: new Date().toISOString(),
      first_close: s.c,
      first_ref_date: SEARCH.ref_date,
      last_seen: new Date().toISOString(),
      last_ref_date: SEARCH.ref_date,
    }});
  }}
  saveHistory(hist);
  renderHistory();
}}

function buildSparkline(closes, w, h) {{
  if (!closes || closes.length < 2) return '';
  const min = Math.min(...closes), max = Math.max(...closes);
  const range = max - min || 1;
  const step = w / (closes.length - 1);
  const points = closes.map((c, i) => {{
    const x = (i * step).toFixed(1);
    const y = (h - (c - min) / range * h).toFixed(1);
    return (i === 0 ? 'M' : 'L') + x + ' ' + y;
  }}).join(' ');
  const stroke = closes[closes.length - 1] >= closes[0] ? '#ef4444' : '#3b82f6';
  return `<svg viewBox="0 0 ${{w}} ${{h}}" width="${{w}}" height="${{h}}" style="display:inline-block;vertical-align:middle;flex-shrink:0;"><path d="${{points}}" stroke="${{stroke}}" stroke-width="1.2" fill="none"/></svg>`;
}}

function renderHistory() {{
  const hist = loadHistory();
  const sec = $('#historySection');
  if (!hist.length) {{
    sec.style.display = 'none';
    return;
  }}
  sec.style.display = 'block';
  $('#histBadge').textContent = `${{hist.length}}종목`;
  const groups = {{}};
  hist.forEach(h => {{
    const k = h.i || '미분류';
    if (!groups[k]) groups[k] = [];
    groups[k].push(h);
  }});
  let html = '';
  Object.keys(groups).sort().forEach(g => {{
    html += `<div class="group-h">${{g}} (${{groups[g].length}})</div>`;
    groups[g].forEach(h => {{
      const live = SEARCH.stocks.find(x => x.t === h.t);
      const now = live ? live.c : null;
      const change = (now && h.first_close) ? ((now / h.first_close - 1) * 100) : null;
      const chartData = CHARTS[h.t];
      const spark = chartData ? buildSparkline(chartData.closes, 60, 26) : '';
      html += `<div class="item" data-ticker="${{h.t}}">
        <div class="name" style="flex:1; min-width:0;">
          <div class="n1">${{h.n}}</div>
          <div class="n2">${{h.t}} · 최초 ${{h.first_ref_date ? h.first_ref_date.slice(4,6)+'/'+h.first_ref_date.slice(6,8) : '-'}} ${{fmtN(h.first_close)}}원</div>
        </div>
        ${{spark}}
        <div class="right">
          <div class="price">${{fmtN(now)}}</div>
          <div class="ret ${{cls(change)}}">${{change != null ? fmtPct(change) : '-'}}</div>
        </div>
      </div>`;
    }});
  }});
  $('#historyList').innerHTML = html;
}}

$('#histClear').addEventListener('click', e => {{
  e.stopPropagation();
  if (confirm('내가 본 종목 기록을 모두 지우시겠어요?')) {{
    localStorage.removeItem(HISTORY_KEY);
    renderHistory();
  }}
}});

$('#copyQueueBtn').addEventListener('click', async e => {{
  e.stopPropagation();
  const q = loadQueue();
  if (!q.length) {{ alert('큐가 비어있습니다. 종목 상세에서 "리포트 요청" 버튼을 눌러주세요.'); return; }}
  const text = JSON.stringify(q);
  try {{
    await navigator.clipboard.writeText(text);
    if (confirm(`${{q.length}}개 종목 코드 복사됨. data/report_queue.json에 붙여넣으세요.\n\n복사 후 큐를 비우시겠어요?`)) {{
      saveQueue([]);
      renderQueueIndicator();
    }}
  }} catch (err) {{
    prompt('이 텍스트를 복사해서 data/report_queue.json에 붙여넣으세요:', text);
  }}
}});

renderHistory();
renderQueueIndicator();

function buildValuationCard(ticker) {{
  if (!VAL || !VAL.items) return '';
  const v = VAL.items[ticker];
  if (!v) return '';
  const m = v.metrics || {{}};
  const med = v.industry_med || {{}};
  const cmp = v.compare || {{}};
  const fmt = x => x == null ? '-' : x;
  const lab = (key, lower_better) => {{
    const c = cmp[key];
    if (!c || c.diff_pct == null) return '';
    const good = lower_better ? c.diff_pct > 10 : c.diff_pct > 10;
    const bad = lower_better ? c.diff_pct < -10 : c.diff_pct < -10;
    return `<span class="v ${{good ? 'good' : bad ? 'bad' : ''}}">${{c.label}}</span>`;
  }};
  const compClass = v.composite >= 0.3 ? 'good' : v.composite <= -0.3 ? 'bad' : '';
  return `<div class="val-card">
    <div class="head"><span>가치판단 — ${{v.verdict}}</span><span class="${{compClass}}">${{v.composite != null ? (v.composite > 0 ? '+' : '') + v.composite : '-'}}</span></div>
    <div style="font-size:11px;color:var(--muted);margin-bottom:6px;">동종업계: ${{v.industry || '-'}} (${{v.industry_n || '?'}}종목)</div>
    <div class="val-grid">
      <div class="val-row"><span class="lbl">PER</span><span>${{fmt(m.per)}} / 업종 ${{fmt(med.per)}}</span></div>
      <div class="val-row"><span class="lbl">PBR</span><span>${{fmt(m.pbr)}} / 업종 ${{fmt(med.pbr)}}</span></div>
      <div class="val-row"><span class="lbl">PSR</span><span>${{fmt(m.psr)}} / 업종 ${{fmt(med.psr)}}</span></div>
      <div class="val-row"><span class="lbl">ROE</span><span>${{fmt(m.roe)}}% / 업종 ${{fmt(med.roe)}}%</span></div>
      <div class="val-row"><span class="lbl">PEG</span><span>${{fmt(m.peg)}}</span></div>
      <div class="val-row"><span class="lbl">EPS 성장</span><span>${{fmt(m.eps_growth)}}%</span></div>
    </div>
    <div class="val-grid" style="margin-top:6px;">
      <div class="val-row"><span class="lbl">PER 위치</span>${{lab('per', true)}}</div>
      <div class="val-row"><span class="lbl">PBR 위치</span>${{lab('pbr', true)}}</div>
      <div class="val-row"><span class="lbl">PSR 위치</span>${{lab('psr', true)}}</div>
      <div class="val-row"><span class="lbl">ROE 위치</span>${{lab('roe', false)}}</div>
    </div>
  </div>`;
}}

function buildReportCard(ticker) {{
  if (!REPORTS || !REPORTS.reports) return '';
  const rep = REPORTS.reports.find(r => r.t === ticker);
  if (!rep || !rep.headline) return '';
  const sec = rep.sections || {{}};
  const labs = {{ value: '가치관/투자방향', earnings: '이익창출능력', products: '주력상품/매출', momentum: '모멘텀', outlook: '향후 전망' }};
  let html = `<div class="val-card" style="border-left-color:#a78bfa;">
    <div class="head" style="color:#a78bfa;">기업 리포트 — ${{rep.headline}}</div>`;
  Object.keys(labs).forEach(k => {{
    if (sec[k]) html += `<div class="detail-section"><div class="label">${{labs[k]}}</div>${{sec[k]}}</div>`;
  }});
  html += '</div>';
  return html;
}}

function setupBench(ticker) {{
  const inp = $('#benchDate-' + ticker);
  if (!inp) return;
  inp.addEventListener('change', () => updateBench(ticker, inp.value));
}}

function updateBench(ticker, dateYMD) {{
  if (!dateYMD) return;
  const ymd = dateYMD.replace(/-/g, '');
  const data = CHARTS_5Y[ticker] || CHARTS[ticker];
  const live = SEARCH.stocks.find(x => x.t === ticker);
  const out = $('#benchOut-' + ticker);
  if (!data || !live) {{ if (out) out.innerHTML = '<span style="color:var(--muted);">데이터 없음</span>'; return; }}
  let baseClose = null, baseDate = null;
  for (let i = 0; i < data.dates.length; i++) {{
    if (data.dates[i] >= ymd) {{ baseClose = data.closes[i]; baseDate = data.dates[i]; break; }}
  }}
  if (!baseClose) {{ out.innerHTML = '<span style="color:var(--muted);">5년 차트 범위 외 (시총 상위 200 풀만 지원)</span>'; return; }}
  const change = (live.c / baseClose - 1) * 100;
  const fmtD = d => d.slice(0,4)+'-'+d.slice(4,6)+'-'+d.slice(6,8);
  out.innerHTML = `<b>${{fmtD(baseDate)}}</b> ${{fmtN(baseClose)}}원 → <b>${{fmtN(live.c)}}원</b> · <span class="${{cls(change)}}"><b>${{change >= 0 ? '+' : ''}}${{change.toFixed(2)}}%</b></span>`;
}}

function buildTrackerBar(ticker) {{
  const hist = loadHistory();
  const h = hist.find(x => x.t === ticker);
  if (!h || !h.first_close) return '';
  const live = SEARCH.stocks.find(x => x.t === ticker);
  if (!live) return '';
  const change = (live.c / h.first_close - 1) * 100;
  return `<div class="tracker-bar">처음 본 시점 (<b>${{h.first_ref_date ? h.first_ref_date.slice(0,4)+'-'+h.first_ref_date.slice(4,6)+'-'+h.first_ref_date.slice(6,8) : '-'}}</b> ${{fmtN(h.first_close)}}원) 이후 <span class="${{cls(change)}}"><b>${{fmtPct(change)}}</b></span></div>`;
}}

if (CATS && (CATS.growth || CATS.dividend || CATS.quality || CATS.value)) {{
  $('#categorySection').style.display = 'block';
  renderCategory('growth');
}}

function buildSvgChart(seriesList, w, h) {{
  if (!seriesList.length) return '';
  const pad = {{ l: 4, r: 4, t: 6, b: 14 }};
  const innerW = w - pad.l - pad.r;
  const innerH = h - pad.t - pad.b;
  const norms = seriesList.map(s => {{
    if (!s.closes || s.closes.length < 2) return null;
    const base = s.closes[0];
    return s.closes.map(c => (c / base - 1) * 100);
  }});
  if (norms.every(n => !n)) return '';
  const maxLen = Math.max(...seriesList.map(s => s.closes ? s.closes.length : 0));
  let minY = Infinity, maxY = -Infinity;
  norms.forEach(n => {{
    if (!n) return;
    n.forEach(v => {{ if (v < minY) minY = v; if (v > maxY) maxY = v; }});
  }});
  if (minY === Infinity) return '';
  if (maxY - minY < 1) {{ minY -= 1; maxY += 1; }}
  const xScale = i => pad.l + (i / (maxLen - 1)) * innerW;
  const yScale = v => pad.t + (1 - (v - minY) / (maxY - minY)) * innerH;
  const zeroY = yScale(0);
  let svg = `<svg viewBox="0 0 ${{w}} ${{h}}" preserveAspectRatio="none">`;
  svg += `<line x1="${{pad.l}}" y1="${{zeroY}}" x2="${{w-pad.r}}" y2="${{zeroY}}" stroke="#475569" stroke-dasharray="2,3" stroke-width="0.5"/>`;
  seriesList.forEach((s, idx) => {{
    const n = norms[idx];
    if (!n) return;
    const path = n.map((v, i) => `${{i === 0 ? 'M' : 'L'}}${{xScale(i).toFixed(1)}} ${{yScale(v).toFixed(1)}}`).join(' ');
    svg += `<path d="${{path}}" stroke="${{s.color}}" stroke-width="${{s.bold ? 1.8 : 1}}" fill="none" opacity="${{s.bold ? 1 : 0.55}}"/>`;
  }});
  svg += `<text x="${{pad.l}}" y="${{h - 2}}" font-size="9" fill="#94a3b8">${{(minY).toFixed(1)}}%</text>`;
  svg += `<text x="${{w - pad.r}}" y="${{h - 2}}" text-anchor="end" font-size="9" fill="#94a3b8">${{(maxY).toFixed(1)}}%</text>`;
  svg += `</svg>`;
  return svg;
}}

function calcMA(closes, period) {{
  const n = closes.length;
  const out = new Array(n).fill(null);
  for (let i = period - 1; i < n; i++) {{
    let sum = 0;
    for (let j = i - period + 1; j <= i; j++) sum += closes[j];
    out[i] = sum / period;
  }}
  return out;
}}

function calcRSI(closes, period) {{
  const n = closes.length;
  const out = new Array(n).fill(null);
  if (n <= period) return out;
  let gain = 0, loss = 0;
  for (let i = 1; i <= period; i++) {{
    const d = closes[i] - closes[i-1];
    if (d > 0) gain += d; else loss -= d;
  }}
  let ag = gain / period, al = loss / period;
  out[period] = 100 - 100 / (1 + ag / (al || 1e-9));
  for (let i = period + 1; i < n; i++) {{
    const d = closes[i] - closes[i-1];
    const g = d > 0 ? d : 0, l = d < 0 ? -d : 0;
    ag = (ag * (period - 1) + g) / period;
    al = (al * (period - 1) + l) / period;
    out[i] = 100 - 100 / (1 + ag / (al || 1e-9));
  }}
  return out;
}}

function calcSignals(closes, ma, rsi) {{
  const sig = [];
  for (let i = 1; i < closes.length; i++) {{
    if (ma[i] == null || ma[i-1] == null) continue;
    const r = rsi[i];
    const cross_up = closes[i-1] < ma[i-1] && closes[i] >= ma[i];
    const cross_down = closes[i-1] > ma[i-1] && closes[i] <= ma[i];
    const oversold = r != null && r < 30;
    const overbought = r != null && r > 70;
    if (cross_up && r != null && r < 60) sig.push({{ i, type: 'buy', strength: oversold ? 2 : 1 }});
    else if (oversold && closes[i] > closes[i-1]) sig.push({{ i, type: 'buy', strength: 1 }});
    if (cross_down && r != null && r > 40) sig.push({{ i, type: 'sell', strength: overbought ? 2 : 1 }});
    else if (overbought && closes[i] < closes[i-1]) sig.push({{ i, type: 'sell', strength: 1 }});
  }}
  return sig;
}}

function buildPriceChart(series, signals, ma, dates, w, h, extraMAs) {{
  if (!series.length) return '';
  const pad = {{ l: 4, r: 42, t: 8, b: 22 }};
  const innerW = w - pad.l - pad.r;
  const innerH = h - pad.t - pad.b;
  const main = series[0];
  const allVals = [];
  series.forEach(s => s.closes.forEach(c => allVals.push(c)));
  if (ma) ma.forEach(v => v != null && allVals.push(v));
  if (extraMAs) extraMAs.forEach(em => em.values.forEach(v => v != null && allVals.push(v)));
  let minY = Math.min(...allVals), maxY = Math.max(...allVals);
  if (maxY - minY < 1) {{ minY -= 1; maxY += 1; }}
  const maxLen = main.closes.length;
  const xScale = i => pad.l + (i / (maxLen - 1)) * innerW;
  const yScale = v => pad.t + (1 - (v - minY) / (maxY - minY)) * innerH;
  const fmtD = d => {{
    if (!d) return '';
    if (d.length === 8) return d.slice(2,4) + '/' + d.slice(4,6) + '/' + d.slice(6,8);
    return d;
  }};
  const fmtPriceLabel = v => Math.round(v).toLocaleString('ko-KR');

  let svg = `<svg viewBox="0 0 ${{w}} ${{h}}" preserveAspectRatio="none">`;
  for (let g = 0; g <= 4; g++) {{
    const gy = pad.t + (innerH * g / 4);
    const gv = maxY - (maxY - minY) * g / 4;
    svg += `<line x1="${{pad.l}}" y1="${{gy}}" x2="${{w-pad.r}}" y2="${{gy}}" stroke="#334155" stroke-width="0.3" opacity="0.5"/>`;
    svg += `<text x="${{w-pad.r+3}}" y="${{gy+3}}" font-size="8" fill="#94a3b8">${{fmtPriceLabel(gv)}}</text>`;
  }}
  series.slice(1).reverse().forEach((s, idx) => {{
    const path = s.closes.map((v, i) => `${{i === 0 ? 'M' : 'L'}}${{xScale(i).toFixed(1)}} ${{yScale(v).toFixed(1)}}`).join(' ');
    svg += `<path d="${{path}}" stroke="${{s.color}}" stroke-width="0.9" fill="none" opacity="0.65"/>`;
  }});
  if (extraMAs && extraMAs.length) {{
    extraMAs.forEach(em => {{
      let mp = '';
      em.values.forEach((v, i) => {{
        if (v == null) return;
        mp += (mp ? 'L' : 'M') + xScale(i).toFixed(1) + ' ' + yScale(v).toFixed(1) + ' ';
      }});
      if (mp) svg += `<path d="${{mp}}" stroke="${{em.color}}" stroke-width="0.9" stroke-dasharray="3,2" fill="none" opacity="0.75"/>`;
    }});
  }} else if (ma) {{
    let mp = '';
    ma.forEach((v, i) => {{
      if (v == null) return;
      mp += (mp ? 'L' : 'M') + xScale(i).toFixed(1) + ' ' + yScale(v).toFixed(1) + ' ';
    }});
    if (mp) svg += `<path d="${{mp}}" stroke="#fbbf24" stroke-width="0.8" stroke-dasharray="3,2" fill="none" opacity="0.7"/>`;
  }}
  const mainPath = main.closes.map((v, i) => `${{i === 0 ? 'M' : 'L'}}${{xScale(i).toFixed(1)}} ${{yScale(v).toFixed(1)}}`).join(' ');
  svg += `<path d="${{mainPath}}" stroke="#f87171" stroke-width="1.5" fill="none"/>`;

  const lastIdx = main.closes.length - 1;
  const lastX = xScale(lastIdx), lastY = yScale(main.closes[lastIdx]);
  svg += `<circle cx="${{lastX}}" cy="${{lastY}}" r="2.2" fill="#f87171"/>`;
  svg += `<rect x="${{w-pad.r+1}}" y="${{lastY-6}}" width="40" height="12" fill="#f87171" rx="2"/>`;
  svg += `<text x="${{w-pad.r+21}}" y="${{lastY+3}}" text-anchor="middle" font-size="9" fill="#0f172a" font-weight="700">${{fmtPriceLabel(main.closes[lastIdx])}}</text>`;

  if (signals) {{
    signals.forEach(s => {{
      const x = xScale(s.i), y = yScale(main.closes[s.i]);
      const sz = s.strength === 2 ? 4 : 3;
      if (s.type === 'buy') {{
        svg += `<polygon points="${{x}},${{y+sz+1}} ${{x-sz}},${{y+sz*2+1}} ${{x+sz}},${{y+sz*2+1}}" fill="#22c55e"/>`;
      }} else {{
        svg += `<polygon points="${{x}},${{y-sz-1}} ${{x-sz}},${{y-sz*2-1}} ${{x+sz}},${{y-sz*2-1}}" fill="#ef4444"/>`;
      }}
    }});
  }}

  if (dates && dates.length) {{
    const ticks = [0, Math.floor(maxLen/4), Math.floor(maxLen/2), Math.floor(maxLen*3/4), maxLen-1];
    ticks.forEach(idx => {{
      const tx = xScale(idx);
      svg += `<line x1="${{tx}}" y1="${{pad.t}}" x2="${{tx}}" y2="${{pad.t+innerH}}" stroke="#334155" stroke-width="0.3" opacity="0.4"/>`;
      const anchor = idx === 0 ? 'start' : idx === maxLen-1 ? 'end' : 'middle';
      svg += `<text x="${{tx}}" y="${{h-6}}" text-anchor="${{anchor}}" font-size="8" fill="#94a3b8">${{fmtD(dates[idx])}}</text>`;
    }});
  }}
  svg += `</svg>`;
  return svg;
}}

function signalSummary(closes, signals) {{
  if (!signals || !signals.length) return '<span style="color:var(--muted);">시그널 없음</span>';
  const recent = signals[signals.length - 1];
  const buys = signals.filter(s => s.type === 'buy').length;
  const sells = signals.filter(s => s.type === 'sell').length;
  const last = recent.type === 'buy' ? '<span style="color:#22c55e;">매수 ▲</span>' : '<span style="color:#ef4444;">매도 ▼</span>';
  const days_ago = closes.length - 1 - recent.i;
  return `최근 시그널: ${{last}} (${{days_ago}}일 전, 강도 ${{recent.strength}}) · 60일내 매수 ${{buys}}회 / 매도 ${{sells}}회`;
}}

function buildChartSection(ticker) {{
  const main = CHARTS[ticker];
  const main5y = CHARTS_5Y[ticker];
  if (!main && !main5y) return '';
  const s = SEARCH.stocks.find(x => x.t === ticker);
  const ind = s ? s.i : '';
  const colors = ['#38bdf8', '#fbbf24', '#a78bfa', '#34d399'];

  const v = (VAL && VAL.items) ? (VAL.items[ticker] || {{}}) : {{}};
  const vm = v.metrics || {{}};
  const vmed = v.industry_med || {{}};
  const valVerdict = v.verdict || '';
  const today = STRENGTH && STRENGTH.ref_label ? STRENGTH.ref_label : '';

  let html = `<div class="chart-wrap"><h4><span>매수/매도 시그널 차트</span>
    <span>
      <button class="ch-tab active" data-mode="60d" data-tk="${{ticker}}">60일</button>
      ${{main5y ? `<button class="ch-tab" data-mode="5y" data-tk="${{ticker}}">5년</button>` : ''}}
    </span></h4>
    <div style="font-size:11px; color:var(--muted); padding:4px 0 6px; border-bottom:1px dashed var(--line); margin-bottom:6px;">
      <b style="color:var(--text);">${{today}}</b> 기준 ·
      PER <b style="color:var(--text);">${{vm.per != null ? vm.per : '-'}}</b><span style="opacity:0.7;">(업종 ${{vmed.per != null ? vmed.per : '-'}})</span> ·
      PBR <b style="color:var(--text);">${{vm.pbr != null ? vm.pbr : '-'}}</b><span style="opacity:0.7;">(업종 ${{vmed.pbr != null ? vmed.pbr : '-'}})</span> ·
      ROE <b style="color:var(--text);">${{vm.roe != null ? vm.roe + '%' : '-'}}</b> ·
      PSR <b style="color:var(--text);">${{vm.psr != null ? vm.psr : '-'}}</b>
      ${{valVerdict ? ' · <span style="color:#fbbf24;">' + valVerdict + '</span>' : ''}}
    </div>
    <div id="chartBody-${{ticker}}"></div>
    <div class="chart-legend" id="chartLegend-${{ticker}}"></div>
    <div id="signalSum-${{ticker}}" style="font-size:11px; color:var(--muted); margin-top:6px;"></div>
  </div>`;
  setTimeout(() => renderPriceChart(ticker, '60d'), 0);
  return html;
}}

function renderPriceChart(ticker, mode) {{
  const target = $('#chartBody-' + ticker);
  if (!target) return;
  const data = mode === '5y' ? CHARTS_5Y[ticker] : CHARTS[ticker];
  if (!data) {{ target.innerHTML = '<div style="color:var(--muted);font-size:11px;text-align:center;padding:10px;">차트 데이터 없음</div>'; return; }}
  const closes = data.closes;
  const period = mode === '5y' ? 13 : 20;
  const ma = calcMA(closes, period);
  const rsi = calcRSI(closes, mode === '5y' ? 9 : 14);
  const sig = calcSignals(closes, ma, rsi);

  const s = SEARCH.stocks.find(x => x.t === ticker);
  const ind = s ? s.i : '';
  const peerData = mode === '5y' ? CHARTS_5Y : CHARTS;
  const peers = ind
    ? SEARCH.stocks
        .filter(x => x.i === ind && x.t !== ticker && peerData[x.t])
        .sort((a, b) => b.m - a.m)
        .slice(0, 3)
    : [];
  const peerColors = ['#38bdf8', '#a78bfa', '#34d399'];
  const series = [
    {{ closes }},
    ...peers.map((p, i) => ({{ closes: peerData[p.t].closes, color: peerColors[i], name: p.n }}))
  ];

  const extraMAs = (mode === '5y') ? [
    {{ values: data.ma20, color: '#fbbf24', label: 'MA20' }},
    {{ values: data.ma60, color: '#22c55e', label: 'MA60' }},
    {{ values: data.ma120, color: '#a78bfa', label: 'MA120' }},
    {{ values: data.ma1000, color: '#f472b6', label: 'MA1000' }},
  ].filter(x => x.values && x.values.some(v => v != null)) : null;
  target.innerHTML = buildPriceChart(series, sig, ma, data.dates, 320, 150, extraMAs);
  attachChartTooltip(ticker, data, ma, sig);

  const legendEl = $('#chartLegend-' + ticker);
  if (legendEl) {{
    let lh = `<span><span class="dot" style="background:#f87171;"></span>${{s ? s.n : ticker}} (메인)</span>`;
    if (mode === '5y' && extraMAs && extraMAs.length) {{
      extraMAs.forEach(em => {{
        lh += `<span><span class="dot" style="background:${{em.color}};"></span>${{em.label}}</span>`;
      }});
    }} else {{
      lh += `<span><span class="dot" style="background:#fbbf24;"></span>MA20</span>`;
    }}
    lh += `<span><span style="color:#22c55e;">▲</span>매수</span><span><span style="color:#ef4444;">▼</span>매도</span>`;
    if (peers.length) {{
      lh += `<br><span style="color:var(--muted); margin-right:6px;">동종업종 비교:</span>`;
      peers.forEach((p, i) => {{
        lh += `<span><span class="dot" style="background:${{peerColors[i]}};"></span>${{p.n}}</span>`;
      }});
    }} else if (ind) {{
      lh += `<br><span style="color:var(--muted);">동종업종(${{ind}}) 비교 데이터 없음</span>`;
    }}
    legendEl.innerHTML = lh;
  }}

  const sum = $('#signalSum-' + ticker);
  if (sum) {{
    const first = closes[0], last = closes[closes.length-1];
    const totalChg = ((last/first - 1) * 100).toFixed(2);
    const hi = Math.max(...closes), lo = Math.min(...closes);
    const hiI = closes.indexOf(hi), loI = closes.indexOf(lo);
    const fmtD = d => d ? d.slice(2,4)+'/'+d.slice(4,6)+'/'+d.slice(6,8) : '-';
    sum.innerHTML = `${{signalSummary(closes, sig)}}
      <br>기간: <b style="color:var(--text);">${{fmtD(data.dates[0])}}~${{fmtD(data.dates[data.dates.length-1])}}</b> · 전체 <span class="${{cls(parseFloat(totalChg))}}"><b>${{totalChg > 0 ? '+' : ''}}${{totalChg}}%</b></span>
      · 최고 <b style="color:var(--text);">${{fmtN(hi)}}</b>(${{fmtD(data.dates[hiI])}}) · 최저 <b style="color:var(--text);">${{fmtN(lo)}}</b>(${{fmtD(data.dates[loI])}})`;
  }}
}}

document.addEventListener('click', e => {{
  const ch = e.target.closest('.ch-tab');
  if (!ch) return;
  e.stopPropagation();
  const wrap = ch.closest('.chart-wrap');
  wrap.querySelectorAll('.ch-tab').forEach(t => t.classList.remove('active'));
  ch.classList.add('active');
  renderPriceChart(ch.dataset.tk, ch.dataset.mode);
}});

const SVG_NS = 'http://www.w3.org/2000/svg';
function attachChartTooltip(ticker, data, ma, signals) {{
  const wrap = $('#chartBody-' + ticker);
  if (!wrap) return;
  wrap.style.position = 'relative';
  const svg = wrap.querySelector('svg');
  if (!svg) return;

  let tt = wrap.querySelector('.chart-tt');
  if (!tt) {{
    tt = document.createElement('div');
    tt.className = 'chart-tt';
    wrap.appendChild(tt);
  }}

  const cross = document.createElementNS(SVG_NS, 'line');
  cross.setAttribute('y1', 8);
  cross.setAttribute('y2', 128);
  cross.setAttribute('stroke', '#94a3b8');
  cross.setAttribute('stroke-dasharray', '2,2');
  cross.setAttribute('stroke-width', '0.5');
  cross.setAttribute('opacity', '0');
  svg.appendChild(cross);

  const dot = document.createElementNS(SVG_NS, 'circle');
  dot.setAttribute('r', '2.5');
  dot.setAttribute('fill', '#fbbf24');
  dot.setAttribute('stroke', '#0f172a');
  dot.setAttribute('stroke-width', '1');
  dot.setAttribute('opacity', '0');
  svg.appendChild(dot);

  const sigByIdx = {{}};
  if (signals) signals.forEach(s => {{ sigByIdx[s.i] = s; }});

  const fmtD = d => d ? d.slice(0,4) + '-' + d.slice(4,6) + '-' + d.slice(6,8) : '-';
  const fmtNN = v => v == null ? '-' : Math.round(Number(v)).toLocaleString('ko-KR');

  const handleMove = (clientX, clientY) => {{
    const rect = svg.getBoundingClientRect();
    const localX = clientX - rect.left;
    const padLpx = rect.width * 4 / 320;
    const innerWpx = rect.width * 274 / 320;
    const ratio = (localX - padLpx) / innerWpx;
    if (ratio < 0 || ratio > 1) {{
      tt.style.display = 'none';
      cross.setAttribute('opacity', '0');
      dot.setAttribute('opacity', '0');
      return;
    }}
    const n = data.closes.length;
    const i = Math.max(0, Math.min(n - 1, Math.round(ratio * (n - 1))));
    const vbX = 4 + (i / (n - 1)) * 274;

    cross.setAttribute('x1', vbX);
    cross.setAttribute('x2', vbX);
    cross.setAttribute('opacity', '0.6');

    const allVals = [];
    data.closes.forEach(c => allVals.push(c));
    if (ma) ma.forEach(v => v != null && allVals.push(v));
    let minY = Math.min(...allVals), maxY = Math.max(...allVals);
    if (maxY - minY < 1) {{ minY -= 1; maxY += 1; }}
    const innerH = 150 - 8 - 22;
    const yScale = v => 8 + (1 - (v - minY) / (maxY - minY)) * innerH;
    dot.setAttribute('cx', vbX);
    dot.setAttribute('cy', yScale(data.closes[i]));
    dot.setAttribute('opacity', '1');

    let prev = i > 0 ? data.closes[i-1] : null;
    let dayChg = prev ? ((data.closes[i] / prev - 1) * 100).toFixed(2) : null;
    let firstChg = ((data.closes[i] / data.closes[0] - 1) * 100).toFixed(2);
    const sg = sigByIdx[i];
    const fundT = (FUND || {{}})[ticker] || {{}};
    const eps = fundT.eps;
    const histRecord = getHistPer(ticker, data.dates[i]);
    let perDisp = null, perSrc = '';
    if (histRecord) {{
      perDisp = histRecord.v.toFixed(2);
      perSrc = `<span style="opacity:0.55;font-size:10px;">(${{histRecord.p}} 분기 실적)</span>`;
    }} else if (eps && eps > 0) {{
      perDisp = (data.closes[i] / eps).toFixed(2);
      perSrc = `<span style="opacity:0.55;font-size:10px;">(현 EPS ${{fmtNN(eps)}}원 기준 추정)</span>`;
    }}
    let html = `<b>${{fmtD(data.dates[i])}}</b><br>`
      + `가격 ${{fmtNN(data.closes[i])}}원`;
    if (dayChg) html += ` <span style="color:${{dayChg >= 0 ? '#fca5a5' : '#93c5fd'}};">(${{dayChg >= 0 ? '+' : ''}}${{dayChg}}%)</span>`;
    if (perDisp) html += `<br>PER ${{perDisp}} ${{perSrc}}`;
    if (ma && ma[i] != null) html += `<br>이평 ${{fmtNN(ma[i])}}원`;
    html += `<br>시작 대비 <span style="color:${{firstChg >= 0 ? '#fca5a5' : '#93c5fd'}};">${{firstChg >= 0 ? '+' : ''}}${{firstChg}}%</span>`;
    if (sg) html += `<br><b style="color:${{sg.type === 'buy' ? '#22c55e' : '#ef4444'}};">${{sg.type === 'buy' ? '매수 ▲' : '매도 ▼'}}</b> 시그널 (강도 ${{sg.strength}})`;

    tt.innerHTML = html;
    tt.style.display = 'block';

    const ttW = tt.offsetWidth || 140;
    let leftPx = localX + 8;
    if (leftPx + ttW > rect.width) leftPx = localX - ttW - 8;
    if (leftPx < 0) leftPx = 0;
    tt.style.left = leftPx + 'px';
    tt.style.top = '4px';
  }};

  svg.addEventListener('mousemove', e => handleMove(e.clientX, e.clientY));
  svg.addEventListener('mouseleave', () => {{
    tt.style.display = 'none';
    cross.setAttribute('opacity', '0');
    dot.setAttribute('opacity', '0');
  }});
  svg.addEventListener('touchmove', e => {{
    if (!e.touches[0]) return;
    handleMove(e.touches[0].clientX, e.touches[0].clientY);
    e.preventDefault();
  }}, {{ passive: false }});
  svg.addEventListener('touchend', () => {{
    setTimeout(() => {{
      tt.style.display = 'none';
      cross.setAttribute('opacity', '0');
      dot.setAttribute('opacity', '0');
    }}, 1500);
  }});
}}

const input = $('#searchInput');
const clearBtn = $('#clearBtn');
const resultsPanel = $('#resultsPanel');
const mainPanel = $('#mainPanel');
let searchTimer;

input.addEventListener('input', () => {{
  clearTimeout(searchTimer);
  const q = input.value.trim();
  clearBtn.style.display = q ? 'block' : 'none';
  if (!q) {{
    resultsPanel.classList.remove('show');
    mainPanel.classList.remove('hide');
    return;
  }}
  searchTimer = setTimeout(() => doSearch(q), 120);
}});

clearBtn.addEventListener('click', () => {{
  input.value = '';
  clearBtn.style.display = 'none';
  resultsPanel.classList.remove('show');
  mainPanel.classList.remove('hide');
  input.focus();
}});

function doSearch(q) {{
  const Q = q.toLowerCase();
  const stocks = SEARCH.stocks.filter(s =>
    s.n.toLowerCase().includes(Q) || s.t.includes(Q) || (s.i || '').toLowerCase().includes(Q)
  );
  const industries = (SEARCH.industries || []).filter(i =>
    i.name.toLowerCase().includes(Q)
  );
  stocks.sort((a, b) => b.m - a.m);
  const stocksLimit = stocks.slice(0, 100);

  let html = '';
  if (industries.length) {{
    html += `<div class="search-section-h">업종 ${{industries.length}}개</div><ul class="list">`;
    industries.slice(0, 20).forEach(ind => {{
      html += `<li data-industry="${{ind.name}}">
        <span class="rank">📁</span>
        <div class="name"><div class="n1">${{ind.name}}</div><div class="n2">${{ind.size}}종목</div></div>
        <div class="right"><div class="ret ${{cls(ind.ret)}}">${{fmtPct(ind.ret)}}</div></div>
      </li>`;
    }});
    html += '</ul>';
  }}
  if (stocks.length) {{
    html += `<div class="search-section-h">종목 ${{stocks.length}}개${{stocks.length > 100 ? ' (상위 100개)' : ''}}</div><ul class="list">`;
    stocksLimit.forEach(s => {{
      html += `<li data-ticker="${{s.t}}">
        <span class="rank"></span>
        <div class="name">
          <div class="n1">${{s.n}}</div>
          <div class="n2">${{s.t}} · ${{s.i || '-'}} · ${{fmtMcap(s.m)}}</div>
        </div>
        <div class="right">
          <div class="price">${{fmtN(s.c)}}</div>
          <div class="ret ${{cls(s.r)}}">${{fmtPct(s.r)}}</div>
        </div>
      </li>`;
    }});
    html += '</ul>';
  }}
  if (!industries.length && !stocks.length) {{
    html = '<div class="empty">결과 없음</div>';
  }}

  $('#searchResults').innerHTML = html;
  $('#searchMeta').textContent = `업종 ${{industries.length}} · 종목 ${{stocks.length}}`;
  resultsPanel.classList.add('show');
  mainPanel.classList.add('hide');
}}

document.addEventListener('click', e => {{
  if (e.target.closest('.ch-tab') || e.target.closest('#histClear') || e.target.closest('#detailClose')) return;
  const elStock = e.target.closest('[data-ticker]');
  if (elStock) {{ openStock(elStock.dataset.ticker); return; }}
  const elInd = e.target.closest('[data-industry]');
  if (elInd) {{ openIndustry(elInd.dataset.industry); return; }}
}});

function openStock(ticker) {{
  const s = SEARCH.stocks.find(x => x.t === ticker);
  if (!s) return;
  recordHistory(ticker);
  const f = FUND[ticker] || {{}};
  const news = (NEWS && NEWS.items) ? NEWS.items[ticker] : null;

  const inQueue = loadQueue().includes(ticker);
  const hasReport = REPORTS && REPORTS.reports && REPORTS.reports.find(r => r.t === ticker && r.headline);
  let html = `<h3>${{s.n}} <span style="font-size:13px;color:var(--muted);font-weight:400;">${{s.t}}</span></h3>
    <div class="sub">${{s.i || '-'}} · ${{s.mk || ''}}
      ${{!hasReport ? `<button onclick="requestReport('${{ticker}}')" style="margin-left:8px;padding:3px 9px;font-size:11px;background:${{inQueue ? 'var(--panel)' : 'var(--accent)'}};color:${{inQueue ? 'var(--muted)' : '#0f172a'}};border:1px solid var(--accent);border-radius:4px;cursor:${{inQueue ? 'default' : 'pointer'}};font-weight:600;" ${{inQueue ? 'disabled' : ''}}>${{inQueue ? '✓ 큐에 있음' : '📋 리포트 요청'}}</button>` : ''}}
    </div>`;
  html += buildTrackerBar(ticker);
  const todayISO = new Date().toISOString().slice(0, 10);
  html += `<div class="bench-wrap">
    <span class="label">기준일 변동:</span>
    <input type="date" id="benchDate-${{ticker}}" max="${{todayISO}}">
    <span id="benchOut-${{ticker}}" style="flex:1; min-width:0;"></span>
  </div>`;
  setTimeout(() => setupBench(ticker), 0);
  html += `<div class="stat-grid">
      <div class="stat"><div class="lab">종가</div><div class="val">${{fmtN(s.c)}}원</div></div>
      <div class="stat"><div class="lab">등락률</div><div class="val ${{cls(s.r)}}">${{fmtPct(s.r)}}</div></div>
      <div class="stat"><div class="lab">시총</div><div class="val">${{fmtMcap(s.m)}}</div></div>
      <div class="stat"><div class="lab">거래대금</div><div class="val">${{fmtMcap(s.a)}}</div></div>`;
  const fmap = [
    ['per','PER'], ['pbr','PBR'], ['roe','ROE(%)'], ['eps','EPS'],
    ['op_margin','영업이익률(%)'], ['net_margin','순이익률(%)'],
    ['dividend_yield','배당수익률(%)'], ['debt_ratio','부채비율(%)'],
    ['per_est','선행PER'], ['roe_est','선행ROE(%)']
  ];
  fmap.forEach(([k, lab]) => {{
    if (f[k] != null) {{
      html += `<div class="stat"><div class="lab">${{lab}}</div><div class="val">${{f[k]}}</div></div>`;
    }}
  }});
  html += '</div>';

  html += buildValuationCard(ticker);
  html += buildReportCard(ticker);
  html += buildChartSection(ticker);

  if (news && (news.summary || (news.reasons && news.reasons.length))) {{
    html += `<div class="analysis">
      <div class="head">${{news.ret >= 0 ? '강세' : '약세'}} 분석 — ${{news.summary || ''}}</div>`;
    (news.reasons || []).forEach(r => {{
      html += `<div class="reason">• ${{r.text || ''}}<div class="src">근거: ${{r.source || ''}}</div></div>`;
    }});
    if (news.news && news.news.length) {{
      html += '<div class="news-list">';
      news.news.slice(0, 5).forEach(n => {{
        html += `<div class="item"><span class="meta">${{n.date || ''}} · ${{n.press || ''}}</span>${{n.title || ''}}</div>`;
      }});
      html += '</div>';
    }}
    html += '</div>';
  }}

  html += `<div style="margin-top:12px;font-size:12px;">
      <a href="https://finance.naver.com/item/main.naver?code=${{s.t}}" target="_blank" rel="noopener" style="color:var(--accent);">네이버 금융 →</a>
    </div>`;
  $('#detailBody').innerHTML = html;
  showDetail();
}}

function openIndustry(name) {{
  const stocks = SEARCH.stocks.filter(s => s.i === name).sort((a, b) => b.m - a.m).slice(0, 30);
  const ind = (SEARCH.industries || []).find(i => i.name === name) || {{}};
  let html = `<h3>${{name}}</h3>
    <div class="sub">${{ind.size || stocks.length}}종목 · 시총가중 ${{ind.ret != null ? fmtPct(ind.ret) : '-'}}</div>
    <ul class="list" style="margin-top:8px;">`;
  stocks.forEach(s => {{
    html += `<li data-ticker="${{s.t}}">
      <span class="rank"></span>
      <div class="name"><div class="n1">${{s.n}}</div><div class="n2">${{fmtMcap(s.m)}}</div></div>
      <div class="right"><div class="price">${{fmtN(s.c)}}</div><div class="ret ${{cls(s.r)}}">${{fmtPct(s.r)}}</div></div>
    </li>`;
  }});
  html += '</ul>';
  $('#detailBody').innerHTML = html;
  showDetail();
}}

function showDetail() {{
  $('#backdrop').classList.add('show');
  $('#detail').classList.add('show');
}}
function hideDetail() {{
  $('#backdrop').classList.remove('show');
  $('#detail').classList.remove('show');
}}
$('#detailClose').addEventListener('click', hideDetail);
$('#backdrop').addEventListener('click', hideDetail);

if ('serviceWorker' in navigator) {{
  navigator.serviceWorker.register('./sw.js').catch(() => {{}});
}}
</script>
</body>
</html>
"""


def build() -> None:
    if not STRENGTH.exists() or not SEARCH.exists():
        raise SystemExit("strength.json 또는 search_index.json 없음. build_data.py 먼저 실행.")

    strength = json.loads(STRENGTH.read_text(encoding="utf-8"))
    search = json.loads(SEARCH.read_text(encoding="utf-8"))
    fund = _load_fundamentals()
    news = _load_json(NEWS, {})
    overview = _load_json(OVERVIEW, {})
    categories = _load_json(CATEGORIES, {})
    recommend = _load_json(RECOMMEND, {})
    chart = _load_json(CHART, {})
    valuation = _load_json(VALUATION, {})
    reports = _load_json(DAILY_REPORTS, {})
    chart_5y = _load_json(CHART_5Y, {})
    cal = _load_json(CALENDAR, {})
    per_hist = _load_per_history()

    html = HTML_TEMPLATE.format(
        ref_label=strength.get("ref_label", ""),
        strength_json=json.dumps(strength, ensure_ascii=False),
        search_json=json.dumps(search, ensure_ascii=False),
        fund_json=json.dumps(fund, ensure_ascii=False),
        news_json=json.dumps(news, ensure_ascii=False),
        overview_json=json.dumps(overview, ensure_ascii=False),
        categories_json=json.dumps(categories, ensure_ascii=False),
        recommend_json=json.dumps(recommend, ensure_ascii=False),
        chart_json=json.dumps(chart, ensure_ascii=False),
        valuation_json=json.dumps(valuation, ensure_ascii=False),
        reports_json=json.dumps(reports, ensure_ascii=False),
        chart_5y_json=json.dumps(chart_5y, ensure_ascii=False),
        per_hist_json=json.dumps(per_hist, ensure_ascii=False),
        calendar_json=json.dumps(cal, ensure_ascii=False),
    )
    OUT.write_text(html, encoding="utf-8")
    size_kb = OUT.stat().st_size / 1024
    print(f"리포트 생성: {OUT} ({size_kb:,.0f} KB)")


if __name__ == "__main__":
    no_open = "--no-open" in sys.argv
    build()
    if not no_open:
        webbrowser.open(OUT.as_uri())
