# -*- coding: utf-8 -*-
"""
누적 종가 CSV를 보기 좋게 출력.

사용법:
    python view.py                  # 최신일 스냅샷 (섹터 카드)
    python view.py 5                # 피벗 — 최근 5일 × 종목
    python view.py 5 반도체         # 피벗 — 섹터명 부분일치 필터
    python view.py snap 반도체      # 스냅샷 — 섹터 필터
"""
import sys
from pathlib import Path

import pandas as pd
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

HERE = Path(__file__).parent
DATA = HERE / "data" / "prices.csv"
console = Console()


def _load() -> pd.DataFrame:
    df = pd.read_csv(DATA, dtype={"ticker": str, "date": str})
    return df


def _color(chg: float) -> tuple[str, str, str]:
    """상승=빨강(한국 관례), 하락=파랑, 보합=회색."""
    if chg > 0:
        return "red", "▲", "+"
    if chg < 0:
        return "blue", "▼", ""
    return "grey50", "·", ""


def snapshot(keyword: str | None = None) -> None:
    df = _load()
    latest = df["date"].max()
    today = df[df["date"] == latest].copy()
    if keyword:
        today = today[today["sector"].str.contains(keyword, na=False)]
        if today.empty:
            console.print(f"[yellow]'{keyword}' 포함 섹터 없음[/]")
            return

    up = int((today["change_pct"] > 0).sum())
    dn = int((today["change_pct"] < 0).sum())
    fl = int((today["change_pct"] == 0).sum())
    d_iso = f"{latest[:4]}-{latest[4:6]}-{latest[6:]}"

    header = Text()
    header.append("주식추적기  ", style="bold")
    header.append(f"{d_iso}  │  ", style="dim")
    header.append(f"{today['sector'].nunique()}개 섹터 / {len(today)}종목   ", style="white")
    header.append(f"▲ {up}  ", style="red")
    header.append(f"▼ {dn}  ", style="blue")
    header.append(f"· {fl}", style="grey50")
    console.print(Panel(header, box=box.DOUBLE, border_style="bright_white"))

    for sector, g in today.groupby("sector", sort=True):
        g = g.sort_values("close", ascending=False)
        t = Table(
            box=box.ROUNDED,
            title=f"[bold cyan]{sector}[/]",
            title_justify="left",
            show_header=True,
            header_style="bold dim",
            pad_edge=False,
            expand=False,
        )
        t.add_column("종목", justify="left", no_wrap=True)
        t.add_column("코드", justify="center", style="dim")
        t.add_column("종가", justify="right")
        t.add_column("등락률", justify="right")
        for _, r in g.iterrows():
            color, arrow, sign = _color(r["change_pct"])
            t.add_row(
                r["name"],
                r["ticker"],
                f"{int(r['close']):,}",
                f"[{color}]{arrow} {sign}{r['change_pct']:.2f}%[/{color}]",
            )
        console.print(t)


def pivot(days: int, keyword: str | None = None) -> None:
    df = _load()
    df["date"] = df["date"].str[:4] + "-" + df["date"].str[4:6] + "-" + df["date"].str[6:]
    sectors = sorted(df["sector"].unique())
    if keyword:
        sectors = [s for s in sectors if keyword in s]
        if not sectors:
            console.print(f"[yellow]'{keyword}' 포함 섹터 없음[/]")
            return

    n_days = df["date"].nunique()
    console.print(
        Panel(
            Text.assemble(
                ("주식추적기 · 시계열 피벗   ", "bold"),
                (f"총 {n_days}일 기록 / 최근 {days}일 표시", "dim"),
            ),
            box=box.DOUBLE,
            border_style="bright_white",
        )
    )

    for sector in sectors:
        g = df[df["sector"] == sector]
        piv = g.pivot_table(index="date", columns="name", values="close", aggfunc="last")
        piv = piv.sort_index().tail(days)

        t = Table(
            box=box.ROUNDED,
            title=f"[bold cyan]{sector}[/]",
            title_justify="left",
            show_header=True,
            header_style="bold dim",
        )
        t.add_column("날짜", justify="left", no_wrap=True, style="white")
        for col in piv.columns:
            t.add_column(str(col), justify="right")

        prev: dict[str, float] = {}
        for date_idx, row in piv.iterrows():
            cells = [str(date_idx)]
            for col in piv.columns:
                v = row[col]
                if pd.isna(v):
                    cells.append("[dim]-[/dim]")
                    continue
                v_int = int(v)
                p = prev.get(col)
                if p is None or p == 0 or v_int == p:
                    cells.append(f"{v_int:,}")
                else:
                    chg = (v_int - p) / p * 100
                    color, arrow, sign = _color(chg)
                    cells.append(f"{v_int:,} [{color}]{arrow}[/{color}]")
                prev[col] = v_int
            t.add_row(*cells)
        console.print(t)


def main() -> None:
    if not DATA.exists():
        console.print("[red]데이터 없음. 먼저 python collect.py 를 실행하세요.[/]")
        return
    args = sys.argv[1:]
    if not args:
        snapshot()
        return
    if args[0] == "snap":
        snapshot(args[1] if len(args) > 1 else None)
        return
    try:
        days = int(args[0])
        pivot(days, args[1] if len(args) > 1 else None)
    except ValueError:
        snapshot(args[0])


if __name__ == "__main__":
    main()
