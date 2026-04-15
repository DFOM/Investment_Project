from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st
import yfinance as yf

from core.database import (
    get_database,
    get_cached_ledger_df,
    get_cached_performance_df,
    record_daily_performance,
)
from core.market_data import get_current_usd_jpy, get_live_price
from core.setup_env import STARTING_JPY_BALANCE, setup_environment
from core.trade_executor import format_currency
from core.user_manager import ensure_team_config, get_active_member_names, get_member_aliases


@st.cache_data(ttl=3600)
def _yf_sector(ticker: str) -> str:
    """Return the yfinance sector for *ticker*; fallback 'Other'."""
    try:
        info = yf.Ticker(ticker).info
        return str(info.get("sector") or info.get("industry") or "Other")
    except Exception:
        return "Other"


@st.cache_data(ttl=3600)
def _yf_long_name(ticker: str) -> str:
    """Return the yfinance longName for *ticker*; fallback to ticker symbol."""
    try:
        info = yf.Ticker(ticker).info
        return str(info.get("longName") or info.get("shortName") or ticker)
    except Exception:
        return ticker


# ── Data loaders ──────────────────────────────────────────────────────────────

def _parse_timestamp(ts_str: str) -> pd.Timestamp:
    """Parse a timestamp string to UTC, handling both TZ-aware and TZ-naive formats.

    pd.to_datetime(..., utc=True) silently coerces mixed-timezone Series rows
    (some with '+00:00', some without) to NaT.  Parsing each value individually
    and then localising naive results to UTC avoids that data loss.
    """
    t = pd.to_datetime(ts_str, errors="coerce")
    if pd.isna(t):
        return pd.NaT
    if t.tzinfo is None:
        return t.tz_localize("UTC")
    return t.tz_convert("UTC")


def _load_ledger() -> pd.DataFrame:
    # Use the @st.cache_data-wrapped version so that st.cache_data.clear()
    # (called by Trading Desk after execution) actually forces a fresh read.
    df = get_cached_ledger_df().copy()
    if df.empty:
        return pd.DataFrame(
            columns=[
                "Timestamp", "Ticker", "Action", "Quantity",
                "Local_Asset_Price", "Total_JPY_Impact",
                "Remaining_JPY_Balance", "Trader_Name",
                "Commission_Paid", "FX_Conversion_Fee_Paid",
            ]
        )
    # Parse each timestamp individually to survive mixed TZ/naive formats.
    df["Timestamp"] = df["Timestamp"].map(_parse_timestamp)
    for c in [
        "Quantity", "Local_Asset_Price", "Total_JPY_Impact",
        "Remaining_JPY_Balance", "Commission_Paid", "FX_Conversion_Fee_Paid",
    ]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df["Ticker"] = df["Ticker"].astype(str).str.upper().str.strip()
    df["Action"] = df["Action"].astype(str).str.upper().str.strip()
    df["Trader_Name"] = df["Trader_Name"].astype(str).str.strip()
    return df.dropna(subset=["Timestamp"]).sort_values("Timestamp")


def _load_historical() -> pd.DataFrame:
    out = get_cached_performance_df().copy()
    if out.empty:
        return pd.DataFrame(columns=["date", "portfolio_value_jpy"])
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out["portfolio_value_jpy"] = pd.to_numeric(out["portfolio_value_jpy"], errors="coerce")
    return out.dropna().sort_values("date")


# ── Live-sync helpers ─────────────────────────────────────────────────────────

def _net_holdings(ledger: pd.DataFrame) -> dict[str, float]:
    """Return {ticker: net_quantity} for all positions with qty > 0."""
    if ledger.empty:
        return {}
    buys = ledger.loc[ledger["Action"] == "BUY"].groupby("Ticker")["Quantity"].sum()
    sells = ledger.loc[ledger["Action"] == "SELL"].groupby("Ticker")["Quantity"].sum()
    net = buys.sub(sells, fill_value=0.0)
    return {str(t): float(q) for t, q in net.items() if float(q) > 0}


def _weighted_avg_cost(ledger: pd.DataFrame) -> dict[str, float]:
    """Weighted average cost in LOCAL currency (USD for US, JPY for JP)."""
    wac: dict[str, float] = {}
    buy_rows = ledger.loc[
        (ledger["Action"] == "BUY") & ledger["Local_Asset_Price"].notna()
    ]
    for ticker, grp in buy_rows.groupby("Ticker"):
        total_cost = (grp["Quantity"] * grp["Local_Asset_Price"]).sum()
        total_qty = grp["Quantity"].sum()
        wac[str(ticker)] = float(total_cost / total_qty) if total_qty > 0 else 0.0
    return wac


def _is_jp_ticker(ticker: str) -> bool:
    return ticker.upper().endswith(".T")


def _sync_live_prices(
    holdings: dict[str, float],
    wac: dict[str, float],
    usd_jpy: float,
) -> tuple[pd.DataFrame, list[str]]:
    """Fetch live prices and compute unrealized P&L per holding.

    Returns (positions_df, skipped_tickers).
    """
    rows: list[dict] = []
    skipped: list[str] = []

    for ticker, qty in holdings.items():
        price = get_live_price(ticker, fallback=None)
        if price is None:
            skipped.append(ticker)
            continue

        cost_basis_local = wac.get(ticker, 0.0)
        is_jp = _is_jp_ticker(ticker)
        fx = 1.0 if is_jp else usd_jpy

        market_value_jpy = qty * float(price) * fx
        cost_value_jpy = qty * cost_basis_local * fx
        unrealized_pnl_jpy = market_value_jpy - cost_value_jpy
        unrealized_pnl_pct = (
            (unrealized_pnl_jpy / cost_value_jpy * 100.0) if cost_value_jpy != 0 else 0.0
        )

        rows.append(
            {
                "Ticker": ticker,
                "Quantity": qty,
                "Avg Cost (Local)": round(cost_basis_local, 4),
                "Live Price (Local)": round(float(price), 4),
                "FX Rate (JPY)": round(fx, 4),
                "Market Value (JPY)": round(market_value_jpy, 2),
                "Unrealized P/L (JPY)": round(unrealized_pnl_jpy, 2),
                "Unrealized P/L (%)": round(unrealized_pnl_pct, 2),
            }
        )

    df = pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=[
            "Ticker", "Quantity", "Avg Cost (Local)", "Live Price (Local)",
            "FX Rate (JPY)", "Market Value (JPY)", "Unrealized P/L (JPY)", "Unrealized P/L (%)",
        ]
    )
    return df, skipped


# ── Main page ─────────────────────────────────────────────────────────────────

def main() -> None:
    st.set_page_config(page_title="Dashboard", layout="wide")
    setup_environment()
    ensure_team_config()

    # If Trading Desk just executed pending orders, force a full cache flush so
    # the newly-written ledger rows are immediately visible here.
    if st.session_state.pop("pending_orders_executed", False):
        get_database.cache_clear()
        st.cache_data.clear()

    st.title("Dashboard & Analysis")

    # ── Sidebar controls ───────────────────────────────────────────────────────
    active_members = get_active_member_names()
    options = ["All Team"] + active_members
    selected = st.sidebar.selectbox("View Analysis For", options, index=0)

    st.sidebar.divider()
    if st.sidebar.button("\U0001f504 Refresh Data", use_container_width=True,
                         help="Clears all caches and reloads the latest data from Google Sheets."):
        get_database.cache_clear()
        st.cache_data.clear()
        st.rerun()

    if st.sidebar.button("\U0001f4f8 Refresh & Record Snapshot", use_container_width=True):
        with st.spinner("Fetching live prices and writing to Performance tab\u2026"):
            try:
                snap = record_daily_performance()
                st.sidebar.success(
                    f"Snapshot saved for {snap['date']}\n\n"
                    f"Total: \u00a5{snap['total_portfolio_value_jpy']:,.2f}"
                )
                if snap["tickers_skipped"]:
                    st.sidebar.warning(
                        "Could not price: " + ", ".join(snap["tickers_skipped"])
                    )
            except Exception as exc:
                st.sidebar.error(f"Snapshot failed: {exc}")
            finally:
                # Always bust both caches regardless of success/failure
                get_database.cache_clear()
                st.cache_data.clear()
        st.rerun()

    # ── Load data ──────────────────────────────────────────────────────────────
    with st.spinner("Loading ledger from Google Sheets\u2026"):
        ledger = _load_ledger()
        historical = _load_historical()

    is_all = selected == "All Team"
    if is_all:
        scoped = ledger
    else:
        aliases = set(get_member_aliases(selected))
        scoped = ledger.loc[ledger["Trader_Name"].isin(aliases)].copy()
        if scoped.empty and not ledger.empty:
            st.info(
                "\u2139\ufe0f No trades found for the selected member. "
                "Orders auto-executed from the queue appear under **All Team** view."
            )

    # ── Live FX rate ───────────────────────────────────────────────────────────
    usd_jpy = get_current_usd_jpy(fallback=150.0) or 150.0

    # ── Cash balance ───────────────────────────────────────────────────────────
    if scoped.empty:
        cash = float(STARTING_JPY_BALANCE)
    elif is_all:
        cash = float(scoped["Remaining_JPY_Balance"].dropna().iloc[-1])
    else:
        cash = float(STARTING_JPY_BALANCE) + float(scoped["Total_JPY_Impact"].sum())

    # ── Live holdings sync ─────────────────────────────────────────────────────
    holdings = _net_holdings(scoped)
    wac = _weighted_avg_cost(scoped)

    with st.spinner("Fetching live prices\u2026"):
        positions_df, skipped = _sync_live_prices(holdings, wac, usd_jpy)

    # Enrich positions with company names (cached 1 h via _yf_long_name)
    if not positions_df.empty:
        positions_df.insert(
            1,
            "Company",
            positions_df["Ticker"].apply(_yf_long_name),
        )

    if skipped:
        for t in skipped:
            st.warning(f"\u26a0\ufe0f Could not fetch live price for **{t}** \u2014 skipped.")

    equity_jpy = float(positions_df["Market Value (JPY)"].sum()) if not positions_df.empty else 0.0
    total = cash + equity_jpy
    roi = ((total - STARTING_JPY_BALANCE) / STARTING_JPY_BALANCE) * 100.0

    # ── KPI metrics ────────────────────────────────────────────────────────────
    m1, m2, m3, m4 = st.columns([1, 1, 1, 1])
    m1.metric("Total Portfolio Value", format_currency(total, "JPY"), help=f"Exact: \u00a5{total:,.2f}")
    m2.metric("Overall ROI", f"{roi:+.2f}%")
    m3.metric("Cash Balance", format_currency(cash, "JPY"), help=f"Exact: \u00a5{cash:,.2f}")
    m4.metric("USD/JPY (Live)", f"{usd_jpy:,.2f}")

    # ── Unrealized P&L table ───────────────────────────────────────────────────
    st.divider()
    st.subheader("\U0001f4ca Open Positions \u2014 Live P/L")
    if positions_df.empty:
        st.info("No open positions.")
    else:
        def _colour_pnl(val: float) -> str:
            return "color: #2ecc71" if val >= 0 else "color: #e74c3c"

        styled = (
            positions_df.style
            .map(_colour_pnl, subset=["Unrealized P/L (JPY)", "Unrealized P/L (%)"])
            .format(
                {
                    "Quantity": "{:,.4f}",
                    "Avg Cost (Local)": "{:,.4f}",
                    "Live Price (Local)": "{:,.4f}",
                    "FX Rate (JPY)": "{:,.4f}",
                    "Market Value (JPY)": "\u00a5{:,.2f}",
                    "Unrealized P/L (JPY)": "\u00a5{:+,.2f}",
                    "Unrealized P/L (%)": "{:+.2f}%",
                }
            )
        )
        st.dataframe(
            styled,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Ticker":               st.column_config.TextColumn("Ticker", width="small"),
                "Quantity":             st.column_config.NumberColumn("Qty", width="small"),
                "Avg Cost (Local)":     st.column_config.NumberColumn("Avg Cost", width="small"),
                "Live Price (Local)":   st.column_config.NumberColumn("Live Price", width="small"),
                "FX Rate (JPY)":        st.column_config.NumberColumn("FX Rate", width="small"),
                "Market Value (JPY)":   st.column_config.NumberColumn("Mkt Value (JPY)", width="medium"),
                "Unrealized P/L (JPY)": st.column_config.NumberColumn("P/L (JPY)", width="medium"),
                "Unrealized P/L (%)":   st.column_config.NumberColumn("P/L %", width="small"),
            },
        )

    # ── Portfolio value over time ──────────────────────────────────────────────
    st.divider()
    st.subheader("Portfolio Value Over Time")
    if is_all and not historical.empty:
        fig = px.line(
            historical, x="date", y="portfolio_value_jpy",
            title="Portfolio Value Over Time", markers=True,
        )
        fig.add_hline(
            y=STARTING_JPY_BALANCE,
            line_dash="dot",
            line_color="gray",
            annotation_text="Starting Capital",
        )
        st.plotly_chart(fig, use_container_width=True)
    elif scoped.empty:
        st.info("No data for selected student.")
    else:
        growth = scoped[["Timestamp", "Total_JPY_Impact"]].copy().sort_values("Timestamp")
        growth["portfolio_value_jpy"] = STARTING_JPY_BALANCE + growth["Total_JPY_Impact"].cumsum()
        fig = px.line(growth, x="Timestamp", y="portfolio_value_jpy",
                      title="Portfolio Value Over Time", markers=True)
        st.plotly_chart(fig, use_container_width=True)

    # ── Allocation Analysis ────────────────────────────────────────────────────
    st.divider()
    st.subheader("\U0001f4ca Allocation Analysis")

    if positions_df.empty:
        st.info("No open positions to analyse — buy some stocks first.")
    else:
        # ── Fetch live sector + name from yfinance (cached 1 h) ───────────────
        with st.spinner("Fetching sector data from yfinance\u2026"):
            alloc_rows: list[dict] = []
            for _, pos_row in positions_df.iterrows():
                tkr = str(pos_row["Ticker"]).upper()
                sector = _yf_sector(tkr)
                long_name = _yf_long_name(tkr)
                pnl_jpy = float(pos_row.get("Unrealized P/L (JPY)", 0.0))
                alloc_rows.append(
                    {
                        "Ticker": tkr,
                        "Company Name": long_name,
                        "Sector": sector,
                        "Value (JPY)": float(pos_row["Market Value (JPY)"]),
                        "Total Gain/Loss (JPY)": pnl_jpy,
                    }
                )

        alloc_df = pd.DataFrame(alloc_rows)

        # Add Cash row
        cash_row = pd.DataFrame(
            [{
                "Ticker": "CASH",
                "Company Name": "Cash (JPY)",
                "Sector": "Cash",
                "Value (JPY)": cash,
                "Total Gain/Loss (JPY)": 0.0,
            }]
        )
        full_df = pd.concat([alloc_df, cash_row], ignore_index=True)

        grand_total = full_df["Value (JPY)"].sum()
        if grand_total <= 0:
            st.info("Portfolio value is zero — nothing to chart.")
        else:
            full_df["% of Portfolio"] = (full_df["Value (JPY)"] / grand_total * 100.0).round(2)

            # ── Sector aggregation ────────────────────────────────────────────
            sector_df = (
                full_df.groupby("Sector", as_index=False)["Value (JPY)"]
                .sum()
                .assign(**{"% of Portfolio": lambda d: (d["Value (JPY)"] / grand_total * 100.0).round(2)})
                .sort_values("% of Portfolio", ascending=False)
            )

            # ── Two-column charts ─────────────────────────────────────────────
            ch_left, ch_right = st.columns(2)

            with ch_left:
                fig_sector = px.pie(
                    sector_df,
                    names="Sector",
                    values="Value (JPY)",
                    hole=0.4,
                    title="Sector Allocation (incl. Cash)",
                    color_discrete_sequence=px.colors.qualitative.Pastel,
                )
                fig_sector.update_traces(
                    textinfo="label+percent",
                    hovertemplate="%{label}<br>\u00a5%{value:,.0f}<br>%{percent}",
                )
                st.plotly_chart(fig_sector, use_container_width=True)

            with ch_right:
                fig_sunburst = px.sunburst(
                    full_df,
                    path=["Sector", "Company Name"],
                    values="Value (JPY)",
                    title="Company Allocation by Sector",
                    color="Sector",
                    color_discrete_sequence=px.colors.qualitative.Pastel,
                )
                fig_sunburst.update_traces(
                    hovertemplate="%{label}<br>\u00a5%{value:,.0f}<br>%{percentRoot:.1%} of total",
                )
                st.plotly_chart(fig_sunburst, use_container_width=True)

            # ── Performance comparison table ──────────────────────────────────
            detail = (
                full_df[["Company Name", "Sector", "% of Portfolio", "Total Gain/Loss (JPY)"]]
                .sort_values("% of Portfolio", ascending=False)
                .reset_index(drop=True)
            )

            def _colour_gl(val: float) -> str:
                if not isinstance(val, (int, float)):
                    return ""
                return "color: #27ae60" if val > 0 else ("color: #e74c3c" if val < 0 else "")

            styled_alloc = (
                detail.style
                .map(_colour_gl, subset=["Total Gain/Loss (JPY)"])
                .format(
                    {
                        "Value (JPY)": "\u00a5{:,.2f}",
                        "% of Portfolio": "{:.2f}%",
                        "Total Gain/Loss (JPY)": "\u00a5{:+,.2f}",
                    }
                )
                .bar(subset=["% of Portfolio"], color="#4a90d9", vmin=0, vmax=100)
            )
            st.dataframe(styled_alloc, use_container_width=True, hide_index=True)

            # ── Analysis rationale ────────────────────────────────────────────
            top_sector_row = sector_df[sector_df["Sector"] != "Cash"].iloc[0] if not sector_df[sector_df["Sector"] != "Cash"].empty else None
            if top_sector_row is not None:
                top_sector = top_sector_row["Sector"]
                top_pct = top_sector_row["% of Portfolio"]
                cash_pct = round(cash / grand_total * 100.0, 1)

                if top_pct >= 50:
                    risk_note = (
                        f"At **{top_pct:.1f}%**, your portfolio is heavily concentrated in "
                        f"**{top_sector}**. High single-sector exposure amplifies drawdown risk "
                        f"if this sector faces a downturn — consider diversifying into uncorrelated sectors."
                    )
                elif top_pct >= 30:
                    risk_note = (
                        f"Your portfolio is **{top_pct:.1f}% concentrated in {top_sector}**. "
                        f"This is a meaningful tilt. Monitor sector-specific catalysts "
                        f"(earnings, regulation, macro) closely."
                    )
                else:
                    risk_note = (
                        f"Sector concentration looks balanced — your largest sector exposure is "
                        f"**{top_sector}** at **{top_pct:.1f}%**. "
                        f"Cash represents **{cash_pct}%** of total capital, providing a liquidity buffer."
                    )

                st.info(f"\U0001f4ac **Top Sector Exposure Analysis**\n\n{risk_note}")

    # ── Trade History ──────────────────────────────────────────────────────────
    st.divider()
    st.subheader("\U0001f4dc Trade History")

    trade_rows = scoped.loc[
        scoped["Action"].isin(["BUY", "SELL"]) & scoped["Local_Asset_Price"].notna()
    ].copy().sort_values("Timestamp", ascending=False)

    if trade_rows.empty:
        st.info("No trade history yet.")
    else:
        unique_tickers = trade_rows["Ticker"].unique().tolist()
        live_prices: dict[str, float | None] = {}
        ticker_names: dict[str, str] = {}
        for t in unique_tickers:
            live_prices[t] = get_live_price(t, fallback=None)
            ticker_names[t] = _yf_long_name(t)

        def _fmt_exec_price(row: pd.Series) -> str:
            price = row["Local_Asset_Price"]
            if pd.isna(price):
                return "—"
            if _is_jp_ticker(row["Ticker"]):
                return f"¥{price:,.2f}"
            return f"${price:,.2f}"

        def _gain_loss_pct(row: pd.Series) -> str:
            if row["Action"] != "BUY":
                return "—"
            purchase = row["Local_Asset_Price"]
            live = live_prices.get(row["Ticker"])
            if pd.isna(purchase) or purchase == 0 or live is None:
                return "—"
            pct = ((live - purchase) / purchase) * 100.0
            return f"{pct:+.2f}%"

        history_display = trade_rows[
            ["Timestamp", "Ticker", "Action", "Quantity", "Local_Asset_Price",
             "Total_JPY_Impact", "Trader_Name"]
        ].copy()
        history_display.insert(
            2, "Company",
            history_display["Ticker"].map(lambda t: ticker_names.get(t, t))
        )
        history_display["Exec Price"] = trade_rows.apply(_fmt_exec_price, axis=1)
        history_display["Gain/Loss % (vs Live)"] = trade_rows.apply(_gain_loss_pct, axis=1)
        history_display = history_display.drop(columns=["Local_Asset_Price"])
        history_display["Timestamp"] = history_display["Timestamp"].dt.strftime("%Y-%m-%d %H:%M UTC")
        history_display["Quantity"] = history_display["Quantity"].map("{:,.4f}".format)
        history_display["Total_JPY_Impact"] = history_display["Total_JPY_Impact"].map(
            lambda v: f"\u00a5{v:+,.0f}" if pd.notna(v) else "—"
        )
        history_display = history_display.rename(columns={"Total_JPY_Impact": "JPY Impact"})

        def _colour_gain(val: str) -> str:
            if isinstance(val, str) and val.startswith("+"):
                return "color: #2ecc71"
            if isinstance(val, str) and val.startswith("-"):
                return "color: #e74c3c"
            return ""

        def _colour_action(val: str) -> str:
            if val == "BUY":
                return "color: #2ecc71; font-weight: bold"
            if val == "SELL":
                return "color: #e74c3c; font-weight: bold"
            return ""

        styled_hist = (
            history_display.style
            .map(_colour_gain, subset=["Gain/Loss % (vs Live)", "JPY Impact"])
            .map(_colour_action, subset=["Action"])
        )
        st.dataframe(styled_hist, use_container_width=True, hide_index=True)


main()


