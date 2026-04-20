"""Transaction History — audit trail of all buy/sell transactions with timestamps."""
from __future__ import annotations

import pandas as pd
import streamlit as st

from core.database import get_cached_ledger_df
from core.setup_env import setup_environment
from core.user_manager import ensure_team_config, get_active_member_names
from core.trade_executor import format_currency


def _parse_timestamp(ts_str: str) -> pd.Timestamp:
    """Parse a timestamp string to UTC, handling both TZ-aware and TZ-naive formats."""
    t = pd.to_datetime(ts_str, errors="coerce")
    if pd.isna(t):
        return pd.NaT
    if t.tzinfo is None:
        return t.tz_localize("UTC")
    return t.tz_convert("UTC")


def _load_ledger() -> pd.DataFrame:
    """Load and process the ledger data."""
    df = get_cached_ledger_df().copy()
    if df.empty:
        return pd.DataFrame(
            columns=[
                "Timestamp", "Ticker", "Action", "Quantity",
                "Local_Asset_Price", "Executed_FX_Rate", "Total_JPY_Impact",
                "Remaining_JPY_Balance", "Trader_Name",
                "Commission_Paid", "FX_Conversion_Fee", "Trade_Rationale",
            ]
        )
    
    # Parse timestamps
    df["Timestamp"] = df["Timestamp"].map(_parse_timestamp)
    
    # Coerce numeric columns
    for col in [
        "Quantity", "Local_Asset_Price", "Executed_FX_Rate", "Total_JPY_Impact",
        "Remaining_JPY_Balance", "Commission_Paid", "FX_Conversion_Fee",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    
    # Clean string columns
    df["Ticker"] = df["Ticker"].astype(str).str.upper().str.strip()
    df["Action"] = df["Action"].astype(str).str.upper().str.strip()
    df["Trader_Name"] = df["Trader_Name"].astype(str).str.strip()
    
    return df.dropna(subset=["Timestamp"]).sort_values("Timestamp", ascending=False)


def main() -> None:
    st.set_page_config(page_title="Transaction History", layout="wide")
    setup_environment()
    ensure_team_config()

    st.title("📊 Transaction History")
    st.caption("Complete audit trail of all buy and sell transactions with timestamps and details.")

    ledger = _load_ledger()

    if ledger.empty:
        st.info("No transactions recorded yet. Execute some trades to see the history.")
        return

    # ── Sidebar filters ────────────────────────────────────────────────────────
    st.sidebar.header("Filters")

    # Member filter
    active_members = get_active_member_names()
    all_traders = ["All Traders"] + active_members
    selected_trader = st.sidebar.selectbox("Trader", all_traders, index=0)

    # Action filter
    actions = ["All Actions"] + sorted(ledger["Action"].unique().tolist())
    selected_action = st.sidebar.selectbox("Action", actions, index=0)

    # Ticker filter
    tickers = ["All Tickers"] + sorted(ledger["Ticker"].unique().tolist())
    selected_ticker = st.sidebar.selectbox("Ticker", tickers, index=0)

    # Date range filter
    min_date = ledger["Timestamp"].min().date()
    max_date = ledger["Timestamp"].max().date()
    date_range = st.sidebar.date_input(
        "Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )
    
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date = date_range
        end_date = date_range

    # ── Apply filters ──────────────────────────────────────────────────────────
    filtered = ledger.copy()

    if selected_trader != "All Traders":
        filtered = filtered[filtered["Trader_Name"] == selected_trader]

    if selected_action != "All Actions":
        filtered = filtered[filtered["Action"] == selected_action]

    if selected_ticker != "All Tickers":
        filtered = filtered[filtered["Ticker"] == selected_ticker]

    filtered = filtered[
        (filtered["Timestamp"].dt.date >= start_date) &
        (filtered["Timestamp"].dt.date <= end_date)
    ]

    # ── Display statistics ─────────────────────────────────────────────────────
    col1, col2, col3, col4 = st.columns(4)

    total_trades = len(filtered)
    buy_trades = len(filtered[filtered["Action"] == "BUY"])
    sell_trades = len(filtered[filtered["Action"] == "SELL"])
    total_commission = filtered["Commission_Paid"].sum()

    col1.metric("Total Transactions", total_trades)
    col2.metric("Buy Orders", buy_trades)
    col3.metric("Sell Orders", sell_trades)
    col4.metric("Total Commission Paid", format_currency(total_commission, "JPY"))

    st.divider()

    # ── Transaction table ──────────────────────────────────────────────────────
    st.subheader("Transaction Details")

    if filtered.empty:
        st.info("No transactions match the selected filters.")
        return

    # Format for display
    display_df = filtered[[
        "Timestamp", "Ticker", "Action", "Quantity",
        "Local_Asset_Price", "Executed_FX_Rate", "Total_JPY_Impact",
        "Remaining_JPY_Balance", "Trader_Name",
        "Commission_Paid", "FX_Conversion_Fee", "Trade_Rationale",
    ]].copy()

    display_df["Timestamp"] = display_df["Timestamp"].dt.strftime("%Y-%m-%d %H:%M UTC")

    # Apply color coding for actions
    def _color_action(action: str) -> str:
        if action == "BUY":
            return "color: #e74c3c"  # Red for buys
        else:
            return "color: #2ecc71"  # Green for sells

    def _color_impact(val: float) -> str:
        if pd.isna(val):
            return ""
        return "color: #2ecc71" if val > 0 else "color: #e74c3c"

    styled_df = (
        display_df.style
        .map(lambda x: _color_action(x) if isinstance(x, str) and x in ["BUY", "SELL"] else "", subset=["Action"])
        .map(lambda x: _color_impact(x) if isinstance(x, (int, float)) else "", subset=["Total_JPY_Impact"])
        .format({
            "Quantity": "{:,.6f}",
            "Local_Asset_Price": "{:,.6f}",
            "Executed_FX_Rate": "{:,.6f}",
            "Total_JPY_Impact": "¥{:+,.2f}",
            "Remaining_JPY_Balance": "¥{:,.2f}",
            "Commission_Paid": "¥{:,.2f}",
            "FX_Conversion_Fee": "¥{:,.2f}",
        })
    )

    st.dataframe(
        styled_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Timestamp": st.column_config.TextColumn("Date/Time (UTC)", width="medium"),
            "Ticker": st.column_config.TextColumn("Ticker", width="small"),
            "Action": st.column_config.TextColumn("Type", width="small"),
            "Quantity": st.column_config.NumberColumn("Qty", width="small"),
            "Local_Asset_Price": st.column_config.NumberColumn("Local Price", width="small"),
            "Executed_FX_Rate": st.column_config.NumberColumn("FX Rate", width="small"),
            "Total_JPY_Impact": st.column_config.NumberColumn("JPY Impact", width="medium"),
            "Remaining_JPY_Balance": st.column_config.NumberColumn("Cash Balance", width="medium"),
            "Trader_Name": st.column_config.TextColumn("Trader", width="small"),
            "Commission_Paid": st.column_config.NumberColumn("Commission", width="small"),
            "FX_Conversion_Fee": st.column_config.NumberColumn("FX Fee", width="small"),
            "Trade_Rationale": st.column_config.TextColumn("Rationale", width="medium"),
        },
    )

    st.divider()

    # ── Transaction summary by trader ──────────────────────────────────────────
    st.subheader("Summary by Trader")
    
    trader_summary = filtered.groupby("Trader_Name").agg({
        "Timestamp": "count",
        "Commission_Paid": "sum",
        "Total_JPY_Impact": "sum",
    }).round(2)
    trader_summary.columns = ["Transaction Count", "Total Commission", "Net JPY Impact"]
    trader_summary = trader_summary.sort_values("Transaction Count", ascending=False)

    summary_styled = (
        trader_summary.style
        .format({
            "Transaction Count": "{:.0f}",
            "Total Commission": "¥{:,.2f}",
            "Net JPY Impact": "¥{:+,.2f}",
        })
        .map(lambda x: "color: #2ecc71" if isinstance(x, float) and x > 0 else "color: #e74c3c", subset=["Net JPY Impact"])
    )

    st.dataframe(summary_styled, use_container_width=True)

    # ── Transaction summary by ticker ──────────────────────────────────────────
    st.subheader("Summary by Ticker")
    
    ticker_summary = filtered.groupby("Ticker").agg({
        "Action": lambda x: (x == "BUY").sum(),
        "Quantity": "sum",
        "Commission_Paid": "sum",
        "Total_JPY_Impact": "sum",
    }).round(2)
    ticker_summary.columns = ["Buy Count", "Total Qty", "Total Commission", "Net JPY Impact"]
    ticker_summary = ticker_summary.sort_values("Buy Count", ascending=False)

    summary_styled = (
        ticker_summary.style
        .format({
            "Buy Count": "{:.0f}",
            "Total Qty": "{:,.6f}",
            "Total Commission": "¥{:,.2f}",
            "Net JPY Impact": "¥{:+,.2f}",
        })
        .map(lambda x: "color: #2ecc71" if isinstance(x, float) and x > 0 else "color: #e74c3c", subset=["Net JPY Impact"])
    )

    st.dataframe(summary_styled, use_container_width=True)


if __name__ == "__main__":
    main()
