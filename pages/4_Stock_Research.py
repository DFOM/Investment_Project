from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from core.research_engine import get_stock_research
from core.setup_env import setup_environment


def _format_metric(value: object) -> str:
    if value is None or value == "Data Unavailable":
        return "Data Unavailable"

    if isinstance(value, (int, float)):
        if value >= 1_000_000_000_000:
            return f"{value / 1_000_000_000_000:,.2f}T"
        if value >= 1_000_000_000:
            return f"{value / 1_000_000_000:,.2f}B"
        if value >= 1_000_000:
            return f"{value / 1_000_000:,.2f}M"
        return f"{value:,.4f}"

    return str(value)


def _render_candlestick(history: pd.DataFrame, ticker: str) -> None:
    if history.empty or not {"Date", "Open", "High", "Low", "Close"}.issubset(history.columns):
        st.info("Candlestick data unavailable for this ticker.")
        return

    fig = go.Figure(
        data=[
            go.Candlestick(
                x=history["Date"],
                open=history["Open"],
                high=history["High"],
                low=history["Low"],
                close=history["Close"],
                name=ticker,
            )
        ]
    )
    fig.update_layout(title=f"{ticker} - 1Y Daily Candlestick", xaxis_title="Date", yaxis_title="Price")
    st.plotly_chart(fig, use_container_width=True)


def _render_statement(frame: pd.DataFrame, empty_message: str) -> None:
    if frame.empty:
        st.info(empty_message)
        return

    out = frame.copy()
    out.columns = [str(c) for c in out.columns]
    st.dataframe(out, use_container_width=True)


def main() -> None:
    setup_environment()

    st.title("Stock Research Terminal")
    st.caption("Analyze ticker fundamentals before executing trades.")

    with st.form("research_form"):
        ticker_input = st.text_input("Ticker", placeholder="AAPL or 7203.T").strip().upper()
        search_clicked = st.form_submit_button("Search")

    if search_clicked:
        st.session_state["research_ticker"] = ticker_input

    ticker = str(st.session_state.get("research_ticker", "")).strip().upper()
    if not ticker:
        st.info("Search a ticker to load chart and financials.")
        return

    result = get_stock_research(ticker)

    if result.get("status") == "data_unavailable":
        st.warning("Core data unavailable for this ticker. Showing any partial data found.")

    warnings = result.get("warnings", [])
    if warnings:
        for warning in warnings:
            st.caption(f"Warning: {warning}")

    history = result.get("history", pd.DataFrame())
    _render_candlestick(history, str(result.get("ticker", ticker)))

    if st.button("Execute Trade With This Ticker", type="primary"):
        st.session_state["trade_prefill_ticker"] = str(result.get("ticker", ticker))
        if hasattr(st, "switch_page"):
            st.switch_page("pages/2_Trading_Desk.py")
        else:
            st.success("Ticker prepared for Trading Desk. Open page 2_Trading_Desk from sidebar.")

    key_stats_tab, income_tab, balance_tab = st.tabs(["Key Statistics", "Income Statement", "Balance Sheet"])

    with key_stats_tab:
        metrics = result.get("key_metrics", {})
        c1, c2, c3 = st.columns(3)
        c1.metric("Market Cap", _format_metric(metrics.get("market_cap")))
        c2.metric("P/E Ratio", _format_metric(metrics.get("pe_ratio")))
        c3.metric("Dividend Yield", _format_metric(metrics.get("dividend_yield")))

    with income_tab:
        _render_statement(result.get("income_statement", pd.DataFrame()), "Income statement unavailable.")

    with balance_tab:
        _render_statement(result.get("balance_sheet", pd.DataFrame()), "Balance sheet unavailable.")


main()
