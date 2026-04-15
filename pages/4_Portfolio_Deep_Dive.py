"""Portfolio Deep Dive — detailed analytics, company profiles, and risk assessment."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from functools import lru_cache

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import yfinance as yf

from core.database import get_cached_ledger_df
from core.market_data import get_current_usd_jpy, get_live_price
from core.setup_env import STARTING_JPY_BALANCE, setup_environment
from core.trade_executor import format_currency
from core.user_manager import ensure_team_config



# ── Cached yfinance helpers ───────────────────────────────────────────────────

@st.cache_data(ttl=300)
def _fetch_ticker_info(ticker: str) -> dict:
    """Fetch yfinance .info dict; return {} on any failure."""
    try:
        info = yf.Ticker(ticker).info
        return dict(info) if isinstance(info, dict) else {}
    except Exception:
        return {}


@st.cache_data(ttl=300)
def _fetch_ticker_history(ticker: str, period: str = "1mo") -> pd.DataFrame:
    """Fetch OHLCV history; return empty DataFrame on failure."""
    try:
        df = yf.Ticker(ticker).history(period=period)
        return df if not df.empty else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=1800)
def _fetch_ticker_news(ticker: str) -> list[dict]:
    """Return up to 10 recent news items from yfinance."""
    try:
        raw = yf.Ticker(ticker).news
        return list(raw)[:10] if raw else []
    except Exception:
        return []


@st.cache_data(ttl=3600)
def _fetch_income_stmt(ticker: str) -> pd.DataFrame:
    try:
        df = yf.Ticker(ticker).income_stmt
        return df if df is not None and not df.empty else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def _fetch_balance_sheet(ticker: str) -> pd.DataFrame:
    try:
        df = yf.Ticker(ticker).balance_sheet
        return df if df is not None and not df.empty else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def _fetch_cash_flow(ticker: str) -> pd.DataFrame:
    try:
        df = yf.Ticker(ticker).cash_flow
        return df if df is not None and not df.empty else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=1800)
def _fetch_recommendations(ticker: str) -> pd.DataFrame:
    try:
        df = yf.Ticker(ticker).recommendations
        return df if df is not None and not df.empty else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def _fmt_financial(val) -> str:
    """Format a raw financial value (often in units of 1) as B/M/K string."""
    try:
        v = float(val)
    except (TypeError, ValueError):
        return "—"
    if v == 0:
        return "0"
    neg = v < 0
    v = abs(v)
    if v >= 1e9:
        s = f"${v / 1e9:.2f}B"
    elif v >= 1e6:
        s = f"${v / 1e6:.2f}M"
    elif v >= 1e3:
        s = f"${v / 1e3:.2f}K"
    else:
        s = f"${v:.2f}"
    return f"-{s}" if neg else s


def _render_financial_table(df: pd.DataFrame, key_rows: list[str] | None = None) -> None:
    """Transpose a yfinance financial DataFrame and display selected rows."""
    if df.empty:
        st.caption("Financial data not available for this ticker.")
        return
    # Transpose: columns=dates → rows=dates, columns=metrics
    t = df.T.copy()
    t.index = pd.to_datetime(t.index, errors="coerce")
    t = t.sort_index(ascending=False)
    t.index = t.index.strftime("%Y")
    if key_rows:
        available = [r for r in key_rows if r in t.columns]
        t = t[available] if available else t
    # Format all cells
    display_df = t.map(_fmt_financial)
    display_df.index.name = "Fiscal Year"
    st.dataframe(display_df, use_container_width=True)


def _company_name(ticker: str, info: dict) -> str:
    return str(info.get("longName") or info.get("shortName") or ticker)


def _is_jp(ticker: str) -> bool:
    return ticker.upper().endswith(".T")


# ── Ledger helpers ────────────────────────────────────────────────────────────

def _parse_ts(ts_str: str) -> pd.Timestamp:
    """Parse a timestamp string tolerating both TZ-aware and TZ-naive formats.

    pd.to_datetime(..., utc=True) applied to a whole Series silently coerces
    TZ-naive strings (e.g. '2026-04-07 01:04') to NaT when other rows in the
    same Series carry TZ-aware strings.  Per-value parsing avoids that loss.
    """
    t = pd.to_datetime(ts_str, errors="coerce")
    if pd.isna(t):
        return pd.NaT
    if t.tzinfo is None:
        return t.tz_localize("UTC")
    return t.tz_convert("UTC")


def _load_ledger() -> pd.DataFrame:
    df = get_cached_ledger_df()
    if df.empty:
        return df
    df = df.copy()
    df["Timestamp"] = df["Timestamp"].map(_parse_ts)
    df["Ticker"] = df["Ticker"].astype(str).str.upper().str.strip()
    df["Action"] = df["Action"].astype(str).str.upper().str.strip()
    return df.dropna(subset=["Timestamp"]).sort_values("Timestamp")


def _net_holdings(ledger: pd.DataFrame) -> dict[str, float]:
    if ledger.empty:
        return {}
    buys = ledger.loc[ledger["Action"] == "BUY"].groupby("Ticker")["Quantity"].sum()
    sells = ledger.loc[ledger["Action"] == "SELL"].groupby("Ticker")["Quantity"].sum()
    net = buys.sub(sells, fill_value=0.0)
    return {str(t): float(q) for t, q in net.items() if float(q) > 0}


def _weighted_avg_cost(ledger: pd.DataFrame) -> dict[str, float]:
    wac: dict[str, float] = {}
    buy_rows = ledger.loc[(ledger["Action"] == "BUY") & ledger["Local_Asset_Price"].notna()]
    for ticker, grp in buy_rows.groupby("Ticker"):
        total_cost = (grp["Quantity"] * grp["Local_Asset_Price"]).sum()
        total_qty = grp["Quantity"].sum()
        wac[str(ticker)] = float(total_cost / total_qty) if total_qty > 0 else 0.0
    return wac


def _cash_balance(ledger: pd.DataFrame) -> float:
    if ledger.empty:
        return float(STARTING_JPY_BALANCE)
    bal = ledger["Remaining_JPY_Balance"].dropna()
    return float(bal.iloc[-1]) if not bal.empty else float(STARTING_JPY_BALANCE)


# ── Core portfolio builder ────────────────────────────────────────────────────

def _build_portfolio_df(
    holdings: dict[str, float],
    wac: dict[str, float],
    usd_jpy: float,
    cash: float,
) -> pd.DataFrame:
    """Return a DataFrame with one row per holding plus a Cash row."""
    rows: list[dict] = []
    skipped: list[str] = []

    for ticker, qty in holdings.items():
        price = get_live_price(ticker, fallback=None)
        if price is None:
            skipped.append(ticker)
            continue
        fx = 1.0 if _is_jp(ticker) else usd_jpy
        avg_cost = wac.get(ticker, 0.0)
        market_value = qty * float(price) * fx
        cost_basis = qty * avg_cost * fx
        pnl_jpy = market_value - cost_basis
        pnl_pct = (pnl_jpy / cost_basis * 100.0) if cost_basis != 0 else 0.0

        info = _fetch_ticker_info(ticker)
        rows.append(
            {
                "Ticker": ticker,
                "Company": _company_name(ticker, info),
                "Industry": str(info.get("sector") or info.get("industry") or "Other"),
                "Quantity": qty,
                "Avg Cost (Local)": avg_cost,
                "Live Price (Local)": float(price),
                "FX Rate": fx,
                "Market Value (JPY)": market_value,
                "Cost Basis (JPY)": cost_basis,
                "Unrealized P/L (JPY)": pnl_jpy,
                "Unrealized P/L (%)": pnl_pct,
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        df = pd.DataFrame(
            columns=[
                "Ticker", "Company", "Industry", "Quantity",
                "Avg Cost (Local)", "Live Price (Local)", "FX Rate",
                "Market Value (JPY)", "Cost Basis (JPY)",
                "Unrealized P/L (JPY)", "Unrealized P/L (%)",
            ]
        )
    return df, skipped


# ── Tab 1: Visual Analytics ───────────────────────────────────────────────────

def _tab_visual(port_df: pd.DataFrame, cash: float, total: float) -> None:
    if port_df.empty:
        st.info("No open positions yet — execute some trades first.")
        return

    # Add cash row for full picture
    cash_row = pd.DataFrame(
        [
            {
                "Ticker": "CASH",
                "Company": "Cash (JPY)",
                "Industry": "Cash",
                "Market Value (JPY)": cash,
                "Unrealized P/L (%)": 0.0,
                "Unrealized P/L (JPY)": 0.0,
            }
        ]
    )
    full_df = pd.concat([port_df, cash_row], ignore_index=True)
    full_df["% of Portfolio"] = (full_df["Market Value (JPY)"] / total * 100.0).round(2)

    # ── Treemap (colour = P/L %) ──────────────────────────────────────────────
    st.subheader("Portfolio Heatmap")
    st.caption("Block size = market value · Colour = unrealized P/L % (green = profit, red = loss, grey = cash)")

    fig_tree = px.treemap(
        full_df,
        path=["Industry", "Company"],
        values="Market Value (JPY)",
        color="Unrealized P/L (%)",
        color_continuous_scale=[(0, "#c0392b"), (0.5, "#576574"), (1, "#27ae60")],
        color_continuous_midpoint=0,
        hover_data={"% of Portfolio": True, "Market Value (JPY)": ":,.0f"},
        title="Holding Heatmap by Value & P/L",
    )
    fig_tree.update_traces(
        texttemplate="<b>%{label}</b><br>¥%{value:,.0f}<br>%{customdata[0]:.1f}%",
        hovertemplate="<b>%{label}</b><br>Market Value: ¥%{value:,.0f}<br>P/L: %{color:.2f}%<extra></extra>",
    )
    fig_tree.update_layout(margin=dict(t=50, l=0, r=0, b=0), coloraxis_showscale=False)
    st.plotly_chart(fig_tree, use_container_width=True)

    # ── Horizontal bar: % of portfolio ────────────────────────────────────────
    st.subheader("Holdings as % of Total Portfolio")
    bar_df = full_df.sort_values("% of Portfolio", ascending=True)
    colours = [
        "#27ae60" if p > 0 else ("#c0392b" if p < 0 else "#576574")
        for p in bar_df["Unrealized P/L (%)"]
    ]
    fig_bar = go.Figure(
        go.Bar(
            y=bar_df["Company"],
            x=bar_df["% of Portfolio"],
            orientation="h",
            marker_color=colours,
            text=bar_df["% of Portfolio"].map("{:.1f}%".format),
            textposition="outside",
            hovertemplate="<b>%{y}</b><br>%{x:.2f}% of portfolio<extra></extra>",
        )
    )
    fig_bar.update_layout(
        xaxis_title="% of Total Portfolio",
        yaxis_title="",
        margin=dict(l=0, r=40, t=30, b=0),
        height=max(300, len(bar_df) * 45),
    )
    st.plotly_chart(fig_bar, use_container_width=True)


# ── Tab 2: Company Deep Dives ─────────────────────────────────────────────────

def _tab_deep_dive(port_df: pd.DataFrame, usd_jpy: float) -> None:  # noqa: C901
    if port_df.empty:
        st.info("No open positions to analyse.")
        return

    tickers = port_df["Ticker"].tolist()
    labels = {
        row["Ticker"]: f"{row['Company']} ({row['Ticker']})"
        for _, row in port_df.iterrows()
    }
    selected_ticker = st.selectbox(
        "\U0001f50d Select Company for Deep Dive",
        options=tickers,
        format_func=lambda t: labels.get(t, t),
    )

    row = port_df.loc[port_df["Ticker"] == selected_ticker].iloc[0]
    info = _fetch_ticker_info(selected_ticker)
    currency_sym = "\u00a5" if _is_jp(selected_ticker) else "$"
    live_px = row["Live Price (Local)"]

    # ── Header ────────────────────────────────────────────────────────────────
    logo_url = info.get("logo_url", "")
    if logo_url:
        h_col, t_col = st.columns([1, 8])
        h_col.image(logo_url, width=64)
        t_col.markdown(f"## {_company_name(selected_ticker, info)}")
    else:
        st.markdown(f"## {_company_name(selected_ticker, info)}")

    country = info.get("country", "")
    exchange = info.get("exchange", "")
    mkt_cap = info.get("marketCap")
    mkt_cap_str = _fmt_financial(mkt_cap) if mkt_cap else "—"
    st.markdown(
        f"**Ticker:** `{selected_ticker}` &nbsp;·&nbsp; "
        f"**Sector:** {info.get('sector', '—')} &nbsp;·&nbsp; "
        f"**Industry:** {row['Industry']} &nbsp;·&nbsp; "
        f"**Exchange:** {exchange} &nbsp;·&nbsp; "
        f"**Country:** {country} &nbsp;·&nbsp; "
        f"**Market Cap:** {mkt_cap_str}"
    )
    website = info.get("website", "")
    if website:
        st.markdown(f"[{website}]({website})", unsafe_allow_html=False)

    # ── Business summary ──────────────────────────────────────────────────────
    summary = info.get("longBusinessSummary", "")
    if summary:
        sentences = summary.replace("  ", " ").split(". ")
        short = ". ".join(sentences[:4]).strip()
        if not short.endswith("."):
            short += "."
        with st.expander("\U0001f4c4 Business Summary", expanded=True):
            st.write(short)
    else:
        st.caption("Business summary not available for this ticker.")

    st.divider()

    # ── Position stats ────────────────────────────────────────────────────────
    st.subheader("\U0001f4bc Your Position")
    p1, p2, p3, p4 = st.columns(4)
    p1.metric("Shares Held", f"{row['Quantity']:,.4f}",
              help="Total number of shares currently held in this position.")
    p2.metric(
        "Avg Buy Price",
        f"{currency_sym}{row['Avg Cost (Local)']:,.4f}",
        help="Weighted average price paid per share across all your BUY trades for this ticker.",
    )
    p3.metric("Market Value (JPY)", f"\u00a5{row['Market Value (JPY)']:,.2f}",
              help="Current market value in JPY: Shares \u00d7 Live Price \u00d7 FX Rate.")
    pnl_delta = f"\u00a5{row['Unrealized P/L (JPY)']:+,.2f}"
    p4.metric(
        "Total P/L",
        f"{row['Unrealized P/L (%)']:+.2f}%",
        delta=pnl_delta,
        delta_color="normal",
        help="Unrealized P/L: (Live Price \u2212 Avg Buy Price) \u00d7 Shares in JPY. Not realised until sold.",
    )

    st.divider()

    # ── Key ratios ────────────────────────────────────────────────────────────
    st.subheader("\U0001f4c8 Key Ratios")
    pe = info.get("trailingPE") or info.get("forwardPE")
    div_yield = info.get("dividendYield")
    week52_high = info.get("fiftyTwoWeekHigh")
    week52_low = info.get("fiftyTwoWeekLow")
    eps = info.get("trailingEps")
    pb = info.get("priceToBook")
    ps = info.get("priceToSalesTrailing12Months")
    beta = info.get("beta")

    ra, rb, rc, rd = st.columns(4)
    ra.metric("P/E Ratio", f"{pe:.2f}" if pe else "—",
              help="Price \u00f7 Earnings Per Share (trailing 12 months, or forward estimate). "
                   "A higher P/E implies the market expects faster earnings growth.")
    rb.metric("Dividend Yield", f"{div_yield * 100:.2f}%" if div_yield else "—",
              help="Annual dividend per share \u00f7 current price. "
                   "Represents the cash return from dividends alone, before price appreciation.")
    rc.metric(
        "52-Week High",
        f"{currency_sym}{week52_high:,.2f}" if week52_high else "—",
        delta=(f"{((live_px - week52_high) / week52_high * 100):+.1f}% from high" if week52_high else None),
        delta_color="inverse",
        help="Highest closing price over the past 52 weeks. "
             "Negative delta = stock is below its yearly peak.",
    )
    rd.metric(
        "52-Week Low",
        f"{currency_sym}{week52_low:,.2f}" if week52_low else "—",
        delta=(f"{((live_px - week52_low) / week52_low * 100):+.1f}% from low" if week52_low else None),
        delta_color="normal",
        help="Lowest closing price over the past 52 weeks. "
             "Positive delta = stock has recovered from its yearly trough.",
    )

    re2, rf, rg, rh = st.columns(4)
    re2.metric("EPS (TTM)", f"{currency_sym}{eps:.2f}" if eps else "—",
               help="Earnings Per Share (trailing twelve months). Net income \u00f7 shares outstanding.")
    rf.metric("P/B Ratio", f"{pb:.2f}" if pb else "—",
              help="Price-to-Book: share price \u00f7 book value per share. "
                   "Values below 1 may indicate undervaluation; higher = premium to assets.")
    rg.metric("P/S Ratio", f"{ps:.2f}" if ps else "—",
              help="Price-to-Sales: market cap \u00f7 annual revenue. "
                   "Used for companies without positive earnings.")
    rh.metric("Beta", f"{beta:.2f}" if beta else "—",
              help="Sensitivity to the market. Beta > 1 = more volatile than the market index; "
                   "< 1 = less volatile; < 0 = tends to move opposite to the market.")

    st.divider()

    # ── Price Chart (Dynamic Period) ──────────────────────────────────────────
    st.subheader(f"\U0001f4c9 {selected_ticker} Price Chart")

    period_options = {
        "1 Month":  "1mo",
        "3 Months": "3mo",
        "6 Months": "6mo",
        "1 Year":   "1y",
        "2 Years":  "2y",
        "5 Years":  "5y",
        "Max":      "max",
    }
    chart_col, opt_col = st.columns([5, 1])
    with opt_col:
        chosen_label = st.selectbox(
            "Period",
            options=list(period_options.keys()),
            index=3,
            key=f"period_sel_{selected_ticker}",
        )
        chart_type = st.radio(
            "Chart type",
            ["Line", "Candlestick"],
            index=0,
            key=f"chart_type_{selected_ticker}",
        )

    chosen_period = period_options[chosen_label]
    hist = _fetch_ticker_history(selected_ticker, period=chosen_period)

    with chart_col:
        if not hist.empty and "Close" in hist.columns:
            hist_r = hist.reset_index()
            avg_cost = row["Avg Cost (Local)"]
            if chart_type == "Candlestick":
                fig_price = go.Figure(
                    go.Candlestick(
                        x=hist_r["Date"],
                        open=hist_r["Open"],
                        high=hist_r["High"],
                        low=hist_r["Low"],
                        close=hist_r["Close"],
                        name=selected_ticker,
                    )
                )
                fig_price.update_layout(
                    title=f"{selected_ticker} Candlestick ({chosen_label})",
                    xaxis_rangeslider_visible=False,
                    height=420,
                )
            else:
                fig_price = px.line(
                    hist_r,
                    x="Date",
                    y="Close",
                    title=f"{selected_ticker} Closing Price ({chosen_label})",
                )
                fig_price.update_layout(height=420)
            if avg_cost > 0:
                fig_price.add_hline(
                    y=avg_cost,
                    line_dash="dot",
                    line_color="#f39c12",
                    annotation_text=f"Avg Buy {currency_sym}{avg_cost:,.2f}",
                    annotation_position="bottom right",
                )
            st.plotly_chart(fig_price, use_container_width=True)
        else:
            st.caption("Price history unavailable for this ticker.")

    st.divider()

    # ── Financial Statements ──────────────────────────────────────────────────
    st.subheader("\U0001f4ca Financial Statements (Annual)")
    st.caption("Sourced from yfinance · values in USD unless otherwise noted")

    fin_tabs = st.tabs([
        "\U0001f4b0 Income Statement",
        "\U0001f3e6 Balance Sheet",
        "\U0001f4b5 Cash Flow",
    ])

    INCOME_ROWS = [
        "Total Revenue", "Gross Profit", "Operating Income",
        "EBITDA", "Net Income", "Basic EPS", "Diluted EPS",
        "Research And Development", "Selling General Administrative",
    ]
    BALANCE_ROWS = [
        "Total Assets", "Total Liabilities Net Minority Interest",
        "Stockholders Equity", "Total Debt", "Cash And Cash Equivalents",
        "Current Assets", "Current Liabilities",
        "Net PPE",
    ]
    CASHFLOW_ROWS = [
        "Operating Cash Flow", "Free Cash Flow",
        "Capital Expenditure", "Investing Cash Flow", "Financing Cash Flow",
        "Repurchase Of Capital Stock", "Cash Dividends Paid",
    ]

    with fin_tabs[0]:
        with st.spinner("Loading income statement..."):
            inc = _fetch_income_stmt(selected_ticker)
        _render_financial_table(inc, key_rows=INCOME_ROWS)

    with fin_tabs[1]:
        with st.spinner("Loading balance sheet..."):
            bal = _fetch_balance_sheet(selected_ticker)
        _render_financial_table(bal, key_rows=BALANCE_ROWS)

    with fin_tabs[2]:
        with st.spinner("Loading cash flow..."):
            cf = _fetch_cash_flow(selected_ticker)
        _render_financial_table(cf, key_rows=CASHFLOW_ROWS)

    st.divider()

    # ── Analyst Recommendations ───────────────────────────────────────────────
    st.subheader("\U0001f9d1\u200d\U0001f4bc Analyst Recommendations")
    target_price = info.get("targetMeanPrice")
    target_high = info.get("targetHighPrice")
    target_low = info.get("targetLowPrice")
    rec_key = info.get("recommendationKey", "").replace("_", " ").title()
    num_analysts = info.get("numberOfAnalystOpinions")

    an1, an2, an3, an4 = st.columns(4)
    an1.metric(
        "Consensus",
        rec_key if rec_key else "—",
        help="Analyst consensus label: Strong Buy / Buy / Hold / Underperform / Sell.",
    )
    an2.metric(
        "Mean Target",
        f"{currency_sym}{target_price:,.2f}" if target_price else "—",
        delta=(
            f"{((target_price - live_px) / live_px * 100):+.1f}% upside"
            if target_price else None
        ),
        delta_color="normal",
        help="Average 12-month price target across all analysts covering this stock.",
    )
    an3.metric(
        "Target Range",
        f"{currency_sym}{target_low:,.0f} – {currency_sym}{target_high:,.0f}"
        if (target_low and target_high) else "—",
        help="Low to high end of analyst 12-month price targets.",
    )
    an4.metric(
        "# Analysts",
        str(num_analysts) if num_analysts else "—",
        help="Number of analysts covering this stock and providing recommendations.",
    )

    with st.spinner("Loading recent analyst history..."):
        rec_df = _fetch_recommendations(selected_ticker)
    if not rec_df.empty:
        # Show last 8 rating changes
        rec_show = rec_df.tail(8).copy()
        rec_show.index = pd.to_datetime(rec_show.index).strftime("%Y-%m-%d")
        rec_show.index.name = "Date"
        with st.expander("Recent Rating Changes", expanded=False):
            st.dataframe(rec_show, use_container_width=True)

    st.divider()

    # ── Recent News ───────────────────────────────────────────────────────────
    st.subheader("\U0001f4f0 Recent News")
    with st.spinner("Loading news..."):
        news_items = _fetch_ticker_news(selected_ticker)

    if news_items:
        for item in news_items:
            content = item.get("content", {})
            title = (
                content.get("title")
                or item.get("title")
                or "Untitled"
            )
            link = (
                content.get("canonicalUrl", {}).get("url")
                or content.get("clickThroughUrl", {}).get("url")
                or item.get("link")
                or "#"
            )
            publisher = (
                content.get("provider", {}).get("displayName")
                or item.get("publisher")
                or ""
            )
            pub_ts = (
                content.get("pubDate")
                or content.get("displayTime")
                or item.get("providerPublishTime")
            )
            if isinstance(pub_ts, (int, float)):
                try:
                    pub_date = datetime.fromtimestamp(pub_ts, tz=timezone.utc).strftime("%b %d, %Y")
                except Exception:
                    pub_date = ""
            elif isinstance(pub_ts, str):
                pub_date = pub_ts[:10]
            else:
                pub_date = ""

            meta = " &nbsp;·&nbsp; ".join(filter(None, [publisher, pub_date]))
            st.markdown(
                f"**[{title}]({link})**  \n"
                f"<small>{meta}</small>",
                unsafe_allow_html=True,
            )
    else:
        st.caption("No recent news found for this ticker.")


# ── Tab 3: Risk Assessment ────────────────────────────────────────────────────

def _tab_risk(port_df: pd.DataFrame, cash: float, total: float) -> None:
    if port_df.empty:
        st.info("No open positions to assess.")
        return

    st.subheader("\U0001f3c6 Performance Attribution")

    # Winner / loser of the week
    if not port_df.empty and "Unrealized P/L (%)" in port_df.columns:
        sorted_pnl = port_df.sort_values("Unrealized P/L (%)", ascending=False)
        winner = sorted_pnl.iloc[0]
        loser = sorted_pnl.iloc[-1]

        wa_col, wl_col = st.columns(2)
        wa_col.success(
            f"**🏆 Best Performer**\n\n"
            f"**{winner['Company']}** (`{winner['Ticker']}`)\n\n"
            f"Unrealized gain: **{winner['Unrealized P/L (%)']:+.2f}%** "
            f"(¥{winner['Unrealized P/L (JPY)']:+,.2f})"
        )
        wl_col.error(
            f"**📉 Worst Performer**\n\n"
            f"**{loser['Company']}** (`{loser['Ticker']}`)\n\n"
            f"Unrealized loss: **{loser['Unrealized P/L (%)']:+.2f}%** "
            f"(¥{loser['Unrealized P/L (JPY)']:+,.2f})"
        )

    st.divider()

    # Portfolio concentration
    st.subheader("\U0001f4cc Portfolio Concentration")
    port_df_sorted = port_df.copy()
    port_df_sorted["% of Portfolio"] = (
        port_df_sorted["Market Value (JPY)"] / total * 100.0
    ).round(2)
    port_df_sorted = port_df_sorted.sort_values("% of Portfolio", ascending=False)

    top = port_df_sorted.iloc[0]
    top_pct = top["% of Portfolio"]
    cash_pct = cash / total * 100.0

    conc1, conc2, conc3 = st.columns(3)
    conc1.metric("Largest Single Position", f"{top['Ticker']}", delta=f"{top_pct:.1f}% of total",
                 help="The single stock that represents the biggest slice of total portfolio value. "
                      "High single-name concentration raises idiosyncratic (company-specific) risk.")
    conc2.metric("Cash Reserve", format_currency(cash, "JPY"), delta=f"{cash_pct:.1f}% of total",
                 help=f"Uninvested JPY balance. Exact: \u00a5{cash:,.2f}. "
                      "Higher cash % reduces market exposure and provides dry powder for future trades.")
    equity_pct = 100.0 - cash_pct
    conc3.metric("Equity Exposure", f"{equity_pct:.1f}%",
                 help="Percentage of total capital currently deployed in stocks. "
                      "100% means fully invested with no cash buffer.")

    # Concentration bar
    conc_df = port_df_sorted[["Company", "% of Portfolio"]].copy()
    cash_conc = pd.DataFrame([{"Company": "Cash (JPY)", "% of Portfolio": round(cash_pct, 2)}])
    conc_df = pd.concat([conc_df, cash_conc], ignore_index=True).sort_values(
        "% of Portfolio", ascending=False
    )
    fig_conc = px.bar(
        conc_df,
        x="Company",
        y="% of Portfolio",
        text="% of Portfolio",
        title="Portfolio Concentration (% of Total Capital)",
        color="% of Portfolio",
        color_continuous_scale=["#27ae60", "#f39c12", "#c0392b"],
    )
    fig_conc.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
    fig_conc.update_layout(coloraxis_showscale=False, xaxis_title="", yaxis_title="% of Total")
    st.plotly_chart(fig_conc, use_container_width=True)

    st.divider()

    # Volatility proxy — rolling stdev of daily returns
    st.subheader(
        "\U0001f4c9 Estimated Volatility",
        help="Volatility measures how much a stock's price moves day-to-day.\n\n"
             "**Daily Vol (Std Dev %)**: Standard deviation of daily percentage returns over the last month. "
             "e.g. 1.5% means the stock typically moves ~1.5% per day.\n\n"
             "**Ann. Vol (%)**: Daily vol scaled to a yearly figure using \u221a252 (trading days). "
             "e.g. 25% annualized vol is typical for an individual tech stock. Above 50% is considered high.",
    )
    vol_rows: list[dict] = []
    for _, row in port_df.iterrows():
        hist = _fetch_ticker_history(row["Ticker"], period="1mo")
        if hist.empty or "Close" not in hist.columns:
            continue
        returns = hist["Close"].pct_change().dropna()
        if len(returns) < 5:
            continue
        vol_rows.append(
            {
                "Company": row["Company"],
                "Ticker": row["Ticker"],
                "Daily Vol (Std Dev %)": round(float(returns.std() * 100), 3),
                "Ann. Vol (%)": round(float(returns.std() * (252 ** 0.5) * 100), 2),
            }
        )
    if vol_rows:
        vol_df = pd.DataFrame(vol_rows).sort_values("Ann. Vol (%)", ascending=False)
        st.dataframe(
            vol_df.style.format(
                {"Daily Vol (Std Dev %)": "{:.3f}%", "Ann. Vol (%)": "{:.2f}%"}
            ).bar(subset=["Ann. Vol (%)"], color="#e74c3c"),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.caption("Insufficient price history to compute volatility.")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    st.set_page_config(page_title="Portfolio Deep Dive", layout="wide")
    setup_environment()
    ensure_team_config()

    st.title("\U0001f52d Portfolio Deep Dive")
    st.caption("Live analysis powered by yfinance · refreshes every 5 minutes")

    # ── Load data ──────────────────────────────────────────────────────────────
    with st.spinner("Loading ledger\u2026"):
        ledger = _load_ledger()

    usd_jpy = get_current_usd_jpy(fallback=150.0) or 150.0
    holdings = _net_holdings(ledger)
    wac = _weighted_avg_cost(ledger)
    cash = _cash_balance(ledger)

    if not holdings:
        st.info(
            "No open positions found. Head to the **Trading Desk** to execute your first trade."
        )
        return

    with st.spinner("Fetching live prices and company data\u2026"):
        port_df, skipped = _build_portfolio_df(holdings, wac, usd_jpy, cash)

    if skipped:
        st.warning(
            "\u26a0\ufe0f Could not fetch live price for: "
            + ", ".join(f"**{t}**" for t in skipped)
            + " \u2014 excluded from analysis."
        )

    equity_jpy = float(port_df["Market Value (JPY)"].sum()) if not port_df.empty else 0.0
    total = cash + equity_jpy

    # ── Top KPIs ───────────────────────────────────────────────────────────────
    roi = (total - STARTING_JPY_BALANCE) / STARTING_JPY_BALANCE * 100.0
    total_pnl = equity_jpy - float(port_df["Cost Basis (JPY)"].sum()) if not port_df.empty else 0.0

    k1, k2, k3, k4, k5 = st.columns([1, 1, 1, 1, 1])
    k1.metric("Total Portfolio (JPY)", format_currency(total, "JPY"), help=f"Exact: \u00a5{total:,.2f}")
    k2.metric("Equity Value (JPY)", format_currency(equity_jpy, "JPY"), help=f"Exact: \u00a5{equity_jpy:,.2f}")
    k3.metric("Cash Reserve (JPY)", format_currency(cash, "JPY"), help=f"Exact: \u00a5{cash:,.2f}")
    k4.metric("ROI vs Starting Capital", f"{roi:+.2f}%")
    k5.metric(
        "Unrealized P/L (JPY)",
        format_currency(total_pnl, "JPY"),
        delta=f"\u00a5{total_pnl:+,.0f}",
        delta_color="normal",
        help=f"Exact: \u00a5{total_pnl:+,.2f}",
    )

    st.divider()

    # ── Tabs ───────────────────────────────────────────────────────────────────
    tab_vis, tab_dive, tab_risk = st.tabs(
        ["\U0001f4ca Visual Analytics", "\U0001f50d Company Deep Dives", "\u26a0\ufe0f Risk Assessment"]
    )

    with tab_vis:
        _tab_visual(port_df, cash, total)

    with tab_dive:
        _tab_deep_dive(port_df, usd_jpy)

    with tab_risk:
        _tab_risk(port_df, cash, total)


main()
