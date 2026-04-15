"""5_Dividends_Tax.py — Dividend Income & Tax Centre

This page shows three things clearly:

  1. DIVIDEND INCOME
       Collect dividends for your current holdings with a single click.
       The system checks every stock you hold, fetches real dividend data
       from yfinance, and credits the net amount (after withholding taxes)
       straight to your JPY balance.

  2. TAX RULES — HOW IT WORKS
       A clear breakdown of every tax rate applied so you always know
       exactly where your money goes.

  3. CAPITAL GAINS TRACKER
       Every SELL you have made is analysed against its average purchase
       cost to calculate your realised gain / loss and the 20.315 % Japanese
       capital-gains tax owed on profitable trades.
"""
from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from core.database import get_cached_ledger_df
from core.dividend_engine import (
    JP_CAPITAL_GAINS_TAX_RATE,
    JP_DIVIDEND_TAX_RATE,
    US_DIVIDEND_EFFECTIVE_RATE,
    US_WITHHOLDING_RATE,
    collect_all_dividends,
    compute_capital_gains_tax,
    compute_dividend_tax,
    fetch_dividend_history,
    get_current_holdings,
    get_dividend_history_from_ledger,
    get_realized_gains_from_ledger,
    get_upcoming_dividends,
)
from core.market_data import get_current_usd_jpy, get_live_price
from core.setup_env import setup_environment
from core.trade_executor import format_currency
from core.user_manager import ensure_team_config, get_active_member_names

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Dividends & Tax Centre",
    page_icon="💴",
    layout="wide",
)
setup_environment()
ensure_team_config()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _fmt_jpy(amount: float) -> str:
    return f"¥{amount:,.0f}"


def _fmt_pct(value: float) -> str:
    return f"{value:.3f}%"


def _member_selector() -> str:
    members = get_active_member_names()
    if not members:
        st.error("No team members found. Configure your team in the Admin Panel.")
        st.stop()
    return st.selectbox("Authorising Team Member", members, key="div_trader_name")


@st.cache_data(ttl=300)
def _cached_holdings():
    return get_current_holdings()


@st.cache_data(ttl=300)
def _cached_div_history():
    return get_dividend_history_from_ledger()


@st.cache_data(ttl=300)
def _cached_gains():
    return get_realized_gains_from_ledger()


@st.cache_data(ttl=300)
def _cached_upcoming(ticker: str) -> pd.DataFrame:
    return get_upcoming_dividends(ticker)


@st.cache_data(ttl=600)
def _cached_yf_div_history(ticker: str) -> pd.DataFrame:
    return fetch_dividend_history(ticker)


# ── Dividend preview table ────────────────────────────────────────────────────

def _estimate_next_ex_date(hist: pd.DataFrame) -> pd.Timestamp | None:
    """Estimate next future ex-dividend date from historical frequency."""
    if hist.empty or len(hist) < 2:
        return None
    today = pd.Timestamp.now(tz="UTC")
    dates = hist["date"].sort_values().dropna()
    last_date = dates.iloc[-1]
    # If yfinance history already has a future date, use it directly
    if last_date > today:
        return last_date
    intervals = dates.diff().dropna()
    if intervals.empty:
        return None
    avg_interval = intervals.median()
    next_date = last_date + avg_interval
    # Advance until strictly in the future
    while next_date <= today:
        next_date += avg_interval
    return next_date


def _render_pending_preview(holdings: dict[str, float], usd_jpy: float) -> None:
    """Show a preview of dividends the user is likely to receive soon."""
    today = pd.Timestamp.now(tz="UTC")
    rows = []
    for ticker, qty in holdings.items():
        upcoming = _cached_upcoming(ticker)
        hist = _cached_yf_div_history(ticker)

        # Most recent dividend per share as a proxy for upcoming
        recent_dps = 0.0
        if not hist.empty:
            recent_dps = float(hist["amount"].iloc[-1])

        if recent_dps <= 0:
            continue  # stock pays no dividend

        # 1. Use confirmed future ex-date from yfinance info (already future-filtered)
        ex_date_str: str | None = None
        confirmed = False
        if not upcoming.empty and "ex_dividend_date" in upcoming.columns:
            ex_ts = upcoming["ex_dividend_date"].iloc[0]
            if pd.notna(ex_ts) and ex_ts > today:
                ex_date_str = ex_ts.strftime("%Y-%m-%d")
                confirmed = True

        # 2. Fall back: estimate next ex-date from historical frequency
        if ex_date_str is None:
            estimated = _estimate_next_ex_date(hist)
            if estimated is not None:
                ex_date_str = f"~{estimated.strftime('%Y-%m-%d')} (est.)"

        # Skip entirely if we cannot determine any future ex-date
        if ex_date_str is None:
            continue

        is_jp = ticker.endswith(".T")
        fx = 1.0 if is_jp else usd_jpy
        currency = "JPY" if is_jp else "USD"
        gross_jpy = recent_dps * qty * fx
        tax = compute_dividend_tax(gross_jpy, ticker)

        rows.append({
            "Ticker": ticker,
            "Shares Held": f"{qty:,.4f}",
            "Last Div/Share": f"{recent_dps:.6f} {currency}",
            "Next Ex-Date": ex_date_str,
            "Estimated Gross": _fmt_jpy(gross_jpy),
            "Estimated Tax": _fmt_jpy(tax["total_tax_jpy"]),
            "Estimated Net": _fmt_jpy(tax["net_jpy"]),
            "Eff. Tax Rate": _fmt_pct(tax["effective_rate_pct"]),
        })

    if rows:
        st.dataframe(
            pd.DataFrame(rows),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info(
            "No upcoming dividend ex-dates found for your current holdings. "
            "Holdings with confirmed or estimated future ex-dates will appear here."
        )


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 1 — DIVIDEND INCOME
# ══════════════════════════════════════════════════════════════════════════════

st.title("💴 Dividends & Tax Centre")
st.caption(
    "Collect dividend income, understand your tax obligations, and track "
    "realised capital gains — all in one place."
)

with st.expander("ℹ️  How this page works", expanded=False):
    st.markdown(
        """
        **Dividend Collection**
        Click **Collect All Dividends** to scan every stock you currently hold.
        The system retrieves real historical dividend data from Yahoo Finance,
        identifies any dividends paid since your last collection, and credits the
        **net amount** (after applicable withholding taxes) directly to your JPY
        balance as a new ledger row.

        **Tax Rules Applied Automatically**
        | Stock Type | Tax Applied | Effective Rate |
        |---|---|---|
        | US (NYSE/NASDAQ) | 10% US withholding + 20.315% JP tax on remainder | ≈ 28.28% |
        | Japanese (TSE) | 20.315% JP withholding | 20.315% |

        **Capital Gains Tax**
        When you sell a stock at a profit, the Realized Gains section shows the
        20.315% Japanese capital gains tax owed.  Losses generate zero tax.
        """,
        unsafe_allow_html=False,
    )

# ── Collect dividends ─────────────────────────────────────────────────────────

st.header("1 — Dividend Income", divider="green")

col_left, col_right = st.columns([1, 2])

with col_left:
    trader_name = _member_selector()
    auth_code = st.text_input("Auth Code", type="password", help="Enter your 6-character authentication code.")

    usd_jpy = float(get_current_usd_jpy(fallback=150.0) or 150.0)
    st.caption(f"Live USD/JPY rate: **{usd_jpy:.2f}**")

    collect_btn = st.button(
        "💰  Collect All Dividends",
        use_container_width=True,
        type="primary",
        help="Scans all held stocks and credits uncollected dividends to your account.",
    )

holdings = _cached_holdings()

with col_right:
    st.subheader("Upcoming Dividend Estimate (Future Ex-Dates Only)", anchor=False)
    if holdings:
        _render_pending_preview(holdings, usd_jpy)
    else:
        st.info("No open positions found. Buy stocks to start earning dividends.")

# ── Collection result ─────────────────────────────────────────────────────────

if collect_btn:
    if not trader_name:
        st.error("Select a team member before collecting dividends.")
    elif not auth_code:
        st.error("Please enter your authentication code.")
    else:
        with st.spinner("Scanning holdings and fetching dividend data…"):
            try:
                results = collect_all_dividends(trader_name, auth_code)
            except PermissionError as e:
                st.error(str(e))
                results = None

        if results is not None and not results:
            st.info(
                "✅ No new dividends to collect. Either your holdings do not pay "
                "dividends or all dividends have already been collected."
            )
        elif results:
            total_gross = sum(e["gross_jpy"] for events in results.values() for e in events)
            total_tax = sum(e["total_tax_jpy"] for events in results.values() for e in events)
            total_net = sum(e["net_jpy"] for events in results.values() for e in events)

            st.success(
                f"✅ Dividends collected for **{len(results)}** stock(s)  |  "
                f"Gross: **{_fmt_jpy(total_gross)}**  |  "
                f"Tax withheld: **{_fmt_jpy(total_tax)}**  |  "
                f"Net credited to balance: **{_fmt_jpy(total_net)}**"
            )

            for ticker, events in results.items():
                with st.expander(f"📄  {ticker} — {len(events)} dividend event(s)", expanded=True):
                    for ev in events:
                        c1, c2, c3, c4 = st.columns(4)
                        c1.metric("Ex-Date", ev["ex_date"].strftime("%Y-%m-%d"))
                        c2.metric(
                            f"Div/Share ({ev['currency']})",
                            f"{ev['amount_local']:.6f}",
                        )
                        c3.metric("Gross (JPY)", _fmt_jpy(ev["gross_jpy"]))
                        c4.metric("Net Credited", _fmt_jpy(ev["net_jpy"]))

                        st.markdown("**Tax breakdown:**")
                        if ev["is_us_stock"]:
                            st.markdown(
                                f"- 🇺🇸 US withholding: **{_fmt_jpy(ev['us_withholding_jpy'])}** (10 %)\n"
                                f"- 🇯🇵 JP tax on remainder: **{_fmt_jpy(ev['jp_tax_jpy'])}** (20.315 %)\n"
                                f"- Total withheld: **{_fmt_jpy(ev['total_tax_jpy'])}** "
                                f"(effective {ev['effective_rate_pct']:.2f} %)"
                            )
                        else:
                            st.markdown(
                                f"- 🇯🇵 JP withholding: **{_fmt_jpy(ev['jp_tax_jpy'])}** (20.315 %)\n"
                                f"- Total withheld: **{_fmt_jpy(ev['total_tax_jpy'])}** "
                                f"(effective {ev['effective_rate_pct']:.2f} %)"
                            )

            # Refresh caches so the history table below picks up the new rows
            st.cache_data.clear()
            st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 2 — TAX RULES EXPLAINED
# ══════════════════════════════════════════════════════════════════════════════

st.header("2 — Tax Rules at a Glance", divider="orange")

tax_col1, tax_col2 = st.columns(2)

with tax_col1:
    st.subheader("📌 Dividend Tax", anchor=False)
    st.markdown(
        f"""
**🇺🇸 US Stocks (NYSE / NASDAQ)**

| Step | Description | Rate |
|---|---|---|
| 1 | US withholding tax (Japan–US treaty) | **10.000 %** |
| 2 | JP domestic tax on remaining 90 % | **20.315 %** |
| — | **Combined effective rate** | **≈ {US_DIVIDEND_EFFECTIVE_RATE * 100:.3f} %** |

> *Japan residents can claim a foreign tax credit for the US 10 % at annual*
> *tax filing to avoid some double-taxation, but this simulation applies the*
> *full withholding at source as most brokerages do.*

---

**🇯🇵 Japanese Stocks (TSE)**

| Tax | Rate |
|---|---|
| National income tax | 15.000 % |
| Local resident tax | 5.105 % |
| **Total withholding** | **{JP_DIVIDEND_TAX_RATE * 100:.3f} %** |
"""
    )

with tax_col2:
    st.subheader("📌 Capital Gains Tax", anchor=False)
    st.markdown(
        f"""
**All Stocks (US and JP)**

Japan taxes realised investment profits at a single flat rate regardless
of the stock's country of listing.

| Tax Component | Rate |
|---|---|
| National income tax | 15.000 % |
| Local restoration surcharge | 0.315 % |
| **Total capital gains tax** | **{JP_CAPITAL_GAINS_TAX_RATE * 100:.3f} %** |

**When does it apply?**
- Only on **profits** (sell proceeds > average cost basis).
- Losses produce **zero** additional tax.
- Each SELL is assessed against the weighted-average cost of all prior BUYs
  for that ticker.

> *A foreign tax credit mechanism exists for US-stock gains but is handled*
> *at annual self-assessment, not at point of sale — consistent with most*
> *Japanese brokerage practice.*
"""
    )

# ── Interactive tax calculator ─────────────────────────────────────────────────

with st.expander("🧮  Interactive Tax Calculator", expanded=False):
    calc_col1, calc_col2 = st.columns(2)

    with calc_col1:
        st.subheader("Dividend Calculator", anchor=False)
        d_gross = st.number_input("Gross Dividend (JPY)", min_value=0.0, value=100_000.0, step=1000.0, key="d_gross")
        d_type = st.radio("Stock Type", ["US Stock", "JP Stock"], horizontal=True, key="d_type")
        proxy_ticker = "AAPL" if d_type == "US Stock" else "7203.T"
        if d_gross > 0:
            d_tax = compute_dividend_tax(d_gross, proxy_ticker)
            st.markdown(f"""
| | Amount |
|---|---|
| Gross dividend | **{_fmt_jpy(d_tax['gross_jpy'])}** |
| US withholding (10%) | **-{_fmt_jpy(d_tax['us_withholding_jpy'])}** |
| JP domestic tax | **-{_fmt_jpy(d_tax['jp_tax_jpy'])}** |
| **Total tax withheld** | **-{_fmt_jpy(d_tax['total_tax_jpy'])}** |
| **Net to your account** | **{_fmt_jpy(d_tax['net_jpy'])}** |
| Effective tax rate | **{_fmt_pct(d_tax['effective_rate_pct'])}** |
""")

    with calc_col2:
        st.subheader("Capital Gains Calculator", anchor=False)
        cg_gain = st.number_input("Realised Gain (JPY)", min_value=0.0, value=500_000.0, step=10_000.0, key="cg_gain")
        if cg_gain > 0:
            cg_tax = compute_capital_gains_tax(cg_gain)
            st.markdown(f"""
| | Amount |
|---|---|
| Realised gain | **{_fmt_jpy(cg_tax['gain_jpy'])}** |
| Capital gains tax (20.315%) | **-{_fmt_jpy(cg_tax['tax_jpy'])}** |
| **Net gain after tax** | **{_fmt_jpy(cg_tax['net_after_tax_jpy'])}** |
| Tax rate | **{_fmt_pct(cg_tax['rate_pct'])}** |
""")


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 3 — DIVIDEND HISTORY
# ══════════════════════════════════════════════════════════════════════════════

st.header("3 — Dividend History", divider="blue")

div_history = _cached_div_history()

if div_history.empty:
    st.info("No dividends have been collected yet. Click **Collect All Dividends** above.")
else:
    # ── Summary KPIs ──────────────────────────────────────────────────────────
    div_history["Total_JPY_Impact"] = pd.to_numeric(div_history["Total_JPY_Impact"], errors="coerce").fillna(0)
    total_div_net = div_history["Total_JPY_Impact"].sum()
    n_events = len(div_history)
    tickers_paid = div_history["Ticker"].nunique()

    k1, k2, k3 = st.columns(3)
    k1.metric("Total Net Dividends Received", _fmt_jpy(total_div_net))
    k2.metric("Dividend Events", str(n_events))
    k3.metric("Stocks That Paid", str(tickers_paid))

    # ── Table ─────────────────────────────────────────────────────────────────
    display_cols = {
        "Timestamp": "Date",
        "Ticker": "Ticker",
        "Quantity": "Shares",
        "Local_Asset_Price": "Div/Share (Local)",
        "Executed_FX_Rate": "FX Rate",
        "Total_JPY_Impact": "Net JPY Credited",
        "Trader_Name": "Collected By",
    }
    available = [c for c in display_cols if c in div_history.columns]
    display_df = div_history[available].rename(columns=display_cols).copy()

    if "Date" in display_df.columns:
        display_df["Date"] = display_df["Date"].apply(
            lambda t: t.strftime("%Y-%m-%d %H:%M") if pd.notna(t) else ""
        )
    if "Net JPY Credited" in display_df.columns:
        display_df["Net JPY Credited"] = display_df["Net JPY Credited"].apply(
            lambda v: _fmt_jpy(float(v)) if pd.notna(v) else "—"
        )

    st.dataframe(display_df, use_container_width=True, hide_index=True)

    # ── Bar chart — net dividends per ticker ──────────────────────────────────
    by_ticker = (
        div_history.groupby("Ticker")["Total_JPY_Impact"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
    )
    fig = go.Figure(
        go.Bar(
            x=by_ticker["Ticker"],
            y=by_ticker["Total_JPY_Impact"],
            marker_color="#2196F3",
            text=by_ticker["Total_JPY_Impact"].apply(_fmt_jpy),
            textposition="outside",
        )
    )
    fig.update_layout(
        title="Net Dividend Income by Stock",
        xaxis_title="Ticker",
        yaxis_title="Net JPY Received",
        template="plotly_dark",
        height=350,
        margin=dict(t=50, b=30),
    )
    st.plotly_chart(fig, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 4 — REALIZED CAPITAL GAINS & TAX
# ══════════════════════════════════════════════════════════════════════════════

st.header("4 — Realised Capital Gains & Tax", divider="red")

gains_df = _cached_gains()

if gains_df.empty:
    st.info(
        "No SELL transactions found yet. Capital gains and tax will appear "
        "here once you sell positions at a profit or loss."
    )
else:
    gains_df["Realized_Gain_JPY"] = pd.to_numeric(gains_df["Realized_Gain_JPY"], errors="coerce").fillna(0)
    gains_df["Tax_Due_JPY"] = pd.to_numeric(gains_df["Tax_Due_JPY"], errors="coerce").fillna(0)
    gains_df["Net_Gain_JPY"] = pd.to_numeric(gains_df["Net_Gain_JPY"], errors="coerce").fillna(0)

    total_gain = gains_df["Realized_Gain_JPY"].sum()
    total_tax_due = gains_df["Tax_Due_JPY"].sum()
    total_net = gains_df["Net_Gain_JPY"].sum()
    profitable_trades = (gains_df["Realized_Gain_JPY"] > 0).sum()
    loss_trades = (gains_df["Realized_Gain_JPY"] < 0).sum()

    # ── KPIs ──────────────────────────────────────────────────────────────────
    cg1, cg2, cg3, cg4, cg5 = st.columns(5)
    cg1.metric("Total Realised Gain/Loss", _fmt_jpy(total_gain), delta=None)
    cg2.metric(
        "Tax Owed (20.315%)",
        _fmt_jpy(total_tax_due),
        help="Applies to profitable trades only. Losses carry zero tax.",
    )
    cg3.metric("Net After Tax", _fmt_jpy(total_net))
    cg4.metric("Profitable Trades", str(profitable_trades))
    cg5.metric("Loss Trades", str(loss_trades))

    if total_tax_due > 0:
        st.warning(
            f"⚠️  You have an estimated capital gains tax liability of "
            f"**{_fmt_jpy(total_tax_due)}** (20.315 % on realised profits). "
            f"This is computed for reference — consult your tax advisor for "
            f"annual filing."
        )

    # ── Detailed table ────────────────────────────────────────────────────────
    table_df = gains_df.copy()
    if "Timestamp" in table_df.columns:
        table_df["Timestamp"] = table_df["Timestamp"].apply(
            lambda t: t.strftime("%Y-%m-%d") if pd.notna(t) else ""
        )

    def _color_gain(val):
        try:
            v = float(str(val).replace("¥", "").replace(",", ""))
            return "color: #4CAF50" if v > 0 else ("color: #F44336" if v < 0 else "")
        except Exception:
            return ""

    fmt_table = table_df.copy()
    for col in ["Sell_Proceeds_JPY", "Cost_Basis_JPY", "Realized_Gain_JPY", "Tax_Due_JPY", "Net_Gain_JPY"]:
        if col in fmt_table.columns:
            fmt_table[col] = fmt_table[col].apply(lambda v: _fmt_jpy(float(v)) if pd.notna(v) else "—")
    if "Quantity" in fmt_table.columns:
        fmt_table["Quantity"] = fmt_table["Quantity"].apply(lambda v: f"{float(v):,.4f}" if pd.notna(v) else "—")

    rename_map = {
        "Timestamp": "Date",
        "Ticker": "Ticker",
        "Quantity": "Shares Sold",
        "Sell_Proceeds_JPY": "Proceeds",
        "Cost_Basis_JPY": "Cost Basis",
        "Realized_Gain_JPY": "Gain / Loss",
        "Tax_Due_JPY": "Tax (20.315%)",
        "Net_Gain_JPY": "Net After Tax",
        "Trader_Name": "Trader",
    }
    available_cg = [c for c in rename_map if c in fmt_table.columns]
    fmt_table = fmt_table[available_cg].rename(columns=rename_map)

    st.dataframe(fmt_table, use_container_width=True, hide_index=True)

    # ── Waterfall chart — cumulative P&L ─────────────────────────────────────
    plot_df = gains_df[["Ticker", "Realized_Gain_JPY"]].copy()
    plot_df = plot_df.groupby("Ticker")["Realized_Gain_JPY"].sum().sort_values(ascending=False).reset_index()

    colors = ["#4CAF50" if v >= 0 else "#F44336" for v in plot_df["Realized_Gain_JPY"]]

    fig2 = go.Figure(
        go.Bar(
            x=plot_df["Ticker"],
            y=plot_df["Realized_Gain_JPY"],
            marker_color=colors,
            text=plot_df["Realized_Gain_JPY"].apply(_fmt_jpy),
            textposition="outside",
        )
    )
    fig2.update_layout(
        title="Realised Gain / Loss by Stock",
        xaxis_title="Ticker",
        yaxis_title="Gain / Loss (JPY)",
        template="plotly_dark",
        height=350,
        margin=dict(t=50, b=30),
    )
    fig2.add_hline(y=0, line_color="white", line_width=1)
    st.plotly_chart(fig2, use_container_width=True)

    # ── Tax rate sanity note ──────────────────────────────────────────────────
    with st.expander("📖  Capital Gains Tax — Calculation Details", expanded=False):
        st.markdown(
            f"""
**Rate applied: {JP_CAPITAL_GAINS_TAX_RATE * 100:.3f} %** (Japanese statutory flat rate)

The table above computes capital gains tax for each SELL row using
the **weighted average cost basis** method (total JPY spent on BUYs ÷
total shares bought before the SELL).

**Formula per SELL:**
```
Average cost basis  = Total BUY cost (JPY) ÷ Total shares bought
Cost of shares sold = Average cost basis × Shares sold
Realised gain       = Sell proceeds (JPY) − Cost of shares sold
Tax owed            = max(Gain, 0) × 20.315 %
```

**Loss offsetting:** Japan allows realised losses to offset gains within
the same tax year when filing an annual self-assessment return.  This
simulation shows each trade independently; netting is done at filing time.
"""
        )
