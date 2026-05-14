"""7_Borrowing_Tab.py — Margin Borrowing & Interest Calculator

This page simulates the Rakuten Securities margin trading (信用取引) system,
allowing team members to "burrow" (borrow) against their portfolio.

## How It Works (Based on Real Rakuten Margin Trading):

### BORROWING LIMITS
- Maximum borrowable amount = 50% of current portfolio value
- This mirrors the Japanese "制度信用取引" (system margin trading) 50% margin requirement

### INTEREST RATES (Current Rakuten Rates - April 2026)
- **Buy-Side Interest (買方金利)**: 2.80% per annum (standard rate)
- **Preferential Rate**: 2.28% per annum (for high-volume traders)
- **Intraday (いちにち信用)**: 0.00% (no interest if closed same day)

### ASSOCIATED COSTS
- **Stock Rental Fee (貸株料)**: 1.10% per annum when shorting
- **Administrative Fee**: ¥0.11 per share (¥110 minimum)
- **Reverse Day-Haba (逆日歩)**: Market-determined daily fee for short positions

### DAILY INTEREST CALCULATION
Daily Interest = (Borrowed Amount × Annual Rate) ÷ 365
"""
from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from datetime import date, datetime, timedelta

from core.database import (
    get_cached_ledger_df,
    get_borrowing_history,
    get_simulation_settings,
    record_borrowing,
    record_repayment,
    resolve_member_initial_allocations,
)
from core.market_data import get_current_usd_jpy, get_live_price
from core.setup_env import setup_environment, STARTING_JPY_BALANCE
from core.trade_executor import format_currency
from core.user_manager import ensure_team_config, get_active_member_names, get_member_aliases

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Margin Borrowing Centre",
    page_icon="🏦",
    layout="wide",
)
setup_environment()
ensure_team_config()


# ── RAKUTEN MARGIN RATES (Accurate as of April 2026) ───────────────────────
RAKUTEN_BUY_SIDE_RATE = 0.028  # 2.80% per annum
RAKUTEN_PREFERENTIAL_RATE = 0.0228  # 2.28% per annum (high-volume traders)
RAKUTEN_INTRADAY_RATE = 0.00  # 0.00% (no interest if closed same day)
RAKUTEN_STOCK_RENTAL_RATE = 0.011  # 1.10% per annum (for short positions)
RAKUTEN_ADMIN_FEE_PER_SHARE = 0.11  # ¥0.11 per share
RAKUTEN_MIN_ADMIN_FEE = 110  # ¥110 minimum


# ── Helpers ───────────────────────────────────────────────────────────────────

def _fmt_jpy(amount: float) -> str:
    return f"¥{amount:,.0f}"


def _fmt_pct(value: float) -> str:
    return f"{value * 100:.2f}%"


def _fmt_rate(value: float) -> str:
    return f"{value * 100:.2f}%"


def _member_selector() -> str:
    members = get_active_member_names()
    if not members:
        st.error("No team members found. Configure your team in the Admin Panel.")
        st.stop()
    return st.selectbox("Select Team Member", members, key="borrow_trader_name")


def _fallback_allocation_for_member(trader_name: str) -> float:
    """Return configured starting allocation for a member, case-insensitively."""
    allocations = resolve_member_initial_allocations()
    for member, allocation in allocations.items():
        if member.casefold() == trader_name.strip().casefold():
            return float(allocation)
    return STARTING_JPY_BALANCE / max(len(allocations), 1)


def _load_member_ledger(trader_name: str) -> pd.DataFrame:
    """Return a member-scoped ledger with PostgreSQL Decimal values coerced to floats."""
    ledger = get_cached_ledger_df()
    if ledger.empty or "Trader_Name" not in ledger.columns:
        return pd.DataFrame()

    aliases = {name.casefold() for name in get_member_aliases(trader_name)}
    if not aliases:
        aliases = {trader_name.strip().casefold()}
    frame = ledger[ledger["Trader_Name"].astype(str).str.strip().str.casefold().isin(aliases)].copy()
    if frame.empty:
        return frame

    if "Ticker" in frame.columns:
        frame["Ticker"] = frame["Ticker"].astype(str).str.strip().str.upper()
    if "Action" in frame.columns:
        frame["Action"] = frame["Action"].astype(str).str.strip().str.upper()
    for column in ["Quantity", "Local_Asset_Price", "Total_JPY_Impact", "Remaining_JPY_Balance"]:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce").astype(float)
    return frame


def _net_holdings_from_ledger(member_ledger: pd.DataFrame) -> dict[str, float]:
    """Calculate current holdings without pandas Decimal/object arithmetic."""
    if member_ledger.empty or not {"Action", "Ticker", "Quantity"}.issubset(member_ledger.columns):
        return {}

    frame = member_ledger.copy()
    frame["Quantity"] = pd.to_numeric(frame["Quantity"], errors="coerce").fillna(0.0).astype(float)
    frame["Action"] = frame["Action"].astype(str).str.strip().str.upper()
    frame["Ticker"] = frame["Ticker"].astype(str).str.strip().str.upper()

    holdings: dict[str, float] = {}
    for _, row in frame.iterrows():
        ticker = str(row.get("Ticker", "")).strip().upper()
        if not ticker:
            continue
        quantity = float(row.get("Quantity", 0.0) or 0.0)
        action = str(row.get("Action", "")).strip().upper()
        if action == "BUY":
            holdings[ticker] = holdings.get(ticker, 0.0) + quantity
        elif action == "SELL":
            holdings[ticker] = holdings.get(ticker, 0.0) - quantity

    return {ticker: quantity for ticker, quantity in holdings.items() if quantity > 0}


@st.cache_data(ttl=300)
def _get_member_portfolio_value(trader_name: str) -> float:
    """Calculate current portfolio value for a member."""
    fallback_allocation = _fallback_allocation_for_member(trader_name)
    trader_ledger = _load_member_ledger(trader_name)
    if trader_ledger.empty:
        return fallback_allocation

    holdings = _net_holdings_from_ledger(trader_ledger)

    # Calculate equity value
    usd_jpy = float(get_current_usd_jpy(fallback=150.0) or 150.0)
    equity = 0.0
    for ticker, qty in holdings.items():
        try:
            price = get_live_price(ticker)
            if price is not None:
                price_value = float(price)
                if ticker.endswith(".T"):
                    equity += qty * price_value  # TSE stocks in JPY
                else:
                    equity += qty * price_value * usd_jpy  # US stocks converted to JPY
        except Exception:
            pass

    # Get cash balance
    if "Remaining_JPY_Balance" in trader_ledger.columns:
        balances = pd.to_numeric(trader_ledger["Remaining_JPY_Balance"], errors="coerce").astype(float).dropna()
        cash = float(balances.iloc[-1]) if not balances.empty else fallback_allocation
    else:
        cash = fallback_allocation

    return cash + equity


@st.cache_data(ttl=300)
def _get_current_holdings(trader_name: str) -> dict[str, float]:
    """Get current stock holdings for a member."""
    return _net_holdings_from_ledger(_load_member_ledger(trader_name))


def _calculate_daily_interest(borrowed_amount: float, annual_rate: float) -> float:
    """Calculate daily interest (based on actual/365 day count)."""
    return (borrowed_amount * annual_rate) / 365


def _calculate_monthly_interest(borrowed_amount: float, annual_rate: float) -> float:
    """Calculate monthly interest (approximate)."""
    return (borrowed_amount * annual_rate) / 12


# ── Main Page ───────────────────────────────────────────────────────────────

st.title("🏦 Margin Borrowing Centre")
st.markdown("""
This page simulates **Rakuten Securities margin trading (信用取引)**. 
Each member can borrow up to the Admin-configured percentage of their portfolio value and pay simulated Rakuten-style interest rates.
""")

# Rate information expander
with st.expander("📊 Current Rakuten Margin Rates (April 2026)", expanded=False):
    st.markdown("""
    ### Buy-Side Interest Rates (買方金利)
    | Rate Type | Annual Rate |
    |---|---|
    | Standard Rate | **2.80%** |
    | Preferential Rate | **2.28%** |
    | Intraday (いちにち信用) | **0.00%** |
    
    ### Other Associated Costs
    | Fee Type | Rate |
    |---|---|
    | Stock Rental Fee (貸株料) | 1.10% per annum |
    | Administrative Fee | ¥0.11/share (¥110 min) |
    | Reverse Day-Haba (逆日歩) | Market-determined |
    
    *Preferential rates apply for traders with ≥¥500M monthly positions.*
    """)

# Member selection
trader_name = _member_selector()
aliases = get_member_aliases(trader_name)
member_display = aliases[0] if aliases else trader_name

# Get portfolio value and Admin-configured borrowing policy
settings = get_simulation_settings()
margin_requirement = float(settings["borrowing_limit_pct"])
local_borrow_rate = float(settings["local_borrow_rate_pct"])
global_borrow_rate = float(settings["global_borrow_rate_pct"])
preferential_borrow_rate = float(settings["preferential_borrow_rate_pct"])
margin_call_pct = float(settings["margin_call_pct"])
forced_liquidation_pct = float(settings["forced_liquidation_pct"])
portfolio_value = _get_member_portfolio_value(trader_name)
max_borrow = portfolio_value * margin_requirement

# ── Borrowing Interface ─────────────────────────────────────────────────────

st.divider()
st.subheader(f"💰 {member_display}'s Borrowing Account")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Portfolio Value", _fmt_jpy(portfolio_value))

with col2:
    st.metric(f"Max Borrowable ({margin_requirement * 100:.0f}%)", _fmt_jpy(max_borrow))

with col3:
    # Get current borrow from database
    borrow_history = get_borrowing_history(trader_name)
    current_borrow = float(borrow_history["balance"].sum()) if not borrow_history.empty and "balance" in borrow_history.columns else 0.0
    st.metric("Current Borrowed", _fmt_jpy(current_borrow))

with col4:
    available_to_borrow = max(max_borrow - current_borrow, 0.0)
    st.metric("Available", _fmt_jpy(available_to_borrow))

# Borrowing form
st.markdown("### 📝 Borrow Funds")

col_a, col_b, col_c = st.columns([2, 2, 1])

with col_a:
    borrow_amount = st.number_input(
        "Amount to Borrow (JPY)",
        min_value=0.0,
        max_value=float(max(available_to_borrow, 0.0)),
        step=100000.0,
        value=0.0,
        key="borrow_amount_input"
    )

with col_b:
    rate_option = st.selectbox(
        "Interest Rate",
        [
            f"Local/Japan ({local_borrow_rate * 100:.2f}%)",
            f"Global/US ({global_borrow_rate * 100:.2f}%)",
            f"Preferential ({preferential_borrow_rate * 100:.2f}%)",
            "Intraday (0.00%)",
        ],
        index=0,
        key="rate_option"
    )
    rate_map = {
        f"Local/Japan ({local_borrow_rate * 100:.2f}%)": local_borrow_rate,
        f"Global/US ({global_borrow_rate * 100:.2f}%)": global_borrow_rate,
        f"Preferential ({preferential_borrow_rate * 100:.2f}%)": preferential_borrow_rate,
        "Intraday (0.00%)": RAKUTEN_INTRADAY_RATE,
    }
    selected_rate = rate_map[rate_option]

with col_c:
    st.write("")  # spacer
    st.write("")  # spacer
    if st.button("💳 Borrow", type="primary", use_container_width=True):
        if borrow_amount <= 0:
            st.warning("Enter an amount to borrow.")
        elif borrow_amount > available_to_borrow:
            st.error("Borrow amount exceeds this member's available borrowing capacity.")
        else:
            try:
                result = record_borrowing(trader_name, borrow_amount, selected_rate)
                if result.get("success"):
                    st.success(f"Borrowed {_fmt_jpy(borrow_amount)} successfully!")
                    st.rerun()
                else:
                    st.error(str(result.get("error", "Borrowing failed.")))
            except Exception as e:
                st.error(f"Error recording borrowing: {e}")

# Repayment form
st.markdown("### 💵 Repay Funds")

col_d, col_e = st.columns([2, 1])

with col_d:
    repay_amount = st.number_input(
        "Amount to Repay (JPY)",
        min_value=0.0,
        max_value=float(current_borrow),
        step=100000.0,
        value=0.0,
        key="repay_amount_input"
    )

with col_e:
    st.write("")  # spacer
    st.write("")  # spacer
    if st.button("💰 Repay", type="secondary", use_container_width=True):
        if repay_amount > 0:
            try:
                result = record_repayment(trader_name, repay_amount)
                st.success(f"Repaid {_fmt_jpy(repay_amount)} successfully!")
                st.rerun()
            except Exception as e:
                st.error(f"Error recording repayment: {e}")
        else:
            st.warning("Enter an amount to repay.")

# ── Interest Calculator ───────────────────────────────────────────────────

st.divider()
st.subheader("📊 Interest Calculator")

if current_borrow > 0:
    # Calculate interest projections
    daily_interest = _calculate_daily_interest(current_borrow, selected_rate)
    monthly_interest = _calculate_monthly_interest(current_borrow, selected_rate)
    annual_interest = current_borrow * selected_rate
    
    # Interest breakdown
    col_i1, col_i2, col_i3, col_i4 = st.columns(4)
    
    with col_i1:
        st.metric("Daily Interest", _fmt_jpy(daily_interest))
    
    with col_i2:
        st.metric("Monthly Interest", _fmt_jpy(monthly_interest))
    
    with col_i3:
        st.metric("Annual Interest", _fmt_jpy(annual_interest))
    
    with col_i4:
        total_cost = current_borrow + annual_interest
        st.metric("Total to Repay", _fmt_jpy(total_cost))
    
    # Interest over time chart
    st.markdown("### 📈 Interest Projection")
    
    projection_days = st.slider("Projection Period (days)", 30, 365, 90, key="projection_days")
    
    # Generate projection data
    projection_data = []
    cumulative_interest = 0.0
    for day in range(1, projection_days + 1):
        cumulative_interest += daily_interest
        projection_data.append({
            "Day": day,
            "Principal": current_borrow,
            "Cumulative Interest": cumulative_interest,
            "Total": current_borrow + cumulative_interest
        })
    
    df_proj = pd.DataFrame(projection_data)
    
    # Create chart
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df_proj["Day"],
        y=df_proj["Principal"],
        mode="lines",
        name="Principal",
        line=dict(color="#3498db", width=2)
    ))
    fig.add_trace(go.Scatter(
        x=df_proj["Day"],
        y=df_proj["Cumulative Interest"],
        mode="lines",
        name="Cumulative Interest",
        line=dict(color="#e74c3c", width=2),
        fill="tozeroy",
        fillcolor="rgba(231, 76, 60, 0.2)"
    ))
    
    fig.update_layout(
        title=f"Interest Accumulation Over {projection_days} Days",
        xaxis_title="Days",
        yaxis_title="Amount (JPY)",
        hovermode="x unified",
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01)
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Cost breakdown table
    st.markdown("### 📋 Cost Breakdown")
    
    cost_data = {
        "Item": ["Principal Borrowed", "Annual Interest Rate", "Daily Interest", 
                 "Monthly Interest", "Annual Interest", "Total Cost (1 year)"],
        "Amount": [
            _fmt_jpy(current_borrow),
            _fmt_rate(selected_rate),
            _fmt_jpy(daily_interest),
            _fmt_jpy(monthly_interest),
            _fmt_jpy(annual_interest),
            _fmt_jpy(current_borrow + annual_interest)
        ]
    }
    st.table(pd.DataFrame(cost_data))
    
else:
    st.info("💡 No outstanding borrowings. Use the form above to borrow funds.")

# ── Margin Requirement Warning ───────────────────────────────────────────

st.divider()
st.subheader("⚠️ Margin Requirements")

col_m1, col_m2 = st.columns(2)

with col_m1:
    st.markdown("""
    ### Japanese Margin Trading Rules
    - **Initial Margin**: 50% of position value required
    - **Maintenance Margin**: 30% minimum (may trigger margin call)
    - **Settlement**: T+2 business days
    """)

with col_m2:
    # Calculate margin status
    if portfolio_value > 0:
        current_margin = (portfolio_value - current_borrow) / portfolio_value
        margin_status = "✅ Safe" if current_margin >= margin_call_pct else "⚠️ Warning"
        
        st.markdown(f"""
        ### Your Margin Status
        - **Equity**: {_fmt_jpy(portfolio_value)}
        - **Borrowed**: {_fmt_jpy(current_borrow)}
        - **Net Equity**: {_fmt_jpy(portfolio_value - current_borrow)}
        - **Margin Ratio**: {current_margin * 100:.1f}%
        - **Status**: {margin_status}
        """)
        
        if current_margin < margin_call_pct:
            st.error("⚠️ Margin Call Warning: Your margin ratio is below the Admin-configured warning threshold. Add funds or reduce borrowing.")
        elif current_margin < forced_liquidation_pct:
            st.error("🚨 Forced Liquidation Risk: Your margin ratio is below the Admin-configured forced-liquidation threshold!")

# ── Historical Borrowing Record ───────────────────────────────────────────

st.divider()
st.subheader("📜 Borrowing History")

# Try to load from database
try:
    borrow_history = get_borrowing_history(trader_name)
    
    if not borrow_history.empty:
        st.dataframe(
            borrow_history[["date", "borrowed_amount", "repaid_amount", "interest_paid", "balance"]],
            use_container_width=True
        )
    else:
        st.info("No borrowing history found.")
except Exception as e:
    st.caption(f"History not available: {e}")

# Footer
st.markdown("---")
st.caption("""
📌 **Disclaimer**: This is a simulation based on Rakuten Securities' actual margin trading rates. 
Actual rates may vary. This tool is for educational purposes only.
""")