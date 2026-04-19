from __future__ import annotations

import math
import random

import pandas as pd
import streamlit as st

from core.database import get_database
from core.market_data import get_current_usd_jpy, get_executed_fx_quote, get_live_price, get_company_name
from core.setup_env import setup_environment
from core.trade_executor import execute_trade, get_cash_balance, queue_order, process_pending_orders, is_market_open, exchange_name
from core.user_manager import ensure_team_config, get_active_member_names

SLIPPAGE_MIN = -0.0005
SLIPPAGE_MAX = 0.0005
FLAT_TRADING_COMMISSION_JPY = 500.0
FX_SPREAD = 0.0025  # 0.25% broker spread on USD/JPY


# ── Helpers ───────────────────────────────────────────────────────────────────

def _is_jp_ticker(ticker: str) -> bool:
    return ticker.upper().endswith(".T")


def _load_recent_ledger(n: int = 5) -> pd.DataFrame:
    return get_database().get_recent_ledger_df(n)


def _load_team_members() -> list[str]:
    ensure_team_config()
    return get_active_member_names()


def _get_current_holdings() -> dict[str, float]:
    """Return {ticker: net_quantity} for all positions with qty > 0, parsed from ledger."""
    import pandas as pd
    df = get_database().get_ledger_df()
    if df.empty:
        return {}
    df["Timestamp"] = df["Timestamp"].map(
        lambda ts: pd.to_datetime(ts, errors="coerce").tz_localize("UTC")
        if pd.to_datetime(ts, errors="coerce").tzinfo is None
        else pd.to_datetime(ts, errors="coerce").tz_convert("UTC")
    )
    df["Ticker"] = df["Ticker"].astype(str).str.strip().str.upper()
    df["Action"] = df["Action"].astype(str).str.strip().str.upper()
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0)
    buys = df[df["Action"] == "BUY"].groupby("Ticker")["Quantity"].sum()
    sells = df[df["Action"] == "SELL"].groupby("Ticker")["Quantity"].sum()
    net = buys.sub(sells, fill_value=0.0)
    return {str(t): float(q) for t, q in net.items() if float(q) > 0}


def _enrich_holdings(holdings: dict[str, float], usd_jpy: float) -> dict[str, dict]:
    """Return {ticker: {qty, price, value_jpy, pct}} using live prices."""
    rows: dict[str, dict] = {}
    total_jpy = 0.0
    prices: dict[str, float] = {}
    for ticker, qty in holdings.items():
        price = get_live_price(ticker, fallback=None)
        if price is None:
            prices[ticker] = 0.0
        else:
            prices[ticker] = float(price)
        fx = 1.0 if _is_jp_ticker(ticker) else usd_jpy
        value_jpy = qty * prices[ticker] * fx
        total_jpy += value_jpy
        rows[ticker] = {"qty": qty, "price": prices[ticker], "value_jpy": value_jpy, "fx": fx}
    for ticker in rows:
        rows[ticker]["pct"] = (rows[ticker]["value_jpy"] / total_jpy * 100.0) if total_jpy > 0 else 0.0
    return rows


def _get_avg_cost_jpy_per_share(ticker: str) -> float:
    """Average JPY cost per share for *ticker* from all BUY rows in the ledger."""
    try:
        df = get_database().get_ledger_df()
        if df.empty:
            return 0.0
        df["Ticker"] = df["Ticker"].astype(str).str.strip().str.upper()
        df["Action"] = df["Action"].astype(str).str.strip().str.upper()
        df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0)
        df["Total_JPY_Impact"] = pd.to_numeric(df["Total_JPY_Impact"], errors="coerce").fillna(0)
        buys = df[(df["Ticker"] == ticker.upper().strip()) & (df["Action"] == "BUY")]
        total_qty = buys["Quantity"].sum()
        if total_qty <= 0:
            return 0.0
        return float(buys["Total_JPY_Impact"].abs().sum() / total_qty)
    except Exception:
        return 0.0


def _refresh_preview_price(ticker: str) -> None:
    """Cache live price + FX rate for ticker. Only fetches when ticker changes."""
    if not ticker:
        return
    if st.session_state.get("_preview_ticker") == ticker:
        return
    price = get_live_price(ticker, fallback=None)
    fx = 1.0 if _is_jp_ticker(ticker) else get_current_usd_jpy(fallback=None)
    st.session_state["_preview_ticker"] = ticker
    st.session_state["_preview_price"] = price
    st.session_state["_preview_fx"] = fx


def _shares_from_jpy(
    jpy_budget: float,
    price: float | None,
    is_jp: bool,
    fx: float | None,
) -> float | None:
    """Max shares purchasable from jpy_budget after commission and FX spread."""
    if price is None or price <= 0:
        return None
    net_jpy = jpy_budget - FLAT_TRADING_COMMISSION_JPY
    if net_jpy <= 0:
        return 0.0
    if is_jp:
        return net_jpy / price
    if fx is None or fx <= 0:
        return None
    cost_per_share_jpy = price * fx * (1.0 + FX_SPREAD)
    return (net_jpy / cost_per_share_jpy) if cost_per_share_jpy > 0 else None


def _build_estimate(
    ticker: str,
    action: str,
    quantity: float,
    authorized_by: str,
    rationale: str,
    sizing_mode: str,
    timing: str,
) -> dict | None:
    """Fetch live pricing and return a populated estimate dict, or None on error."""
    is_jp = _is_jp_ticker(ticker)
    local_price_raw = get_live_price(ticker, fallback=None)
    if local_price_raw is None:
        st.error("Unable to fetch live price. Please check the ticker symbol.")
        return None

    slippage_factor = random.uniform(SLIPPAGE_MIN, SLIPPAGE_MAX)
    slipped_price = float(local_price_raw) * (1.0 + slippage_factor)
    asset_notional_local = quantity * slipped_price

    base = {
        "ticker": ticker,
        "action": action,
        "quantity": quantity,
        "authorized_by": authorized_by,
        "rationale": rationale,
        "sizing_mode": sizing_mode,
        "timing": timing,
        "local_price": float(local_price_raw),
        "slipped_local_price": slipped_price,
        "slippage_pct": slippage_factor * 100.0,
        "asset_notional_local": asset_notional_local,
        "commission_paid": FLAT_TRADING_COMMISSION_JPY,
    }

    if not is_jp:
        fx_quote = get_executed_fx_quote(action, usd_notional=asset_notional_local, fallback=None)
        if fx_quote is None:
            st.error("Unable to fetch USD/JPY FX rate. Please try again.")
            return None
        live_mid = float(fx_quote["live_mid_market_rate"])
        exec_fx = float(fx_quote["executed_rate"])
        fx_fee = float(fx_quote["fx_fee_amount_jpy"])
        if action == "BUY":
            jpy_impact = -(asset_notional_local * live_mid + fx_fee + FLAT_TRADING_COMMISSION_JPY)
        else:
            jpy_impact = asset_notional_local * live_mid - fx_fee - FLAT_TRADING_COMMISSION_JPY
        return {
            **base,
            "live_mid_market_fx_rate": live_mid,
            "executed_fx_rate": exec_fx,
            "fx_conversion_fee_paid": fx_fee,
            "total_jpy_impact": jpy_impact,
            "is_us_ticker": True,
        }
    else:
        if action == "BUY":
            jpy_impact = -(asset_notional_local + FLAT_TRADING_COMMISSION_JPY)
        else:
            jpy_impact = asset_notional_local - FLAT_TRADING_COMMISSION_JPY
        return {
            **base,
            "live_mid_market_fx_rate": 1.0,
            "executed_fx_rate": 1.0,
            "fx_conversion_fee_paid": 0.0,
            "total_jpy_impact": jpy_impact,
            "is_us_ticker": False,
        }


def main() -> None:
    st.set_page_config(page_title="Trading Desk", layout="wide")
    setup_environment()
    members = _load_team_members()

    st.title("Trading Desk")

    if "trade_estimate" not in st.session_state:
        st.session_state["trade_estimate"] = None

    # ── ORDER ENTRY ────────────────────────────────────────────────────────────
    st.subheader("Order Entry")

    col_auth, col_action_top = st.columns(2)
    with col_auth:
        authorized_by = st.selectbox(
            "Authorized By",
            ["-- Select group member --", *members],
            index=0,
        )
        auth_code = st.text_input("Auth Code", type="password", help="Enter your 6-character authentication code.")
    with col_action_top:
        action = st.radio("Action", ["Buy", "Sell"], horizontal=True)

    # Clear stale estimate whenever action changes (prevents old BUY firing as SELL)
    if st.session_state.get("_last_action") != action:
        st.session_state["trade_estimate"] = None
        st.session_state["_last_action"] = action

    # When selling, auto-detect current holdings and enrich with live prices
    holdings: dict[str, float] = {}
    enriched: dict[str, dict] = {}
    if action == "Sell":
        holdings = _get_current_holdings()
        if holdings:
            usd_jpy_live = get_current_usd_jpy(fallback=150.0) or 150.0
            with st.spinner("Fetching live prices for your positions…"):
                enriched = _enrich_holdings(holdings, usd_jpy_live)

    col_ticker, col_timing = st.columns(2)
    with col_ticker:
        if action == "Sell" and holdings:
            held_tickers = sorted(holdings.keys())
            prev_sell = st.session_state.get("_sell_ticker_select", held_tickers[0])
            default_sell_idx = held_tickers.index(prev_sell) if prev_sell in held_tickers else 0

            def _sell_label(t: str) -> str:
                d = enriched.get(t, {})
                qty = d.get("qty", holdings.get(t, 0))
                val = d.get("value_jpy", 0.0)
                pct = d.get("pct", 0.0)
                return f"{t}  |  {qty:,.2f} shares  |  ¥{val:,.0f}  |  {pct:.1f}% of equity"

            ticker = st.selectbox(
                "Select Position to Sell",
                held_tickers,
                index=default_sell_idx,
                format_func=_sell_label,
                key="sell_stock_selector",
            )
            # Clear stale estimate when the selected sell ticker changes
            if st.session_state.get("_sell_ticker_select") != ticker:
                st.session_state["trade_estimate"] = None
            st.session_state["_sell_ticker_select"] = ticker
        elif action == "Sell" and not holdings:
            st.warning("You have no open positions to sell.")
            ticker = st.text_input(
                "Ticker Symbol",
                value="",
                placeholder="AAPL or 7203.T",
            ).strip().upper()
        else:
            prefetched = str(st.session_state.pop("trade_prefill_ticker", "")).strip().upper()
            if prefetched:
                st.session_state["last_trade_ticker"] = prefetched
            default_ticker = str(st.session_state.get("last_trade_ticker", "")).strip().upper()
            ticker = st.text_input(
                "Ticker Symbol",
                value=default_ticker,
                placeholder="AAPL or 7203.T",
            ).strip().upper()
            # Clear stale estimate when buy ticker changes
            if st.session_state.get("_last_ticker") != ticker:
                st.session_state["trade_estimate"] = None
                st.session_state["_last_ticker"] = ticker

    # ── AUTO TIMING DETECTION ──────────────────────────────────────────────────
    # Determine timing automatically based on whether the ticker's exchange is
    # currently open.  No manual radio — the system decides and shows the status.
    if ticker:
        try:
            market_open = is_market_open(ticker)
            exch = exchange_name(ticker)
            company_name = get_company_name(ticker)
        except Exception:
            market_open = False
            exch = "Unknown"
            company_name = ticker
        
        # Display company name and market status
        st.write(f"**{company_name}** ({ticker}) — Listed on {exch}")
        
        if market_open:
            timing = "Execute Now (Market)"
            with col_timing:
                st.success(f"🟢 **{exch} is open** — order will execute immediately.")
        else:
            timing = "Execute at Next Market Open"
            with col_timing:
                st.warning(f"🔴 **{exch} is closed** — order will be queued for next open.")
    else:
        timing = "Execute Now (Market)"
        with col_timing:
            st.info("Enter a ticker to detect market hours.")

    # ── POSITION SIZING ────────────────────────────────────────────────────────
    st.divider()
    st.subheader("Position Sizing")

    sizing_mode = st.radio(
        "Sizing Mode",
        ["Shares", "Fixed JPY Amount", "% of Portfolio"],
        horizontal=True,
    )

    _refresh_preview_price(ticker)
    preview_price: float | None = st.session_state.get("_preview_price")
    preview_fx: float | None = st.session_state.get("_preview_fx")
    is_jp = _is_jp_ticker(ticker) if ticker else True
    cash_balance = get_cash_balance()

    # Sell-specific context ── enriched data + cost basis
    sell_data: dict = enriched.get(ticker, {}) if action == "Sell" and ticker else {}
    held_qty: float = sell_data.get("qty", 0.0)
    position_value_jpy: float = sell_data.get("value_jpy", 0.0)
    avg_cost_per_share = _get_avg_cost_jpy_per_share(ticker) if action == "Sell" and ticker else 0.0

    def _proceeds_preview(qty: float) -> tuple[float, float | None]:
        """(approx_proceeds_jpy, profit_jpy | None) for a sell of *qty* shares."""
        if not sell_data or qty <= 0:
            return 0.0, None
        price_local = sell_data.get("price", 0.0)
        fx = sell_data.get("fx", 1.0)
        proceeds = max(0.0, qty * price_local * fx - FLAT_TRADING_COMMISSION_JPY)
        profit = (proceeds - qty * avg_cost_per_share) if avg_cost_per_share > 0 else None
        return proceeds, profit

    computed_quantity: float = 0.0

    if sizing_mode == "Shares":
        if action == "Sell":
            default_qty = held_qty if held_qty > 0 else 0.0
            if sell_data:
                d = sell_data
                curr_sym = "¥" if is_jp else "$"
                full_proceeds, full_profit = _proceeds_preview(held_qty)
                profit_str = (
                    f" · Full P&L: **{'▲' if full_profit >= 0 else '▼'} ¥{abs(full_profit):,.0f}**"
                    if full_profit is not None else ""
                )
                st.info(
                    f"**{ticker}** — {d['qty']:,.4f} shares · "
                    f"Live: {curr_sym}{d['price']:,.2f} · "
                    f"Full value: **¥{position_value_jpy:,.0f}**{profit_str}. "
                    f"Adjust below to partially sell."
                )
            computed_quantity = st.number_input(
                "Shares to Sell",
                min_value=0.0,
                max_value=float(held_qty) if held_qty > 0 else 1e9,
                step=1.0,
                format="%.4f",
                value=float(default_qty),
            )
            if computed_quantity > 0 and sell_data:
                proceeds, profit = _proceeds_preview(computed_quantity)
                pc1, pc2, pc3 = st.columns(3)
                pc1.metric("Shares to Sell", f"{computed_quantity:,.4f}")
                pc2.metric("Est. Proceeds", f"¥{proceeds:,.0f}")
                if profit is not None:
                    pc3.metric("Est. P&L", f"¥{profit:+,.0f}")
        else:
            computed_quantity = st.number_input(
                "Quantity (Shares)", min_value=0.0, step=1.0, format="%.4f", value=0.0
            )

    elif sizing_mode == "Fixed JPY Amount":
        if action == "Sell":
            max_val = max(position_value_jpy, 1.0)
            jpy_target = st.number_input(
                "Target Sale Proceeds (¥ JPY)",
                min_value=0.0,
                max_value=float(max_val),
                step=10_000.0,
                format="%.2f",
                value=0.0,
            )
            if preview_price and preview_price > 0:
                fx_eff = 1.0 if is_jp else (preview_fx or 150.0) * (1.0 - FX_SPREAD)
                denom = preview_price * (fx_eff if fx_eff > 0 else 1.0)
                raw_shares = (jpy_target + FLAT_TRADING_COMMISSION_JPY) / denom if denom > 0 else 0.0
                est_shares = min(raw_shares, held_qty) if held_qty > 0 else raw_shares
                proceeds, profit = _proceeds_preview(est_shares)
                pc1, pc2, pc3 = st.columns(3)
                pc1.metric("Position Value", f"¥{position_value_jpy:,.0f}")
                pc2.metric("≈ Shares to Sell", f"{est_shares:,.4f}")
                if profit is not None:
                    pc3.metric("Est. P&L", f"¥{profit:+,.0f}")
                else:
                    pc3.metric("Est. Proceeds", f"¥{proceeds:,.0f}")
                computed_quantity = max(0.0, est_shares)
            else:
                st.info("Enter a ticker above to see share estimate.")
                computed_quantity = 0.0
        else:
            jpy_input = st.number_input(
                "Amount to Spend (¥ JPY)",
                min_value=0.0,
                max_value=float(cash_balance),
                step=10_000.0,
                format="%.2f",
                value=0.0,
            )
            est_shares = _shares_from_jpy(jpy_input, preview_price, is_jp, preview_fx)
            c1, c2 = st.columns(2)
            c1.metric("Available Cash", f"¥{cash_balance:,.2f}")
            c2.metric(
                "≈ Shares to Order",
                f"{est_shares:,.4f}" if est_shares is not None else "—  enter ticker first",
            )
            computed_quantity = max(0.0, est_shares) if est_shares is not None else 0.0

    else:  # % of Portfolio / Position
        if action == "Sell":
            pct = st.slider("% of Position to Sell", min_value=0, max_value=100, value=100, step=1)
            est_shares = held_qty * pct / 100.0 if held_qty > 0 else 0.0
            proceeds, profit = _proceeds_preview(est_shares)
            pc1, pc2, pc3 = st.columns(3)
            pc1.metric("Shares to Sell", f"{est_shares:,.4f}")
            pc2.metric("Est. Proceeds", f"¥{proceeds:,.0f}")
            if profit is not None:
                pc3.metric("Est. P&L", f"¥{profit:+,.0f}")
            else:
                pc3.metric("Position Value", f"¥{position_value_jpy:,.0f}")
            computed_quantity = est_shares
        else:
            pct = st.slider("% of Cash Balance", min_value=0, max_value=100, value=10, step=1)
            jpy_to_spend = cash_balance * pct / 100.0
            est_shares = _shares_from_jpy(jpy_to_spend, preview_price, is_jp, preview_fx)
            c1, c2, c3 = st.columns(3)
            c1.metric("Available Cash", f"¥{cash_balance:,.2f}")
            c2.metric("JPY to Spend", f"¥{jpy_to_spend:,.2f}")
            c3.metric(
                "≈ Shares to Order",
                f"{est_shares:,.4f}" if est_shares is not None else "—  enter ticker first",
            )
            computed_quantity = max(0.0, est_shares) if est_shares is not None else 0.0

    # ── RATIONALE ──────────────────────────────────────────────────────────────
    st.divider()
    rationale = st.text_area(
        "Trade Rationale / Analysis",
        placeholder=(
            "Explain the reason for this trade "
            "(fundamental analysis, technical signal, news catalyst, etc.)."
        ),
        height=100,
    )
    st.info("Analysis is highly advised for your university presentation.")

    normalized_action = action.upper()
    final_rationale = rationale.strip() or "No rationale provided."

    # ── CALCULATE BUTTON ───────────────────────────────────────────────────────
    calculate_clicked = st.button("\U0001f50d Calculate Estimate", type="secondary")

    if calculate_clicked:
        st.session_state["last_trade_ticker"] = ticker
        if authorized_by == "-- Select group member --":
            st.error("Please select an authorized group member.")
        elif not ticker:
            st.error("Please enter a ticker symbol.")
        elif computed_quantity <= 0:
            st.error("Quantity must be greater than zero. Adjust your sizing inputs.")
        else:
            estimate = _build_estimate(
                ticker=ticker,
                action=normalized_action,
                quantity=computed_quantity,
                authorized_by=authorized_by,
                rationale=final_rationale,
                sizing_mode=sizing_mode,
                timing=timing,
            )
            if estimate is not None:
                st.session_state["trade_estimate"] = estimate

    # ── ESTIMATE DISPLAY + EXECUTE / QUEUE ─────────────────────────────────────
    est = st.session_state.get("trade_estimate")
    if est:
        st.divider()
        st.subheader("\U0001f4cb Trade Estimate")

        is_queued_order = est.get("timing") == "Execute at Next Market Open"
        if is_queued_order:
            st.warning(
                "\u23f0 **Market Open Order** \u2014 queued in the Order Book, "
                "NOT executed immediately."
            )

        e1, e2, e3 = st.columns(3)
        e1.metric("Live Price (Local)", f"{est['local_price']:,.4f}")
        e2.metric("Slippage (%)", f"{est['slippage_pct']:+.4f}%")
        e3.metric("Est. JPY Impact", f"\u00a5{est['total_jpy_impact']:,.2f}")

        if est.get("is_us_ticker"):
            receipt = pd.DataFrame([
                {"Item": "Asset Cost (Live + Slippage)",
                 "Value": f"{est['asset_notional_local']:,.6f} USD"},
                {"Item": "Live Mid-Market FX Rate",
                 "Value": f"{est['live_mid_market_fx_rate']:,.4f} JPY/USD"},
                {"Item": "Broker Executed FX Rate (0.25% spread)",
                 "Value": f"{est['executed_fx_rate']:,.4f} JPY/USD"},
                {"Item": "Flat Commission",
                 "Value": f"\u00a5{est['commission_paid']:,.2f}"},
                {"Item": "FX Conversion Fee",
                 "Value": f"\u00a5{est['fx_conversion_fee_paid']:,.2f}"},
                {"Item": "Total JPY Deducted" if est["action"] == "BUY" else "Total JPY Added",
                 "Value": f"\u00a5{abs(est['total_jpy_impact']):,.2f}"},
            ])
            st.markdown("#### Pre-Trade FX Cost Receipt")
            st.dataframe(receipt, use_container_width=True, hide_index=True)

        # ── SELL P&L SUMMARY ───────────────────────────────────────────────────
        if est.get("action") == "SELL":
            avg_cost = _get_avg_cost_jpy_per_share(est["ticker"])
            proceeds_jpy = abs(est["total_jpy_impact"])
            sp1, sp2, sp3 = st.columns(3)
            sp1.metric("Sale Proceeds", f"¥{proceeds_jpy:,.0f}")
            if avg_cost > 0:
                total_cost = est["quantity"] * avg_cost
                profit_jpy = proceeds_jpy - total_cost
                sp2.metric("Est. Cost Basis", f"¥{total_cost:,.0f}")
                sp3.metric(
                    "Est. P&L",
                    f"¥{profit_jpy:+,.0f}",
                    delta=f"{'▲' if profit_jpy >= 0 else '▼'} {abs(profit_jpy) / total_cost * 100:.1f}%" if total_cost > 0 else None,
                )
            else:
                sp2.metric("Avg Cost Basis", "—  no buy data")
                sp3.metric("Est. P&L", "—")

        st.caption(
            f"Sizing: **{est['sizing_mode']}** \u00b7 "
            f"Qty: **{est['quantity']:,.4f}** \u00b7 "
            f"Rationale: *{est['rationale']}*"
        )

        st.divider()
        col_exec, col_cancel = st.columns([2, 1])

        exec_label = (
            "\U0001f4cb Queue for Market Open"
            if is_queued_order
            else "\u2705 Execute Trade Now"
        )
        exec_style: str = "secondary" if is_queued_order else "primary"

        if col_exec.button(exec_label, type=exec_style, key="execute_btn", use_container_width=True):
            if is_queued_order:
                _mode_map = {
                    "Shares": "SHARES",
                    "Fixed JPY Amount": "FIXED_JPY",
                    "% of Portfolio": "PERCENT",
                }
                with st.spinner("Writing to Order Book\u2026"):
                    try:
                        result = queue_order(
                            action=est["action"],
                            ticker=est["ticker"],
                            quantity=est["quantity"],
                            trader_name=est["authorized_by"],
                            mode=_mode_map.get(est["sizing_mode"], "SHARES"),
                            value=f"{est['quantity']:.6f}",
                            rationale=est["rationale"],
                                auth_code=auth_code,
                        )
                    except Exception as exc:
                        result = {"status": "error", "message": str(exc)}
                if result.get("status") == "queued":
                    st.success(
                        f"\U0001f4cb Queued! {est['action']} {est['quantity']:,.4f} \u00d7 "
                        f"{est['ticker']} will execute at next market open."
                    )
                    st.session_state["trade_estimate"] = None
                else:
                    st.error(f"\u274c Queue failed: {result.get('message', 'Unknown error.')}")
            else:
                with st.spinner("Submitting trade to Google Sheets\u2026"):
                    try:
                        result = execute_trade(
                                action=est["action"],
                                ticker=est["ticker"],
                                quantity=est["quantity"],
                                trader_name=est["authorized_by"],
                                rationale=est["rationale"],
                                auth_code=auth_code,
                        )
                    except Exception as exc:
                        result = {"status": "error", "message": str(exc)}
                if str(result.get("status", "")).lower() == "success":
                    st.success(
                        f"\u2705 Trade recorded! {result['action']} {result['quantity']:,.4f} \u00d7 "
                        f"{result['ticker']} | JPY balance: \u00a5{result['remaining_jpy_balance']:,.2f}"
                    )
                    st.json(result)
                    st.session_state["trade_estimate"] = None
                else:
                    st.error(f"\u274c Trade rejected: {result.get('message', 'Unknown error.')}")

        if col_cancel.button("\u2716 Cancel", key="cancel_btn", use_container_width=True):
            st.session_state["trade_estimate"] = None
            st.rerun()

    # ── ORDER BOOK (PENDING) ───────────────────────────────────────────────────
    st.divider()
    st.subheader("\U0001f4d1 Pending Orders")
    try:
        ob_df = get_database().get_order_book_df()
        pending = pd.DataFrame(columns=ob_df.columns)
        if not ob_df.empty and "Status" in ob_df.columns:
            pending = ob_df[ob_df["Status"] == "PENDING"].reset_index(drop=True)

        if pending.empty:
            st.info("No pending orders.")
        else:
            exec_col, _ = st.columns([2, 3])
            with exec_col:
                if st.button(
                    "\u25b6\ufe0f Execute Pending Orders Now",
                    type="primary",
                    use_container_width=True,
                    help="Runs all PENDING orders where the market is currently open.",
                ):
                    with st.spinner("Processing pending orders\u2026"):
                        results = process_pending_orders()
                    if not results:
                        st.info("No orders were eligible for execution.")
                    else:
                        for res in results:
                            ticker_label = res.get("ticker", "?")
                            if res.get("status") == "success":
                                st.success(
                                    f"\u2705 **{ticker_label}** — "
                                    f"{res.get('action', '')} {res.get('quantity', 0):,.4f} shares "
                                    f"executed. JPY impact: \u00a5{res.get('total_jpy_impact', 0):,.2f}"
                                )
                            elif res.get("status") == "skipped":
                                st.warning(
                                    f"\u23f0 **{ticker_label}** — skipped: {res.get('message', '')}"
                                )
                            else:
                                st.error(
                                    f"\u274c **{ticker_label}** — failed: {res.get('message', 'Unknown error')}"
                                )
                    st.session_state["pending_orders_executed"] = True
                    get_database.cache_clear()
                    st.cache_data.clear()
                    st.rerun()

            for i, row in pending.iterrows():
                ticker = row.get('Ticker', '?').strip().upper()
                try:
                    company_name = get_company_name(ticker)
                except Exception:
                    company_name = ticker
                
                action_emoji = "🛒" if row.get('Action', '').upper() == "BUY" else "💰"
                label = (
                    f"{action_emoji} {row.get('Action', '?')} {row.get('Value', '?')} ({company_name}) — "
                    f"Mode: {row.get('Mode', '?')} — {row.get('Timestamp', '')[:16].replace('T', ' ')} UTC"
                )
                with st.expander(label, expanded=False):
                    detail_cols = st.columns([3, 1])
                    with detail_cols[0]:
                        st.markdown(
                            f"**Ticker:** {ticker} ({company_name})  \n"
                            f"**Action:** {row.get('Action', '')}  \n"
                            f"**Mode:** {row.get('Mode', '')}  \n"
                            f"**Value:** {row.get('Value', '')}  \n"
                            f"**Trader:** {row.get('Trader_Name', 'N/A')}  \n"
                            f"**Rationale:** {row.get('Rationale', '')}  \n"
                            f"**Queued at:** {row.get('Timestamp', '')}"
                        )
                    with detail_cols[1]:
                        if st.button(
                            "\u274c Cancel Order",
                            key=f"cancel_order_{i}_{row.get('Timestamp', i)}",
                            use_container_width=True,
                        ):
                            ts = str(row.get("Timestamp", ""))
                            with st.spinner("Cancelling order\u2026"):
                                try:
                                    deleted = get_database().cancel_order(ts)
                                except Exception as exc:
                                    deleted = False
                                    st.error(f"Cancel failed: {exc}")
                            if deleted:
                                st.success("Order cancelled.")
                                st.cache_data.clear()
                                st.rerun()
                            else:
                                st.warning("Order not found — it may have already been removed.")
    except Exception as exc:
        st.info(f"Order Book not yet initialized — queue an order to create it. ({exc})")

    # ── RECENT LEDGER ──────────────────────────────────────────────────────────
    st.divider()
    st.subheader("Recent Ledger Entries")
    recent = _load_recent_ledger()
    if recent.empty:
        st.info("No ledger rows available yet.")
    else:
        st.dataframe(recent, use_container_width=True)


main()
