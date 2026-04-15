from __future__ import annotations

import datetime as dt
import random
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Final

import pandas as pd
import pytz

from core.database import clear_data_cache, get_cached_ledger_df, get_database
from core.market_data import get_executed_fx_quote, get_live_price
from core.setup_env import STARTING_JPY_BALANCE

FLAT_COMMISSION_JPY: Final[Decimal] = Decimal("500.00")
SLIPPAGE_MIN: Final[float] = -0.0005
SLIPPAGE_MAX: Final[float] = 0.0005
_TSE_DIGIT_PATTERN: Final[re.Pattern[str]] = re.compile(r"^\d{4}$")


def _d(value: float | int | str | Decimal) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"Invalid numeric value: {value}") from exc


def _money(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _qty(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)


def _normalize_action(action: str) -> str:
    normalized = action.strip().upper()
    if normalized not in {"BUY", "SELL"}:
        raise ValueError("action must be 'BUY' or 'SELL'.")
    return normalized


def _normalize_ticker(ticker: str) -> str:
    symbol = ticker.strip().upper()
    if not symbol:
        raise ValueError("ticker cannot be empty.")
    if _TSE_DIGIT_PATTERN.match(symbol):
        return f"{symbol}.T"
    return symbol


def _database_ready():
    return get_database()


def _load_ledger() -> pd.DataFrame:
    # Use the Streamlit-cached version to avoid redundant Sheets API calls.
    # Numeric coercion is already applied inside get_ledger_df().
    df = get_cached_ledger_df()
    if df.empty:
        return df
    df = df.copy()
    df["Ticker"] = df["Ticker"].astype(str).str.strip().str.upper()
    df["Action"] = df["Action"].astype(str).str.strip().str.upper()
    return df


def _latest_balance() -> float:
    df = _load_ledger()
    if df.empty:
        return STARTING_JPY_BALANCE

    value = df["Remaining_JPY_Balance"].dropna()
    if value.empty:
        return STARTING_JPY_BALANCE
    return float(value.iloc[-1])


def _current_holdings() -> dict[str, Decimal]:
    df = _load_ledger()
    if df.empty:
        return {}

    buy = df.loc[df["Action"] == "BUY"].groupby("Ticker")["Quantity"].sum()
    sell = df.loc[df["Action"] == "SELL"].groupby("Ticker")["Quantity"].sum()
    net = buy.sub(sell, fill_value=0.0)

    holdings: dict[str, Decimal] = {}
    for ticker, quantity in net.items():
        qty_value = _d(float(quantity))
        if qty_value > 0:
            holdings[str(ticker)] = qty_value
    return holdings


def is_market_open(ticker: str) -> bool:
    """Public wrapper — True if the exchange for *ticker* is currently trading."""
    return _is_market_open(_normalize_ticker(ticker))


def exchange_name(ticker: str) -> str:
    """Public wrapper — returns 'TSE' or 'NYSE/NASDAQ'."""
    return _exchange_name(_normalize_ticker(ticker))


def _is_tse_ticker(ticker: str) -> bool:
    return ticker.upper().endswith(".T")


# CME Globex futures tickers end with "=F" (e.g. GC=F, SI=F, CL=F)
def _is_futures_ticker(ticker: str) -> bool:
    return ticker.upper().endswith("=F")


def _is_market_open(ticker: str) -> bool:
    if _is_tse_ticker(ticker):
        now_local = dt.datetime.now(pytz.timezone("Asia/Tokyo"))
        open_time = now_local.replace(hour=9, minute=0, second=0, microsecond=0)
        close_time = now_local.replace(hour=15, minute=0, second=0, microsecond=0)
        if now_local.weekday() >= 5:
            return False
        return open_time <= now_local <= close_time

    if _is_futures_ticker(ticker):
        # CME Globex trades nearly 24h, Sunday 6 PM – Friday 5 PM ET
        # Simplified: open Mon–Fri all day; closed Sat, and Sun before 18:00 ET
        now_et = dt.datetime.now(pytz.timezone("America/New_York"))
        wd = now_et.weekday()  # 0=Mon … 6=Sun
        if wd == 5:  # Saturday fully closed
            return False
        if wd == 6:  # Sunday: open from 18:00 ET only
            return now_et.hour >= 18
        # Mon–Fri: closed only in the 5 PM ET maintenance window (17:00–18:00)
        return not (now_et.hour == 17)

    # Default: NYSE/NASDAQ regular hours
    now_local = dt.datetime.now(pytz.timezone("America/New_York"))
    open_time = now_local.replace(hour=9, minute=30, second=0, microsecond=0)
    close_time = now_local.replace(hour=16, minute=0, second=0, microsecond=0)
    if now_local.weekday() >= 5:
        return False
    return open_time <= now_local <= close_time


def _exchange_name(ticker: str) -> str:
    if _is_tse_ticker(ticker):
        return "TSE"
    if _is_futures_ticker(ticker):
        return "CME/COMEX"
    return "NYSE/NASDAQ"


def _append_trade_row(row: dict[str, str]) -> None:
    _database_ready().append_ledger_row(row)


def execute_trade(
    action: str,
    ticker: str,
    quantity: float,
    trader_name: str,
    rationale: str = "",  # NEW: student trade justification for grading
    auth_code: str = "",  # NEW: verify user code
) -> dict[str, float | str]:
    from core.user_manager import authenticate_user
    normalized_action = _normalize_action(action)
    symbol = _normalize_ticker(ticker)

    student = trader_name.strip()
    if not student:
        return {
            "status": "error",
            "message": "Trader_Name is required",
            "remaining_jpy_balance": _latest_balance(),
        }

    if auth_code != "AUTO" and student.casefold() != "system":
        if not authenticate_user(student, auth_code):
            return {
                "status": "error",
                "message": f"Authentication failed for {student}. Invalid code.",
                "remaining_jpy_balance": _latest_balance(),
            }

    if not _is_market_open(symbol):
        return {
            "status": "error",
            "message": f"{_exchange_name(symbol)} market is currently closed. Order rejected.",
            "exchange": _exchange_name(symbol),
            "remaining_jpy_balance": _latest_balance(),
        }

    qty = _d(quantity)
    if qty <= 0:
        raise ValueError("quantity must be greater than 0.")

    last_seen_price_raw = get_live_price(symbol, fallback=None)
    if last_seen_price_raw is None:
        return {
            "status": "error",
            "message": f"Live price unavailable for {symbol}. Order rejected.",
            "remaining_jpy_balance": _latest_balance(),
        }

    last_seen_price = _d(last_seen_price_raw)
    slippage_factor = _d(random.uniform(SLIPPAGE_MIN, SLIPPAGE_MAX))
    local_asset_price = last_seen_price * (Decimal("1") + slippage_factor)

    if _is_tse_ticker(symbol):
        live_mid_fx_rate = Decimal("1")
        executed_fx_rate = Decimal("1")
        fx_conversion_fee_paid = Decimal("0")
    else:
        usd_notional = float(qty * local_asset_price)
        fx_quote = get_executed_fx_quote(normalized_action, usd_notional=usd_notional, fallback=None)
        if fx_quote is None:
            return {
                "status": "error",
                "message": "USD/JPY FX rate unavailable. Order rejected.",
                "remaining_jpy_balance": _latest_balance(),
            }

        live_mid_fx_rate = _d(fx_quote["live_mid_market_rate"])
        executed_fx_rate = _d(fx_quote["executed_rate"])
        fx_conversion_fee_paid = _d(fx_quote["fx_fee_amount_jpy"])

    usd_notional_decimal = qty * local_asset_price
    gross_jpy_mid = usd_notional_decimal * live_mid_fx_rate
    if normalized_action == "BUY":
        total_jpy_impact = -(gross_jpy_mid + fx_conversion_fee_paid + FLAT_COMMISSION_JPY)
    else:
        total_jpy_impact = gross_jpy_mid - fx_conversion_fee_paid - FLAT_COMMISSION_JPY

    previous_balance = _d(_latest_balance())

    if normalized_action == "SELL":
        holdings = _current_holdings()
        available_qty = holdings.get(symbol, Decimal("0"))
        if qty > available_qty:
            return {
                "status": "error",
                "message": f"Insufficient holdings for {symbol}. Available={float(available_qty):.6f}",
                "remaining_jpy_balance": float(previous_balance),
            }

    remaining_balance = previous_balance + total_jpy_impact
    if remaining_balance < 0:
        return {
            "status": "error",
            "message": "Insufficient JPY cash balance.",
            "remaining_jpy_balance": float(previous_balance),
        }

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    row = {
        "Timestamp": timestamp,
        "Ticker": symbol,
        "Action": normalized_action,
        "Quantity": f"{_qty(qty):f}",
        "Local_Asset_Price": f"{_qty(local_asset_price):f}",
        "Executed_FX_Rate": f"{_qty(executed_fx_rate):f}",
        "Total_JPY_Impact": f"{_money(total_jpy_impact):f}",
        "Remaining_JPY_Balance": f"{_money(remaining_balance):f}",
        "Trader_Name": student,
        "Commission_Paid": f"{_money(FLAT_COMMISSION_JPY):f}",
        "FX_Conversion_Fee": f"{_money(fx_conversion_fee_paid):f}",
        "Trade_Rationale": rationale.strip(),  # NEW: 12th column
    }

    _append_trade_row(row)
    # Invalidate the Streamlit data cache so the UI immediately reflects the
    # new balance and the updated ledger without waiting for the TTL to expire.
    clear_data_cache()

    return {
        "status": "success",
        "timestamp": timestamp,
        "trader_name": student,
        "exchange": _exchange_name(symbol),
        "ticker": symbol,
        "action": normalized_action,
        "quantity": float(_qty(qty)),
        "last_seen_price": float(_qty(last_seen_price)),
        "local_asset_price": float(_qty(local_asset_price)),
        "slippage_pct": round(float(slippage_factor * Decimal("100")), 5),
        "live_mid_market_fx_rate": float(_qty(live_mid_fx_rate)),
        "executed_fx_rate": float(_qty(executed_fx_rate)),
        "commission_paid": float(_money(FLAT_COMMISSION_JPY)),
        "fx_conversion_fee_paid": float(_money(fx_conversion_fee_paid)),
        "total_jpy_impact": float(_money(total_jpy_impact)),
        "remaining_jpy_balance": float(_money(remaining_balance)),
    }


def get_cash_balance() -> float:
    """Return the current JPY cash balance from the cached ledger df.

    Re-uses whichever cached read is already in-flight for the current page
    render instead of issuing a second Sheets API call.
    """
    df = get_cached_ledger_df()
    if df.empty:
        return STARTING_JPY_BALANCE
    bal = df["Remaining_JPY_Balance"].dropna()
    return float(bal.iloc[-1]) if not bal.empty else STARTING_JPY_BALANCE


def queue_order(
    action: str,
    ticker: str,
    quantity: float,
    trader_name: str,
    mode: str,
    value: str,
    rationale: str = "",
    auth_code: str = "",
) -> dict[str, str]:
    """Write a pending order to the Order_Book worksheet instead of the Ledger.

    Parameters
    ----------
    action:      "BUY" or "SELL"
    ticker:      stock symbol (e.g. "AAPL" or "7203.T")
    quantity:    computed share count for this order
    trader_name: authorizing team member
    mode:        "SHARES", "FIXED_JPY", or "PERCENT"
    value:       raw input value as a string (shares / jpy / pct)
    rationale:   optional trade justification
    auth_code:   trader's private authentication code
    """
    from core.database import get_database, ORDER_BOOK_COLUMNS  # local to avoid circular
    from core.user_manager import authenticate_user

    normalized_action = _normalize_action(action)
    symbol = _normalize_ticker(ticker)
    student = trader_name.strip()
    if not student:
        return {"status": "error", "message": "Trader_Name is required."}

    if not authenticate_user(student, auth_code):
        return {"status": "error", "message": f"Authentication failed for {student}. Invalid code."}

    row = {
        "Timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        "Ticker": symbol,
        "Action": normalized_action,
        "Mode": mode.strip().upper(),
        "Value": str(value).strip(),
        "Rationale": rationale.strip() or "No rationale provided.",
        "Status": "PENDING",
        "Trader_Name": student,
    }

    get_database().append_order_book_row(row)

    return {
        "status": "queued",
        "timestamp": row["Timestamp"],
        "ticker": symbol,
        "action": normalized_action,
        "mode": row["Mode"],
        "value": row["Value"],
        "rationale": row["Rationale"],
        "trader_name": student,
    }


def process_pending_orders() -> list[dict]:
    """Execute all PENDING orders in the Order_Book where the market is currently open.

    For each PENDING order:
    - If the market for that ticker is open, calls execute_trade immediately.
    - On success, marks the order EXECUTED in the Order_Book.
    - On failure, marks the order FAILED and records the reason.
    - If the market is still closed, skips the order (leaves it PENDING).

    Returns a list of result dicts — one entry per processed order.
    """
    db = get_database()
    try:
        ob_df = db.get_order_book_df()
    except Exception as exc:
        return [{"status": "error", "message": f"Could not read Order_Book: {exc}"}]

    if ob_df.empty or "Status" not in ob_df.columns:
        return []

    pending = ob_df[ob_df["Status"].astype(str).str.upper() == "PENDING"].reset_index(drop=True)
    if pending.empty:
        return []

    results: list[dict] = []

    for _, order in pending.iterrows():
        raw_ticker = str(order.get("Ticker", "")).strip()
        if not raw_ticker:
            continue

        try:
            symbol = _normalize_ticker(raw_ticker)
        except ValueError as exc:
            results.append({"ticker": raw_ticker, "status": "error", "message": str(exc)})
            continue

        timestamp = str(order.get("Timestamp", "")).strip()

        if not _is_market_open(symbol):
            results.append({
                "ticker": symbol,
                "order_timestamp": timestamp,
                "status": "skipped",
                "message": f"{_exchange_name(symbol)} market is currently closed.",
            })
            continue

        action = str(order.get("Action", "")).strip().upper()
        value_str = str(order.get("Value", "0")).strip()
        rationale = str(order.get("Rationale", "")).strip()
        trader_name = str(order.get("Trader_Name", "")).strip() or "Auto-Execution"

        # Value is always stored as the pre-computed share count (float string)
        try:
            quantity = float(value_str)
        except (ValueError, TypeError):
            db.update_order_status(timestamp, "FAILED")
            results.append({
                "ticker": symbol,
                "order_timestamp": timestamp,
                "status": "error",
                "message": f"Invalid quantity value stored in Order_Book: '{value_str}'",
            })
            continue

        if quantity <= 0:
            db.update_order_status(timestamp, "FAILED")
            results.append({
                "ticker": symbol,
                "order_timestamp": timestamp,
                "status": "error",
                "message": "Computed quantity is zero or negative.",
            })
            continue

        result = execute_trade(
            action=action,
            ticker=symbol,
            quantity=quantity,
            trader_name=trader_name,
            rationale=f"[Market-Open Auto-Execution] {rationale}",
            auth_code="AUTO",
        )

        result["order_timestamp"] = timestamp

        if result.get("status") == "success":
            db.update_order_status(timestamp, "EXECUTED")
        else:
            db.update_order_status(timestamp, "FAILED")

        results.append(result)

    return results


# ── Currency formatter ────────────────────────────────────────────────────────

def format_currency(amount: float, currency: str = "JPY") -> str:
    """Return a compact, human-readable currency string.

    Rules
    -----
    * |amount| >= 1 000 000  →  ¥100.5M  /  $1.2M
    * |amount| >= 1 000      →  ¥150.3K  /  $5.2K
    * Otherwise              →  ¥850     /  $9.99
    """
    sym = "¥" if currency.upper() == "JPY" else "$"
    sign = "-" if amount < 0 else ""
    abs_val = abs(amount)

    if abs_val >= 1_000_000:
        return f"{sign}{sym}{abs_val / 1_000_000:.2f}M"
    if abs_val >= 1_000:
        return f"{sign}{sym}{abs_val / 1_000:.2f}K"

    # Below 1 000: JPY shows no decimals, USD shows 2
    decimals = 0 if currency.upper() == "JPY" else 2
    return f"{sign}{sym}{abs_val:,.{decimals}f}"
