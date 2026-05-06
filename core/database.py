"""Database abstraction layer - PostgreSQL backend."""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
import json
from typing import Any, Final
import logging
import sys

import pandas as pd

from core.db_postgres import (
    PostgreSQLDatabase,
    get_database as get_postgres_db,
    get_connection_status as get_postgres_connection_status,
)

logger = logging.getLogger(__name__)

LEDGER_COLUMNS: Final[list[str]] = [
    "Timestamp", "Ticker", "Action", "Quantity", "Local_Asset_Price",
    "Executed_FX_Rate", "Total_JPY_Impact", "Remaining_JPY_Balance",
    "Trader_Name", "Commission_Paid", "FX_Conversion_Fee", "Trade_Rationale",
]

PERFORMANCE_COLUMNS: Final[list[str]] = ["date", "Trader_Name", "portfolio_value_jpy"]
ORDER_BOOK_COLUMNS: Final[list[str]] = [
    "Timestamp", "Ticker", "Action", "Mode", "Value", "Rationale", "Status", "Trader_Name",
]
TEAM_AUTH_COLUMNS: Final[list[str]] = ["Trader_Name", "Auth_Code", "Active", "Created_At", "Initial_Allocation_JPY"]



_LEDGER_RENAME_MAP: Final[dict[str, str]] = {
    "id": "ID",
    "timestamp": "Timestamp",
    "ticker": "Ticker",
    "action": "Action",
    "quantity": "Quantity",
    "local_asset_price": "Local_Asset_Price",
    "executed_fx_rate": "Executed_FX_Rate",
    "total_jpy_impact": "Total_JPY_Impact",
    "remaining_jpy_balance": "Remaining_JPY_Balance",
    "trader_name": "Trader_Name",
    "commission_paid": "Commission_Paid",
    "fx_conversion_fee_paid": "FX_Conversion_Fee",
    "trade_rationale": "Trade_Rationale",
    "created_at": "Created_At",
}

_PERFORMANCE_RENAME_MAP: Final[dict[str, str]] = {
    "trader_name": "Trader_Name",
}

_ORDER_BOOK_RENAME_MAP: Final[dict[str, str]] = {
    "id": "ID",
    "timestamp": "Timestamp",
    "ticker": "Ticker",
    "action": "Action",
    "mode": "Mode",
    "value": "Value",
    "rationale": "Rationale",
    "status": "Status",
    "trader_name": "Trader_Name",
    "created_at": "Created_At",
    "updated_at": "Updated_At",
}

_TEAM_AUTH_RENAME_MAP: Final[dict[str, str]] = {
    "id": "ID",
    "trader_name": "Trader_Name",
    "auth_code": "Auth_Code",
    "active": "Active",
    "initial_allocation_jpy": "Initial_Allocation_JPY",
    "created_at": "Created_At",
}


def _rows_to_frame(rows: list[Any], rename_map: dict[str, str]) -> pd.DataFrame:
    """Convert psycopg2 DictRow results into a DataFrame with legacy UI column names."""
    if not rows:
        return pd.DataFrame(columns=list(rename_map.values()))
    return pd.DataFrame([dict(row) for row in rows]).rename(columns=rename_map)

_DEFAULT_SIMULATION_SETTINGS: Final[dict[str, float]] = {
    "total_starting_capital_jpy": 100_000_000.0,
    "borrowing_limit_pct": 0.50,
    "margin_call_pct": 0.50,
    "forced_liquidation_pct": 0.35,
    "local_borrow_rate_pct": 0.028,
    "global_borrow_rate_pct": 0.028,
    "preferential_borrow_rate_pct": 0.0228,
}
_SIMULATION_SETTINGS_KEY: Final[str] = "simulation_settings"


def get_simulation_settings() -> dict[str, float]:
    """Return configurable class-project simulation settings."""
    settings = dict(_DEFAULT_SIMULATION_SETTINGS)
    try:
        raw = get_postgres_db().get_config_value(_SIMULATION_SETTINGS_KEY)
        if raw:
            loaded = json.loads(raw)
            for key, default_value in _DEFAULT_SIMULATION_SETTINGS.items():
                if key in loaded:
                    settings[key] = float(loaded[key])
    except Exception as e:
        logger.error(f"Failed to read simulation settings: {e}")
    return settings


def update_simulation_settings(**updates: float) -> dict[str, Any]:
    """Merge and persist class-project simulation settings."""
    try:
        settings = get_simulation_settings()
        for key, value in updates.items():
            if key not in _DEFAULT_SIMULATION_SETTINGS:
                raise ValueError(f"Unknown simulation setting: {key}")
            settings[key] = float(value)
        get_postgres_db().set_config_value(_SIMULATION_SETTINGS_KEY, json.dumps(settings, sort_keys=True))
        clear_data_cache()
        return {"success": True, "settings": settings}
    except Exception as e:
        logger.error(f"Failed to update simulation settings: {e}")
        return {"success": False, "error": str(e)}


def get_member_initial_allocations() -> dict[str, float]:
    """Return configured initial JPY allocations keyed by active trader name."""
    df = get_cached_team_auth_df()
    if df.empty:
        return {}
    active = df[df.get("Active", True).astype(str).str.lower().isin(["true", "1", "yes"])] if "Active" in df.columns else df
    allocations: dict[str, float] = {}
    for _, row in active.iterrows():
        name = str(row.get("Trader_Name", "")).strip()
        if not name:
            continue
        value = pd.to_numeric(pd.Series([row.get("Initial_Allocation_JPY")]), errors="coerce").iloc[0]
        allocations[name] = float(value) if pd.notna(value) and float(value) > 0 else 0.0
    return allocations


def resolve_member_initial_allocations(total_capital_jpy: float | None = None) -> dict[str, float]:
    """Resolve final starting-capital allocations for currently active members.

    Configured positive allocations are honored first. Any unallocated remainder
    is split equally across active members without a configured allocation.
    """
    settings = get_simulation_settings()
    total = float(total_capital_jpy if total_capital_jpy is not None else settings["total_starting_capital_jpy"])
    configured = get_member_initial_allocations()
    if not configured:
        return {}

    positive = {name: amount for name, amount in configured.items() if amount > 0}
    missing = [name for name, amount in configured.items() if amount <= 0]
    positive_total = sum(positive.values())
    if positive_total > total:
        raise ValueError("Configured member allocations exceed total starting capital.")

    resolved = dict(positive)
    remainder = total - positive_total
    if missing:
        per_member = remainder / len(missing)
        for name in missing:
            resolved[name] = per_member
    return resolved


def set_member_initial_allocation(trader_name: str, allocation_jpy: float | None) -> dict[str, Any]:
    """Persist a member-specific starting-capital allocation."""
    try:
        normalized = trader_name.strip()
        if not normalized:
            raise ValueError("Trader name is required.")
        value = None if allocation_jpy is None else max(float(allocation_jpy), 0.0)
        updated = get_postgres_db().set_member_allocation(normalized, value)
        if not updated:
            raise ValueError(f"Member '{normalized}' not found.")
        clear_data_cache()
        return {"success": True, "trader_name": normalized, "allocation_jpy": value}
    except Exception as e:
        logger.error(f"Failed to update member allocation: {e}")
        return {"success": False, "error": str(e)}



def split_member_allocations_equally(total_capital_jpy: float | None = None) -> dict[str, Any]:
    """Set every active member's allocation to an equal share of total capital."""
    try:
        settings = get_simulation_settings()
        total = float(total_capital_jpy if total_capital_jpy is not None else settings["total_starting_capital_jpy"])
        df = get_cached_team_auth_df()
        if df.empty:
            raise ValueError("No team members are configured.")
        active = df[df.get("Active", True).astype(str).str.lower().isin(["true", "1", "yes"])] if "Active" in df.columns else df
        names = [str(name).strip() for name in active.get("Trader_Name", []) if str(name).strip()]
        if not names:
            raise ValueError("No active team members are configured.")
        per_member = total / len(names)
        db = get_postgres_db()
        for name in names:
            db.set_member_allocation(name, per_member)
        update_simulation_settings(total_starting_capital_jpy=total)
        clear_data_cache()
        return {"success": True, "allocation_jpy": per_member, "members": names, "total": total}
    except Exception as e:
        logger.error(f"Failed to split member allocations equally: {e}")
        return {"success": False, "error": str(e)}


def update_member_auth_code(trader_name: str, current_auth_code: str, new_auth_code: str) -> dict[str, Any]:
    """Verify a member's current auth code and set a replacement code."""
    try:
        normalized = trader_name.strip()
        replacement = new_auth_code.strip()
        if not normalized:
            raise ValueError("Trader name is required.")
        if not replacement:
            raise ValueError("New auth code cannot be empty.")
        if len(replacement) < 4:
            raise ValueError("New auth code must be at least 4 characters.")

        df = get_cached_team_auth_df()
        matched = df[df["Trader_Name"].astype(str).str.casefold() == normalized.casefold()] if not df.empty else pd.DataFrame()
        if matched.empty:
            raise ValueError(f"Member '{normalized}' not found.")
        expected = str(matched.iloc[0].get("Auth_Code", "")).strip()
        if expected != current_auth_code.strip():
            raise ValueError("Current auth code is incorrect.")

        updated = get_postgres_db().update_auth_code(normalized, replacement)
        if not updated:
            raise ValueError(f"Member '{normalized}' not found.")
        clear_data_cache()
        return {"success": True, "trader_name": normalized}
    except Exception as e:
        logger.error(f"Failed to update auth code: {e}")
        return {"success": False, "error": str(e)}

def record_trade(timestamp, ticker, action, quantity, local_asset_price, executed_fx_rate,
                 total_jpy_impact, remaining_jpy_balance, trader_name, commission_paid=0,
                 fx_conversion_fee=0, trade_rationale="") -> dict[str, Any]:
    """Record a trade to the ledger."""
    try:
        db = get_postgres_db()
        db.execute_update("""
            INSERT INTO ledger (timestamp, ticker, action, quantity, local_asset_price,
                executed_fx_rate, total_jpy_impact, remaining_jpy_balance,
                trader_name, commission_paid, fx_conversion_fee_paid, trade_rationale)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (timestamp, ticker, action, float(quantity), float(local_asset_price),
                  float(executed_fx_rate), float(total_jpy_impact), float(remaining_jpy_balance),
                  trader_name, float(commission_paid), float(fx_conversion_fee), trade_rationale))
        return {"success": True, "message": "Trade recorded"}
    except Exception as e:
        logger.error(f"Failed to record trade: {e}")
        return {"success": False, "error": str(e)}


def get_cached_ledger_df() -> pd.DataFrame:
    """Fetch complete ledger as DataFrame."""
    try:
        db = get_postgres_db()
        results = db.execute_query("SELECT * FROM ledger ORDER BY timestamp, id")
        return _rows_to_frame(results, _LEDGER_RENAME_MAP)
    except Exception as e:
        logger.error(f"Failed to fetch ledger: {e}")
        return pd.DataFrame()


def _upsert_daily_performance_row(
    trader_name: str,
    portfolio_value_jpy: Decimal,
    snapshot_date: date | None = None,
) -> dict[str, Any]:
    """Insert or update one daily portfolio valuation row."""
    db = get_postgres_db()
    effective_date = snapshot_date or date.today()
    db.execute_update("""
        INSERT INTO performance (date, trader_name, portfolio_value_jpy)
        VALUES (%s, %s, %s)
        ON CONFLICT (date, trader_name)
        DO UPDATE SET portfolio_value_jpy = EXCLUDED.portfolio_value_jpy
        """, (effective_date, trader_name, float(portfolio_value_jpy)))
    return {"success": True, "date": str(effective_date), "trader_name": trader_name}


def _calculate_current_holdings(ledger: pd.DataFrame) -> dict[str, float]:
    if ledger.empty or "Action" not in ledger.columns:
        return {}

    trade_rows = ledger[ledger["Action"].isin(["BUY", "SELL"])].copy()
    if trade_rows.empty:
        return {}

    trade_rows["Quantity"] = pd.to_numeric(trade_rows["Quantity"], errors="coerce").fillna(0)
    buys = trade_rows.loc[trade_rows["Action"] == "BUY"].groupby("Ticker")["Quantity"].sum()
    sells = trade_rows.loc[trade_rows["Action"] == "SELL"].groupby("Ticker")["Quantity"].sum()
    net = buys.sub(sells, fill_value=0.0)
    return {str(ticker): float(quantity) for ticker, quantity in net.items() if float(quantity) > 0}


def _latest_cash_balances_by_trader(ledger: pd.DataFrame) -> dict[str, float]:
    if ledger.empty or "Remaining_JPY_Balance" not in ledger.columns or "Trader_Name" not in ledger.columns:
        return {}
    frame = ledger.copy()
    frame["Remaining_JPY_Balance"] = pd.to_numeric(frame["Remaining_JPY_Balance"], errors="coerce")
    frame = frame.dropna(subset=["Remaining_JPY_Balance"])
    if frame.empty:
        return {}
    latest = frame.sort_values("Timestamp").groupby("Trader_Name")["Remaining_JPY_Balance"].last()
    return {str(trader): float(balance) for trader, balance in latest.items()}


def _latest_cash_balance(ledger: pd.DataFrame) -> float:
    from core.setup_env import STARTING_JPY_BALANCE

    balances = _latest_cash_balances_by_trader(ledger)
    if balances:
        return float(sum(balances.values()))
    return float(STARTING_JPY_BALANCE)


def _value_holdings_jpy(holdings: dict[str, float], usd_jpy: float) -> tuple[float, list[str], dict[str, float]]:
    from core.market_data import get_live_price

    equity_jpy = 0.0
    skipped: list[str] = []
    live_prices: dict[str, float] = {}

    for ticker, quantity in holdings.items():
        price = get_live_price(ticker, fallback=None)
        if price is None:
            skipped.append(ticker)
            continue

        resolved_price = float(price)
        live_prices[ticker] = resolved_price
        if ticker.upper().endswith(".T"):
            equity_jpy += quantity * resolved_price
        else:
            equity_jpy += quantity * resolved_price * usd_jpy

    return equity_jpy, skipped, live_prices


def _record_portfolio_snapshot() -> dict[str, Any]:
    """Calculate and persist All Team plus per-member daily portfolio snapshots."""
    from core.market_data import get_current_usd_jpy
    from core.setup_env import STARTING_JPY_BALANCE

    ledger = get_cached_ledger_df().copy()
    if not ledger.empty:
        ledger["Ticker"] = ledger["Ticker"].astype(str).str.strip().str.upper()
        ledger["Action"] = ledger["Action"].astype(str).str.strip().str.upper()
        ledger["Trader_Name"] = ledger["Trader_Name"].astype(str).str.strip()

    cash = _latest_cash_balance(ledger)
    holdings = _calculate_current_holdings(ledger)
    usd_jpy = get_current_usd_jpy(fallback=150.0) or 150.0
    equity_jpy, skipped, live_prices = _value_holdings_jpy(holdings, usd_jpy)
    total_jpy = cash + equity_jpy
    today = datetime.now(timezone.utc).date()

    _upsert_daily_performance_row("All Team", Decimal(str(total_jpy)), today)

    if not ledger.empty and "Trader_Name" in ledger.columns:
        cash_by_trader = _latest_cash_balances_by_trader(ledger)
        traders = [
            trader for trader in ledger["Trader_Name"].dropna().unique()
            if str(trader).strip().casefold() not in {"", "system", "all team"}
        ]
        for trader in traders:
            trader_df = ledger[ledger["Trader_Name"] == trader].copy()
            trader_holdings = _calculate_current_holdings(trader_df)
            trader_equity = 0.0
            for ticker, quantity in trader_holdings.items():
                if ticker not in live_prices:
                    continue
                if ticker.upper().endswith(".T"):
                    trader_equity += quantity * live_prices[ticker]
                else:
                    trader_equity += quantity * live_prices[ticker] * usd_jpy

            trader_cash = cash_by_trader.get(str(trader), 0.0)
            trader_value = trader_cash + trader_equity
            _upsert_daily_performance_row(str(trader), Decimal(str(trader_value)), today)

    return {
        "success": True,
        "date": today.isoformat(),
        "cash_jpy": cash,
        "equity_jpy": equity_jpy,
        "total_portfolio_value_jpy": total_jpy,
        "usd_jpy_rate": usd_jpy,
        "tickers_skipped": skipped,
    }


def record_daily_performance(
    trader_name: str | None = None,
    portfolio_value_jpy: Decimal | float | int | str | None = None,
) -> dict[str, Any]:
    """Record daily performance.

    With no arguments, calculate and persist a full current portfolio snapshot for
    the background worker and dashboard refresh button. With arguments, upsert a
    single explicit trader valuation row for compatibility with direct callers.
    """
    try:
        if trader_name is None and portfolio_value_jpy is None:
            return _record_portfolio_snapshot()
        if trader_name is None or portfolio_value_jpy is None:
            raise ValueError("trader_name and portfolio_value_jpy must be provided together.")
        return _upsert_daily_performance_row(trader_name, Decimal(str(portfolio_value_jpy)))
    except Exception as e:
        logger.error(f"Failed to record performance: {e}")
        return {"success": False, "error": str(e)}


def get_cached_performance_df() -> pd.DataFrame:
    """Fetch all performance records as DataFrame."""
    try:
        db = get_postgres_db()
        results = db.execute_query("SELECT * FROM performance ORDER BY date, trader_name")
        return _rows_to_frame(results, _PERFORMANCE_RENAME_MAP)
    except Exception as e:
        logger.error(f"Failed to fetch performance: {e}")
        return pd.DataFrame()


def record_pending_order(timestamp, ticker, action, mode, value, rationale, trader_name) -> dict[str, Any]:
    """Record a pending order."""
    try:
        db = get_postgres_db()
        db.execute_update("""
            INSERT INTO order_book (timestamp, ticker, action, mode, value, rationale, status, trader_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (timestamp, ticker, action, mode, float(value), rationale, "PENDING", trader_name))
        return {"success": True}
    except Exception as e:
        logger.error(f"Failed to record order: {e}")
        return {"success": False, "error": str(e)}


def get_pending_orders() -> pd.DataFrame:
    """Fetch all pending orders as DataFrame."""
    try:
        db = get_postgres_db()
        results = db.execute_query("SELECT * FROM order_book WHERE status = 'PENDING' ORDER BY timestamp, id")
        return _rows_to_frame(results, _ORDER_BOOK_RENAME_MAP)
    except Exception as e:
        logger.error(f"Failed to fetch pending orders: {e}")
        return pd.DataFrame()


def update_order_status(order_id: int, status: str) -> dict[str, Any]:
    """Update order status."""
    try:
        db = get_postgres_db()
        db.execute_update("UPDATE order_book SET status = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s",
                         (status, order_id))
        return {"success": True}
    except Exception as e:
        logger.error(f"Failed to update order: {e}")
        return {"success": False, "error": str(e)}


def get_cached_team_auth_df() -> pd.DataFrame:
    """Fetch team authentication data as DataFrame."""
    try:
        db = get_postgres_db()
        results = db.execute_query("SELECT * FROM team_auth ORDER BY trader_name")
        return _rows_to_frame(results, _TEAM_AUTH_RENAME_MAP)
    except Exception as e:
        logger.error(f"Failed to fetch team auth: {e}")
        return pd.DataFrame()


def add_team_member(trader_name: str, auth_code: str) -> dict[str, Any]:
    """Add a team member."""
    try:
        db = get_postgres_db()
        db.execute_update("""
            INSERT INTO team_auth (trader_name, auth_code, active)
            VALUES (%s, %s, true) ON CONFLICT (trader_name) DO NOTHING
            """, (trader_name, auth_code))
        return {"success": True}
    except Exception as e:
        logger.error(f"Failed to add team member: {e}")
        return {"success": False, "error": str(e)}


def remove_team_member(trader_name: str) -> dict[str, Any]:
    """Deactivate a team member."""
    try:
        db = get_postgres_db()
        db.execute_update("UPDATE team_auth SET active = false WHERE trader_name = %s", (trader_name,))
        return {"success": True}
    except Exception as e:
        logger.error(f"Failed to remove team member: {e}")
        return {"success": False, "error": str(e)}


def get_borrowing_history(trader_name: str) -> pd.DataFrame:
    """Fetch borrowing history for a trader."""
    try:
        db = get_postgres_db()
        results = db.execute_query("SELECT * FROM borrowing WHERE trader_name = %s ORDER BY borrow_date DESC",
                                 (trader_name,))
        if not results:
            return pd.DataFrame()
        df = pd.DataFrame([dict(row) for row in results])
        df = df.rename(columns={
            "borrow_date": "date",
            "amount_jpy": "borrowed_amount",
            "repaid_amount": "repaid_amount",
            "interest_rate": "interest_rate",
            "status": "status",
        })
        df["borrowed_amount"] = pd.to_numeric(df.get("borrowed_amount"), errors="coerce").fillna(0)
        df["repaid_amount"] = pd.to_numeric(df.get("repaid_amount"), errors="coerce").fillna(0)
        df["balance"] = (df["borrowed_amount"] - df["repaid_amount"]).clip(lower=0)
        df["interest_paid"] = 0.0
        return df
    except Exception as e:
        logger.error(f"Failed to fetch borrowing history: {e}")
        return pd.DataFrame()


def record_borrowing(trader_name: str, borrowed_amount: Decimal, interest_rate: Decimal = Decimal(0.05)) -> dict[str, Any]:
    """Record a borrowing transaction."""
    try:
        db = get_postgres_db()
        db.execute_update("""
            INSERT INTO borrowing (trader_name, borrow_date, amount_jpy, status, interest_rate)
            VALUES (%s, CURRENT_TIMESTAMP, %s, 'ACTIVE', %s)
            """, (trader_name, float(borrowed_amount), float(interest_rate)))
        return {"success": True}
    except Exception as e:
        logger.error(f"Failed to record borrowing: {e}")
        return {"success": False, "error": str(e)}


def record_repayment(trader_name: str, repaid_amount: Decimal) -> dict[str, Any]:
    """Record a repayment."""
    try:
        db = get_postgres_db()
        result = db.execute_query(
            "SELECT id FROM borrowing WHERE trader_name = %s AND status = 'ACTIVE' ORDER BY borrow_date DESC LIMIT 1",
            (trader_name,))
        
        if not result:
            return {"success": False, "error": "No active borrowing found"}
        
        borrow_id = result[0][0]
        current = db.execute_query("SELECT amount_jpy, COALESCE(repaid_amount, 0) FROM borrowing WHERE id = %s", (borrow_id,))
        amount_jpy = float(current[0][0])
        already_repaid = float(current[0][1])
        new_repaid = min(amount_jpy, already_repaid + float(repaid_amount))
        status = "REPAID" if new_repaid >= amount_jpy else "ACTIVE"
        db.execute_update(
            "UPDATE borrowing SET repay_date = CURRENT_TIMESTAMP, repaid_amount = %s, status = %s WHERE id = %s",
            (new_repaid, status, borrow_id))
        return {"success": True, "remaining_balance": max(amount_jpy - new_repaid, 0.0)}
    except Exception as e:
        logger.error(f"Failed to record repayment: {e}")
        return {"success": False, "error": str(e)}


def calculate_accrued_interest(trader_name: str) -> Decimal:
    """Calculate total accrued interest for active borrowings."""
    try:
        db = get_postgres_db()
        results = db.execute_query("""
            SELECT SUM(amount_jpy * interest_rate * EXTRACT(DAY FROM (CURRENT_TIMESTAMP - borrow_date)) / 365.0)
            FROM borrowing WHERE trader_name = %s AND status = 'ACTIVE'
            """, (trader_name,))
        
        if results and results[0][0]:
            return Decimal(str(results[0][0]))
        return Decimal(0)
    except Exception as e:
        logger.error(f"Failed to calculate interest: {e}")
        return Decimal(0)


def get_database() -> PostgreSQLDatabase:
    """Get database instance."""
    return get_postgres_db()


def clear_data_cache() -> None:
    """Clear in-process Streamlit caches when Streamlit is loaded."""
    streamlit_module = sys.modules.get("streamlit")
    cache_data = getattr(streamlit_module, "cache_data", None) if streamlit_module else None
    clear = getattr(cache_data, "clear", None) if cache_data else None
    if callable(clear):
        clear()


# Backward compatibility for pages that previously used an lru_cache-wrapped
# Google Sheets get_database() function and manually called cache_clear().
get_database.cache_clear = clear_data_cache  # type: ignore[attr-defined]


def start_new_simulation(starting_capital: Decimal | float | int | str | None = None) -> dict[str, Any]:
    """Initialize a new simulation by funding every active member."""
    try:
        total_capital = float(starting_capital) if starting_capital is not None else get_simulation_settings()["total_starting_capital_jpy"]
        allocations = resolve_member_initial_allocations(total_capital)
        db = get_postgres_db()
        timestamp = datetime.now(timezone.utc)

        if allocations:
            for trader_name, allocation in allocations.items():
                db.execute_update("""
                    INSERT INTO ledger (timestamp, ticker, action, quantity, local_asset_price,
                        executed_fx_rate, total_jpy_impact, remaining_jpy_balance,
                        trader_name, commission_paid, fx_conversion_fee_paid, trade_rationale)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (timestamp, "JPY_CASH", "INITIAL_FUNDING", 0, 1, 1,
                          allocation, allocation, trader_name, 0, 0, "Initial member funding"))
        else:
            db.execute_update("""
                INSERT INTO ledger (timestamp, ticker, action, quantity, local_asset_price,
                    executed_fx_rate, total_jpy_impact, remaining_jpy_balance,
                    trader_name, commission_paid, fx_conversion_fee_paid, trade_rationale)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (timestamp, "JPY_CASH", "INITIAL_FUNDING", 0, 1, 1,
                      total_capital, total_capital, "System", 0, 0, "Initial Funding"))

        update_simulation_settings(total_starting_capital_jpy=total_capital)
        clear_data_cache()
        return {"success": True, "starting_capital": total_capital, "allocations": allocations}
    except Exception as e:
        logger.error(f"Failed to start simulation: {e}")
        return {"success": False, "error": str(e)}


def initialize_database_schema() -> dict[str, Any]:
    """Initialize database schema."""
    try:
        db = get_postgres_db()
        return {"success": True, "message": "PostgreSQL schema initialized", "database": db.dbname}
    except Exception as e:
        logger.error(f"Failed to initialize schema: {e}")
        return {"success": False, "error": str(e)}


def get_connection_status() -> dict[str, Any]:
    """Get database connection status."""
    return get_postgres_connection_status()


def get_google_sheets_connection_status() -> dict[str, Any]:
    """Get database connection status (backward compatibility)."""
    status = get_postgres_connection_status()
    if status.get("connected"):
        return {"connected": True, "message": "PostgreSQL connected.",
                "spreadsheet_title": status.get("database_name"), "spreadsheet_id": status.get("host")}
    return {"connected": False, "message": f"Connection failed: {status.get('error')}",
            "spreadsheet_title": None, "spreadsheet_id": None}
