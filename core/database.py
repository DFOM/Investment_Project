"""Database abstraction layer - PostgreSQL backend."""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
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
TEAM_AUTH_COLUMNS: Final[list[str]] = ["Trader_Name", "Auth_Code", "Active", "Created_At"]



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
    "created_at": "Created_At",
}


def _rows_to_frame(rows: list[Any], rename_map: dict[str, str]) -> pd.DataFrame:
    """Convert psycopg2 DictRow results into a DataFrame with legacy UI column names."""
    if not rows:
        return pd.DataFrame(columns=list(rename_map.values()))
    return pd.DataFrame([dict(row) for row in rows]).rename(columns=rename_map)

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


def _latest_cash_balance(ledger: pd.DataFrame) -> float:
    from core.setup_env import STARTING_JPY_BALANCE

    if ledger.empty or "Remaining_JPY_Balance" not in ledger.columns:
        return float(STARTING_JPY_BALANCE)
    balances = pd.to_numeric(ledger["Remaining_JPY_Balance"], errors="coerce").dropna()
    return float(balances.iloc[-1]) if not balances.empty else float(STARTING_JPY_BALANCE)


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

            trader_df["Total_JPY_Impact"] = pd.to_numeric(
                trader_df.get("Total_JPY_Impact", pd.Series(dtype=float)),
                errors="coerce",
            ).fillna(0)
            buys_impact = trader_df.loc[trader_df["Action"] == "BUY", "Total_JPY_Impact"].sum()
            sells_impact = trader_df.loc[trader_df["Action"] == "SELL", "Total_JPY_Impact"].sum()
            net_invested = abs(float(buys_impact)) - abs(float(sells_impact))
            starting_allocation = float(STARTING_JPY_BALANCE) / max(len(traders), 1)
            trader_value = starting_allocation + net_invested + trader_equity
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
        return pd.DataFrame([dict(row) for row in results]) if results else pd.DataFrame()
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
        db.execute_update(
            "UPDATE borrowing SET repay_date = CURRENT_TIMESTAMP, repaid_amount = %s, status = 'REPAID' WHERE id = %s",
            (float(repaid_amount), borrow_id))
        return {"success": True}
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


def start_new_simulation(starting_capital: Decimal) -> dict[str, Any]:
    """Initialize a new simulation with starting capital."""
    try:
        from datetime import datetime, timezone
        db = get_postgres_db()
        db.execute_update("""
            INSERT INTO ledger (timestamp, ticker, action, quantity, local_asset_price,
                executed_fx_rate, total_jpy_impact, remaining_jpy_balance,
                trader_name, commission_paid, fx_conversion_fee_paid, trade_rationale)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (datetime.now(timezone.utc), "JPY_CASH", "INITIAL_FUNDING", 0, 1, 1,
                  float(starting_capital), float(starting_capital), "System", 0, 0, "Initial Funding"))
        return {"success": True, "starting_capital": float(starting_capital)}
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
