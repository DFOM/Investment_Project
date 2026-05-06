"""Database abstraction layer - PostgreSQL backend."""

from __future__ import annotations

import os
from datetime import date
from decimal import Decimal
from typing import Any, Final, Optional
import logging

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
        results = db.execute_query("SELECT * FROM ledger ORDER BY timestamp")
        return pd.DataFrame(results) if results else pd.DataFrame()
    except Exception as e:
        logger.error(f"Failed to fetch ledger: {e}")
        return pd.DataFrame()


def record_daily_performance(trader_name: str, portfolio_value_jpy: Decimal) -> dict[str, Any]:
    """Record daily portfolio valuation."""
    try:
        db = get_postgres_db()
        today = date.today()
        db.execute_update("""
            INSERT INTO performance (date, trader_name, portfolio_value_jpy)
            VALUES (%s, %s, %s)
            ON CONFLICT (date, trader_name) DO UPDATE SET portfolio_value_jpy = EXCLUDED.portfolio_value_jpy
            """, (today, trader_name, float(portfolio_value_jpy)))
        return {"success": True, "date": str(today), "trader_name": trader_name}
    except Exception as e:
        logger.error(f"Failed to record performance: {e}")
        return {"success": False, "error": str(e)}


def get_cached_performance_df() -> pd.DataFrame:
    """Fetch all performance records as DataFrame."""
    try:
        db = get_postgres_db()
        results = db.execute_query("SELECT * FROM performance ORDER BY date, trader_name")
        return pd.DataFrame(results) if results else pd.DataFrame()
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
        results = db.execute_query("SELECT * FROM order_book WHERE status = 'PENDING' ORDER BY timestamp")
        return pd.DataFrame(results) if results else pd.DataFrame()
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
        results = db.execute_query("SELECT * FROM team_auth")
        return pd.DataFrame(results) if results else pd.DataFrame()
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
        return pd.DataFrame(results) if results else pd.DataFrame()
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
    """Clear any in-process caches."""
    pass


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
