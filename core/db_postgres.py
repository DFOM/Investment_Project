"""PostgreSQL database module for Investment Portfolio Simulator.

Replaces Google Sheets with PostgreSQL, supporting Railway.app and local deployments.
All data structures mirror the professor's trading template + existing app functionality.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Optional
from decimal import Decimal
import logging

import psycopg2
import psycopg2.extras
from psycopg2.pool import SimpleConnectionPool

logger = logging.getLogger(__name__)


class DatabaseConfigError(RuntimeError):
    """Raised when database configuration is missing or invalid."""
    pass


def _get_database_url() -> str:
    """Load database URL from environment."""
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        return db_url
    
    db_url = os.getenv("POSTGRES_URL")
    if db_url:
        return db_url
    
    host = os.getenv("POSTGRES_HOST", "localhost")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "")
    dbname = os.getenv("POSTGRES_DB", "investment_db")
    port = os.getenv("POSTGRES_PORT", "5432")
    
    if password:
        return f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    else:
        return f"postgresql://{user}@{host}:{port}/{dbname}"


class PostgreSQLDatabase:
    """PostgreSQL database handler for investment portfolio simulator."""
    
    _instance: Optional[PostgreSQLDatabase] = None
    _pool: Optional[SimpleConnectionPool] = None
    _initialized: bool = False
    
    def __new__(cls) -> PostgreSQLDatabase:
        """Singleton pattern - returns single instance."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self) -> None:
        """Initialize database connection pool."""
        if self._initialized:
            return
        
        try:
            db_url = _get_database_url()
            from urllib.parse import urlparse
            parsed = urlparse(db_url)
            
            self.host = parsed.hostname or "localhost"
            self.port = parsed.port or 5432
            self.user = parsed.username or "postgres"
            self.password = parsed.password or ""
            self.dbname = parsed.path.lstrip("/") if parsed.path else "investment_db"
            
            self._pool = SimpleConnectionPool(
                1, 20,
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.dbname,
                connect_timeout=10
            )
            
            logger.info(f"✅ PostgreSQL connected: {self.user}@{self.host}:{self.port}/{self.dbname}")
            self._initialized = True
            self.ensure_schema()
            
        except Exception as e:
            raise DatabaseConfigError(f"Failed to initialize PostgreSQL: {e}") from e
    
    def get_connection(self) -> psycopg2.extensions.connection:
        """Get a connection from the pool."""
        if self._pool is None:
            raise DatabaseConfigError("Database pool not initialized")
        try:
            return self._pool.getconn()
        except psycopg2.OperationalError as e:
            raise DatabaseConfigError(f"Failed to get database connection: {e}") from e
    
    def return_connection(self, conn: psycopg2.extensions.connection) -> None:
        """Return connection to the pool."""
        if self._pool is not None:
            self._pool.putconn(conn)
    
    def execute_query(self, query: str, params: tuple = ()) -> list[tuple[Any, ...]]:
        """Execute a SELECT query and return results."""
        conn = self.get_connection()
        try:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cursor.execute(query, params)
            results = cursor.fetchall()
            cursor.close()
            return results
        finally:
            self.return_connection(conn)
    
    def execute_update(self, query: str, params: tuple = ()) -> int:
        """Execute INSERT/UPDATE/DELETE and return affected rows."""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(query, params)
            affected_rows = cursor.rowcount
            conn.commit()
            cursor.close()
            return affected_rows
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            self.return_connection(conn)
    
    def ensure_schema(self) -> None:
        """Create all tables if they don't exist."""
        self.execute_update("""
            CREATE TABLE IF NOT EXISTS trades (
                id SERIAL PRIMARY KEY,
                trader_name VARCHAR(255),
                entry_date TIMESTAMP WITH TIME ZONE,
                stock_name VARCHAR(255),
                stock_code VARCHAR(20),
                direction VARCHAR(10),
                entry_unit DECIMAL(18, 8),
                entry_price DECIMAL(18, 8),
                entry_value DECIMAL(18, 2),
                commission_entry DECIMAL(18, 2),
                market_price DECIMAL(18, 8),
                market_value DECIMAL(18, 2),
                exit_date TIMESTAMP WITH TIME ZONE,
                exit_price DECIMAL(18, 8),
                exit_value DECIMAL(18, 2),
                commission_exit DECIMAL(18, 2),
                profit_loss DECIMAL(18, 2),
                status VARCHAR(50),
                reason TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        self.execute_update("""
            CREATE TABLE IF NOT EXISTS ledger (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP WITH TIME ZONE,
                ticker VARCHAR(50),
                action VARCHAR(50),
                quantity DECIMAL(18, 8),
                local_asset_price DECIMAL(18, 8),
                executed_fx_rate DECIMAL(18, 8),
                total_jpy_impact DECIMAL(18, 2),
                remaining_jpy_balance DECIMAL(18, 2),
                trader_name VARCHAR(255),
                commission_paid DECIMAL(18, 2),
                fx_conversion_fee_paid DECIMAL(18, 2),
                trade_rationale TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        self.execute_update("""
            CREATE TABLE IF NOT EXISTS performance (
                id SERIAL PRIMARY KEY,
                date DATE,
                trader_name VARCHAR(255),
                portfolio_value_jpy DECIMAL(18, 2),
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(date, trader_name)
            )
        """)
        
        self.execute_update("""
            CREATE TABLE IF NOT EXISTS order_book (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP WITH TIME ZONE,
                ticker VARCHAR(50),
                action VARCHAR(50),
                mode VARCHAR(50),
                value DECIMAL(18, 2),
                rationale TEXT,
                status VARCHAR(50),
                trader_name VARCHAR(255),
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        self.execute_update("""
            CREATE TABLE IF NOT EXISTS team_auth (
                id SERIAL PRIMARY KEY,
                trader_name VARCHAR(255) UNIQUE,
                auth_code VARCHAR(255),
                active BOOLEAN DEFAULT TRUE,
                initial_allocation_jpy DECIMAL(18, 2),
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.execute_update("""
            ALTER TABLE team_auth
            ADD COLUMN IF NOT EXISTS initial_allocation_jpy DECIMAL(18, 2)
        """)
        
        self.execute_update("""
            CREATE TABLE IF NOT EXISTS borrowing (
                id SERIAL PRIMARY KEY,
                trader_name VARCHAR(255),
                borrow_date TIMESTAMP WITH TIME ZONE,
                amount_jpy DECIMAL(18, 2),
                repay_date TIMESTAMP WITH TIME ZONE,
                repaid_amount DECIMAL(18, 2),
                status VARCHAR(50),
                interest_rate DECIMAL(5, 3),
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        self.execute_update("""
            CREATE TABLE IF NOT EXISTS session_config (
                id SERIAL PRIMARY KEY,
                key VARCHAR(255) UNIQUE,
                value TEXT,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        logger.info("✅ Database schema verified/created")

    def append_ledger_row(self, row: dict[str, Any]) -> None:
        """Append a legacy Ledger-style row to the PostgreSQL ledger table."""
        self.execute_update("""
            INSERT INTO ledger (timestamp, ticker, action, quantity, local_asset_price,
                executed_fx_rate, total_jpy_impact, remaining_jpy_balance,
                trader_name, commission_paid, fx_conversion_fee_paid, trade_rationale)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row.get("Timestamp"),
                row.get("Ticker"),
                row.get("Action"),
                row.get("Quantity", 0),
                row.get("Local_Asset_Price", 0),
                row.get("Executed_FX_Rate", 1),
                row.get("Total_JPY_Impact", 0),
                row.get("Remaining_JPY_Balance", 0),
                row.get("Trader_Name", ""),
                row.get("Commission_Paid", 0),
                row.get("FX_Conversion_Fee", row.get("FX_Conversion_Fee_Paid", 0)),
                row.get("Trade_Rationale", ""),
            ))

    def append_order_book_row(self, row: dict[str, Any]) -> None:
        """Append a legacy Order_Book-style row to the PostgreSQL order book."""
        self.execute_update("""
            INSERT INTO order_book (timestamp, ticker, action, mode, value, rationale, status, trader_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row.get("Timestamp"),
                row.get("Ticker"),
                row.get("Action"),
                row.get("Mode"),
                row.get("Value", 0),
                row.get("Rationale", ""),
                row.get("Status", "PENDING"),
                row.get("Trader_Name", ""),
            ))

    def get_order_book_df(self):
        """Return the order book using the legacy column names expected by the UI."""
        import pandas as pd

        results = self.execute_query("SELECT * FROM order_book ORDER BY timestamp, id")
        if not results:
            return pd.DataFrame(columns=[
                "ID", "Timestamp", "Ticker", "Action", "Mode", "Value",
                "Rationale", "Status", "Trader_Name", "Created_At", "Updated_At",
            ])

        df = pd.DataFrame([dict(row) for row in results])
        return df.rename(columns={
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
        })

    def update_order_status(self, order_id_or_timestamp: Any, status: str) -> bool:
        """Update an order by numeric id when available, otherwise by timestamp."""
        if isinstance(order_id_or_timestamp, int) or str(order_id_or_timestamp).isdigit():
            affected = self.execute_update(
                "UPDATE order_book SET status = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s",
                (status, int(order_id_or_timestamp)),
            )
        else:
            affected = self.execute_update(
                "UPDATE order_book SET status = %s, updated_at = CURRENT_TIMESTAMP WHERE timestamp = %s",
                (status, order_id_or_timestamp),
            )
        return affected > 0

    def upsert_team_auth(
        self,
        trader_name: str,
        auth_code: str,
        active: bool = True,
        initial_allocation_jpy: float | None = None,
    ) -> None:
        """Insert or update a team member authentication row."""
        self.execute_update("""
            INSERT INTO team_auth (trader_name, auth_code, active, initial_allocation_jpy)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (trader_name)
            DO UPDATE SET
                auth_code = EXCLUDED.auth_code,
                active = EXCLUDED.active,
                initial_allocation_jpy = COALESCE(
                    EXCLUDED.initial_allocation_jpy,
                    team_auth.initial_allocation_jpy
                )
            """, (trader_name, auth_code, active, initial_allocation_jpy))

    def set_member_allocation(self, trader_name: str, allocation_jpy: float | None) -> bool:
        """Set the starting-capital allocation for a team member."""
        affected = self.execute_update(
            "UPDATE team_auth SET initial_allocation_jpy = %s WHERE LOWER(trader_name) = LOWER(%s)",
            (allocation_jpy, trader_name),
        )
        return affected > 0

    def get_config_value(self, key: str) -> str | None:
        """Fetch a raw config value from session_config."""
        rows = self.execute_query("SELECT value FROM session_config WHERE key = %s", (key,))
        if not rows:
            return None
        return rows[0][0]

    def set_config_value(self, key: str, value: str) -> None:
        """Upsert a raw config value into session_config."""
        self.execute_update("""
            INSERT INTO session_config (key, value, updated_at)
            VALUES (%s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (key)
            DO UPDATE SET value = EXCLUDED.value, updated_at = CURRENT_TIMESTAMP
            """, (key, value))

    def rename_team_auth(self, old_name: str, new_name: str) -> bool:
        """Rename a team member in the auth table."""
        affected = self.execute_update(
            "UPDATE team_auth SET trader_name = %s WHERE LOWER(trader_name) = LOWER(%s)",
            (new_name, old_name),
        )
        return affected > 0

    def initialize_and_format_worksheets(self) -> dict[str, Any]:
        """Backward-compatible no-op for old Google Sheets admin action."""
        self.ensure_schema()
        return {
            "ledger_header_columns": 12,
            "performance_header_columns": 3,
            "genesis_row_written": False,
        }


_db: Optional[PostgreSQLDatabase] = None


def get_database() -> PostgreSQLDatabase:
    """Get or create database instance."""
    global _db
    if _db is None:
        _db = PostgreSQLDatabase()
    return _db


def get_connection_status() -> dict[str, Any]:
    """Check database connection status."""
    try:
        db = get_database()
        conn = db.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT version()")
        version = cursor.fetchone()[0]
        cursor.close()
        db.return_connection(conn)
        
        return {
            "connected": True,
            "database_name": db.dbname,
            "host": db.host,
            "version": version.split(",")[0] if version else "unknown"
        }
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        return {
            "connected": False,
            "error": str(e)
        }
