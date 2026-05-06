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
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
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
