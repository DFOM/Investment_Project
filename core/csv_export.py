"""CSV export/import utilities for professor's trading template."""

from __future__ import annotations

import csv
import io
from datetime import datetime
from decimal import Decimal
from typing import Any, Optional
import logging

import pandas as pd

logger = logging.getLogger(__name__)


def export_trades_to_csv(trader_name: Optional[str] = None) -> str:
    """Export trades to CSV matching professor's template format."""
    try:
        from core.db_postgres import get_database
        db = get_database()
        
        if trader_name:
            query = "SELECT * FROM trades WHERE trader_name = %s ORDER BY entry_date"
            results = db.execute_query(query, (trader_name,))
        else:
            query = "SELECT * FROM trades ORDER BY entry_date"
            results = db.execute_query(query)
        
        df = pd.DataFrame(results) if results else pd.DataFrame()
        
        if df.empty:
            logger.warning(f"No trades found for {trader_name or 'all traders'}")
            return ""
        
        output = io.StringIO()
        df.to_csv(output, index=False)
        return output.getvalue()
    except Exception as e:
        logger.error(f"Failed to export trades: {e}")
        return ""


def export_all_traders_csv() -> str:
    """Export all traders' data to CSV."""
    try:
        from core.db_postgres import get_database
        db = get_database()
        
        results = db.execute_query("SELECT * FROM trades ORDER BY trader_name, entry_date")
        df = pd.DataFrame(results) if results else pd.DataFrame()
        
        if df.empty:
            return ""
        
        output = io.StringIO()
        df.to_csv(output, index=False)
        return output.getvalue()
    except Exception as e:
        logger.error(f"Failed to export: {e}")
        return ""


def export_ledger_to_csv() -> str:
    """Export full ledger (all transactions)."""
    try:
        from core.db_postgres import get_database
        db = get_database()
        
        results = db.execute_query("SELECT * FROM ledger ORDER BY timestamp")
        df = pd.DataFrame(results) if results else pd.DataFrame()
        
        if df.empty:
            return ""
        
        output = io.StringIO()
        df.to_csv(output, index=False)
        return output.getvalue()
    except Exception as e:
        logger.error(f"Failed to export ledger: {e}")
        return ""


def get_portfolio_summary_csv(trader_name: Optional[str] = None) -> str:
    """Export portfolio summary with current valuations."""
    try:
        from core.db_postgres import get_database
        db = get_database()
        
        if trader_name:
            query = """
            SELECT stock_code, stock_name, direction,
                   SUM(entry_unit) as total_units,
                   AVG(entry_price) as avg_entry_price,
                   MAX(market_price) as current_price,
                   SUM(profit_loss) as total_pnl
            FROM trades WHERE trader_name = %s AND status = 'Open'
            GROUP BY stock_code, stock_name, direction ORDER BY total_pnl DESC
            """
            results = db.execute_query(query, (trader_name,))
        else:
            query = """
            SELECT trader_name, stock_code, stock_name, direction,
                   SUM(entry_unit) as total_units,
                   AVG(entry_price) as avg_entry_price,
                   MAX(market_price) as current_price,
                   SUM(profit_loss) as total_pnl
            FROM trades WHERE status = 'Open'
            GROUP BY trader_name, stock_code, stock_name, direction ORDER BY trader_name, total_pnl DESC
            """
            results = db.execute_query(query)
        
        df = pd.DataFrame(results) if results else pd.DataFrame()
        
        if df.empty:
            return ""
        
        output = io.StringIO()
        df.to_csv(output, index=False)
        return output.getvalue()
    except Exception as e:
        logger.error(f"Failed to export summary: {e}")
        return ""
