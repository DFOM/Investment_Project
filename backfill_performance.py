#!/usr/bin/env python3
"""
Backfill historical portfolio performance data.

This script calculates historical portfolio values for each member from their
first trade date to today using yfinance historical price data, then writes
the results to the Google Sheets Performance tab.

Usage:
    python backfill_performance.py [--dry-run]
"""

from __future__ import annotations

import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yfinance as yf

# Allow direct execution
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_database
from core.market_data import get_current_usd_jpy
from core.setup_env import STARTING_JPY_BALANCE


def _is_tse_ticker(ticker: str) -> bool:
    return ticker.upper().endswith(".T")


def _load_ledger() -> pd.DataFrame:
    """Load and process the ledger data.
    
    Tries Google Sheets first, falls back to local CSV if credentials unavailable.
    """
    # Try Google Sheets first
    try:
        df = get_database().get_ledger_df()
        if not df.empty:
            print("Loaded ledger from Google Sheets")
            # Parse timestamps - handle both string and datetime formats
            if "Timestamp" in df.columns:
                df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")
            return df
    except Exception as e:
        print(f"Google Sheets not available: {e}")
    
    # Fall back to local CSV
    local_path = PROJECT_ROOT / "data" / "ledger.csv"
    if local_path.exists():
        print(f"Loading ledger from local file: {local_path}")
        df = pd.read_csv(local_path)
        if df.empty:
            return df
        
        # Parse timestamps
        df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")
        
        # Coerce numeric columns
        for col in ["Quantity", "Local_Asset_Price", "Executed_FX_Rate", 
                    "Total_JPY_Impact", "Remaining_JPY_Balance"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        
        # Clean string columns
        df["Ticker"] = df["Ticker"].astype(str).str.upper().str.strip()
        df["Action"] = df["Action"].astype(str).str.upper().str.strip()
        df["Trader_Name"] = df["Trader_Name"].astype(str).str.strip()
        
        return df.dropna(subset=["Timestamp"]).sort_values("Timestamp")
    
    return pd.DataFrame()


def _get_historical_price(ticker: str, target_date: date) -> float | None:
    """Get the closing price for a ticker on a specific date."""
    try:
        # For TSE stocks, yfinance uses .T suffix
        # For US stocks, use the ticker as-is
        yf_ticker = ticker if _is_tse_ticker(ticker) else ticker
        
        # Get data up to the target date (inclusive)
        start = (target_date - timedelta(days=7)).isoformat()
        end = (target_date + timedelta(days=1)).isoformat()
        
        hist = yf.download(yf_ticker, start=start, end=end, progress=False)
        if hist.empty:
            return None
        
        # Find the closest date <= target_date
        hist.index = pd.to_datetime(hist.index).tz_localize(None)
        valid_dates = hist.index[hist.index.date <= target_date]
        if valid_dates.empty:
            return None
        
        closest_date = valid_dates[-1]
        return float(hist.loc[closest_date, "Close"])
    except Exception as e:
        print(f"  Warning: Could not get price for {ticker} on {target_date}: {e}")
        return None


def _calculate_member_value_on_date(
    ledger: pd.DataFrame,
    trader_name: str,
    target_date: date,
    usd_jpy: float
) -> dict[str, Any]:
    """Calculate a member's portfolio value on a specific date."""
    # Filter trades up to and including the target date
    trader_ledger = ledger[
        (ledger["Trader_Name"] == trader_name) & 
        (ledger["Timestamp"].dt.date <= target_date)
    ]
    
    if trader_ledger.empty:
        return {"cash": 0.0, "equity": 0.0, "total": 0.0, "holdings": {}}
    
    # Calculate net holdings up to this date
    buys = trader_ledger[trader_ledger["Action"] == "BUY"].groupby("Ticker")["Quantity"].sum()
    sells = trader_ledger[trader_ledger["Action"] == "SELL"].groupby("Ticker")["Quantity"].sum()
    net = buys.sub(sells, fill_value=0.0)
    holdings = {str(t): float(q) for t, q in net.items() if float(q) > 0}
    
    # Calculate equity using historical prices
    equity = 0.0
    for ticker, qty in holdings.items():
        price = _get_historical_price(ticker, target_date)
        if price is not None:
            if _is_tse_ticker(ticker):
                equity += qty * price
            else:
                equity += qty * price * usd_jpy
    
    # Get cash balance at this point
    if "Remaining_JPY_Balance" in trader_ledger.columns:
        cash = float(trader_ledger["Remaining_JPY_Balance"].dropna().iloc[-1])
    else:
        cash = STARTING_JPY_BALANCE
    
    return {
        "cash": cash,
        "equity": equity,
        "total": cash + equity,
        "holdings": holdings
    }


def _get_all_trading_dates(start_date: date, end_date: date) -> list[date]:
    """Generate list of trading dates (weekdays only)."""
    dates = []
    current = start_date
    while current <= end_date:
        if current.weekday() < 5:  # Monday = 0, Friday = 4
            dates.append(current)
        current += timedelta(days=1)
    return dates


def run_backfill(dry_run: bool = False) -> dict[str, Any]:
    """Run the historical performance backfill."""
    print("Loading ledger data...")
    ledger = _load_ledger()
    
    if ledger.empty:
        print("No ledger data found.")
        return {"status": "error", "message": "No ledger data"}
    
    # Get unique traders (exclude system)
    all_traders = ledger["Trader_Name"].unique()
    traders = [t for t in all_traders if t and t.lower() not in ("system", "all team", "")]
    
    print(f"Found {len(traders)} traders: {traders}")
    
    # Get USD/JPY rate
    usd_jpy = get_current_usd_jpy(fallback=150.0) or 150.0
    print(f"Using USD/JPY rate: {usd_jpy}")
    
    # Find first trade date for each member
    first_trade_dates = {}
    for trader in traders:
        trader_trades = ledger[ledger["Trader_Name"] == trader]
        if not trader_trades.empty:
            first_trade_dates[trader] = trader_trades["Timestamp"].min().date()
    
    print(f"First trade dates: {first_trade_dates}")
    
    # Determine date range
    today = date.today()
    all_start_dates = list(first_trade_dates.values())
    if not all_start_dates:
        print("No member trades found.")
        return {"status": "error", "message": "No member trades"}
    
    start_date = min(all_start_dates)
    print(f"Backfilling from {start_date} to {today}")
    
    # Get all trading dates
    trading_dates = _get_all_trading_dates(start_date, today)
    print(f"Total trading days to process: {len(trading_dates)}")
    
    # Collect all unique tickers across all traders
    all_tickers = set()
    for trader in traders:
        trader_ledger = ledger[ledger["Trader_Name"] == trader]
        buys = trader_ledger[trader_ledger["Action"] == "BUY"]["Ticker"].unique()
        all_tickers.update(buys)
    
    print(f"Total unique tickers: {len(all_tickers)}")
    
    # Pre-fetch historical prices for all tickers (cache for efficiency)
    print("Pre-fetching historical prices...")
    price_cache: dict[tuple[str, date], float | None] = {}
    
    for ticker in all_tickers:
        print(f"  Fetching {ticker}...")
        try:
            yf_ticker = ticker if _is_tse_ticker(ticker) else ticker
            hist = yf.download(yf_ticker, start=start_date.isoformat(), 
                             end=(today + timedelta(days=1)).isoformat(), 
                             progress=False)
            if not hist.empty:
                hist.index = pd.to_datetime(hist.index).tz_localize(None)
                for d in trading_dates:
                    valid_dates = hist.index[hist.index.date <= d]
                    if not valid_dates.empty:
                        closest = valid_dates[-1]
                        price_cache[(ticker, d)] = float(hist.loc[closest, "Close"])
                    else:
                        price_cache[(ticker, d)] = None
            else:
                for d in trading_dates:
                    price_cache[(ticker, d)] = None
        except Exception as e:
            print(f"    Warning: Could not fetch {ticker}: {e}")
            for d in trading_dates:
                price_cache[(ticker, d)] = None
    
    print("Price fetching complete.")
    
    # Calculate values for each trader on each date
    results: list[dict[str, Any]] = []
    
    for trader in traders:
        print(f"Processing {trader}...")
        trader_start = first_trade_dates.get(trader, start_date)
        
        for d in trading_dates:
            if d < trader_start:
                continue  # Skip dates before their first trade
            
            # Get holdings up to this date
            trader_ledger = ledger[
                (ledger["Trader_Name"] == trader) & 
                (ledger["Timestamp"].dt.date <= d)
            ]
            
            if trader_ledger.empty:
                continue
            
            buys = trader_ledger[trader_ledger["Action"] == "BUY"].groupby("Ticker")["Quantity"].sum()
            sells = trader_ledger[trader_ledger["Action"] == "SELL"].groupby("Ticker")["Quantity"].sum()
            net = buys.sub(sells, fill_value=0.0)
            holdings = {str(t): float(q) for t, q in net.items() if float(q) > 0}
            
            # Calculate equity
            equity = 0.0
            for ticker, qty in holdings.items():
                price = price_cache.get((ticker, d))
                if price is not None:
                    if _is_tse_ticker(ticker):
                        equity += qty * price
                    else:
                        equity += qty * price * usd_jpy
            
            # Get cash balance
            if "Remaining_JPY_Balance" in trader_ledger.columns:
                cash = float(trader_ledger["Remaining_JPY_Balance"].dropna().iloc[-1])
            else:
                cash = STARTING_JPY_BALANCE
            
            total = cash + equity
            
            results.append({
                "date": d.isoformat(),
                "Trader_Name": trader,
                "portfolio_value_jpy": round(total, 2)
            })
            
            if len(results) % 100 == 0:
                print(f"  Generated {len(results)} records...")
    
    print(f"Total records generated: {len(results)}")
    
    if dry_run:
        print("\n--- DRY RUN: Not writing to Google Sheets ---")
        print(f"Sample records:")
        for r in results[:5]:
            print(f"  {r}")
        return {"status": "dry_run", "records": len(results)}
    
    # Try to write to Google Sheets, fall back to local JSON
    try:
        print("\nWriting to Google Sheets Performance tab...")
        db = get_database()
        
        # Get existing records to avoid duplicates
        existing = db.get_performance_df()
        existing_keys = set()
        if not existing.empty and "date" in existing.columns and "Trader_Name" in existing.columns:
            for _, row in existing.iterrows():
                existing_keys.add((str(row["date"]), str(row["Trader_Name"])))
        
        print(f"Existing records: {len(existing_keys)}")
        
        # Filter out duplicates
        new_records = [r for r in results if (r["date"], r["Trader_Name"]) not in existing_keys]
        print(f"New records to add: {len(new_records)}")
        
        if not new_records:
            print("No new records to add.")
            return {"status": "success", "records_added": 0, "records_skipped": len(results)}
        
        # Batch write (in groups of 50 to avoid API limits)
        batch_size = 50
        for i in range(0, len(new_records), batch_size):
            batch = new_records[i:i+batch_size]
            for row in batch:
                db.upsert_performance_row(row)
            print(f"  Wrote batch {i//batch_size + 1}/{(len(new_records)-1)//batch_size + 1}")
        
        return {
            "status": "success",
            "records_added": len(new_records),
            "records_skipped": len(results) - len(new_records),
            "total_traders": len(traders),
            "date_range": f"{start_date} to {today}"
        }
    except Exception as e:
        print(f"Google Sheets not available: {e}")
        print("Falling back to local JSON file...")
        
        # Load existing from local JSON
        local_path = PROJECT_ROOT / "data" / "historical_performance.json"
        existing_records = []
        if local_path.exists():
            import json
            with open(local_path, "r") as f:
                existing_records = json.load(f)
        
        existing_keys = set()
        for r in existing_records:
            existing_keys.add((str(r.get("date", "")), str(r.get("Trader_Name", "All Team"))))
        
        # Filter out duplicates
        new_records = [r for r in results if (r["date"], r["Trader_Name"]) not in existing_keys]
        print(f"New records to add: {len(new_records)}")
        
        # Append new records
        import json
        all_records = existing_records + new_records
        with open(local_path, "w") as f:
            json.dump(all_records, f, indent=2)
        
        print(f"Written to {local_path}")
        
        return {
            "status": "success_local",
            "records_added": len(new_records),
            "records_skipped": len(results) - len(new_records),
            "total_traders": len(traders),
            "date_range": f"{start_date} to {today}"
        }


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    result = run_backfill(dry_run)
    print(f"\nResult: {result}")