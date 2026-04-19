import time
from datetime import datetime, timezone
import logging
import sys
import os
import pytz
import datetime as dt

# Ensure project root is in Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.trade_executor import process_pending_orders
from core.database import record_daily_performance

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def _is_us_market_hours() -> bool:
    """Check if US market (NYSE/NASDAQ) is currently open."""
    now_et = dt.datetime.now(pytz.timezone("America/New_York"))
    # US Market: 9:30 AM - 4:00 PM ET, weekdays only
    open_time = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    close_time = now_et.replace(hour=16, minute=0, second=0, microsecond=0)
    is_weekday = now_et.weekday() < 5  # 0=Mon, 4=Fri
    return is_weekday and open_time <= now_et <= close_time


def _is_jp_market_hours() -> bool:
    """Check if Japanese market (TSE) is currently open."""
    now_jst = dt.datetime.now(pytz.timezone("Asia/Tokyo"))
    # JP Market: 9:00 AM - 3:00 PM JST, weekdays only
    open_time = now_jst.replace(hour=9, minute=0, second=0, microsecond=0)
    close_time = now_jst.replace(hour=15, minute=0, second=0, microsecond=0)
    is_weekday = now_jst.weekday() < 5  # 0=Mon, 4=Fri
    return is_weekday and open_time <= now_jst <= close_time


def _is_market_hours() -> bool:
    """Check if any market (US or JP) is currently open."""
    return _is_us_market_hours() or _is_jp_market_hours()


def _get_update_interval() -> int:
    """Return update interval in seconds based on market hours.
    
    During market hours: update every 10 minutes (600 seconds)
    Outside market hours: update every 60 minutes (3600 seconds)
    """
    return 600 if _is_market_hours() else 3600


def run_hourly():
    logging.info("Executing pending orders...")
    try:
        results = process_pending_orders()
        for r in results:
            logging.info(f"Order: {r}")
    except Exception as e:
        logging.error(f"Error in hourly task: {e}")


def run_daily():
    logging.info("Recording daily portfolio performance for all members...")
    try:
        snap = record_daily_performance()
        logging.info(f"Snapshot successful: {snap['total_portfolio_value_jpy']} JPY")
    except Exception as e:
        logging.error(f"Error in daily task: {e}")


if __name__ == "__main__":
    logging.info("Starting autonomous background worker...")
    logging.info("Market hours detection enabled:")
    logging.info("  US Market: 9:30 AM - 4:00 PM ET (weekdays)")
    logging.info("  JP Market: 9:00 AM - 3:00 PM JST (weekdays)")
    
    last_order_run = time.time()
    last_daily_run_date = None

    # Run initial execution immediately on start
    run_hourly()

    while True:
        now = time.time()
        update_interval = _get_update_interval()
        
        # Update based on market hours
        # During market hours: every 10 minutes (600 seconds)
        # Outside: every 60 minutes (3600 seconds)
        if now - last_order_run >= update_interval:
            logging.info(f"Market hours: {_is_market_hours()} | Update interval: {update_interval}s")
            run_hourly()
            last_order_run = now

        # Once every new UTC day
        current_date = datetime.now(timezone.utc).date()
        if last_daily_run_date != current_date:
            run_daily()
            last_daily_run_date = current_date

        time.sleep(30)  # Wake up every 30 seconds to check schedule