from __future__ import annotations

import logging
import math
import re
from collections.abc import Iterable
from pathlib import Path
from typing import Final

import pandas as pd
import yfinance as yf

from core.setup_env import LEDGER_PATH

_ALLOWED_TIMEFRAMES: Final[set[str]] = {"D", "W", "M"}
_TSE_DIGIT_PATTERN: Final[re.Pattern[str]] = re.compile(r"^\d{4}$")
_YFINANCE_USDJPY_SYMBOL: Final[str] = "JPY=X"
_DEFAULT_PRICE_FALLBACK: Final[float] = 0.0
_DEFAULT_USDJPY_FALLBACK: Final[float] = 150.0
FX_SPREAD_PERCENTAGE: Final[float] = 0.25
_LAST_KNOWN_QUOTES: dict[str, float] = {}

LOGGER = logging.getLogger(__name__)


def _normalize_ticker(ticker: str) -> str:
    symbol = ticker.strip().upper()
    if not symbol:
        raise ValueError("Ticker cannot be empty.")

    # yfinance requires TSE tickers with a .T suffix; users commonly input 4-digit codes.
    if _TSE_DIGIT_PATTERN.match(symbol):
        return f"{symbol}.T"

    return symbol


def _is_positive_number(value: object) -> bool:
    if value is None or not isinstance(value, (int, float, str)):
        return False

    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return False

    return math.isfinite(numeric) and numeric > 0


def _is_nonnegative_number(value: object) -> bool:
    if value is None or not isinstance(value, (int, float, str)):
        return False

    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return False

    return math.isfinite(numeric) and numeric >= 0


def _from_series_close(frame: pd.DataFrame) -> float | None:
    if frame.empty or "Close" not in frame.columns:
        return None

    close = frame["Close"].dropna()
    if close.empty:
        return None

    candidate = float(close.iloc[-1])
    return candidate if _is_positive_number(candidate) else None


def _from_fast_info(ticker_obj: yf.Ticker) -> float | None:
    fast = getattr(ticker_obj, "fast_info", None)
    if not fast:
        return None

    for key in ("lastPrice", "regularMarketPrice", "previousClose"):
        candidate = fast.get(key)
        if _is_positive_number(candidate):
            return float(candidate)

    return None


def _latest_quote(symbol: str) -> float:
    ticker_obj = yf.Ticker(symbol)

    intraday = ticker_obj.history(period="1d", interval="1m", prepost=True)
    intraday_value = _from_series_close(intraday)
    if intraday_value is not None:
        return intraday_value

    fast_value = _from_fast_info(ticker_obj)
    if fast_value is not None:
        return fast_value

    daily = ticker_obj.history(period="5d", interval="1d")
    daily_value = _from_series_close(daily)
    if daily_value is not None:
        return daily_value

    raise ValueError(f"Unable to fetch a valid price for ticker '{symbol}'.")


def _resolve_fallback(symbol: str, fallback: float | None) -> float | None:
    if _is_nonnegative_number(fallback):
        if fallback is None:
            return None
        return float(fallback)

    cached = _LAST_KNOWN_QUOTES.get(symbol)
    if _is_positive_number(cached):
        if cached is None:
            return None
        return float(cached)

    return None


def get_live_price(ticker: str, fallback: float | None = _DEFAULT_PRICE_FALLBACK) -> float | None:
    symbol = _normalize_ticker(ticker)

    try:
        price = _latest_quote(symbol)
        _LAST_KNOWN_QUOTES[symbol] = price
        return price
    except Exception as exc:
        LOGGER.warning("Price fetch failed for %s: %s", symbol, exc)
        return _resolve_fallback(symbol, fallback)


def get_current_usd_jpy(fallback: float | None = _DEFAULT_USDJPY_FALLBACK) -> float | None:
    try:
        rate = _latest_quote(_YFINANCE_USDJPY_SYMBOL)
    except Exception as exc:
        LOGGER.warning("USD/JPY fetch failed: %s", exc)
        return _resolve_fallback(_YFINANCE_USDJPY_SYMBOL, fallback)

    if not _is_positive_number(rate):
        LOGGER.warning("USD/JPY fetch returned non-positive value: %s", rate)
        return _resolve_fallback(_YFINANCE_USDJPY_SYMBOL, fallback)

    resolved = float(rate)
    _LAST_KNOWN_QUOTES[_YFINANCE_USDJPY_SYMBOL] = resolved
    return resolved


def get_live_fx(fallback: float | None = _DEFAULT_USDJPY_FALLBACK) -> float | None:
    return get_current_usd_jpy(fallback=fallback)


def get_executed_fx_quote(
    action: str,
    usd_notional: float,
    fallback: float | None = _DEFAULT_USDJPY_FALLBACK,
) -> dict[str, float] | None:
    normalized_action = action.strip().upper()
    if normalized_action not in {"BUY", "SELL"}:
        raise ValueError("action must be 'BUY' or 'SELL'.")
    if usd_notional < 0:
        raise ValueError("usd_notional must be non-negative.")

    live_mid_rate = get_live_fx(fallback=fallback)
    if live_mid_rate is None:
        return None

    spread_factor = FX_SPREAD_PERCENTAGE / 100.0
    if normalized_action == "BUY":
        executed_rate = live_mid_rate * (1.0 + spread_factor)
        fx_fee_amount_jpy = usd_notional * (executed_rate - live_mid_rate)
    else:
        executed_rate = live_mid_rate * (1.0 - spread_factor)
        fx_fee_amount_jpy = usd_notional * (live_mid_rate - executed_rate)

    return {
        "live_mid_market_rate": live_mid_rate,
        "executed_rate": executed_rate,
        "fx_fee_amount_jpy": max(float(fx_fee_amount_jpy), 0.0),
        "spread_percentage": FX_SPREAD_PERCENTAGE,
    }


def fetch_live_stock_prices(
    tickers: Iterable[str], fallback: float | None = _DEFAULT_PRICE_FALLBACK
) -> dict[str, float | None]:
    out: dict[str, float | None] = {}
    for ticker in tickers:
        symbol = _normalize_ticker(ticker)
        out[symbol] = get_live_price(symbol, fallback=fallback)
    return out


def _find_portfolio_value_column(frame: pd.DataFrame) -> str:
    candidates = [
        "portfolio_value_jpy",
        "Portfolio Value (JPY)",
        "Total Portfolio Value (JPY)",
        "total_portfolio_value_jpy",
        "Remaining_JPY_Balance",
    ]
    for col in candidates:
        if col in frame.columns:
            return col
    raise ValueError("Could not find a portfolio value column.")


def resample_portfolio_history(ledger_path: str | Path | None, timeframe: str) -> pd.DataFrame:
    tf = timeframe.strip().upper()
    if tf not in _ALLOWED_TIMEFRAMES:
        raise ValueError("timeframe must be one of 'D', 'W', or 'M'.")

    path = Path(ledger_path) if ledger_path else LEDGER_PATH
    ledger = pd.read_csv(path)

    if ledger.empty:
        raise ValueError("Ledger is empty.")
    if "Timestamp" not in ledger.columns:
        raise ValueError("Ledger must contain a 'Timestamp' column.")

    frame = ledger.copy()
    frame["Timestamp"] = pd.to_datetime(frame["Timestamp"], errors="coerce", utc=True)
    frame = frame.dropna(subset=["Timestamp"]).sort_values("Timestamp")
    if frame.empty:
        raise ValueError("No valid timestamp rows found in ledger.")

    value_col = _find_portfolio_value_column(frame)
    frame[value_col] = pd.to_numeric(frame[value_col], errors="coerce")
    frame = frame.dropna(subset=[value_col])
    if frame.empty:
        raise ValueError("No valid numeric portfolio values found in ledger.")

    ts_indexed = frame.set_index("Timestamp")[[value_col]]
    resampled = ts_indexed.resample(tf).last().dropna(how="all")
    out = resampled.reset_index()
    out.columns = ["Timestamp", "PortfolioValue"]
    return out
