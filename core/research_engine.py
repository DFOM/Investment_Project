from __future__ import annotations

import re
from typing import Any

import pandas as pd
import yfinance as yf

_TSE_DIGIT_PATTERN = re.compile(r"^\d{4}$")


def _normalize_ticker(ticker: str) -> str:
    symbol = ticker.strip().upper()
    if not symbol:
        raise ValueError("Ticker cannot be empty.")
    if _TSE_DIGIT_PATTERN.match(symbol):
        return f"{symbol}.T"
    return symbol


def _safe_frame(frame: Any) -> pd.DataFrame:
    if isinstance(frame, pd.DataFrame):
        return frame.copy()
    return pd.DataFrame()


def _safe_metric(info: dict[str, Any], key: str) -> Any:
    value = info.get(key)
    if value is None:
        return "Data Unavailable"
    return value


def get_stock_research(ticker: str) -> dict[str, Any]:
    symbol = _normalize_ticker(ticker)

    payload: dict[str, Any] = {
        "status": "ok",
        "ticker": symbol,
        "history": pd.DataFrame(),
        "income_statement": pd.DataFrame(),
        "balance_sheet": pd.DataFrame(),
        "cash_flow": pd.DataFrame(),
        "key_metrics": {
            "market_cap": "Data Unavailable",
            "pe_ratio": "Data Unavailable",
            "dividend_yield": "Data Unavailable",
        },
        "warnings": [],
    }

    try:
        obj = yf.Ticker(symbol)
    except Exception as exc:
        payload["status"] = "data_unavailable"
        payload["warnings"].append(f"Ticker initialization failed: {exc}")
        return payload

    try:
        history = obj.history(period="1y", interval="1d", auto_adjust=False)
        history = _safe_frame(history)
        if history.empty:
            payload["warnings"].append("1-year historical price data unavailable.")
        else:
            history = history.reset_index()
            if "Date" in history.columns:
                history["Date"] = pd.to_datetime(history["Date"], errors="coerce")
            elif "Datetime" in history.columns:
                history = history.rename(columns={"Datetime": "Date"})
                history["Date"] = pd.to_datetime(history["Date"], errors="coerce")
            payload["history"] = history
    except Exception as exc:
        payload["warnings"].append(f"Historical data unavailable: {exc}")

    try:
        info = obj.info or {}
        payload["key_metrics"] = {
            "market_cap": _safe_metric(info, "marketCap"),
            "pe_ratio": _safe_metric(info, "trailingPE"),
            "dividend_yield": _safe_metric(info, "dividendYield"),
        }
    except Exception as exc:
        payload["warnings"].append(f"Key metrics unavailable: {exc}")

    try:
        income_statement = _safe_frame(getattr(obj, "financials", pd.DataFrame()))
        if income_statement.empty:
            payload["warnings"].append("Income statement unavailable.")
        payload["income_statement"] = income_statement
    except Exception as exc:
        payload["warnings"].append(f"Income statement unavailable: {exc}")

    try:
        balance_sheet = _safe_frame(getattr(obj, "balance_sheet", pd.DataFrame()))
        if balance_sheet.empty:
            payload["warnings"].append("Balance sheet unavailable.")
        payload["balance_sheet"] = balance_sheet
    except Exception as exc:
        payload["warnings"].append(f"Balance sheet unavailable: {exc}")

    try:
        cash_flow = _safe_frame(getattr(obj, "cashflow", pd.DataFrame()))
        if cash_flow.empty:
            payload["warnings"].append("Cash flow statement unavailable.")
        payload["cash_flow"] = cash_flow
    except Exception as exc:
        payload["warnings"].append(f"Cash flow statement unavailable: {exc}")

    if (
        payload["history"].empty
        and payload["income_statement"].empty
        and payload["balance_sheet"].empty
        and payload["cash_flow"].empty
    ):
        payload["status"] = "data_unavailable"

    return payload
