from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pandas as pd

# Allow direct execution: `python hooks/daily_valuation.py`.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.market_data import get_current_usd_jpy, get_live_price
from core.database import get_database
from core.setup_env import STARTING_JPY_BALANCE


def _load_ledger() -> pd.DataFrame:
    df = get_database().get_ledger_df()
    if df.empty:
        return df

    for col in ["Quantity", "Local_Asset_Price", "Executed_FX_Rate", "Total_JPY_Impact", "Remaining_JPY_Balance"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["Action"] = df["Action"].astype(str).str.strip().str.upper()
    df["Ticker"] = df["Ticker"].astype(str).str.strip().str.upper()
    return df


def _current_cash_balance(ledger: pd.DataFrame) -> float:
    if ledger.empty:
        return float(STARTING_JPY_BALANCE)

    cash_series = pd.to_numeric(ledger["Remaining_JPY_Balance"], errors="coerce").dropna()
    if cash_series.empty:
        return float(STARTING_JPY_BALANCE)

    return float(cash_series.iloc[-1])


def _net_holdings(ledger: pd.DataFrame) -> pd.Series:
    if ledger.empty:
        return pd.Series(dtype="float64")

    buys = ledger.loc[ledger["Action"] == "BUY"].groupby("Ticker")["Quantity"].sum()
    sells = ledger.loc[ledger["Action"] == "SELL"].groupby("Ticker")["Quantity"].sum()
    net = buys.sub(sells, fill_value=0.0)
    return net[net > 0]


def _is_tse_ticker(ticker: str) -> bool:
    return ticker.upper().endswith(".T")


def _upsert_nav_record(valuation_date: str, total_value_jpy: float) -> None:
    get_database().upsert_performance_row(
        {"date": valuation_date, "portfolio_value_jpy": round(total_value_jpy, 2)}
    )


def run_daily_valuation() -> dict[str, float | int | str]:
    ledger = _load_ledger()

    cash_balance_jpy = _current_cash_balance(ledger)
    holdings = _net_holdings(ledger)

    usd_jpy = get_current_usd_jpy(fallback=None)
    assets_value_jpy = 0.0

    for ticker, qty in holdings.items():
        live_price = get_live_price(str(ticker), fallback=None)
        if live_price is None:
            raise RuntimeError(f"Price unavailable for holding ticker {ticker}.")

        if _is_tse_ticker(str(ticker)):
            fx_rate = 1.0
        else:
            if usd_jpy is None:
                raise RuntimeError("USD/JPY rate unavailable; cannot value US holdings.")
            fx_rate = float(usd_jpy)

        assets_value_jpy += float(qty) * float(live_price) * fx_rate

    total_portfolio_value_jpy = cash_balance_jpy + assets_value_jpy
    valuation_date = date.today().isoformat()
    _upsert_nav_record(valuation_date, total_portfolio_value_jpy)

    return {
        "date": valuation_date,
        "cash_balance_jpy": round(cash_balance_jpy, 2),
        "assets_value_jpy": round(assets_value_jpy, 2),
        "total_portfolio_value_jpy": round(total_portfolio_value_jpy, 2),
        "open_positions": int(len(holdings)),
    }


if __name__ == "__main__":
    print(run_daily_valuation())
