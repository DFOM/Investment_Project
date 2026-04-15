"""Dividend Engine — fetches live dividend data, applies applicable taxes,
and credits net income to the portfolio JPY balance.

============================  TAX RULES APPLIED  ============================

US Stocks (NYSE / NASDAQ / CME):
  Step 1 ─ US Withholding Tax at 10%
            (Japan–US tax treaty rate for Japan-resident investors)
  Step 2 ─ Japanese Domestic Tax at 20.315% applied to the REMAINING 90%
            (15% national income tax + 5.105% local resident tax)
  Effective total tax rate ≈ 28.28%

  Example: gross ¥10,000
    US withholding : ¥1,000  (10%)
    JP tax on ¥9,000 : ¥1,828  (20.315%)
    Total tax       : ¥2,828
    Net to portfolio: ¥7,172

Japanese Stocks (TSE — tickers ending in .T):
  Japanese Withholding Tax at 20.315%
  (15% national income tax + 5.105% local resident tax)
  No US withholding applies.

  Example: gross ¥10,000
    JP withholding  : ¥2,032  (20.315%)
    Net to portfolio: ¥7,968

Capital Gains:
  Japan taxes realized profits at 20.315% regardless of stock origin.
  US stocks: foreign tax credit can be claimed at annual filing, but is
  NOT automatically applied at point-of-sale in this simulation.

=============================================================================
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from functools import lru_cache
from typing import Final

import pandas as pd
import yfinance as yf

from core.database import clear_data_cache, get_cached_ledger_df, get_database
from core.market_data import get_current_usd_jpy
from core.setup_env import STARTING_JPY_BALANCE

LOGGER = logging.getLogger(__name__)

# ── Tax Rate Constants ─────────────────────────────────────────────────────────

# Japan–US tax treaty: US withholding rate on dividends for Japan residents
US_WITHHOLDING_RATE: Final[float] = 0.10          # 10 %

# Japanese domestic withholding on dividend/capital-gain income
JP_DIVIDEND_TAX_RATE: Final[float] = 0.20315      # 20.315 %
JP_CAPITAL_GAINS_TAX_RATE: Final[float] = 0.20315 # 20.315 %

# Pre-computed effective rate for US dividends received by Japan residents
# = US_WITHHELD + JP_TAX × (1 - US_WITHHELD)
US_DIVIDEND_EFFECTIVE_RATE: Final[float] = (
    US_WITHHOLDING_RATE + (1.0 - US_WITHHOLDING_RATE) * JP_DIVIDEND_TAX_RATE
)  # ≈ 0.2828


# ── Decimal helpers ────────────────────────────────────────────────────────────

def _d(value) -> Decimal:
    return Decimal(str(value))


def _money(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


# ── Ticker helpers ─────────────────────────────────────────────────────────────

def _is_tse_ticker(ticker: str) -> bool:
    return ticker.upper().strip().endswith(".T")


def _currency_label(ticker: str) -> str:
    return "JPY" if _is_tse_ticker(ticker) else "USD"


# ── Ledger loaders ─────────────────────────────────────────────────────────────

def _load_ledger() -> pd.DataFrame:
    """Load and normalise the ledger DataFrame."""
    df = get_cached_ledger_df().copy()
    if df.empty:
        return df

    df["Ticker"] = df["Ticker"].astype(str).str.strip().str.upper()
    df["Action"] = df["Action"].astype(str).str.strip().str.upper()
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0)
    df["Total_JPY_Impact"] = pd.to_numeric(df["Total_JPY_Impact"], errors="coerce").fillna(0)
    df["Remaining_JPY_Balance"] = pd.to_numeric(df["Remaining_JPY_Balance"], errors="coerce")

    def _parse_ts(ts_str):
        t = pd.to_datetime(ts_str, errors="coerce")
        if pd.isna(t):
            return pd.NaT
        return t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")

    df["Timestamp"] = df["Timestamp"].apply(_parse_ts)
    return df


def _latest_balance() -> float:
    df = _load_ledger()
    if df.empty:
        return STARTING_JPY_BALANCE
    val = df["Remaining_JPY_Balance"].dropna()
    return float(val.iloc[-1]) if not val.empty else STARTING_JPY_BALANCE


# ── Portfolio helpers ──────────────────────────────────────────────────────────

def get_current_holdings() -> dict[str, float]:
    """Return ``{ticker: net_quantity}`` for every position with qty > 0."""
    df = _load_ledger()
    if df.empty:
        return {}
    trade_df = df[df["Action"].isin({"BUY", "SELL"})]
    buys = trade_df[trade_df["Action"] == "BUY"].groupby("Ticker")["Quantity"].sum()
    sells = trade_df[trade_df["Action"] == "SELL"].groupby("Ticker")["Quantity"].sum()
    net = buys.sub(sells, fill_value=0.0)
    return {str(t): float(q) for t, q in net.items() if float(q) > 0}


def _holding_since(ticker: str) -> pd.Timestamp | None:
    """UTC timestamp of the very first BUY for *ticker*."""
    df = _load_ledger()
    buys = df[(df["Ticker"] == ticker.upper()) & (df["Action"] == "BUY")]
    if buys.empty:
        return None
    ts = buys["Timestamp"].min()
    return ts if pd.notna(ts) else None


def _last_dividend_collection_date(ticker: str) -> pd.Timestamp | None:
    """UTC timestamp of the most recent DIVIDEND ledger row for *ticker*, or None."""
    df = _load_ledger()
    divs = df[(df["Ticker"] == ticker.upper()) & (df["Action"] == "DIVIDEND")]
    if divs.empty:
        return None
    ts = divs["Timestamp"].max()
    return ts if pd.notna(ts) else None


# ── Dividend Data ──────────────────────────────────────────────────────────────

@lru_cache(maxsize=64)
def _fetch_dividend_history_cached(ticker: str) -> pd.DataFrame:
    """Fetch historical dividend events from yfinance (cached per process)."""
    try:
        t = yf.Ticker(ticker)
        divs = t.dividends
        if divs is None or divs.empty:
            return pd.DataFrame(columns=["date", "amount"])
        df = divs.reset_index()
        df.columns = ["date", "amount"]
        df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")
        df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
        return df
    except Exception as exc:
        LOGGER.warning("Dividend history fetch failed for %s: %s", ticker, exc)
        return pd.DataFrame(columns=["date", "amount"])


def fetch_dividend_history(ticker: str) -> pd.DataFrame:
    """Public wrapper — clears lru_cache stale data by using a fresh call."""
    return _fetch_dividend_history_cached(ticker.upper().strip())


def get_upcoming_dividends(ticker: str) -> pd.DataFrame:
    """
    Return yfinance-reported upcoming dividend info for *ticker*, if available.
    Only returns data when the ex-dividend date is strictly in the future.

    Returns a one-row DataFrame with columns: ex_dividend_date, dividend_rate,
    or an empty DataFrame when no future ex-date is confirmed.
    """
    try:
        info = yf.Ticker(ticker).info
        ex_date = info.get("exDividendDate")
        rate = info.get("dividendRate") or info.get("trailingAnnualDividendRate")
        if ex_date:
            ex_ts = pd.Timestamp(ex_date, unit="s", tz="UTC")
            today = pd.Timestamp.now(tz="UTC")
            if ex_ts > today:  # only surface confirmed future ex-dates
                return pd.DataFrame([{"ex_dividend_date": ex_ts, "annual_dividend_rate": rate or 0.0}])
    except Exception:
        pass
    return pd.DataFrame()


# ── Tax Calculations ─────────────────────────────────────────────────────────

def compute_dividend_tax(gross_jpy: float, ticker: str) -> dict[str, float]:
    """
    Compute the full tax breakdown for a dividend received on *ticker*.

    US stocks (NYSE / NASDAQ):
        - Step 1: US withholding at 10 % (Japan–US treaty)
        - Step 2: Japanese tax at 20.315 % on the remaining 90 %
        - Effective combined rate ≈ 28.28 %

    Japanese stocks (TSE — ending .T):
        - Japanese withholding at 20.315 %
        - No US withholding

    Returns
    -------
    dict with keys:
      gross_jpy, us_withholding_jpy, jp_tax_jpy, total_tax_jpy,
      net_jpy, effective_rate_pct, is_us_stock
    """
    is_us = not _is_tse_ticker(ticker)
    g = _d(gross_jpy)

    if is_us:
        us_wh = _money(g * _d(US_WITHHOLDING_RATE))
        after_us_wh = g - us_wh
        jp_tax = _money(after_us_wh * _d(JP_DIVIDEND_TAX_RATE))
        total_tax = _money(us_wh + jp_tax)
    else:
        us_wh = _d("0.00")
        jp_tax = _money(g * _d(JP_DIVIDEND_TAX_RATE))
        total_tax = _money(jp_tax)

    net = _money(g - total_tax)
    effective_rate = float(total_tax / g * _d("100")) if g > 0 else 0.0

    return {
        "gross_jpy": float(_money(g)),
        "us_withholding_jpy": float(us_wh),
        "jp_tax_jpy": float(jp_tax),
        "total_tax_jpy": float(total_tax),
        "net_jpy": float(net),
        "effective_rate_pct": round(effective_rate, 4),
        "is_us_stock": is_us,
    }


def compute_capital_gains_tax(realized_gain_jpy: float) -> dict[str, float]:
    """
    Compute Japanese capital gains tax on a realized profit.

    Japan taxes capital gains at 20.315 % flat regardless of stock origin.
    The foreign tax credit for US stocks must be claimed at annual tax filing
    and is NOT automatically applied at point-of-sale in this simulation.

    Parameters
    ----------
    realized_gain_jpy : float
        Profit in JPY (sell proceeds minus cost basis).  Losses (< 0) produce
        zero tax.

    Returns
    -------
    dict with keys: gain_jpy, tax_jpy, net_after_tax_jpy, rate_pct
    """
    gain = _d(max(realized_gain_jpy, 0.0))   # no tax on losses
    tax = _money(gain * _d(JP_CAPITAL_GAINS_TAX_RATE))
    net = _money(gain - tax)

    return {
        "gain_jpy": float(gain),
        "tax_jpy": float(tax),
        "net_after_tax_jpy": float(net),
        "rate_pct": JP_CAPITAL_GAINS_TAX_RATE * 100,
    }


# ── Dividend Collection ────────────────────────────────────────────────────────

def find_uncollected_dividends(ticker: str, held_qty: float) -> list[dict]:
    """
    Find dividend events for *ticker* that have not yet been collected.

    A dividend is considered uncollected when:
      - Its ex-dividend date is strictly AFTER the portfolio's last collection
        for that ticker (or after the first BUY date if never collected), AND
      - Its ex-dividend date is on or before today (already paid out).

    Parameters
    ----------
    ticker    : normalised ticker symbol (e.g. "AAPL", "7203.T")
    held_qty  : current net shares held

    Returns
    -------
    List of dicts with keys: ticker, ex_date, amount_local, held_qty
    """
    symbol = ticker.upper().strip()
    last_collection = _last_dividend_collection_date(symbol)
    holding_since = _holding_since(symbol)

    if last_collection is not None:
        cutoff = last_collection
    elif holding_since is not None:
        cutoff = holding_since
    else:
        return []  # never bought this stock

    hist = fetch_dividend_history(symbol)
    if hist.empty:
        return []

    today = pd.Timestamp.now(tz="UTC")
    pending = hist[(hist["date"] > cutoff) & (hist["date"] <= today)]

    return [
        {
            "ticker": symbol,
            "ex_date": row["date"],
            "amount_local": float(row["amount"]),
            "held_qty": held_qty,
        }
        for _, row in pending.iterrows()
    ]


def collect_dividends_for_ticker(
    ticker: str,
    held_qty: float,
    trader_name: str,
    usd_jpy: float,
) -> list[dict]:
    """
    Collect all uncollected dividends for a single ticker and write them to
    the ledger.

    Each dividend event produces ONE ledger row:
      Action = "DIVIDEND"
      Total_JPY_Impact = net dividend after all applicable taxes (positive)
      Trade_Rationale  = full tax breakdown for transparency

    Parameters
    ----------
    ticker      : normalised ticker symbol
    held_qty    : current shares held
    trader_name : team member authorising the collection
    usd_jpy     : live USD/JPY rate (1.0 for TSE tickers)

    Returns
    -------
    List of result dicts, one per dividend event collected.
    """
    uncollected = find_uncollected_dividends(ticker, held_qty)
    if not uncollected:
        return []

    fx_rate = 1.0 if _is_tse_ticker(ticker) else usd_jpy
    currency = _currency_label(ticker)
    db = get_database()
    results: list[dict] = []

    for event in uncollected:
        amount_local = event["amount_local"]
        qty = event["held_qty"]

        gross_local = amount_local * qty
        gross_jpy = gross_local * fx_rate

        tax_info = compute_dividend_tax(gross_jpy, ticker)
        net_jpy = tax_info["net_jpy"]

        current_balance = _latest_balance()
        new_balance = current_balance + net_jpy
        ts_now = datetime.now(timezone.utc).isoformat()

        # ── Build transparent rationale ────────────────────────────────────
        if tax_info["is_us_stock"]:
            tax_detail = (
                f"US withholding 10%: -¥{tax_info['us_withholding_jpy']:,.2f} | "
                f"JP tax 20.315% on remainder: -¥{tax_info['jp_tax_jpy']:,.2f}"
            )
        else:
            tax_detail = f"JP withholding 20.315%: -¥{tax_info['jp_tax_jpy']:,.2f}"

        rationale = (
            f"Dividend income | {qty:,.4f} shares × {amount_local:.6f} {currency}/share"
            f" | Ex-date: {event['ex_date'].strftime('%Y-%m-%d')}"
            f" | FX rate: {fx_rate:.4f}"
            f" | Gross: ¥{tax_info['gross_jpy']:,.2f}"
            f" | {tax_detail}"
            f" | Effective tax: {tax_info['effective_rate_pct']:.2f}%"
            f" | Net credited: ¥{net_jpy:,.2f}"
        )

        div_row = {
            "Timestamp": ts_now,
            "Ticker": ticker.upper().strip(),
            "Action": "DIVIDEND",
            "Quantity": f"{qty:.6f}",
            "Local_Asset_Price": f"{amount_local:.6f}",
            "Executed_FX_Rate": f"{fx_rate:.6f}",
            "Total_JPY_Impact": f"{net_jpy:.2f}",
            "Remaining_JPY_Balance": f"{new_balance:.2f}",
            "Trader_Name": trader_name,
            "Commission_Paid": "0.00",
            "FX_Conversion_Fee": "0.00",
            "Trade_Rationale": rationale,
        }
        db.append_ledger_row(div_row)
        clear_data_cache()

        results.append({
            "ticker": ticker,
            "ex_date": event["ex_date"],
            "amount_local": amount_local,
            "currency": currency,
            "held_qty": qty,
            "fx_rate": fx_rate,
            "gross_jpy": tax_info["gross_jpy"],
            "us_withholding_jpy": tax_info["us_withholding_jpy"],
            "jp_tax_jpy": tax_info["jp_tax_jpy"],
            "total_tax_jpy": tax_info["total_tax_jpy"],
            "net_jpy": net_jpy,
            "effective_rate_pct": tax_info["effective_rate_pct"],
            "is_us_stock": tax_info["is_us_stock"],
            "new_balance": new_balance,
        })

    return results


def collect_all_dividends(trader_name: str, auth_code: str = "") -> dict[str, list[dict]]:
    """
    Scan every current holding and collect any dividends that have not yet
    been credited to the account.

    Parameters
    ----------
    trader_name : team member authorising the collection sweep

    Returns
    -------
    ``{ticker: [event_result_dicts]}`` — only tickers with new dividends.
    """
    from core.user_manager import authenticate_user
    if not authenticate_user(trader_name, auth_code):
        raise PermissionError(f"Authentication failed for {trader_name}. Invalid code.")

    holdings = get_current_holdings()
    if not holdings:
        return {}

    usd_jpy = float(get_current_usd_jpy(fallback=150.0) or 150.0)
    results: dict[str, list[dict]] = {}

    for ticker, qty in holdings.items():
        if ticker in {"JPY_CASH", "INITIAL_FUNDING"}:
            continue
        events = collect_dividends_for_ticker(ticker, qty, trader_name, usd_jpy)
        if events:
            results[ticker] = events

    return results


# ── Ledger Queries ─────────────────────────────────────────────────────────────

def get_dividend_history_from_ledger() -> pd.DataFrame:
    """Return all DIVIDEND rows from the active ledger as a DataFrame."""
    df = _load_ledger()
    if df.empty:
        return pd.DataFrame()
    mask = df["Action"] == "DIVIDEND"
    return df[mask].copy().sort_values("Timestamp", ascending=False).reset_index(drop=True)


def get_realized_gains_from_ledger() -> pd.DataFrame:
    """
    Compute realised capital gains/losses from SELL rows in the ledger.

    For every SELL event the function walks the preceding BUY history for
    that ticker to derive the average cost basis, then calculates:
      - Realised gain/loss in JPY
      - Capital gains tax owed at 20.315 % (on gains only; losses = ¥0 tax)

    Returns
    -------
    DataFrame with columns:
      Timestamp, Ticker, Quantity, Sell_Proceeds_JPY, Cost_Basis_JPY,
      Realized_Gain_JPY, Tax_Due_JPY, Net_Gain_JPY, Trader_Name
    """
    df = _load_ledger()
    if df.empty:
        return pd.DataFrame()

    records: list[dict] = []

    for ticker, group in df.groupby("Ticker"):
        group = group.sort_values("Timestamp")
        held_shares = _d("0")
        total_cost_jpy = _d("0")

        for _, row in group.iterrows():
            action = str(row["Action"]).upper()
            qty = _d(float(row["Quantity"]))
            impact = _d(float(row["Total_JPY_Impact"]))

            if action == "BUY":
                held_shares += qty
                total_cost_jpy += abs(impact)

            elif action == "SELL" and held_shares > 0:
                avg_cost = total_cost_jpy / held_shares
                sell_proceeds = impact                     # positive (cash in)
                cost_basis = _money(avg_cost * qty)
                gain = _money(sell_proceeds - cost_basis)

                tax_info = compute_capital_gains_tax(float(max(gain, _d("0"))))

                records.append({
                    "Timestamp": row["Timestamp"],
                    "Ticker": str(ticker),
                    "Quantity": float(qty),
                    "Sell_Proceeds_JPY": float(_money(sell_proceeds)),
                    "Cost_Basis_JPY": float(cost_basis),
                    "Realized_Gain_JPY": float(gain),
                    "Tax_Due_JPY": tax_info["tax_jpy"] if gain > 0 else 0.0,
                    "Net_Gain_JPY": float(gain) - (tax_info["tax_jpy"] if gain > 0 else 0.0),
                    "Trader_Name": str(row.get("Trader_Name", "")),
                })

                # shrink cost basis proportionally for partial sells
                if held_shares >= qty:
                    total_cost_jpy -= cost_basis
                    held_shares -= qty

    if not records:
        return pd.DataFrame()

    result = pd.DataFrame(records)
    return result.sort_values("Timestamp", ascending=False).reset_index(drop=True)
