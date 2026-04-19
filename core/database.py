from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Final

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

LEDGER_WORKSHEET_NAME: Final[str] = "Ledger"
PERFORMANCE_WORKSHEET_NAME: Final[str] = "Performance"
ORDER_BOOK_WORKSHEET_NAME: Final[str] = "Order_Book"
TEAM_AUTH_WORKSHEET_NAME: Final[str] = "Team_Auth"

LEDGER_COLUMNS: Final[list[str]] = [
    "Timestamp",
    "Ticker",
    "Action",
    "Quantity",
    "Local_Asset_Price",
    "Executed_FX_Rate",
    "Total_JPY_Impact",
    "Remaining_JPY_Balance",
    "Trader_Name",
    "Commission_Paid",
    "FX_Conversion_Fee",
    "Trade_Rationale",  # NEW: student justification required for grading
]
PERFORMANCE_COLUMNS: Final[list[str]] = ["date", "Trader_Name", "portfolio_value_jpy"]
ORDER_BOOK_COLUMNS: Final[list[str]] = [
    "Timestamp",
    "Ticker",
    "Action",
    "Mode",
    "Value",
    "Rationale",
    "Status",
    "Trader_Name",
]
TEAM_AUTH_COLUMNS: Final[list[str]] = ["Trader_Name", "Auth_Code", "Active", "Created_At"]

GOOGLE_CREDENTIALS_ENV: Final[str] = "GOOGLE_CREDENTIALS"
GOOGLE_CREDENTIALS_FILE_ENV: Final[str] = "GOOGLE_CREDENTIALS_FILE"
GOOGLE_SHEET_ID_ENV: Final[str] = "GOOGLE_SHEET_ID"
SPREADSHEET_ID_ENV: Final[str] = "GOOGLE_SHEETS_SPREADSHEET_ID"
SPREADSHEET_NAME_ENV: Final[str] = "GOOGLE_SHEETS_SPREADSHEET_NAME"

GOOGLE_SCOPES: Final[list[str]] = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]

SESSION_CONFIG_WORKSHEET_NAME: Final[str] = "Active Session"
SESSION_CONFIG_COLUMNS: Final[list[str]] = ["key", "value"]
ACTIVE_LEDGER_KEY: Final[str] = "active_ledger_worksheet"
ACTIVE_PERFORMANCE_KEY: Final[str] = "active_performance_worksheet"
ACTIVE_STARTED_AT_KEY: Final[str] = "active_session_started_at"


class DatabaseConfigError(RuntimeError):
    pass


def _maybe_read_streamlit_secret(key: str) -> Any | None:
    try:
        import streamlit as st
    except Exception:
        return None

    try:
        secrets = getattr(st, "secrets", None)
        if secrets is not None and key in secrets:
            return secrets[key]
    except Exception:
        return None

    return None


def _load_service_account_info() -> dict[str, Any]:
    def _validate_payload(payload: dict[str, Any]) -> dict[str, Any]:
        required_keys = {"client_email", "private_key", "token_uri"}
        missing = sorted(k for k in required_keys if k not in payload)
        if missing:
            raise DatabaseConfigError(
                "Service account JSON is missing required keys: " + ", ".join(missing)
            )
        return payload

    raw_secret = os.getenv(GOOGLE_CREDENTIALS_ENV)
    if not raw_secret:
        secret_from_streamlit = _maybe_read_streamlit_secret(GOOGLE_CREDENTIALS_ENV)
        if secret_from_streamlit is not None:
            if isinstance(secret_from_streamlit, dict):
                return _validate_payload(dict(secret_from_streamlit))
            raw_secret = str(secret_from_streamlit)

    credentials_file = os.getenv(GOOGLE_CREDENTIALS_FILE_ENV)
    if not credentials_file:
        secret_file = _maybe_read_streamlit_secret(GOOGLE_CREDENTIALS_FILE_ENV)
        if secret_file is not None:
            credentials_file = str(secret_file)

    if raw_secret:
        try:
            payload = json.loads(raw_secret)
        except json.JSONDecodeError as exc:
            raise DatabaseConfigError(
                f"{GOOGLE_CREDENTIALS_ENV} must be valid JSON from a service account key file."
            ) from exc
        return _validate_payload(payload)

    if credentials_file:
        try:
            with open(credentials_file, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except FileNotFoundError as exc:
            raise DatabaseConfigError(
                f"{GOOGLE_CREDENTIALS_FILE_ENV} points to a missing file: {credentials_file}"
            ) from exc
        except json.JSONDecodeError as exc:
            raise DatabaseConfigError(
                f"{GOOGLE_CREDENTIALS_FILE_ENV} must point to a valid service account JSON file."
            ) from exc
        except OSError as exc:
            raise DatabaseConfigError(
                f"Unable to read credentials file from {GOOGLE_CREDENTIALS_FILE_ENV}: {exc}"
            ) from exc

        if not isinstance(payload, dict):
            raise DatabaseConfigError(
                f"{GOOGLE_CREDENTIALS_FILE_ENV} must point to a JSON object from a service account key file."
            )

        return _validate_payload(payload)

    raise DatabaseConfigError(
        "Missing Google service account credentials. "
        f"Set {GOOGLE_CREDENTIALS_ENV} as a JSON string or set {GOOGLE_CREDENTIALS_FILE_ENV} to a key file path."
    )


def _load_spreadsheet_locator() -> tuple[str | None, str | None]:
    sheet_id = os.getenv(GOOGLE_SHEET_ID_ENV) or os.getenv(SPREADSHEET_ID_ENV)
    sheet_name = os.getenv(SPREADSHEET_NAME_ENV)

    if not sheet_id:
        secret_google_sheet_id = _maybe_read_streamlit_secret(GOOGLE_SHEET_ID_ENV)
        if secret_google_sheet_id is not None:
            sheet_id = str(secret_google_sheet_id)

    if not sheet_id:
        secret_id = _maybe_read_streamlit_secret(SPREADSHEET_ID_ENV)
        if secret_id is not None:
            sheet_id = str(secret_id)

    if not sheet_name:
        secret_name = _maybe_read_streamlit_secret(SPREADSHEET_NAME_ENV)
        if secret_name is not None:
            sheet_name = str(secret_name)

    if not sheet_id and not sheet_name:
        raise DatabaseConfigError(
            f"Missing sheet locator. Set either {SPREADSHEET_ID_ENV} or {SPREADSHEET_NAME_ENV}."
        )

    return sheet_id, sheet_name


class GoogleSheetsDatabase:
    def __init__(self) -> None:
        service_account_info = _load_service_account_info()
        sheet_id, sheet_name = _load_spreadsheet_locator()

        credentials = Credentials.from_service_account_info(service_account_info, scopes=GOOGLE_SCOPES)
        self._client = gspread.authorize(credentials)

        if sheet_id:
            self._spreadsheet = self._client.open_by_key(sheet_id)
        else:
            self._spreadsheet = self._client.open(sheet_name or "")

        self._ledger_ws: Any = None
        self._performance_ws: Any = None
        self._schema_verified: bool = False

    def _get_or_create_session_config_worksheet(self):
        worksheet = self._get_or_create_worksheet(
            SESSION_CONFIG_WORKSHEET_NAME,
            rows=100,
            cols=max(2, len(SESSION_CONFIG_COLUMNS)),
        )
        current_header = worksheet.row_values(1)
        if current_header != SESSION_CONFIG_COLUMNS:
            worksheet.clear()
            worksheet.update([SESSION_CONFIG_COLUMNS], "A1")
        return worksheet

    def _read_session_config(self) -> dict[str, str]:
        worksheet = self._get_or_create_session_config_worksheet()
        records = worksheet.get_all_records()

        config: dict[str, str] = {}
        for record in records:
            key = str(record.get("key", "")).strip()
            if not key:
                continue
            config[key] = str(record.get("value", "")).strip()
        return config

    def _write_session_config(self, updates: dict[str, str]) -> None:
        worksheet = self._get_or_create_session_config_worksheet()
        existing = self._read_session_config()
        existing.update({k: v for k, v in updates.items() if str(k).strip()})

        worksheet.clear()
        worksheet.update([SESSION_CONFIG_COLUMNS], "A1")
        if existing:
            rows = [[key, value] for key, value in sorted(existing.items())]
            worksheet.append_rows(rows)

    def _get_active_worksheet_names(self) -> tuple[str, str]:
        config = self._read_session_config()
        ledger_name = config.get(ACTIVE_LEDGER_KEY, LEDGER_WORKSHEET_NAME) or LEDGER_WORKSHEET_NAME
        performance_name = (
            config.get(ACTIVE_PERFORMANCE_KEY, PERFORMANCE_WORKSHEET_NAME) or PERFORMANCE_WORKSHEET_NAME
        )
        return ledger_name, performance_name

    @property
    def spreadsheet_title(self) -> str:
        return str(getattr(self._spreadsheet, "title", ""))

    @property
    def spreadsheet_id(self) -> str:
        return str(getattr(self._spreadsheet, "id", ""))

    def ensure_schema(self) -> None:
        if self._schema_verified:
            return
        ledger_name, performance_name = self._get_active_worksheet_names()
        self._ledger_ws = self._ensure_worksheet_with_headers(ledger_name, LEDGER_COLUMNS)
        self._performance_ws = self._ensure_worksheet_with_headers(performance_name, PERFORMANCE_COLUMNS)

        self._write_session_config(
            {
                ACTIVE_LEDGER_KEY: ledger_name,
                ACTIVE_PERFORMANCE_KEY: performance_name,
            }
        )
        self._schema_verified = True

    def _ensure_worksheet_with_headers(self, title: str, headers: list[str]):
        worksheet = self._get_or_create_worksheet(title, max(1000, 2), max(20, len(headers)))
        current_header = worksheet.row_values(1)

        if not current_header:
            worksheet.update([headers], "A1")
            return worksheet

        if current_header == headers:
            return worksheet

        if set(current_header).issubset(set(headers)):
            records = worksheet.get_all_records()
            worksheet.clear()
            worksheet.update([headers], "A1")
            if records:
                rows = [[record.get(col, "") for col in headers] for record in records]
                worksheet.append_rows(rows)
            return worksheet

        # If only a header row exists (no data rows), safely re-write the header
        # rather than blocking the app. This repairs worksheets created with the
        # wrong schema (e.g. from an earlier initialization attempt).
        all_values = worksheet.get_all_values()
        data_rows = [r for r in all_values[1:] if any(str(c).strip() for c in r)]
        if not data_rows:
            worksheet.clear()
            worksheet.update([headers], "A1")
            return worksheet

        raise DatabaseConfigError(
            f"Worksheet '{title}' has unexpected header shape. Expected: {headers}, Found: {current_header}"
        )

    def _get_or_create_worksheet(self, title: str, rows: int, cols: int):
        try:
            return self._spreadsheet.worksheet(title)
        except gspread.WorksheetNotFound:
            return self._spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)

    def _column_letter(self, number: int) -> str:
        result = ""
        n = max(number, 1)
        while n > 0:
            n, remainder = divmod(n - 1, 26)
            result = chr(65 + remainder) + result
        return result

    def _format_header_row(self, worksheet, columns: list[str]) -> None:
        last_col = self._column_letter(len(columns))
        worksheet.format(
            f"A1:{last_col}1",
            {
                "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
                "backgroundColor": {"red": 0.17, "green": 0.39, "blue": 0.64},
            },
        )
        worksheet.freeze(rows=1)

    def _worksheet_exists(self, title: str) -> bool:
        try:
            self._spreadsheet.worksheet(title)
            return True
        except gspread.WorksheetNotFound:
            return False

    def _unique_worksheet_name(self, base_name: str) -> str:
        if not self._worksheet_exists(base_name):
            return base_name

        suffix = datetime.now(timezone.utc).strftime("%H%M%S")
        candidate = f"{base_name}_{suffix}"
        index = 1
        while self._worksheet_exists(candidate):
            index += 1
            candidate = f"{base_name}_{suffix}_{index}"
        return candidate

    def start_new_simulation(self, starting_capital: float) -> dict[str, Any]:
        if float(starting_capital) <= 0:
            raise ValueError("starting_capital must be greater than 0.")

        date_suffix = datetime.now(timezone.utc).strftime("%Y_%m_%d")
        ledger_name = self._unique_worksheet_name(f"Ledger_{date_suffix}")
        performance_name = self._unique_worksheet_name(f"Performance_{date_suffix}")

        ledger_ws = self._spreadsheet.add_worksheet(
            title=ledger_name,
            rows=5000,
            cols=max(20, len(LEDGER_COLUMNS)),
        )
        performance_ws = self._spreadsheet.add_worksheet(
            title=performance_name,
            rows=5000,
            cols=max(5, len(PERFORMANCE_COLUMNS)),
        )

        ledger_ws.update([LEDGER_COLUMNS], "A1")
        self._format_header_row(ledger_ws, LEDGER_COLUMNS)

        genesis_row = [
            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"),
            "JPY_CASH",
            "INITIAL_FUNDING",
            "0.000000",
            "1.000000",
            "1.000000",
            f"{float(starting_capital):.2f}",
            f"{float(starting_capital):.2f}",
            "System",
            "0.00",
            "0.00",
            "Initial Simulation Funding",
        ]
        ledger_ws.update([genesis_row], "A2")

        performance_ws.update([PERFORMANCE_COLUMNS], "A1")
        self._format_header_row(performance_ws, PERFORMANCE_COLUMNS)
        performance_ws.update(
            [[datetime.now(timezone.utc).date().isoformat(), "All Team", f"{float(starting_capital):.2f}"]],
            "A2",
        )

        self._write_session_config(
            {
                ACTIVE_LEDGER_KEY: ledger_name,
                ACTIVE_PERFORMANCE_KEY: performance_name,
                ACTIVE_STARTED_AT_KEY: datetime.now(timezone.utc).isoformat(),
            }
        )

        self._ledger_ws = ledger_ws
        self._performance_ws = performance_ws

        return {
            "active_ledger_worksheet": ledger_name,
            "active_performance_worksheet": performance_name,
            "starting_capital": float(starting_capital),
            "genesis_row_written": True,
        }

    def initialize_and_format_worksheets(self) -> dict[str, Any]:
        """Initialize worksheets with canonical headers and inject a genesis row
        if the Ledger is empty.  Delegates to ensure_schema() for header
        verification so the active-session config is respected."""
        self.ensure_schema()
        self._format_header_row(self._ledger_ws, LEDGER_COLUMNS)
        self._format_header_row(self._performance_ws, PERFORMANCE_COLUMNS)

        row2 = self._ledger_ws.row_values(2)
        wrote_genesis = False
        if not row2 or all(str(x).strip() == "" for x in row2):
            genesis_row = [
                datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"),
                "JPY_CASH",
                "INITIAL_FUNDING",
                "0.000000",
                "1.000000",
                "1.000000",
                "100000000.00",
                "100000000.00",
                "System",
                "0.00",
                "0.00",
                "Initial System Funding",
            ]
            self._ledger_ws.update([genesis_row], "A2")
            wrote_genesis = True

        return {
            "spreadsheet_title": self.spreadsheet_title,
            "spreadsheet_id": self.spreadsheet_id,
            "ledger_header_columns": len(LEDGER_COLUMNS),
            "performance_header_columns": len(PERFORMANCE_COLUMNS),
            "genesis_row_written": wrote_genesis,
        }

    def append_ledger_row(self, row: dict[str, Any]) -> None:
        self.ensure_schema()
        payload = []
        for col in LEDGER_COLUMNS:
            if col in row:
                payload.append(row.get(col, ""))
            elif col == "FX_Conversion_Fee":
                payload.append(row.get("FX_Conversion_Fee_Paid", ""))
            else:
                payload.append("")
        self._ledger_ws.append_row(payload)

    def append_order_book_row(self, row: dict[str, Any]) -> None:
        ws = self._ensure_worksheet_with_headers(ORDER_BOOK_WORKSHEET_NAME, ORDER_BOOK_COLUMNS)
        payload = [row.get(col, "") for col in ORDER_BOOK_COLUMNS]
        ws.append_row(payload)

    def get_team_auth_df(self) -> pd.DataFrame:
        ws = self._ensure_worksheet_with_headers(TEAM_AUTH_WORKSHEET_NAME, TEAM_AUTH_COLUMNS)
        records = ws.get_all_records()
        df = pd.DataFrame(records)
        if df.empty:
            return pd.DataFrame(columns=TEAM_AUTH_COLUMNS)
        return df

    def upsert_team_auth(self, trader_name: str, auth_code: str, active: bool = True) -> None:
        ws = self._ensure_worksheet_with_headers(TEAM_AUTH_WORKSHEET_NAME, TEAM_AUTH_COLUMNS)
        records = ws.get_all_records()
        now = datetime.now(timezone.utc).isoformat()
        for index, existing in enumerate(records, start=2):
            if str(existing.get("Trader_Name", "")).strip().casefold() == trader_name.strip().casefold():
                created_at = str(existing.get("Created_At", now))
                if not created_at:
                    created_at = now
                payload = [[trader_name, auth_code, str(active), created_at]]
                ws.update(payload, f"A{index}:D{index}")
                return
        payload = [trader_name, auth_code, str(active), now]
        ws.append_row(payload)

    def rename_team_auth(self, old_name: str, new_name: str) -> bool:
        ws = self._ensure_worksheet_with_headers(TEAM_AUTH_WORKSHEET_NAME, TEAM_AUTH_COLUMNS)
        records = ws.get_all_records()
        for index, existing in enumerate(records, start=2):
            if str(existing.get("Trader_Name", "")).strip().casefold() == old_name.strip().casefold():
                ws.update_cell(index, 1, new_name)
                return True
        return False

    def get_order_book_df(self) -> pd.DataFrame:
        ws = self._ensure_worksheet_with_headers(ORDER_BOOK_WORKSHEET_NAME, ORDER_BOOK_COLUMNS)
        records = ws.get_all_records()
        df = pd.DataFrame(records)
        if df.empty:
            return pd.DataFrame(columns=ORDER_BOOK_COLUMNS)
        return df

    def update_order_status(self, timestamp: str, new_status: str) -> bool:
        """Update the Status column of the Order_Book row matching *timestamp*.

        Returns True if the row was found and updated, False otherwise.
        """
        ws = self._ensure_worksheet_with_headers(ORDER_BOOK_WORKSHEET_NAME, ORDER_BOOK_COLUMNS)
        all_values = ws.get_all_values()  # includes header
        if not all_values:
            return False
        header = all_values[0]
        try:
            status_col_index = header.index("Status") + 1  # 1-based for gspread
        except ValueError:
            return False
        for sheet_row_index, row_values in enumerate(all_values[1:], start=2):
            if str(row_values[0]).strip() == str(timestamp).strip():
                ws.update_cell(sheet_row_index, status_col_index, new_status)
                return True
        return False

    def cancel_order(self, timestamp: str) -> bool:
        """Delete the Order_Book row whose Timestamp matches *timestamp*.

        Rows are indexed from 1 in gspread; row 1 is the header, so data rows
        start at index 2.  Returns True if a row was deleted, False if not found.
        """
        ws = self._ensure_worksheet_with_headers(ORDER_BOOK_WORKSHEET_NAME, ORDER_BOOK_COLUMNS)
        all_values = ws.get_all_values()  # includes header
        for sheet_row_index, row_values in enumerate(all_values[1:], start=2):
            # Timestamp is the first column
            if str(row_values[0]).strip() == str(timestamp).strip():
                ws.delete_rows(sheet_row_index)
                return True
        return False

    def append_performance_row(self, row: dict[str, Any]) -> None:
        self.ensure_schema()
        payload = [row.get(col, "") for col in PERFORMANCE_COLUMNS]
        self._performance_ws.append_row(payload)

    def upsert_performance_row(self, row: dict[str, Any]) -> None:
        self.ensure_schema()
        target_date = str(row.get("date", "")).strip()
        trader_name = str(row.get("Trader_Name", "All Team")).strip()
        if not target_date:
            raise ValueError("Performance row requires a non-empty 'date'.")

        records = self._performance_ws.get_all_records()
        for index, existing in enumerate(records, start=2):
            if str(existing.get("date", "")).strip() == target_date and str(existing.get("Trader_Name", "All Team")).strip() == trader_name:
                payload = [[row.get(col, "") for col in PERFORMANCE_COLUMNS]]
                end_col = self._column_letter(len(PERFORMANCE_COLUMNS))
                self._performance_ws.update(payload, f"A{index}:{end_col}{index}")
                return

        self.append_performance_row(row)

    def get_ledger_df(self) -> pd.DataFrame:
        self.ensure_schema()
        records = self._ledger_ws.get_all_records()
        df = pd.DataFrame(records)
        if df.empty:
            return pd.DataFrame(columns=LEDGER_COLUMNS)

        for col in [
            "Quantity",
            "Local_Asset_Price",
            "Executed_FX_Rate",
            "Total_JPY_Impact",
            "Remaining_JPY_Balance",
            "Commission_Paid",
            "FX_Conversion_Fee",
        ]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        if "FX_Conversion_Fee" in df.columns and "FX_Conversion_Fee_Paid" not in df.columns:
            df["FX_Conversion_Fee_Paid"] = df["FX_Conversion_Fee"]

        return df

    def get_performance_df(self) -> pd.DataFrame:
        self.ensure_schema()
        records = self._performance_ws.get_all_records()
        df = pd.DataFrame(records)
        if df.empty:
            return pd.DataFrame(columns=PERFORMANCE_COLUMNS)

        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
        if "portfolio_value_jpy" in df.columns:
            df["portfolio_value_jpy"] = pd.to_numeric(df["portfolio_value_jpy"], errors="coerce")

        return df

    def get_recent_ledger_df(self, n: int = 5) -> pd.DataFrame:
        df = self.get_ledger_df()
        if df.empty:
            return df
        return df.tail(max(int(n), 1))

    def record_daily_performance(self) -> dict[str, Any]:
        """Calculate current portfolio value and upsert today's date into Performance.

        Steps
        -----
        1. Load the full Ledger and derive current cash balance and net holdings.
        2. Fetch live prices (falls back to last-close when market is closed).
        3. Value each holding in JPY using current USD/JPY if needed.
        4. Sum cash + equity value and upsert a row keyed on today's ISO date.

        Returns a dict with the computed values for display in the UI.
        """
        from core.market_data import get_current_usd_jpy, get_live_price
        from core.setup_env import STARTING_JPY_BALANCE

        df = self.get_ledger_df()

        if df.empty:
            cash = float(STARTING_JPY_BALANCE)
        else:
            bal_col = df["Remaining_JPY_Balance"].dropna()
            cash = float(bal_col.iloc[-1]) if not bal_col.empty else float(STARTING_JPY_BALANCE)

        # Net holdings per ticker
        buys = (
            df.loc[df["Action"] == "BUY"]
            .groupby("Ticker")["Quantity"]
            .sum()
        )
        sells = (
            df.loc[df["Action"] == "SELL"]
            .groupby("Ticker")["Quantity"]
            .sum()
        )
        net = buys.sub(sells, fill_value=0.0)
        holdings = {str(t): float(q) for t, q in net.items() if float(q) > 0}

        usd_jpy = get_current_usd_jpy(fallback=150.0) or 150.0
        equity_jpy = 0.0
        skipped: list[str] = []
        live_prices: dict[str, float] = {}

        for ticker, qty in holdings.items():
            price = get_live_price(ticker, fallback=None)
            if price is None:
                skipped.append(ticker)
                continue
            
            live_prices[ticker] = float(price)
            if ticker.upper().endswith(".T"):
                equity_jpy += qty * float(price)
            else:
                equity_jpy += qty * float(price) * usd_jpy

        total_jpy = cash + equity_jpy
        today = datetime.now(timezone.utc).date().isoformat()

        self.upsert_performance_row({"date": today, "Trader_Name": "All Team", "portfolio_value_jpy": f"{total_jpy:.2f}"})

        # Track per-member performance
        unique_traders = [t for t in df["Trader_Name"].unique() if str(t).strip().casefold() not in ("system", "all team", "")]
        for trader in unique_traders:
            trader_df = df[df["Trader_Name"] == trader]
            t_buys = trader_df.loc[trader_df["Action"] == "BUY"].groupby("Ticker")["Quantity"].sum()
            t_sells = trader_df.loc[trader_df["Action"] == "SELL"].groupby("Ticker")["Quantity"].sum()
            t_net = t_buys.sub(t_sells, fill_value=0.0)
            t_holdings = {str(t): float(q) for t, q in t_net.items() if float(q) > 0}
            
            t_equity = 0.0
            for t, q in t_holdings.items():
                if t in live_prices:
                    if t.upper().endswith(".T"):
                        t_equity += q * live_prices[t]
                    else:
                        t_equity += q * live_prices[t] * usd_jpy
                        
            # Individual value = STARTING_CAPITAL + (Sum of their Trades JPY Impact + their Equity)
            # This makes their graph comparable to the main fund's graph starting at 100M
            t_profit = trader_df["Total_JPY_Impact"].sum() + t_equity
            t_value = STARTING_JPY_BALANCE + t_profit
            self.upsert_performance_row({"date": today, "Trader_Name": trader, "portfolio_value_jpy": f"{t_value:.2f}"})

        return {
            "date": today,
            "cash_jpy": cash,
            "equity_jpy": equity_jpy,
            "total_portfolio_value_jpy": total_jpy,
            "usd_jpy_rate": usd_jpy,
            "tickers_skipped": skipped,
        }


@lru_cache(maxsize=1)
def get_database() -> GoogleSheetsDatabase:
    db = GoogleSheetsDatabase()
    db.ensure_schema()
    return db


# ── Streamlit-cached read helpers ─────────────────────────────────────────────
# These module-level functions are decorated with @st.cache_data(ttl=60) when
# running inside Streamlit so the app hits Google Sheets at most once per
# minute.  Outside Streamlit (CLI hooks, tests) they fall back to plain calls.


def get_cached_ledger_df() -> pd.DataFrame:
    """Return the Ledger as a DataFrame (cached for 60 s inside Streamlit)."""
    return get_database().get_ledger_df()


def get_cached_performance_df() -> pd.DataFrame:
    """Return the Performance tab as a DataFrame (cached for 60 s inside Streamlit)."""
    return get_database().get_performance_df()

def get_cached_team_auth_df() -> pd.DataFrame:
    """Return the Team Auth tab as a DataFrame."""
    return get_database().get_team_auth_df()


def clear_data_cache() -> None:
    """Bust all @st.cache_data caches.  Must be called after every write so the
    UI shows fresh data on the next interaction."""
    try:
        import streamlit as st
        st.cache_data.clear()
    except Exception:
        pass


# Apply Streamlit caching at import time (safe no-op when running outside Streamlit).
try:
    import streamlit as _st
    get_cached_ledger_df = _st.cache_data(ttl=60)(get_cached_ledger_df)
    get_cached_performance_df = _st.cache_data(ttl=60)(get_cached_performance_df)
    get_cached_team_auth_df = _st.cache_data(ttl=60)(get_cached_team_auth_df)
    del _st
except Exception:
    pass


def start_new_simulation(starting_capital: float) -> dict[str, Any]:
    return get_database().start_new_simulation(starting_capital)


def record_daily_performance() -> dict[str, Any]:
    """Module-level convenience wrapper — call from UI or scheduled hook."""
    return get_database().record_daily_performance()


def get_google_sheets_connection_status() -> dict[str, Any]:
    configured_sheet_id = os.getenv(GOOGLE_SHEET_ID_ENV) or os.getenv(SPREADSHEET_ID_ENV)
    try:
        db = get_database()
        return {
            "connected": True,
            "message": "Google Sheets database is linked.",
            "configured_sheet_id": configured_sheet_id,
            "spreadsheet_id": db.spreadsheet_id,
            "spreadsheet_title": db.spreadsheet_title,
        }
    except Exception as exc:
        return {
            "connected": False,
            "message": f"Google Sheets connection failed: {exc}",
            "configured_sheet_id": configured_sheet_id,
            "spreadsheet_id": None,
            "spreadsheet_title": None,
        }


def initialize_database_schema(sheet_id: str) -> dict[str, Any]:
    """Authenticate, connect to the given Sheet, and autonomously build the
    database schema.  Safe to call on an already-initialised sheet — existing
    worksheets are left untouched.

    Steps
    -----
    1. Authenticate with the configured service-account credentials.
    2. Open the spreadsheet by *sheet_id*.
    3. Create the "Ledger" worksheet (if absent), write the canonical header
       row, and insert the 100 000 000 JPY Genesis funding row as Row 2.
    4. Create the "Performance" worksheet (if absent) and write its headers.
    5. Delete the default "Sheet1" tab if it still exists.
    6. Update the in-process environment variable so the rest of the running
       application automatically targets this new sheet.
    """
    if not sheet_id or not sheet_id.strip():
        raise ValueError("sheet_id must be a non-empty string.")

    sheet_id = sheet_id.strip()

    service_account_info = _load_service_account_info()
    credentials = Credentials.from_service_account_info(service_account_info, scopes=GOOGLE_SCOPES)
    client = gspread.authorize(credentials)
    spreadsheet = client.open_by_key(sheet_id)

    existing_titles = [ws.title for ws in spreadsheet.worksheets()]

    # ── Ledger ────────────────────────────────────────────────────────────────
    ledger_created = False
    genesis_written = False
    if "Ledger" not in existing_titles:
        ledger_ws = spreadsheet.add_worksheet("Ledger", rows=1000, cols=15)
        ledger_ws.update([LEDGER_COLUMNS], "A1")

        genesis_row = [
            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"),
            "JPY_CASH",
            "FUNDING",
            "0.000000",
            "1.000000",
            "1.000000",
            "100000000.00",
            "100000000.00",
            "System",
            "0.00",
            "0.00",
            "Initial System Funding",
        ]
        ledger_ws.update([genesis_row], "A2")
        ledger_created = True
        genesis_written = True

    # ── Performance ───────────────────────────────────────────────────────────
    performance_created = False
    if "Performance" not in existing_titles:
        spreadsheet.add_worksheet("Performance", rows=1000, cols=10)
        performance_ws = spreadsheet.worksheet("Performance")
        performance_ws.update([PERFORMANCE_COLUMNS], "A1")
        performance_created = True

    # ── Remove default Sheet1 ─────────────────────────────────────────────────
    sheet1_deleted = False
    if "Sheet1" in existing_titles:
        spreadsheet.del_worksheet(spreadsheet.worksheet("Sheet1"))
        sheet1_deleted = True

    # ── Update running environment so the app targets the new sheet ───────────
    os.environ[GOOGLE_SHEET_ID_ENV] = sheet_id
    get_database.cache_clear()

    return {
        "spreadsheet_title": spreadsheet.title,
        "spreadsheet_id": sheet_id,
        "ledger_created": ledger_created,
        "genesis_written": genesis_written,
        "performance_created": performance_created,
        "sheet1_deleted": sheet1_deleted,
    }
