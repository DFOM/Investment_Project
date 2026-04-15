from __future__ import annotations

import csv
import json
import shutil
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Final

BASE_DIR: Final[Path] = Path(__file__).resolve().parents[1]
DATA_DIR: Final[Path] = BASE_DIR / "data"
LEDGER_PATH: Final[Path] = DATA_DIR / "ledger.csv"
HISTORICAL_PATH: Final[Path] = DATA_DIR / "historical_performance.json"

ROOT_LEDGER_PATH: Final[Path] = BASE_DIR / "ledger.csv"
ROOT_HISTORICAL_PATH: Final[Path] = BASE_DIR / "historical_performance.json"

STARTING_JPY_BALANCE: Final[float] = 100_000_000.0
LEDGER_HEADER: Final[list[str]] = [
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
    "FX_Conversion_Fee_Paid",
]

LEGACY_LEDGER_HEADER: Final[list[str]] = [
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
]


def _genesis_row() -> dict[str, str]:
    return {
        "Timestamp": datetime.now(timezone.utc).isoformat(),
        "Ticker": "JPY_CASH",
        "Action": "INITIAL_FUNDING",
        "Quantity": "0.000000",
        "Local_Asset_Price": "1.000000",
        "Executed_FX_Rate": "1.000000",
        "Total_JPY_Impact": f"{STARTING_JPY_BALANCE:.2f}",
        "Remaining_JPY_Balance": f"{STARTING_JPY_BALANCE:.2f}",
        "Trader_Name": "System",
        "Commission_Paid": "0.00",
        "FX_Conversion_Fee_Paid": "0.00",
    }


def _ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def _migrate_root_files_if_needed() -> None:
    _ensure_data_dir()

    if not LEDGER_PATH.exists() and ROOT_LEDGER_PATH.exists():
        shutil.copy2(ROOT_LEDGER_PATH, LEDGER_PATH)

    if not HISTORICAL_PATH.exists() and ROOT_HISTORICAL_PATH.exists():
        shutil.copy2(ROOT_HISTORICAL_PATH, HISTORICAL_PATH)


def _migrate_legacy_ledger_schema(path: Path) -> bool:
    if not path.exists() or path.stat().st_size == 0:
        return False

    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        source_header = list(reader.fieldnames or [])
        if source_header == LEDGER_HEADER:
            return False
        if source_header != LEGACY_LEDGER_HEADER:
            raise ValueError(
                "ledger.csv schema mismatch. Expected columns: "
                + ", ".join(LEDGER_HEADER)
            )
        rows = list(reader)

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=LEDGER_HEADER)
        writer.writeheader()
        for row in rows:
            row["FX_Conversion_Fee_Paid"] = "0.00"
            writer.writerow({key: row.get(key, "") for key in LEDGER_HEADER})

    return True


def _validate_existing_ledger(path: Path) -> tuple[bool, bool]:
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        has_rows = next(reader, None) is not None

    if header is None:
        return False, False

    if header != LEDGER_HEADER:
        raise ValueError(
            "ledger.csv schema mismatch. Expected exact columns: " + ", ".join(LEDGER_HEADER)
        )

    return True, has_rows


def initialize_ledger(path: Path = LEDGER_PATH) -> bool:
    _ensure_data_dir()

    if not path.exists() or path.stat().st_size == 0:
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=LEDGER_HEADER)
            writer.writeheader()
            writer.writerow(_genesis_row())
        return True

    migrated = _migrate_legacy_ledger_schema(path)

    has_header, has_rows = _validate_existing_ledger(path)
    if not has_header:
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=LEDGER_HEADER)
            writer.writeheader()
            writer.writerow(_genesis_row())
        return True

    if not has_rows:
        with path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=LEDGER_HEADER)
            writer.writerow(_genesis_row())
        return True

    return migrated


def initialize_historical(path: Path = HISTORICAL_PATH) -> bool:
    _ensure_data_dir()
    if path.exists() and path.stat().st_size > 0:
        return False

    payload = [
        {
            "date": date.today().isoformat(),
            "portfolio_value_jpy": round(STARTING_JPY_BALANCE, 2),
        }
    ]
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    return True


def setup_environment() -> dict[str, str]:
    """Return the Google Sheets connection status.

    Local CSV/JSON files are no longer the authoritative data store — all
    trade and performance data lives in Google Sheets.  This function now
    checks the live connection and returns a status dict that pages can
    display in their sidebars.
    """
    # Lazy import to avoid circular dependency (database imports STARTING_JPY_BALANCE
    # from this module inside a method, not at module level).
    from core.database import get_google_sheets_connection_status  # noqa: PLC0415

    status = get_google_sheets_connection_status()
    return {
        "google_sheets": "connected" if status["connected"] else "disconnected",
        "spreadsheet_title": status.get("spreadsheet_title") or "N/A",
        "message": status.get("message") or "",
    }


if __name__ == "__main__":
    print(setup_environment())
