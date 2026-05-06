from __future__ import annotations

import re
import unittest
from pathlib import Path

import pandas as pd

from core import database
from core import trade_executor
from core.daily_valuation import run_daily_valuation
from core.db_postgres import PostgreSQLDatabase


class DatabaseCompatibilityTests(unittest.TestCase):


    def test_streamlit_config_has_no_invalid_client_logger(self) -> None:
        body = Path(".streamlit/config.toml").read_text(encoding="utf-8")

        self.assertNotIn("logger.level", body.split("[logger]", maxsplit=1)[0])
        self.assertIn("[browser]", body)
        self.assertIn("gatherUsageStats = false", body)

    def test_conflict_markers_are_absent_from_pr_hotspots(self) -> None:
        hotspots = [
            "core/database.py",
            "core/db_postgres.py",
            "pages/3_Admin_Panel.py",
            "railway.json",
            "tests/test_database_compat.py",
        ]
        conflict_marker = re.compile(r"^(<<<<<<<|=======|>>>>>>>)", re.MULTILINE)

        offenders = []
        for hotspot in hotspots:
            body = Path(hotspot).read_text(encoding="utf-8")
            if conflict_marker.search(body):
                offenders.append(hotspot)

        self.assertEqual(offenders, [])

    def test_postgres_adapter_exposes_legacy_methods(self) -> None:
        required_methods = [
            "append_ledger_row",
            "append_order_book_row",
            "cancel_order",
            "get_ledger_df",
            "get_order_book_df",
            "get_recent_ledger_df",
            "update_auth_code",
            "update_order_status",
            "initialize_and_format_worksheets",
            "upsert_team_auth",
            "rename_team_auth",
        ]

        missing = [name for name in required_methods if not hasattr(PostgreSQLDatabase, name)]

        self.assertEqual(missing, [])

    def test_rows_to_frame_renames_postgres_columns_for_legacy_ui(self) -> None:
        frame = database._rows_to_frame(  # noqa: SLF001 - compatibility helper coverage
            [
                {
                    "timestamp": "2026-05-06 12:00",
                    "ticker": "AAPL",
                    "action": "BUY",
                    "quantity": "1.5",
                    "remaining_jpy_balance": "999.00",
                    "trader_name": "Student",
                }
            ],
            database._LEDGER_RENAME_MAP,  # noqa: SLF001 - compatibility helper coverage
        )

        self.assertIn("Timestamp", frame.columns)
        self.assertIn("Ticker", frame.columns)
        self.assertIn("Action", frame.columns)
        self.assertIn("Quantity", frame.columns)
        self.assertIn("Remaining_JPY_Balance", frame.columns)
        self.assertIn("Trader_Name", frame.columns)
        self.assertEqual(frame.loc[0, "Ticker"], "AAPL")

    def test_holding_calculation_nets_buys_and_sells(self) -> None:
        ledger = pd.DataFrame(
            {
                "Ticker": ["AAPL", "AAPL", "7203.T"],
                "Action": ["BUY", "SELL", "BUY"],
                "Quantity": ["3", "1.25", "2"],
            }
        )

        holdings = database._calculate_current_holdings(ledger)  # noqa: SLF001

        self.assertEqual(holdings, {"7203.T": 2.0, "AAPL": 1.75})



    def test_member_cash_balance_uses_allocation_when_ledger_empty(self) -> None:
        original_ledger_loader = trade_executor.get_cached_ledger_df
        original_resolver = database.resolve_member_initial_allocations
        try:
            trade_executor.get_cached_ledger_df = lambda: pd.DataFrame()
            database.resolve_member_initial_allocations = lambda total_capital_jpy=None: {
                "Alice": 25_000_000.0,
                "Bob": 75_000_000.0,
            }

            self.assertEqual(trade_executor.get_cash_balance("Alice"), 25_000_000.0)
            self.assertEqual(trade_executor.get_cash_balance("Bob"), 75_000_000.0)
            self.assertEqual(trade_executor.get_cash_balance("Unknown"), 0.0)
        finally:
            trade_executor.get_cached_ledger_df = original_ledger_loader
            database.resolve_member_initial_allocations = original_resolver

    def test_member_cash_balance_uses_allocation_when_member_has_no_ledger_rows(self) -> None:
        original_ledger_loader = trade_executor.get_cached_ledger_df
        original_resolver = database.resolve_member_initial_allocations
        try:
            trade_executor.get_cached_ledger_df = lambda: pd.DataFrame(
                {
                    "Timestamp": ["2026-05-06 00:00"],
                    "Ticker": ["JPY_CASH"],
                    "Action": ["INITIAL_FUNDING"],
                    "Remaining_JPY_Balance": [100_000_000.0],
                    "Trader_Name": ["System"],
                }
            )
            database.resolve_member_initial_allocations = lambda total_capital_jpy=None: {
                "Alice": 40_000_000.0,
            }

            self.assertEqual(trade_executor.get_cash_balance("Alice"), 40_000_000.0)
        finally:
            trade_executor.get_cached_ledger_df = original_ledger_loader
            database.resolve_member_initial_allocations = original_resolver

    def test_latest_cash_balance_sums_member_balances(self) -> None:
        ledger = pd.DataFrame(
            {
                "Timestamp": pd.to_datetime([
                    "2026-05-06T00:00:00Z",
                    "2026-05-06T00:01:00Z",
                    "2026-05-06T00:02:00Z",
                ]),
                "Trader_Name": ["Alice", "Bob", "Alice"],
                "Remaining_JPY_Balance": [50_000_000, 30_000_000, 45_000_000],
            }
        )

        self.assertEqual(database._latest_cash_balance(ledger), 75_000_000)  # noqa: SLF001

    def test_equal_split_and_auth_helpers_exist(self) -> None:
        self.assertTrue(callable(database.split_member_allocations_equally))
        self.assertTrue(callable(database.update_member_auth_code))

    def test_default_simulation_settings_include_class_controls(self) -> None:
        self.assertEqual(database._DEFAULT_SIMULATION_SETTINGS["total_starting_capital_jpy"], 100_000_000.0)  # noqa: SLF001
        self.assertIn("borrowing_limit_pct", database._DEFAULT_SIMULATION_SETTINGS)  # noqa: SLF001
        self.assertIn("local_borrow_rate_pct", database._DEFAULT_SIMULATION_SETTINGS)  # noqa: SLF001

    def test_get_database_preserves_cache_clear_compatibility(self) -> None:
        self.assertTrue(callable(getattr(database.get_database, "cache_clear", None)))

    def test_daily_valuation_entrypoint_is_available(self) -> None:
        self.assertTrue(callable(run_daily_valuation))


if __name__ == "__main__":
    unittest.main()
