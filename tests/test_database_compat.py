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

    def test_postgres_get_ledger_df_returns_renamed_frame(self) -> None:
        body = Path("core/db_postgres.py").read_text(encoding="utf-8")

        get_ledger_section = body.split("def get_ledger_df", maxsplit=1)[1].split("def get_recent_ledger_df", maxsplit=1)[0]
        self.assertIn("return renamed", get_ledger_section)

    def test_order_book_schema_preserves_fractional_quantities(self) -> None:
        body = Path("core/db_postgres.py").read_text(encoding="utf-8")

        self.assertIn("value DECIMAL(18, 8)", body)
        self.assertIn("ALTER COLUMN value TYPE DECIMAL(18, 8)", body)

    def test_trading_desk_pending_filter_is_status_case_insensitive(self) -> None:
        body = Path("pages/2_Trading_Desk.py").read_text(encoding="utf-8")

        self.assertIn("str.strip().str.upper()", body)
        self.assertIn('normalized_status.eq("PENDING")', body)

    def test_trading_desk_pending_orders_are_not_hidden_by_member_selector(self) -> None:
        body = Path("pages/2_Trading_Desk.py").read_text(encoding="utf-8")

        self.assertIn("Showing all queued orders for the team", body)
        self.assertIn('pending = ob_df[normalized_status.eq("PENDING")].reset_index(drop=True)', body)
        self.assertNotIn("trader_match", body)

    def test_trading_desk_formats_timestamp_objects_without_slicing(self) -> None:
        body = Path("pages/2_Trading_Desk.py").read_text(encoding="utf-8")

        self.assertIn("def _format_order_timestamp", body)
        self.assertIn('pd.to_datetime(value, errors="coerce", utc=True)', body)
        self.assertIn("queued_at = _format_order_timestamp", body)
        self.assertNotIn("row.get('Timestamp', '')[:16]", body)

    def test_dashboard_counts_pending_buys_in_portfolio_preview(self) -> None:
        body = Path("pages/1_Dashboard.py").read_text(encoding="utf-8")

        self.assertIn("def _pending_buy_value_jpy", body)
        self.assertIn("total = cash + equity_jpy", body)
        self.assertIn("not added to Total Portfolio Value", body)

        database_body = Path("core/database.py").read_text(encoding="utf-8")
        self.assertIn("pending_buy_value_jpy", database_body)
        self.assertIn("total_jpy = cash + equity_jpy", database_body)
        self.assertNotIn("total_jpy = cash + equity_jpy + pending_buy_value", database_body)

    def test_portfolio_deep_dive_coerces_postgres_decimal_numbers(self) -> None:
        body = Path("pages/4_Portfolio_Deep_Dive.py").read_text(encoding="utf-8")

        self.assertIn('for column in ["Quantity", "Local_Asset_Price", "Total_JPY_Impact", "Remaining_JPY_Balance"]', body)
        self.assertIn('df[column] = pd.to_numeric(df[column], errors="coerce")', body)
        self.assertIn('resolve_member_initial_allocations(settings["total_starting_capital_jpy"])', body)

    def test_dashboard_member_comparison_uses_each_members_own_ledger(self) -> None:
        body = Path("pages/1_Dashboard.py").read_text(encoding="utf-8")

        self.assertIn('comparison_mode = view_mode == "Member Comparison"', body)
        self.assertIn('is_all = selected == "All Team" or comparison_mode', body)
        self.assertIn('member_ledger = ledger.loc[ledger["Trader_Name"].isin(member_aliases)].copy()', body)
        self.assertIn('metrics = _get_member_metrics(member, member_ledger, member_holdings, usd_jpy)', body)

    def test_dashboard_cash_includes_untraded_member_allocations(self) -> None:
        body = Path("pages/1_Dashboard.py").read_text(encoding="utf-8")

        self.assertIn("def _latest_member_cash_balances", body)
        self.assertIn('balances: dict[str, float] = {member: float(allocation) for member, allocation in allocations.items()}', body)
        self.assertIn('return float(sum(balances.values())) if balances else float(settings["total_starting_capital_jpy"])', body)

    def test_dashboard_history_supports_daily_weekly_monthly_current_point(self) -> None:
        body = Path("pages/1_Dashboard.py").read_text(encoding="utf-8")

        self.assertIn("def _build_portfolio_history_view", body)
        self.assertIn('rule_map = {"Daily": "D", "Weekly": "W", "Monthly": "M"}', body)
        self.assertIn('history_timeframe = st.radio', body)
        self.assertIn('["Daily", "Weekly", "Monthly"]', body)
        self.assertIn("current_total=current_history_total", body)
        self.assertIn("The latest point uses the current live portfolio value", body)

    def test_background_worker_records_snapshot_on_startup(self) -> None:
        body = Path("background_worker.py").read_text(encoding="utf-8")

        self.assertIn("run_daily()", body)
        self.assertIn("last_daily_run_date = datetime.now(timezone.utc).date()", body)
        self.assertIn("upserts by (date, trader)", body)

    def test_trade_executor_current_holdings_handles_decimal_quantities(self) -> None:
        from decimal import Decimal

        original_ledger_loader = trade_executor.get_cached_ledger_df
        try:
            trade_executor.get_cached_ledger_df = lambda: pd.DataFrame(
                {
                    "Trader_Name": ["Alice", "Alice", "Alice"],
                    "Ticker": ["AAPL", "AAPL", "GC=F"],
                    "Action": ["BUY", "SELL", "BUY"],
                    "Quantity": [Decimal("2.5"), Decimal("1.0"), Decimal("0.25")],
                }
            )

            holdings = trade_executor._current_holdings("Alice")  # noqa: SLF001

            self.assertEqual(float(holdings["AAPL"]), 1.5)
            self.assertEqual(float(holdings["GC=F"]), 0.25)
        finally:
            trade_executor.get_cached_ledger_df = original_ledger_loader

    def test_metal_aliases_normalize_for_execution(self) -> None:
        self.assertEqual(trade_executor._normalize_ticker("gold"), "GC=F")  # noqa: SLF001
        self.assertEqual(trade_executor._normalize_ticker("silver"), "SI=F")  # noqa: SLF001

    def test_trade_executor_sell_holdings_avoid_decimal_subtraction(self) -> None:
        body = Path("core/trade_executor.py").read_text(encoding="utf-8")

        holdings_section = body.split("def _current_holdings", maxsplit=1)[1].split("def is_market_open", maxsplit=1)[0]
        self.assertIn('quantity = _d(row.get("Quantity", 0) or 0)', holdings_section)
        self.assertIn('totals[ticker] = totals.get(ticker, Decimal("0")) + quantity', holdings_section)
        self.assertIn('totals[ticker] = totals.get(ticker, Decimal("0")) - quantity', holdings_section)
        self.assertNotIn(".sub(", holdings_section)

    def test_pending_orders_update_status_by_id_when_available(self) -> None:
        body = Path("core/trade_executor.py").read_text(encoding="utf-8")

        process_section = body.split("def process_pending_orders", maxsplit=1)[1].split("def format_currency", maxsplit=1)[0]
        self.assertIn('order_id = order.get("ID")', process_section)
        self.assertIn('order_key = int(order_id) if pd.notna(order_id) else timestamp', process_section)
        self.assertIn('db.update_order_status(order_key, "EXECUTED")', process_section)
        self.assertIn('db.update_order_status(order_key, "FAILED")', process_section)

    def test_trading_desk_supports_metals_and_decimal_safe_holdings(self) -> None:
        body = Path("pages/2_Trading_Desk.py").read_text(encoding="utf-8")

        self.assertIn("METAL_TICKER_OPTIONS", body)
        self.assertIn('"Gold futures (GC=F)": "GC=F"', body)
        self.assertIn('"Silver futures (SI=F)": "SI=F"', body)
        self.assertIn('placeholder="AAPL, 7203.T, GC=F, SI=F, GOLD, or SILVER"', body)
        holdings_section = body.split("def _get_current_holdings", maxsplit=1)[1].split("def _enrich_holdings", maxsplit=1)[0]
        self.assertIn('totals[ticker] = totals.get(ticker, 0.0) + quantity', holdings_section)
        self.assertIn('totals[ticker] = totals.get(ticker, 0.0) - quantity', holdings_section)
        self.assertNotIn(".sub(", holdings_section)

    def test_market_and_executor_normalize_metal_aliases(self) -> None:
        market_body = Path("core/market_data.py").read_text(encoding="utf-8")
        executor_body = Path("core/trade_executor.py").read_text(encoding="utf-8")

        self.assertIn('"GOLD": "GC=F"', market_body)
        self.assertIn('"SILVER": "SI=F"', market_body)
        self.assertIn('return _METAL_TICKER_ALIASES.get(symbol, symbol)', market_body)
        self.assertIn('"GOLD": "GC=F"', executor_body)
        self.assertIn('"SILVER": "SI=F"', executor_body)
        self.assertIn('return METAL_TICKER_ALIASES.get(symbol, symbol)', executor_body)

    def test_borrowing_tab_coerces_postgres_decimal_numbers(self) -> None:
        body = Path("pages/7_Borrowing_Tab.py").read_text(encoding="utf-8")

        self.assertIn("def _load_member_ledger", body)
        self.assertIn('for column in ["Quantity", "Local_Asset_Price", "Total_JPY_Impact", "Remaining_JPY_Balance"]', body)
        self.assertIn('frame[column] = pd.to_numeric(frame[column], errors="coerce").astype(float)', body)
        self.assertIn('frame["Quantity"] = pd.to_numeric(frame["Quantity"], errors="coerce").fillna(0.0).astype(float)', body)
        self.assertIn('holdings[ticker] = holdings.get(ticker, 0.0) + quantity', body)
        self.assertIn('holdings[ticker] = holdings.get(ticker, 0.0) - quantity', body)
        self.assertIn("price_value = float(price)", body)
        self.assertNotIn('@st.cache_data(ttl=300)\ndef _get_member_portfolio_value', body)
        self.assertNotIn('@st.cache_data(ttl=300)\ndef _get_current_holdings', body)
        self.assertNotIn('ledger["Trader_Name"] == trader_name', body)
        self.assertNotIn('buys.sub(sells', body)

    def test_railway_build_uses_nixpacks_default_install_once(self) -> None:
        body = Path("railway.json").read_text(encoding="utf-8")

        self.assertIn('"builder": "NIXPACKS"', body)
        self.assertNotIn("buildCommand", body)

    def test_requirements_avoid_unused_heavy_optional_packages(self) -> None:
        body = Path("requirements.txt").read_text(encoding="utf-8")

        self.assertNotIn("SQLAlchemy", body)
        self.assertNotIn("openpyxl", body)


if __name__ == "__main__":
    unittest.main()
