from __future__ import annotations

from core.database import record_daily_performance


def run_daily_valuation() -> dict:
    """Run the PostgreSQL-backed daily portfolio valuation snapshot."""
    return record_daily_performance()


if __name__ == "__main__":
    print(run_daily_valuation())
