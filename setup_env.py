from core.setup_env import (  # noqa: F401
    HISTORICAL_PATH,
    LEDGER_PATH,
    initialize_historical,
    initialize_ledger,
    setup_environment,
)


if __name__ == "__main__":
    print(setup_environment())
