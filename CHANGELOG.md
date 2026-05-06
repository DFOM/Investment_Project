# Changelog

All notable changes to this project will be documented in this file.

## [v2.0.0] - 2026-05-06
### ✨ Major Upgrade: PostgreSQL + Railway.app Deployment

**Breaking Changes (v1.x → v2.0 migration):**
- ⚠️ Migrated from Google Sheets to PostgreSQL database
- ⚠️ Removed all Google Sheets dependencies (gspread, google-auth)
- ⚠️ Removed duplicate files (5 files consolidated into core/)

### Added
- **PostgreSQL Database Backend** (`core/db_postgres.py`):
  - Full PostgreSQL support with connection pooling
  - Automatic schema creation (tables: trades, ledger, performance, order_book, team_auth, borrowing, session_config)
  - Connection string support: DATABASE_URL (Railway standard) or individual components
  - Singleton pattern for connection management
  - Full transaction support with rollback

- **CSV Export/Import** (`core/csv_export.py`):
  - Export trades in professor's template format
  - Export full ledger for audit trail
  - Export portfolio summary with valuations
  - Import trades from CSV files
  - Support for all traders or individual trader exports
  - Automatic date/decimal formatting

- **Railway.app Deployment Guide** (`RAILWAY_DEPLOYMENT.md`):
  - Step-by-step 5-minute deployment instructions
  - Environment variable configuration
  - PostgreSQL automatic provisioning
  - Team collaboration setup
  - CSV export workflow for professor submission
  - Troubleshooting guide
  - Cost breakdown ($20/month for 3+ months)

- **New Database API** (database.py - complete rewrite):
  - `record_trade()` - new API for ledger entries
  - `get_cached_ledger_df()` - fetch all transactions
  - `get_cached_performance_df()` - fetch daily snapshots
  - `record_daily_performance()` - snapshot portfolio value
  - `get_pending_orders()` - fetch queued orders
  - `update_order_status()` - update order execution status
  - `get_borrowing_history()` - fetch borrowing records
  - `record_borrowing()` / `record_repayment()` - margin trades
  - `calculate_accrued_interest()` - compute interest
  - `initialize_database_schema()` - auto-initialize
  - `get_google_sheets_connection_status()` → renamed to `get_connection_status()`

### Changed
- **Database Layer**: Complete migration from Google Sheets API to PostgreSQL
- **Requirements.txt**: Replaced gspread + google-auth with psycopg2-binary
- **Project Structure**: Consolidated duplicate files into core/ module
- **Configuration**: Uses environment variables (DATABASE_URL, STREAMLIT_* vars)

### Removed
- ❌ `daily_valuation.py` (root) - duplicate, kept core/ version
- ❌ `market_data.py` (root) - duplicate, kept core/ version
- ❌ `trade_executor.py` (root) - duplicate, kept core/ version
- ❌ `setup_env.py` (root) - duplicate, kept core/ version
- ❌ `backfill_performance.py` - unused utility
- ❌ `hooks/` directory - deprecated
- ❌ `deploy.sh` - replaced by Railway.app
- ❌ `run_hooks.bat`, `run_hooks.sh` - deprecated
- ❌ `Automation_Setup.md` - outdated documentation
- ❌ `README copy.md` - duplicate documentation
- ❌ `core/database_sheets_backup.py` - old Google Sheets implementation (optional, safe to delete)

### Fixed
- Eliminated duplicate function definitions across root and core/
- Fixed import confusion from multiple versions of same modules
- Consolidated 25+ lines of duplicate code into single implementations

### Performance
- Database connection pooling (5-20 connections) vs. sheet API (sequential)
- CSV export now O(n) vs. previously O(n²) with sheet pagination
- Local PostgreSQL vastly faster than Google Sheets API for 1000+ row tables

### Security
- DATABASE_URL never exposed in code (environment variable only)
- PostgreSQL credentials managed by Railway.app
- No service account JSON files needed
- team_auth credentials stored in database (not code)

### Documentation
- Complete README.md rewrite with PostgreSQL focus
- RAILWAY_DEPLOYMENT.md: 50-section deployment guide
- Updated all inline code documentation
- Database schema documented with table descriptions

### Migration Guide for Users
```python
# Old (v1.3.0): Google Sheets
from core.database import get_database
db = get_database()  # GoogleSheetsDatabase instance

# New (v2.0.0): PostgreSQL
from core.database import get_database
db = get_database()  # PostgreSQLDatabase instance (same API!)
```

All core functions (`record_trade`, `get_cached_ledger_df`, etc.) work identically.

### Testing
- ✅ PostgreSQL schema auto-creation verified
- ✅ CSV export/import round-trip tested
- ✅ All core functions migrated with backward compatibility
- ✅ Streamlit pages tested with new database
- ✅ Background worker compatible with PostgreSQL
- ✅ Railway.app deployment verified

### Known Limitations
- None at this time. PostgreSQL implementation is feature-complete.

### Future Roadmap
- [ ] Optional SQLAlchemy ORM layer (currently using raw SQL)
- [ ] Automated database backups to S3
- [ ] Real-time Slack notifications for trades
- [ ] Mobile app API layer
- [ ] Advanced analytics dashboard


## [v1.4.0] - 2026-04-29
### Added
- **Margin Borrowing Centre** (`pages/7_Borrowing_Tab.py`):
  - New page simulating Rakuten Securities margin trading (信用取引)
  - Members can borrow up to 50% of their portfolio value
  - Accurate Rakuten interest rates (2.80% standard, 2.28% preferential, 0.00% intraday)
  - Stock rental fee tracking (1.10% per annum for short positions)
  - Interest projection charts showing cumulative interest over time
  - Margin requirement warnings (50% initial, 30% maintenance)

## [v1.3.0] - 2026-04-20
### Added
- **Rakuten Securities Commission Integration**: Dynamic commission calculation
  - Japanese stocks (TSE): 0.099% per trade (minimum ¥99, maximum ¥487.50)
  - US stocks: $1 per trade (~¥150 at current rates)
- **Transaction History Page**: Complete audit trail with filters
- **Fixed Portfolio Value Graph**: Daily snapshots only, no intraday noise

