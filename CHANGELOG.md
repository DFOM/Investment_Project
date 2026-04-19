# Changelog

All notable changes to this project will be documented in this file.

## [v1.2.0] - 2026-04-19
### Added
- **Market-Aware Background Worker**: Enhanced `background_worker.py` with timezone-aware market hours detection for US (NYSE/NASDAQ: 9:30 AM - 4:00 PM ET) and Japanese (TSE: 9:00 AM - 3:00 PM JST) markets. Update frequency now automatically adjusts to every 10 minutes during market hours and 60 minutes outside market hours.
- **Company Name Lookup**: New `get_company_name()` function in `core/market_data.py` that fetches and caches company names from yfinance for all tickers, with graceful fallbacks for API failures.
- **Dashboard View Modes**: Three new dashboard view modes in `pages/1_Dashboard.py`:
  - **Combined Portfolio**: Displays all team members' combined metrics and holdings
  - **Member Comparison**: Shows performance comparison table ranking all members by ROI
  - **Specific Member**: Detailed individual member metrics including total spent, earnings, and ROI
- **Per-Member Performance Tracking**: New `_get_member_metrics()` function calculates for each member:
  - Total capital spent on trades
  - Current portfolio value
  - Net earnings/losses
  - Individual ROI percentage
- **Enhanced Trading Desk**: 
  - Company name display next to ticker symbol with exchange listing
  - Improved pending orders display showing 🛒 (BUY) and 💰 (SELL) emojis
  - Trader name and company name visible in order details
  - Enhanced order labels with better formatting and readability
- **Member Names in Holdings**: Added "Trader Name" column to open positions table when viewing all team members

### Changed
- Dashboard KPI metrics now dynamically update based on selected view mode
- Trading Desk order book display significantly improved with company names and trader attribution
- Background worker logging now includes market hours status and update interval information
- Background worker wake-up interval reduced from 60 seconds to 30 seconds for faster response to market hour transitions

### Performance
- Company name results cached in memory to reduce API calls (1-hour implicit cache via yfinance)
- Background worker now intelligently updates frequency based on actual market hours, reducing unnecessary processing

## [v1.1.3] - 2026-04-15
### Added
- Added `background_worker.py` script to run autonomously, processing pending market-open orders every hour and taking end-of-day performance snapshots daily.
- Added `Trader_Name` column to the `Performance` worksheet to track both total portfolio value and individual member portfolio growth over time.

### Changed
- Dashboard ROI logic: Individual member ROI is now calculated as `Net Profit / Total Capital Deployed` rather than out of the global starting balance.
- Dashboard Metrics: The UI now explicitly displays "Individual Portfolio Value" (Starting cash + their absolute profit) when isolating analysis to a single student.

## [v1.1.2] - 2026-04-15
### Fixed
- Fixed the Dashboard to always display the true shared team cash balance, even when filtering analysis for a specific team member.
- Fixed the Selectbox resetting issue on the Trading Desk's Sell tab by applying a unique widget key.
- Updated Trading Desk to integrate the new `Auth_Code` requirement for executing and queueing trades.

## [v1.1.1] - 2026-04-15
### Fixed
- Removed hardcoded placeholder member names; team members must now be explicitly added via the Admin Panel and are fetched exclusively from the Google Sheets database.

## [v1.1.0] - 2026-04-15
### Added
- Pure Google Sheets-based Team Authentication system (`Team_Auth` worksheet).
- Core execution layer enforcement for `Auth_Code` on trade execution and order queueing.
- Core execution layer enforcement for `Auth_Code` on dividend collection.
- UI fields for Auth Code input in the Trading Desk and Dividends & Tax pages.

### Fixed
- Streamlit `selectbox` state resetting issue on the Trading Desk (Sell dropdown) by applying unique widget keys.

### Changed
- Removed local `team_config.json` logic entirely in favor of the Google Sheets auth system.

## [v1.0.0] - Initial Release
### Added
- Initial release of the Stock Investment Simulation project.
- Integration with Google Sheets (`Ledger`, `Performance`, `Order_Book`).
- Live market data and FX rates fetching via `yfinance`.
- Trade execution and pending orders management.
- Dividend and Japanese tax rule processing engine.
