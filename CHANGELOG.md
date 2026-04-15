# Changelog

All notable changes to this project will be documented in this file.

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
