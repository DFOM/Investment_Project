# Investment_Project

**Version: v1.3.0** — Class project for investment course by Liu Ming

## Overview

Stock Portfolio Simulator — An educational Streamlit application for team-based stock trading practice with real-time market data, portfolio tracking, and comprehensive performance analytics.

## 🚀 Key Features (v1.3.0)

### Portfolio Management
- **Combined Portfolio View**: See all team members' investments aggregated with individual member identification
- **Member Comparison Dashboard**: Rank team members by ROI with per-member spending and earnings metrics
- **Detailed Member Analysis**: Deep dive into individual member performance including capital deployed, earnings, and ROI%

### Market Automation
- **Smart Background Worker**: Automatically updates every 10 minutes during market hours, 60 minutes off-hours
- **Market Hours Detection**: Aware of US (NYSE/NASDAQ 9:30AM-4:00PM ET) and Japanese (TSE 9:00AM-3:00PM JST) market hours
- **Auto-Execute Pending Orders**: Queued orders automatically execute when their market opens

### Trading Interface
- **Company Name Lookup**: See full company names alongside ticker symbols
- **Enhanced Order Book**: View pending orders with company names, trader attribution, and buy/sell indicators
- **Live Price & FX Integration**: Real-time USD/JPY rates with 0.25% spread simulation

### Performance Tracking
- **Per-Member Metrics**: Track each member's spending, earnings, losses, and ROI
- **Sector Allocation Analysis**: Pie charts and sunburst diagrams by sector
- **Historical Performance**: Portfolio value tracking over time with daily snapshots
- **Trade History**: Complete audit trail with company names and trader attribution

## 📊 Dashboard View Modes

1. **Combined Portfolio** - All members' holdings and metrics together
2. **Member Comparison** - Table ranking all members by performance
3. **Specific Member** - Individual member detailed analytics

## 📝 Trading Features

### Order Types
- **Market Orders**: Execute immediately when market is open
- **Market-Open Orders**: Queue for execution at next market open
- **Position Sizing**: By shares, fixed JPY amount, or % of portfolio

### Portfolio Mechanics
- **Starting Balance**: ¥10,000,000 per member
- **Commission**: Rakuten Securities rates
  - **TSE (Japanese stocks)**: 0.099% per trade (min ¥99, max ¥487.50)
  - **US stocks**: $1 per trade (~¥150 at current rates)
- **FX Conversion**: 0.25% broker spread on USD/JPY conversions
- **Tax Simulation**: 
  - US Stocks: ~28.28% (10% US withholding + 20.315% Japan tax)
  - Japanese Stocks: 20.315% withholding

## 🛠️ Tech Stack

- **Streamlit**: Multi-page web UI
- **Google Sheets**: Real-time collaborative database
- **yfinance**: Live market data and fundamentals
- **Pandas**: Data manipulation
- **Plotly**: Interactive visualizations
- **Decimal**: Precise monetary calculations

## 📁 Project Structure

```
├── app.py                           # Main entry point
├── background_worker.py             # Autonomous order processing & snapshots
├── core/                            # Business logic
│   ├── database.py                  # Google Sheets API
│   ├── market_data.py               # yfinance integration + company names
│   ├── trade_executor.py            # Trade execution & validation
│   ├── dividend_engine.py           # Tax-aware dividend processing
│   ├── daily_valuation.py           # Portfolio snapshots
│   ├── research_engine.py           # Fundamental data
│   └── user_manager.py              # Team authentication
└── pages/                           # Streamlit pages
    ├── 1_Dashboard.py               # Portfolio analytics (Combined, Member Comparison, Detailed)
    ├── 2_Trading_Desk.py            # Trade execution + pending orders
    ├── 3_Admin_Panel.py             # Team management
    ├── 4_Portfolio_Deep_Dive.py     # Detailed position analysis
    ├── 4_Stock_Research.py          # Company fundamentals
    ├── 5_Dividends_Tax.py           # Dividend & tax center
    └── 6_Transaction_History.py     # Complete audit trail with filters & summaries
```

## 🔄 Update Schedule

Background worker automatically adjusts its update frequency based on market hours:
- **During Market Hours** (US or JP): Every 10 minutes
- **Outside Market Hours**: Every 60 minutes
- **Daily**: Portfolio performance snapshots (once per UTC day)

## 📊 Recent Updates (v1.3.0)

✅ **Rakuten Securities Commission Integration**: Dynamic commission calculation
   - TSE: 0.099% (min ¥99, max ¥487.50)
   - US Stocks: $1 per trade
✅ **Fixed Portfolio Value Chart**: Daily snapshots only, no intraday trades
✅ **Member-Scoped Charts**: Graphs start from each member's first trade date
✅ **New Transaction History Page**: Complete audit trail with:
   - Timestamp filtering by date range
   - Trader and action (buy/sell) filtering
   - Ticker filtering
   - Transaction summary by trader and ticker
   - Sortable transaction table with all details

## 📊 Previous Updates (v1.2.0)

✅ Market-aware background worker with timezone detection
✅ Company name fetching and caching from yfinance
✅ Three dashboard view modes (Combined, Comparison, Detailed)
✅ Per-member performance metrics and analytics
✅ Enhanced trading desk with company names
✅ Improved pending orders display with trader attribution

See [CHANGELOG.md](CHANGELOG.md) for complete version history.
