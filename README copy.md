# iCLA 100M JPY Stock Engine

A production-grade stock portfolio simulation platform backed by Google Sheets, built for the iCLA Spring 2026 Investment course. Operators start with ¥100,000,000 in simulated capital and execute real-time trades against live market data with zero imposed constraints.

---

## Philosophy

This engine operates on a single principle: **operator autonomy**.

The system imposes no forced risk management, no minimum cash reserve, and no position-size ceiling. If the operator's strategy demands going 100% into a single equity, the engine executes without interference. If a conviction trade wipes the balance to zero, the ledger records it accurately and moves on.

The platform is a neutral execution layer. It does not protect the operator from themselves — nor should it. Risk discipline is the operator's responsibility, not the software's.

> **"The system is a mirror of the operator's conviction."**

---

## Features

### Live Data Sync
- Real-time price fetching via `yfinance` for NYSE, NASDAQ, and TSE (Tokyo Stock Exchange) tickers
- Live USD/JPY FX rate with bid/ask spread simulation for USD positions
- Sector and company metadata (`longName`, `sector`) fetched dynamically per ticker — no hardcoded lookup tables

### Order Book
- **Instant execution**: trades settle immediately against live prices with configurable slippage
- **Pending orders**: queued to the `Order_Book` worksheet and held until the operator confirms execution
- Three position-sizing modes: fixed share count, fixed JPY notional, or percentage of portfolio
- Per-trade rationale field for grading and audit trail

### Deep Analytics
- Sector allocation donut chart (hole=0.4) with Cash as a first-class slice
- Company sunburst chart: sector → company drill-down at a glance
- Performance comparison table: `[Company Name, Sector, % of Portfolio, Total Gain/Loss (JPY)]`
- Automated concentration-risk analysis narrating the top sector exposure
- Portfolio Deep Dive page: treemap heatmap, per-company profiles, volatility table, best/worst performers

### Cloud Infrastructure
- All trade records, order book entries, and performance snapshots persist in **Google Sheets** via the Sheets API — no local database required
- Schema auto-provisioned on first connect: `Ledger`, `Performance`, `Order_Book`, and `Active Session` worksheets
- `@st.cache_data(ttl=60)` on all reads; cache busted immediately after every write — quota-safe on free-tier Sheets API

---

## Architecture

```
iCLA Stock Engine
├── app.py                    Entry point — session state init, connectivity guard
├── pages/
│   ├── 1_Dashboard.py        Live P&L, allocation charts, trade history
│   ├── 2_Trading_Desk.py     Order entry, pending order management
│   ├── 3_Admin_Panel.py      Sheet init, team roster, session management
│   └── 4_Portfolio_Deep_Dive.py  Heatmaps, company profiles, risk metrics
└── core/
    ├── database.py           Google Sheets I/O, schema management, caching layer
    ├── trade_executor.py     Trade validation, slippage model, ledger writes
    ├── market_data.py        yfinance price/FX fetch wrappers
    ├── setup_env.py          Environment bootstrap, constants (STARTING_JPY_BALANCE)
    └── user_manager.py       Team member config, alias resolution
```

**Frontend**: Streamlit — each `pages/` file is a self-contained page with its own data loading and rendering logic.

**Backend (`core/`)**: Stateless modules. `database.py` owns all Google Sheets I/O through a singleton `GoogleSheetsDatabase` instance (LRU-cached). `trade_executor.py` validates trades, applies slippage, computes FX impact, and delegates writes to `database.py`. Neither module holds application state between renders.

**Auth**: Google service account credentials loaded from `st.secrets` (Streamlit Cloud) or the `GOOGLE_CREDENTIALS` environment variable (self-hosted / Render). No OAuth flow.

---

## Installation

### 1. Clone and install dependencies

```bash
git clone <repo-url>
cd "Stock Project"
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure Google Sheets credentials

Create `.streamlit/secrets.toml` in the project root:

```toml
# .streamlit/secrets.toml

GOOGLE_SHEET_ID = "your_google_spreadsheet_id_here"

[GOOGLE_CREDENTIALS]
type                        = "service_account"
project_id                  = "your-project-id"
private_key_id              = "key-id"
private_key                 = "-----BEGIN RSA PRIVATE KEY-----\n...\n-----END RSA PRIVATE KEY-----\n"
client_email                = "your-service-account@your-project.iam.gserviceaccount.com"
client_id                   = "000000000000000000000"
auth_uri                    = "https://accounts.google.com/o/oauth2/auth"
token_uri                   = "https://oauth2.googleapis.com/token"
auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
client_x509_cert_url        = "https://www.googleapis.com/robot/v1/metadata/x509/your-service-account%40your-project.iam.gserviceaccount.com"
```

> Share the target Google Sheet with the service account email (`editor` permission). The app provisions all worksheets automatically on first connection.

### 3. Run

```bash
streamlit run app.py
```

Open `http://localhost:8501`. Navigate to **3_Admin_Panel** first to initialize the sheet schema, then proceed to the Trading Desk.

### 4. Deploy to Render / Streamlit Cloud

Set the following environment variables (or Streamlit Secrets) on the host:

| Variable | Value |
|---|---|
| `GOOGLE_SHEET_ID` | The spreadsheet ID from the Google Sheets URL |
| `GOOGLE_CREDENTIALS` | The full service account JSON as a single-line string |

Start command: `streamlit run app.py --server.port $PORT --server.headless true`

---

## Ledger Schema

Every executed trade writes one row to the `Ledger` worksheet:

| Column | Type | Description |
|---|---|---|
| `Timestamp` | `YYYY-MM-DD HH:mm` UTC | Execution time |
| `Ticker` | `str` | Normalized symbol (e.g. `AAPL`, `7203.T`) |
| `Action` | `BUY` / `SELL` | Direction |
| `Quantity` | `float` | Shares executed (6 d.p.) |
| `Local_Asset_Price` | `float` | Slippage-adjusted price in local currency |
| `Executed_FX_Rate` | `float` | USD/JPY rate applied (1.0 for TSE) |
| `Total_JPY_Impact` | `float` | Net cash change in JPY (negative for buys) |
| `Remaining_JPY_Balance` | `float` | Running cash balance after this trade |
| `Trader_Name` | `str` | Team member who authorized the trade |
| `Commission_Paid` | `float` | Flat ¥500 per trade |
| `FX_Conversion_Fee` | `float` | FX spread cost in JPY (0 for TSE) |
| `Trade_Rationale` | `str` | Operator-supplied justification |

---

## Hooks

`hooks/daily_valuation.py` can be scheduled (cron, GitHub Actions, Render cron job) to snapshot the daily portfolio value into the `Performance` worksheet:

```bash
python hooks/daily_valuation.py
```

---

## Disclaimer

> **This is a simulation tool. Financial outcomes, whether high-alpha gains or total capital depletion, are the result of user-defined strategy. The system is a mirror of the operator's conviction.**

All prices, FX rates, and portfolio values are simulated. No real capital is at risk. This project is an academic exercise for the iCLA Spring 2026 Investment course and is not investment advice.
