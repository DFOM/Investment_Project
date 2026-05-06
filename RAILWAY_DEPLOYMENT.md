# Railway.app Deployment Guide

**Stock Portfolio Simulator on Railway.app** — Free 3-month deployment for class projects

## 🚀 Quick Start (5 Minutes)

### Prerequisites
- GitHub account
- 3 team members with GitHub accounts
- Project pushed to public GitHub repo

### Step 1: Create Railway Account & Project

1. Go to [railway.app](https://railway.app)
2. Click "Login with GitHub"
3. Create new project
4. Select "Deploy from GitHub repo"
5. Connect GitHub account
6. Select your `Investment_Project` repository

### Step 2: Configure PostgreSQL Database

Railway automatically provisions PostgreSQL!

1. In Railway dashboard: Click "New" → "Database" → "PostgreSQL"
2. PostgreSQL automatically added
3. Railway injects `DATABASE_URL` environment variable ✅

### Step 3: Deploy Streamlit App

1. Click "New" → "GitHub Repo"
2. Select your forked repository
3. Railway will read `railway.json`, build with `pip install -r requirements.txt`, and run `bash start.sh`.
4. Click Deploy

`start.sh` initializes the PostgreSQL schema, starts the daily background worker, and then starts Streamlit on Railway's `$PORT`.

**Your app will be live in 2-3 minutes!** 🎉

## ⏰ Daily Price Updates

The Railway start command runs both processes in one service:

```bash
bash start.sh
```

`start.sh` starts `background_worker.py` before Streamlit. The worker:
- executes queued orders every 10 minutes during US/JP market hours and every 60 minutes off-hours;
- records one daily UTC portfolio snapshot using live yfinance prices and USD/JPY FX;
- writes the snapshot into the PostgreSQL `performance` table for dashboard history and reports.

If Railway restarts the service, the worker runs again and records at most one snapshot per UTC date because the performance table upserts on `(date, trader_name)`.

---

## 📋 Environment Variables

Railway automatically sets:
- `DATABASE_URL` (from PostgreSQL plugin)
- `PORT` (for the public web service)

Optional manual variables:
- `POSTGRES_URL` if you are not using Railway's `DATABASE_URL`
- `TZ=UTC` if you want logs and daily snapshot checks to stay explicitly UTC

No manual Streamlit port variables are required because `start.sh` passes Railway's `$PORT` to Streamlit.

---

## 📥 CSV Export for Professor

### From Dashboard
1. Go to **Admin Panel**
2. Click "📥 Export Trades to CSV"
3. Download and submit

### Format
The export matches your professor's template exactly:
- Entry/Exit dates
- Stock codes and names
- Commissions and P&L
- Status and rationale

---

## 👥 Team Collaboration

1. **Admin Panel** → "👤 Add Team Member"
2. Enter names: Alice, Bob, Charlie
3. Configure total class capital and per-member allocations in **Admin Panel → Class Simulation Settings / Starting Capital Allocation**
4. Click **Start New Simulation** to write each member's initial JPY funding row
5. Each member receives their own auth code and portfolio balance

---

## 🛠️ Troubleshooting

### "Database connection failed"
- Check: PostgreSQL plugin added to Railway
- Check: DATABASE_URL environment variable exists
- Wait: 30 seconds for initialization

### "Streamlit connection refused"
- Check: `railway.json` start command is `bash start.sh`
- Check: Railway provides the `PORT` variable
- Check deploy logs for the schema initialization message before Streamlit starts

### "No trades showing"
- Execute a test trade first
- Wait 10 minutes for market data

---

## 💾 Data Backup

Use CSV export weekly:

```python
from core.csv_export import export_all_traders_csv
csv = export_all_traders_csv()
with open("backup.csv", "w") as f:
    f.write(csv)
```

---

## 📞 Support

- Railway: [railway.app/docs](https://railway.app/docs)
- PostgreSQL: [postgresql.org/docs](https://postgresql.org/docs)
- Streamlit: [docs.streamlit.io](https://docs.streamlit.io)

---

**Your 3-month free deployment starts now!** ✨
