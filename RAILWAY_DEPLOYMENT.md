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
3. Set Environment Variables:

```
STREAMLIT_SERVER_HEADLESS=true
STREAMLIT_SERVER_PORT=8501
STREAMLIT_SERVER_ADDRESS=0.0.0.0
```

4. Click Deploy

**Your app will be live in 2-3 minutes!** 🎉

---

## 📋 Environment Variables

Railway automatically sets:
- `DATABASE_URL` (from PostgreSQL plugin)

You set manually:
- `STREAMLIT_SERVER_HEADLESS=true`
- `STREAMLIT_SERVER_ADDRESS=0.0.0.0`

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
3. Each gets ¥10,000,000 starting capital
4. Each gets individual portfolio

---

## 🛠️ Troubleshooting

### "Database connection failed"
- Check: PostgreSQL plugin added to Railway
- Check: DATABASE_URL environment variable exists
- Wait: 30 seconds for initialization

### "Streamlit connection refused"
- Check: STREAMLIT_SERVER_ADDRESS=0.0.0.0
- Check: STREAMLIT_SERVER_HEADLESS=true
- Port: 8501 (default)

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
