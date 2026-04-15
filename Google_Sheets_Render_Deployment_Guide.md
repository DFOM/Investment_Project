# Google Sheets + Render Deployment Guide

## 1) Create Google Cloud Service Account JSON Key
1. Open Google Cloud Console.
2. Create or select a project.
3. Enable these APIs for the project:
   - Google Sheets API
   - Google Drive API
4. Go to IAM & Admin -> Service Accounts.
5. Click Create Service Account.
6. Give it a name (for example: render-sheets-bot).
7. Create key:
   - Key type: JSON
   - Download the JSON file.

Important:
- Do not commit the JSON file to GitHub.
- Treat this file as a secret.

## 2) Share Google Sheet With Service Account
1. Create your Google Spreadsheet.
2. Add two worksheets exactly named:
   - Ledger
   - Performance
3. Open the service account JSON file and copy `client_email`.
4. Click Share on the spreadsheet.
5. Share with that `client_email` as Editor.

## 3) Render Build and Start Commands
Use these exact values in Render service settings:

Build Command:
```bash
pip install -r requirements.txt
```

Start Command:
```bash
streamlit run app.py --server.port $PORT --server.address 0.0.0.0
```

## 4) Render Environment Variables
Set the following environment variables in Render:

1. `GOOGLE_CREDENTIALS`
   - Paste the entire JSON content from your service-account key file.
   - Keep it as one valid JSON string block.

2. `GOOGLE_SHEETS_SPREADSHEET_ID`
   - Put your sheet ID from URL:
   - `https://docs.google.com/spreadsheets/d/<THIS_PART>/edit`

Optional alternative:
- `GOOGLE_SHEETS_SPREADSHEET_NAME` (if not using sheet ID)

## 5) Streamlit Local Optional (if not using env var)
You can also place secrets in Streamlit secrets with same keys:
- `GOOGLE_CREDENTIALS`
- `GOOGLE_SHEETS_SPREADSHEET_ID`

## 6) Data Flow Summary
- New trades append to worksheet `Ledger`.
- Daily NAV upserts into worksheet `Performance` by date.
- Dashboard and Trading UI read from Google Sheets and convert to pandas DataFrames for charts and tables.

## 7) Quick Validation Checklist
1. Render logs show app starts without auth errors.
2. Opening app creates/validates worksheet headers.
3. Executing trade adds a new row in `Ledger`.
4. Running valuation updates today row in `Performance`.
5. Restarting Render service preserves all historical data in Google Sheets.
