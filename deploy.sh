#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

if [[ ! -f "app.py" ]]; then
  echo "ERROR: app.py not found in project root."
  exit 1
fi

if [[ ! -d ".venv" ]]; then
  python3 -m venv .venv
fi

# shellcheck disable=SC1091
source .venv/bin/activate

python -m pip install --upgrade pip
python -m pip install streamlit pandas plotly yfinance fpdf reportlab pytz

exec streamlit run app.py --server.headless true --browser.gatherUsageStats false
