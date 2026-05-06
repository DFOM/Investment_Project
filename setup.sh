#!/bin/bash
set -e
echo "📦 Setting up Investment Portfolio Simulator..."
python3 -m venv .venv || true
source .venv/bin/activate
pip install -q -r requirements.txt
[ -f ".env" ] || cp .env.example .env
echo "✅ Setup complete! Run: streamlit run app.py"
