#!/usr/bin/env bash
set -euo pipefail

# Run from this script's directory (project root).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Prefer project virtual environment if it exists.
if [[ -f ".venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source ".venv/bin/activate"
  python daily_valuation.py
  exit 0
fi

# Fallback to system Python when .venv is not found.
python3 daily_valuation.py
