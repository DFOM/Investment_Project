#!/usr/bin/env bash
set -euo pipefail

: "${PORT:=8501}"

python - <<'PY'
from core.database import initialize_database_schema
result = initialize_database_schema()
if not result.get("success"):
    raise SystemExit(f"Database initialization failed: {result.get('error')}")
print(result.get("message", "Database schema ready."))
PY

python background_worker.py &
WORKER_PID=$!
trap 'kill "$WORKER_PID" 2>/dev/null || true' EXIT

exec streamlit run app.py \
  --server.port "$PORT" \
  --server.address 0.0.0.0 \
  --server.headless true \
  --browser.gatherUsageStats false
