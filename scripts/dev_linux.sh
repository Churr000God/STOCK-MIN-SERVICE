#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if [ ! -d ".venv" ]; then
  echo "Falta .venv. Ejecuta primero: ./scripts/init_linux.sh"
  exit 1
fi

. .venv/bin/activate
exec python -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
