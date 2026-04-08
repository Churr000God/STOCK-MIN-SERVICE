#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

PY="python3"
command -v python3 >/dev/null 2>&1 || PY="python"

if [ ! -d ".venv" ]; then
  "$PY" -m venv .venv
fi

. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
mkdir -p data/output

echo "OK: entorno listo. Inicia con: ./scripts/dev_linux.sh"
