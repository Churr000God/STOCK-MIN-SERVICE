#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if command -v docker >/dev/null 2>&1; then
  docker compose up --build -d stock-min-service
  echo "OK: actualizado en Docker. UI: http://127.0.0.1:8001/ui"
  exit 0
fi

echo "OK: si corres local con --reload, solo refresca el navegador."
echo "Si no usas --reload, reinicia el servidor con: ./scripts/dev_linux.sh"
