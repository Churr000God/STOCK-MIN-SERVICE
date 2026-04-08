$ErrorActionPreference = "Stop"

Set-Location (Split-Path -Parent $PSScriptRoot)

if (-not (Test-Path ".venv")) {
  Write-Host "Falta .venv. Ejecuta primero: .\\scripts\\init_windows.ps1"
  exit 1
}

$activate = Join-Path ".venv" "Scripts\\Activate.ps1"
& $activate

python -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
