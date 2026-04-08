$ErrorActionPreference = "Stop"

Set-Location (Split-Path -Parent $PSScriptRoot)

if (-not (Test-Path ".venv")) {
  python -m venv .venv
}

$activate = Join-Path ".venv" "Scripts\\Activate.ps1"
& $activate

python -m pip install --upgrade pip
python -m pip install -r requirements.txt

New-Item -ItemType Directory -Force -Path "data\\output" | Out-Null

Write-Host "OK: entorno listo. Inicia con: .\\scripts\\dev_windows.ps1"
