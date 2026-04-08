$ErrorActionPreference = "Stop"

Set-Location (Split-Path -Parent $PSScriptRoot)

$docker = Get-Command docker -ErrorAction SilentlyContinue
if ($null -ne $docker) {
  docker compose up --build -d stock-min-service | Out-Host
  Write-Host "OK: actualizado en Docker. UI: http://127.0.0.1:8001/ui"
  exit 0
}

Write-Host "OK: si corres local con --reload, solo refresca el navegador."
Write-Host "Si no usas --reload, reinicia el servidor con: .\\scripts\\dev_windows.ps1"
