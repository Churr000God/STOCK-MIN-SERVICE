# stock-min-service

Servicio FastAPI para calcular stock mínimo sugerido por SKU y visualizar resultados en una UI web (incluye serie semanal y comportamiento mensual con promedio por categoría/general y pronóstico).

## Qué hace

- Lee archivos CSV de entrada (inventario, detalle de cotización, cotizaciones y ventas).
- Calcula métricas de demanda (90/180 días), rotación y stock mínimo sugerido.
- Genera un CSV de salida con los resultados.
- Expone endpoints para:
  - Ejecutar el cálculo y guardar el output.
  - Listar/descargar outputs.
  - Ver la serie semanal por SKU.
  - Ver comportamiento mensual (SKU / promedio categoría / promedio general) con pronóstico.
- Incluye una UI en `/ui` para explorar el output y graficar series.

## Requisitos

- Python 3.11+ recomendado
- Dependencias: ver `requirements.txt`

## Ejecutar local

```bash
python -m venv .venv
# Windows PowerShell
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
uvicorn app.main:app --reload
```

Abrir:
- UI: http://127.0.0.1:8000/ui
- Swagger: http://127.0.0.1:8000/docs

## Ejecutar con Docker

```bash
docker compose up --build
```

## Endpoints principales

- POST `/run-stock-min`: ejecuta el cálculo y guarda un CSV en `data/output`.
- GET `/api/outputs`: lista outputs generados.
- GET `/api/output/{filename}`: devuelve filas/columnas del CSV.
- GET `/api/sku/{sku}/weekly?weeks_back=26`: serie semanal del SKU (últimas N semanas).
- GET `/api/behavior/monthly?...`: comportamiento mensual con promedios y pronóstico.
- GET `/api/categories`: lista categorías detectadas en el inventario.
- GET `/api/diagnostics/date-coverage`: diagnóstico de columnas de fechas parseables.

## Datos de entrada

Rutas por defecto en `data/input/`:
- `gestion_inventario.csv`
- `detalle_cotizacion.csv`
- `cotizaciones_clientes.csv`
- `reporte_ventas.csv`

Los CSV se normalizan a minúsculas y `_` en nombres de columnas.

## Documentación de cálculos

Ver [README_CALCULOS.md](file:///c:/Users/dhgui/Documents/SCRIPTS/stock-min-service/README_CALCULOS.md) para:
- Lógica de demanda/rotación/stock mínimo sugerido
- Cómo se calcula el promedio por categoría/general
- Cómo funciona el pronóstico (Fourier) y cómo interpretar la gráfica (línea sólida vs punteada, puntos y bandas)

## Registro de cambios

Ver [CAMBIOS.md](file:///c:/Users/dhgui/Documents/SCRIPTS/stock-min-service/CAMBIOS.md)
