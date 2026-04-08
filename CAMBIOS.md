# Cambios implementados

Fecha: 2026-04-08

Este documento resume los cambios realizados en el servicio y la UI.

## Backend / API

- Se agregó endpoint de comportamiento mensual unificado:
  - GET `/api/behavior/monthly`
  - Parámetros:
    - `sku` (opcional): si viene, devuelve serie del SKU + promedio categoría + promedio general.
    - `category` (opcional): si NO hay `sku`, permite elegir categoría para ver promedio categoría + general.
    - `total_months` (default 12), `history_months` (default 9), `forecast_months` (default 3), `harmonics` (default 2).
  - Respuesta incluye:
    - `available_categories`
    - `series[]` con puntos mensuales `kind=actual|forecast` y (para forecast) banda `low/high` + `error_pct`.
  - Implementación: [monthly_behavior_series](file:///c:/Users/dhgui/Documents/SCRIPTS/stock-min-service/app/calculator.py#L410-L618) y [get_monthly_behavior](file:///c:/Users/dhgui/Documents/SCRIPTS/stock-min-service/app/main.py#L317-L341)

- Se agregó endpoint para categorías:
  - GET `/api/categories`
  - Implementación: [get_categories](file:///c:/Users/dhgui/Documents/SCRIPTS/stock-min-service/app/main.py#L294-L315)

- Se mantuvieron endpoints anteriores:
  - GET `/api/sku/{sku}/monthly` (serie mensual simple por SKU)
  - GET `/api/sku/{sku}/weekly` (serie semanal por SKU)

## Cálculos

- Ventana mensual “inteligente” (12 meses):
  - Si hay al menos `history_months` (9) meses: se toman los 9 más recientes + 3 de pronóstico.
  - Si hay menos historia: se inicia en el primer mes con datos y se completa hasta 12 meses.
  - La ventana se alinea para que SKU/categoría/general se vean en el mismo rango.
  - Implementación: [monthly_behavior_series](file:///c:/Users/dhgui/Documents/SCRIPTS/stock-min-service/app/calculator.py#L410-L618)

- Promedios cuando no hay SKU seleccionado:
  - Promedio categoría: promedio por mes entre los SKUs de la categoría (rellenando 0 donde no hubo movimiento).
  - Promedio general: promedio por mes entre todos los SKUs activos.
  - Implementación: [build_avg_series](file:///c:/Users/dhgui/Documents/SCRIPTS/stock-min-service/app/calculator.py#L484-L499)

- Pronóstico 3 meses (Fourier):
  - Modelo con intercepto + tendencia lineal + estacionalidad anual (periodo 12) con `harmonics` armónicos.
  - Se calcula error tipo MAPE y se acota a ±5%–±10% (`error_pct`).
  - Se retornan bandas `low/high` para cada punto forecast.
  - Implementación: [_fourier_forecast_monthly](file:///c:/Users/dhgui/Documents/SCRIPTS/stock-min-service/app/calculator.py#L356-L408)

- Salida del cálculo incluye categoría cuando existe en inventario:
  - Se conserva columna `categoria`/equivalente en el CSV de salida (si está presente en el inventario).
  - Implementación: [process_data](file:///c:/Users/dhgui/Documents/SCRIPTS/stock-min-service/app/calculator.py#L49-L277)

## UI (/ui)

- Se agregó selector de categoría y se integró el endpoint mensual unificado:
  - Modo “Mes” usa `/api/behavior/monthly`.
  - Modo “Semana” usa `/api/sku/{sku}/weekly`.
  - Implementación UI JS: [loadMonthlyBehavior/renderMonthlyBehavior](file:///c:/Users/dhgui/Documents/SCRIPTS/stock-min-service/app/main.py#L714-L850)

- Se agregó sección “Guía: cómo interpretar y usar los datos” en la página:
  - Secciones desplegables con `<details>` (abierto/cerrado por atributo `open`).
  - HTML: [panel de guía](file:///c:/Users/dhgui/Documents/SCRIPTS/stock-min-service/app/main.py#L540-L587)

- Se corrigió crecimiento vertical indefinido (Chart.js):
  - Los `<canvas>` se encapsulan en contenedores con altura fija (`.chartBoxSm`, `.chartBoxLg`).
  - `maintainAspectRatio: false` se usa con altura estable del contenedor.
  - HTML/CSS: [chartBox](file:///c:/Users/dhgui/Documents/SCRIPTS/stock-min-service/app/main.py#L405-L417) y canvases envueltos [aquí](file:///c:/Users/dhgui/Documents/SCRIPTS/stock-min-service/app/main.py#L501-L535)

- Responsive:
  - Se definió un diseño mobile-first y un breakpoint de laptop `min-width: 1024px`.
  - Ajustes de padding, grid, altura de charts, distribución de controles/acciones.
  - CSS: [bloque responsive](file:///c:/Users/dhgui/Documents/SCRIPTS/stock-min-service/app/main.py#L380-L434)

## Schemas / Contratos

- Se agregaron modelos Pydantic para el endpoint mensual y categorías:
  - `MonthlyBehaviorPoint`, `MonthlyBehaviorSeries`, `MonthlyBehaviorResponse`, `CategoriesResponse`
  - Archivo: [schemas.py](file:///c:/Users/dhgui/Documents/SCRIPTS/stock-min-service/app/schemas.py)

## Documentación

- README del proyecto actualizado (ejecución, endpoints, link a cálculos).
  - Archivo: [README.md](file:///c:/Users/dhgui/Documents/SCRIPTS/stock-min-service/README.md)

- Documento de cálculo e interpretación agregado:
  - Archivo: [README_CALCULOS.md](file:///c:/Users/dhgui/Documents/SCRIPTS/stock-min-service/README_CALCULOS.md)

## Scripts de inicialización y actualización (Linux/Windows)

Se agregaron scripts para:
- inicializar entorno (crear venv, instalar requirements, preparar `data/output`)
- correr en modo desarrollo con `--reload` (para que al guardar cambios se refleje en la UI)
- actualizar despliegue Docker (rebuild del servicio)

Archivos:
- Linux: [scripts/init_linux.sh](file:///c:/Users/dhgui/Documents/SCRIPTS/stock-min-service/scripts/init_linux.sh), [scripts/dev_linux.sh](file:///c:/Users/dhgui/Documents/SCRIPTS/stock-min-service/scripts/dev_linux.sh), [scripts/update_linux.sh](file:///c:/Users/dhgui/Documents/SCRIPTS/stock-min-service/scripts/update_linux.sh)
- Windows: [scripts/init_windows.ps1](file:///c:/Users/dhgui/Documents/SCRIPTS/stock-min-service/scripts/init_windows.ps1), [scripts/dev_windows.ps1](file:///c:/Users/dhgui/Documents/SCRIPTS/stock-min-service/scripts/dev_windows.ps1), [scripts/update_windows.ps1](file:///c:/Users/dhgui/Documents/SCRIPTS/stock-min-service/scripts/update_windows.ps1)

