# README - Cálculos y cómo interpretar la UI

Este documento explica cómo el servicio calcula:
- Demanda (90/180 días) y rotación
- Stock mínimo sugerido
- Serie semanal por SKU
- Comportamiento mensual (SKU / promedio por categoría / promedio general)
- Pronóstico de 3 meses (Fourier) y cómo interpretar líneas/puntos

## 1) Normalización y fuentes de datos

Los CSV se cargan desde `data/input/` y se normalizan los nombres de columnas a:
- minúsculas
- espacios y `-` se vuelven `_`

La fuente principal para demanda/series es `detalle_cotizacion.csv` (cantidad empacada y fechas). Las cotizaciones pueden filtrarse por “aprobada” usando `cotizaciones_clientes.csv`.

## 2) Demanda histórica por SKU (90 y 180 días)

Para cada SKU se calcula demanda sumando la cantidad empacada (`cantidad_empacada` / `qty_empacada`) en ventanas:
- `demanda_90`: suma de los últimos 90 días
- `demanda_180`: suma de los últimos 180 días

Luego se calcula demanda diaria:
- `demanda_diaria_90 = demanda_90 / 90`
- `demanda_diaria_180 = demanda_180 / 180`

Si existen cotizaciones y estatus, se filtran cotizaciones “aprobada”. Si no hay estatus/ids, se usa lo disponible.

Código: [process_data](file:///c:/Users/dhgui/Documents/SCRIPTS/stock-min-service/app/calculator.py#L49-L277)

## 3) Clasificación de rotación

La rotación “calculada” se asigna con reglas simples basadas en demanda:

- Alta: `demanda_180 >= 20` o `demanda_90 >= 10`
- Media: `demanda_180 >= 6` o `demanda_90 >= 3`
- Baja: `demanda_180 > 0` o `demanda_90 > 0`
- Nula: sin demanda (y opcionalmente “días sin movimiento” alto)

Código: [classify_rotation](file:///c:/Users/dhgui/Documents/SCRIPTS/stock-min-service/app/calculator.py#L24-L33)

## 4) Demanda base para stock mínimo

Se elige una demanda diaria “base” según rotación:
- Alta: `demanda_diaria_90`
- Media/Baja: `demanda_diaria_180`
- Nula: 0 (marca revisión manual)

Código: [choose_base_daily dentro de process_data](file:///c:/Users/dhgui/Documents/SCRIPTS/stock-min-service/app/calculator.py#L204-L221)

## 5) Stock mínimo sugerido (auto)

Con lead time (días) y demanda base diaria:
- `consumo_esperado_lead_time = demanda_diaria_base * lead_time`
- `stock_seguridad_auto = consumo_esperado_lead_time * (factor - 1)`
- `stock_minimo_sugerido_auto = ceil(consumo_esperado_lead_time + stock_seguridad_auto)`

El `factor` depende de la rotación (se define en `app/config.py` como `ROTATION_FACTORS`).  
Para rotación `Nula`, el stock mínimo sugerido es 0.

Código: [calculate_stock_minimum](file:///c:/Users/dhgui/Documents/SCRIPTS/stock-min-service/app/calculator.py#L36-L46)

## 6) Serie semanal por SKU (UI “Semana”)

La serie semanal agrupa la cantidad empacada por semana (inicio lunes) y rellena semanas faltantes con 0 dentro del rango solicitado (`weeks_back`).

Endpoint:
- GET `/api/sku/{sku}/weekly?weeks_back=26`

Código: [weekly_demand_series](file:///c:/Users/dhgui/Documents/SCRIPTS/stock-min-service/app/calculator.py#L381-L466)

Interpretación:
- Línea continua: valores reales agregados por semana (0 significa “no hubo cantidad empacada registrada esa semana”).

## 7) Comportamiento mensual con promedios (UI “Mes”)

El endpoint mensual devuelve hasta 3 series simultáneas:
- SKU (si hay SKU exacto)
- Promedio categoría (de la categoría del SKU o la categoría seleccionada)
- Promedio general (de todos los SKUs activos con datos)

Endpoint:
- GET `/api/behavior/monthly`

Código: [monthly_behavior_series](file:///c:/Users/dhgui/Documents/SCRIPTS/stock-min-service/app/calculator.py#L410-L602)

### 7.1) Cómo se calcula el “promedio”

Se calcula como promedio por SKU por mes:
1) Se agrupa por `sku` y `mes` y se suma `quantity`.
2) Se arma una tabla `mes × sku` (cada SKU es una columna).
3) Para meses sin movimiento de un SKU se rellena con 0.
4) El promedio del mes es la media de todas las columnas (SKUs) para ese mes.

En fórmula:
`promedio_mes = (q_sku1_mes + ... + q_skuN_mes) / N`

Esto hace que:
- Si una categoría solo tiene 1 SKU activo, el “promedio de categoría” coincide con ese SKU.
- El promedio general suele ser mucho menor que SKUs con picos altos, porque divide entre muchos SKUs.

Código: [build_avg_series](file:///c:/Users/dhgui/Documents/SCRIPTS/stock-min-service/app/calculator.py#L484-L499)

### 7.2) Ventana temporal (12 meses)

La ventana de la gráfica se alinea para todas las series (SKU/categoría/general) usando una referencia:
- Si hay SKU, se usa el SKU como referencia.
- Si no hay SKU pero hay categoría, se usa la categoría.
- Si no hay ni SKU ni categoría, se usa el promedio general.

Reglas:
- Si hay al menos 9 meses de historia: se toman los 9 meses más recientes con datos + 3 meses estimados.
- Si hay menos de 9 meses de historia: se inicia en el mes del primer registro y se completa hasta 12 meses (estimando lo faltante).

## 8) Pronóstico (3 meses) con Fourier

El estimado se calcula con un ajuste por mínimos cuadrados de un modelo con:
- Intercepto
- Tendencia lineal
- Estacionalidad anual (periodo 12 meses) usando senos y cosenos
- `harmonics=2` por defecto (2 pares seno/coseno)

Después se extrapola para los meses futuros.

Código: [_fourier_forecast_monthly](file:///c:/Users/dhgui/Documents/SCRIPTS/stock-min-service/app/calculator.py#L356-L408)

### 8.1) Error aceptable (5%–10%) y banda

Se calcula un error tipo MAPE sobre el tramo ajustado:
- `MAPE = promedio(|y - y_hat| / |y|)` (con protección cuando `y=0`)

Luego se acota:
- `error_pct = clamp(MAPE, 0.05, 0.10)`

Para cada punto pronosticado:
- `low = pred * (1 - error_pct)`
- `high = pred * (1 + error_pct)`

Los valores pronosticados se recortan a `>= 0` para evitar predicciones negativas.

## 9) Cómo interpretar la gráfica (líneas, puntos, “dots”)

En la UI, cada serie se dibuja como una línea:
- Tramo “actual”: línea sólida (datos observados).
- Tramo “forecast”: línea punteada (estimado).

Los puntos (dots) corresponden a cada mes en el eje X:
- En meses “actual”, el punto representa el valor agregado real del mes.
- En meses “forecast”, el punto representa el valor estimado (y el endpoint también trae `low/high` como banda posible, aunque la UI por ahora solo dibuja la línea).

Si observas que el pronóstico “se va a 0”:
- Puede ser porque la tendencia reciente cae fuerte y el mejor ajuste termina cerca de cero.
- También porque el modelo pudo extrapolar negativo y se recortó a 0.

## 10) Endpoints útiles para depurar

- GET `/api/diagnostics/date-coverage`: muestra qué columnas de fecha se detectan y cuántas filas son parseables. Si una serie sale vacía, este endpoint ayuda a encontrar por qué.
