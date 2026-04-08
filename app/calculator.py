from __future__ import annotations

import math
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

from app.config import DEFAULT_LEAD_TIME_DAYS, ROTATION_FACTORS
from app.utils import to_numeric, to_datetime


def to_datetime_naive(series: pd.Series) -> pd.Series:
    s = to_datetime(series)
    tz = getattr(s.dt, "tz", None)
    return s.dt.tz_convert(None) if tz is not None else s


def detect_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for col in candidates:
        if col in df.columns:
            return col
    return None


def classify_rotation(d90: float, d180: float, days_no_movement: float | None) -> str:
    if d180 >= 20 or d90 >= 10:
        return "Alta"
    if d180 >= 6 or d90 >= 3:
        return "Media"
    if d180 > 0 or d90 > 0:
        return "Baja"
    if days_no_movement is not None and days_no_movement > 180:
        return "Nula"
    return "Nula"


def calculate_stock_minimum(base_daily_demand: float, lead_time_days: int, rotation: str) -> tuple[float, float, int]:
    factor = ROTATION_FACTORS.get(rotation, 0.0)

    if rotation == "Nula":
        return 0.0, 0.0, 0

    expected_consumption = base_daily_demand * lead_time_days
    stock_safety = expected_consumption * (factor - 1.0)
    suggested_minimum = math.ceil(expected_consumption + stock_safety)

    return expected_consumption, stock_safety, suggested_minimum


def process_data(
    inventory: pd.DataFrame,
    details: pd.DataFrame,
    quotes: pd.DataFrame,
    sales: pd.DataFrame,
) -> pd.DataFrame:
    today = pd.Timestamp(datetime.now().date())
    d90_start = today - timedelta(days=90)
    d180_start = today - timedelta(days=180)

    # -------------------------
    # Detectar columnas clave
    # -------------------------
    inventory_sku_col = detect_column(inventory, ["sku", "codigo_interno", "codigo", "name"])
    inventory_id_col = detect_column(inventory, ["id"])
    inventory_category_col = detect_column(inventory, ["categoria", "category", "familia", "linea", "linea_producto"])

    details_sku_col = detect_column(details, ["sku", "codigo_interno", "producto", "name"])
    details_qty_emp_col = detect_column(details, ["cantidad_empacada", "qty_empacada"])
    details_qty_sol_col = detect_column(details, ["cantidad_solicitada", "qty_solicitada"])
    details_quote_id_col = detect_column(details, ["cotizacion_id", "cotizaciones_a_clientes", "cotizacion"])
    details_date_col = detect_column(
        details,
        ["fecha_de_salida", "fecha_salida_valida", "ultima_edicion", "fecha", "last_edited_time"],
    )

    quotes_id_col = detect_column(quotes, ["id", "cotizacion_id"])
    quotes_status_col = detect_column(quotes, ["estado", "status"])
    quotes_approved_date_col = detect_column(quotes, ["fecha_aprobacion", "approved_date", "fecha_de_aprobacion"])

    # -------------------------
    # Limpieza de detalles
    # -------------------------
    details = details.copy()
    quotes = quotes.copy()
    inventory = inventory.copy()

    if details_qty_emp_col:
        details[details_qty_emp_col] = to_numeric(details[details_qty_emp_col])
    if details_qty_sol_col:
        details[details_qty_sol_col] = to_numeric(details[details_qty_sol_col])
    if details_date_col:
        details[details_date_col] = to_datetime_naive(details[details_date_col])

    if quotes_approved_date_col:
        quotes[quotes_approved_date_col] = to_datetime_naive(quotes[quotes_approved_date_col])

    # -------------------------
    # Filtrar cotizaciones aprobadas
    # -------------------------
    if quotes_status_col:
        approved_quotes = quotes[
            quotes[quotes_status_col].astype(str).str.lower().str.contains("aprobada", na=False)
        ].copy()
    else:
        approved_quotes = quotes.copy()

    approved_quote_ids = set(approved_quotes[quotes_id_col].astype(str)) if quotes_id_col else set()

    if details_quote_id_col:
        details[details_quote_id_col] = details[details_quote_id_col].astype(str)
        valid_details = details[details[details_quote_id_col].isin(approved_quote_ids)].copy()
    else:
        valid_details = details.copy()

    if details_qty_emp_col:
        valid_details = valid_details[valid_details[details_qty_emp_col] > 0].copy()

    # -------------------------
    # Demanda histórica por SKU
    # -------------------------
    if details_date_col:
        valid_details_90 = valid_details[valid_details[details_date_col] >= d90_start].copy()
        valid_details_180 = valid_details[valid_details[details_date_col] >= d180_start].copy()
    else:
        valid_details_90 = valid_details.copy()
        valid_details_180 = valid_details.copy()

    demand_90 = (
        valid_details_90.groupby(details_sku_col, dropna=False)[details_qty_emp_col].sum().reset_index(name="demanda_90")
        if details_sku_col and details_qty_emp_col
        else pd.DataFrame(columns=[details_sku_col or "sku", "demanda_90"])
    )

    demand_180 = (
        valid_details_180.groupby(details_sku_col, dropna=False)[details_qty_emp_col]
        .sum()
        .reset_index(name="demanda_180")
        if details_sku_col and details_qty_emp_col
        else pd.DataFrame(columns=[details_sku_col or "sku", "demanda_180"])
    )

    last_exit = (
        valid_details.groupby(details_sku_col, dropna=False)[details_date_col]
        .max()
        .reset_index(name="ultima_salida_valida_calculada")
        if details_sku_col and details_date_col
        else pd.DataFrame(columns=[details_sku_col or "sku", "ultima_salida_valida_calculada"])
    )

    # -------------------------
    # Merge con inventario
    # -------------------------
    result = inventory.copy()

    result = result.merge(demand_90, how="left", left_on=inventory_sku_col, right_on=details_sku_col)

    result = result.merge(
        demand_180[[details_sku_col, "demanda_180"]],
        how="left",
        left_on=inventory_sku_col,
        right_on=details_sku_col,
        suffixes=("", "_180"),
    )

    result = result.merge(
        last_exit,
        how="left",
        left_on=inventory_sku_col,
        right_on=details_sku_col,
        suffixes=("", "_last"),
    )

    # -------------------------
    # Normalización numérica
    # -------------------------
    if "demanda_90" not in result.columns:
        result["demanda_90"] = 0
    if "demanda_180" not in result.columns:
        result["demanda_180"] = 0

    result["demanda_90"] = to_numeric(result["demanda_90"])
    result["demanda_180"] = to_numeric(result["demanda_180"])

    result["demanda_diaria_90"] = result["demanda_90"] / 90
    result["demanda_diaria_180"] = result["demanda_180"] / 180

    result["ultima_salida_valida_calculada"] = to_datetime_naive(result["ultima_salida_valida_calculada"])

    inventory_last_exit_col = detect_column(inventory, ["ultima_salida_confirmada", "ultima_salida"])
    if inventory_last_exit_col:
        result[inventory_last_exit_col] = to_datetime_naive(result[inventory_last_exit_col])
        result["ultima_salida_valida_calculada"] = result["ultima_salida_valida_calculada"].fillna(
            result[inventory_last_exit_col]
        )

    last_exit_date = result["ultima_salida_valida_calculada"].dt.normalize()
    result["dias_sin_movimiento_calculado"] = (today - last_exit_date).dt.days

    # -------------------------
    # Clasificación de rotación
    # -------------------------
    result["clasificacion_rotacion_calculada"] = result.apply(
        lambda row: classify_rotation(
            row["demanda_90"],
            row["demanda_180"],
            None if pd.isna(row["dias_sin_movimiento_calculado"]) else row["dias_sin_movimiento_calculado"],
        ),
        axis=1,
    )

    # -------------------------
    # Demanda base por rotación
    # -------------------------
    def choose_base_daily(row):
        rotation = row["clasificacion_rotacion_calculada"]
        if rotation == "Alta":
            return row["demanda_diaria_90"], "Demanda 90 días"
        if rotation in ("Media", "Baja"):
            return row["demanda_diaria_180"], "Demanda 180 días"
        return 0.0, "Revisión manual"

    result[["demanda_diaria_base", "metodo_demanda_base"]] = result.apply(
        lambda row: pd.Series(choose_base_daily(row)),
        axis=1,
    )

    # -------------------------
    # Lead time
    # -------------------------
    if "lead_time_proveedor" not in result.columns:
        result["lead_time_proveedor"] = DEFAULT_LEAD_TIME_DAYS

    result["lead_time_proveedor"] = to_numeric(result["lead_time_proveedor"]).astype(int)

    # -------------------------
    # Stock mínimo sugerido
    # -------------------------
    calc_cols = result.apply(
        lambda row: pd.Series(
            calculate_stock_minimum(
                base_daily_demand=row["demanda_diaria_base"],
                lead_time_days=row["lead_time_proveedor"],
                rotation=row["clasificacion_rotacion_calculada"],
            )
        ),
        axis=1,
    )

    calc_cols.columns = [
        "consumo_esperado_lead_time",
        "stock_seguridad_auto",
        "stock_minimo_sugerido_auto",
    ]

    result = pd.concat([result, calc_cols], axis=1)

    # -------------------------
    # Decisión final
    # -------------------------
    result["actualizar_auto"] = result["clasificacion_rotacion_calculada"].isin(["Alta", "Media", "Baja"])
    result["requiere_revision_manual"] = result["clasificacion_rotacion_calculada"].eq("Nula")

    cols_to_keep = []

    if inventory_id_col:
        cols_to_keep.append(inventory_id_col)
    if inventory_sku_col:
        cols_to_keep.append(inventory_sku_col)
    if inventory_category_col and inventory_category_col not in cols_to_keep:
        cols_to_keep.append(inventory_category_col)

    optional_cols = [
        "demanda_90",
        "demanda_180",
        "demanda_diaria_90",
        "demanda_diaria_180",
        "demanda_diaria_base",
        "metodo_demanda_base",
        "ultima_salida_valida_calculada",
        "dias_sin_movimiento_calculado",
        "clasificacion_rotacion_calculada",
        "lead_time_proveedor",
        "consumo_esperado_lead_time",
        "stock_seguridad_auto",
        "stock_minimo_sugerido_auto",
        "actualizar_auto",
        "requiere_revision_manual",
    ]

    for col in optional_cols:
        if col in result.columns:
            cols_to_keep.append(col)

    return result[cols_to_keep].copy()


def _valid_details_for_demand(details: pd.DataFrame, quotes: pd.DataFrame) -> pd.DataFrame:
    details = details.copy()
    quotes = quotes.copy()

    details_sku_col = detect_column(details, ["sku", "codigo_interno", "producto", "name"])
    details_qty_emp_col = detect_column(details, ["cantidad_empacada", "qty_empacada"])
    details_quote_id_col = detect_column(details, ["cotizacion_id", "cotizaciones_a_clientes", "cotizacion"])
    details_date_col = detect_column(
        details,
        ["fecha_de_salida", "fecha_salida_valida", "ultima_edicion", "fecha", "last_edited_time"],
    )

    quotes_id_col = detect_column(quotes, ["id", "cotizacion_id"])
    quotes_status_col = detect_column(quotes, ["estado", "status"])
    quotes_approved_date_col = detect_column(quotes, ["fecha_aprobacion", "approved_date", "fecha_de_aprobacion"])

    if not details_sku_col or not details_qty_emp_col:
        return pd.DataFrame(columns=["sku", "quantity", "_date"])

    details[details_qty_emp_col] = to_numeric(details[details_qty_emp_col])

    if details_quote_id_col:
        details[details_quote_id_col] = details[details_quote_id_col].astype(str)

    if details_date_col:
        details[details_date_col] = to_datetime_naive(details[details_date_col])

    if quotes_approved_date_col:
        quotes[quotes_approved_date_col] = to_datetime_naive(quotes[quotes_approved_date_col])

    if quotes_status_col:
        approved_quotes = quotes[
            quotes[quotes_status_col].astype(str).str.lower().str.contains("aprobada", na=False)
        ].copy()
    else:
        approved_quotes = quotes.copy()

    if quotes_id_col:
        approved_quotes[quotes_id_col] = approved_quotes[quotes_id_col].astype(str)
        approved_ids = set(approved_quotes[quotes_id_col].astype(str))
    else:
        approved_ids = set()

    valid_details = details.copy()
    if details_quote_id_col and approved_ids:
        valid_details = valid_details[valid_details[details_quote_id_col].isin(approved_ids)].copy()

    valid_details = valid_details[valid_details[details_qty_emp_col] > 0].copy()

    date_series = None
    if details_date_col:
        date_series = valid_details[details_date_col]
    elif details_quote_id_col and quotes_id_col and quotes_approved_date_col:
        date_map = approved_quotes.set_index(quotes_id_col)[quotes_approved_date_col]
        date_series = valid_details[details_quote_id_col].map(date_map)

    if date_series is None:
        return pd.DataFrame(columns=["sku", "quantity", "_date"])

    valid_details = valid_details.assign(_date=to_datetime_naive(date_series))
    valid_details = valid_details[valid_details["_date"].notna()].copy()

    out = valid_details[[details_sku_col, details_qty_emp_col, "_date"]].copy()
    out.columns = ["sku", "quantity", "_date"]
    out["sku"] = out["sku"].astype(str)
    out["quantity"] = to_numeric(out["quantity"])
    return out


def _month_start(ts: pd.Timestamp) -> pd.Timestamp:
    return pd.Timestamp(ts).to_period("M").to_timestamp()


def _add_months(ms: pd.Timestamp, months: int) -> pd.Timestamp:
    return (pd.Timestamp(ms) + pd.DateOffset(months=int(months))).to_period("M").to_timestamp()


def _fourier_forecast_monthly(
    y: np.ndarray,
    forecast_steps: int,
    period: int = 12,
    harmonics: int = 2,
) -> tuple[np.ndarray, float]:
    y = np.asarray(y, dtype=float)
    n = int(y.shape[0])
    if n < 4 or forecast_steps <= 0:
        return np.zeros(int(forecast_steps), dtype=float), 0.10

    t = np.arange(n, dtype=float)
    cols = [np.ones(n), t]
    for k in range(1, int(harmonics) + 1):
        w = 2.0 * math.pi * k / float(period)
        cols.append(np.cos(w * t))
        cols.append(np.sin(w * t))
    X = np.column_stack(cols)
    coef, *_ = np.linalg.lstsq(X, y, rcond=None)
    y_hat = X @ coef
    denom = np.maximum(np.abs(y), 1e-9)
    mape = float(np.mean(np.abs((y - y_hat) / denom)))
    error_pct = min(max(mape, 0.05), 0.10)

    tf = np.arange(n, n + int(forecast_steps), dtype=float)
    cols_f = [np.ones(tf.shape[0]), tf]
    for k in range(1, int(harmonics) + 1):
        w = 2.0 * math.pi * k / float(period)
        cols_f.append(np.cos(w * tf))
        cols_f.append(np.sin(w * tf))
    Xf = np.column_stack(cols_f)
    y_pred = Xf @ coef
    y_pred = np.maximum(y_pred, 0.0)
    return y_pred.astype(float), error_pct


def monthly_behavior_series(
    inventory: pd.DataFrame,
    details: pd.DataFrame,
    quotes: pd.DataFrame,
    sku: str | None = None,
    category: str | None = None,
    total_months: int = 12,
    history_months: int = 9,
    forecast_months: int = 3,
    harmonics: int = 2,
) -> dict[str, object]:
    total_months = int(total_months)
    history_months = int(history_months)
    forecast_months = int(forecast_months)
    harmonics = int(harmonics)

    if total_months < 1 or total_months > 60:
        raise ValueError("total_months fuera de rango")
    if history_months < 1 or history_months > total_months:
        raise ValueError("history_months fuera de rango")
    if forecast_months < 0 or forecast_months > total_months:
        raise ValueError("forecast_months fuera de rango")

    inventory = inventory.copy()
    inventory_sku_col = detect_column(inventory, ["sku", "codigo_interno", "codigo", "name"])
    inventory_category_col = detect_column(inventory, ["categoria", "category", "familia", "linea", "linea_producto"])

    if not inventory_sku_col:
        return {
            "sku": sku,
            "category": category,
            "available_categories": [],
            "series": [],
        }

    inv = inventory[[inventory_sku_col] + ([inventory_category_col] if inventory_category_col else [])].copy()
    inv[inventory_sku_col] = inv[inventory_sku_col].astype(str)
    if inventory_category_col:
        inv[inventory_category_col] = inv[inventory_category_col].astype(str)
    else:
        inv["categoria"] = "Sin categoría"
        inventory_category_col = "categoria"

    inv = inv[inv[inventory_sku_col].notna() & (inv[inventory_sku_col].astype(str).str.len() > 0)].copy()
    inv = inv.drop_duplicates(subset=[inventory_sku_col], keep="first")

    available_categories = sorted(
        [c for c in inv[inventory_category_col].dropna().astype(str).unique().tolist() if c.strip()]
    )

    valid = _valid_details_for_demand(details=details, quotes=quotes)
    if valid.empty:
        return {
            "sku": sku,
            "category": category,
            "available_categories": available_categories,
            "series": [],
        }

    active_skus = set(valid["sku"].unique().tolist())
    inv_active = inv[inv[inventory_sku_col].isin(active_skus)].copy()
    sku_to_cat = dict(zip(inv_active[inventory_sku_col].tolist(), inv_active[inventory_category_col].tolist()))

    def build_sku_series(target_sku: str) -> pd.Series:
        df = valid[valid["sku"].astype(str) == str(target_sku)].copy()
        if df.empty:
            return pd.Series(dtype=float)
        df = df.assign(month=df["_date"].dt.to_period("M").dt.to_timestamp())
        g = df.groupby("month", dropna=False)["quantity"].sum().sort_index()
        start = _month_start(g.index.min())
        end = _month_start(g.index.max())
        idx = pd.date_range(start=start, end=end, freq="MS")
        return g.reindex(idx, fill_value=0.0)

    def build_avg_series(target_skus: list[str]) -> pd.Series:
        target_skus = [str(s) for s in target_skus if s]
        if not target_skus:
            return pd.Series(dtype=float)
        df = valid[valid["sku"].isin(target_skus)].copy()
        if df.empty:
            return pd.Series(dtype=float)
        df = df.assign(month=df["_date"].dt.to_period("M").dt.to_timestamp())
        g = df.groupby(["sku", "month"], dropna=False)["quantity"].sum().reset_index()
        pivot = g.pivot_table(index="month", columns="sku", values="quantity", aggfunc="sum", fill_value=0.0)
        start = _month_start(pivot.index.min())
        end = _month_start(pivot.index.max())
        idx = pd.date_range(start=start, end=end, freq="MS")
        pivot = pivot.reindex(idx, fill_value=0.0)
        return pivot.mean(axis=1).astype(float)

    def window_and_forecast(
        name: str,
        s: pd.Series,
        months_idx: pd.DatetimeIndex,
        actual_months_idx: pd.DatetimeIndex,
        cutoff_month: pd.Timestamp,
    ) -> dict[str, object]:
        if s.empty:
            return {
                "key": name,
                "points": [],
                "model": None,
                "error_pct": None,
                "first_month": None,
                "last_month_observed": None,
            }

        first_month = _month_start(s.index.min())
        last_month_observed = _month_start(s.index.max())

        actual_vals = s.reindex(actual_months_idx, fill_value=0.0).astype(float).values
        f_steps = max(0, len(months_idx) - len(actual_months_idx))

        if f_steps > 0:
            y_pred, error_pct = _fourier_forecast_monthly(
                y=actual_vals,
                forecast_steps=f_steps,
                period=12,
                harmonics=harmonics,
            )
            model = "fourier"
        else:
            y_pred = np.zeros(0, dtype=float)
            error_pct = 0.10
            model = None

        points: list[dict[str, object]] = []
        for i, m in enumerate(months_idx):
            if m <= cutoff_month:
                v = float(s.reindex([m], fill_value=0.0).iloc[0]) if m <= last_month_observed else 0.0
                points.append({"month": m.strftime("%Y-%m"), "quantity": v, "kind": "actual"})
            else:
                j = i - len(actual_months_idx)
                v = float(y_pred[j]) if 0 <= j < len(y_pred) else 0.0
                low = max(0.0, v * (1.0 - float(error_pct)))
                high = v * (1.0 + float(error_pct))
                points.append(
                    {
                        "month": m.strftime("%Y-%m"),
                        "quantity": v,
                        "kind": "forecast",
                        "low": low,
                        "high": high,
                    }
                )

        return {
            "key": name,
            "points": points,
            "model": model,
            "error_pct": float(error_pct) if error_pct is not None else None,
            "first_month": first_month.strftime("%Y-%m"),
            "last_month_observed": last_month_observed.strftime("%Y-%m"),
        }

    category_name = None
    if sku is not None:
        category_name = sku_to_cat.get(str(sku))
    if category is not None:
        category_name = str(category)

    sku_series = build_sku_series(str(sku)) if sku is not None else pd.Series(dtype=float)

    category_series = pd.Series(dtype=float)
    if category_name:
        skus_in_cat = inv_active[inv_active[inventory_category_col].astype(str) == str(category_name)][
            inventory_sku_col
        ].astype(str).tolist()
        category_series = build_avg_series(skus_in_cat)

    overall_series = build_avg_series(inv_active[inventory_sku_col].astype(str).tolist())

    ref_series = sku_series if not sku_series.empty else (category_series if not category_series.empty else overall_series)
    if ref_series.empty:
        return {
            "sku": sku,
            "category": category_name,
            "available_categories": available_categories,
            "series": [],
        }

    ref_first = _month_start(ref_series.index.min())
    ref_last = _month_start(ref_series.index.max())

    if len(ref_series) >= history_months:
        window_start = _add_months(ref_last, -(history_months - 1))
        fit_start = window_start
        window_end = _add_months(ref_last, forecast_months)
    else:
        window_start = ref_first
        fit_start = ref_first
        window_end = _add_months(ref_first, total_months - 1)

    months_idx = pd.date_range(start=window_start, end=window_end, freq="MS")
    actual_months_idx = pd.date_range(start=fit_start, end=ref_last, freq="MS")

    overall_payload = window_and_forecast("overall", overall_series, months_idx, actual_months_idx, ref_last)
    overall_payload["label"] = "Promedio general"

    category_payload = None
    if category_name:
        category_payload = window_and_forecast("category", category_series, months_idx, actual_months_idx, ref_last)
        category_payload["label"] = f"Promedio categoría: {category_name}"

    sku_payload = None
    if sku is not None:
        sku_payload = window_and_forecast("sku", sku_series, months_idx, actual_months_idx, ref_last)
        sku_payload["label"] = f"SKU: {sku}"

    series_out = [x for x in [sku_payload, category_payload, overall_payload] if x is not None]

    return {
        "sku": sku,
        "category": category_name,
        "available_categories": available_categories,
        "series": series_out,
    }


def monthly_demand_series(
    details: pd.DataFrame,
    quotes: pd.DataFrame,
    sku: str,
    months_back: int = 24,
) -> pd.DataFrame:
    today = pd.Timestamp(datetime.now().date())
    start = (today - pd.DateOffset(months=months_back)).replace(day=1)

    details = details.copy()
    quotes = quotes.copy()

    details_sku_col = detect_column(details, ["sku", "codigo_interno", "producto", "name"])
    details_qty_emp_col = detect_column(details, ["cantidad_empacada", "qty_empacada"])
    details_quote_id_col = detect_column(details, ["cotizacion_id", "cotizaciones_a_clientes", "cotizacion"])
    details_date_col = detect_column(
        details,
        ["fecha_de_salida", "fecha_salida_valida", "ultima_edicion", "fecha", "last_edited_time"],
    )

    quotes_id_col = detect_column(quotes, ["id", "cotizacion_id"])
    quotes_status_col = detect_column(quotes, ["estado", "status"])
    quotes_approved_date_col = detect_column(quotes, ["fecha_aprobacion", "approved_date", "fecha_de_aprobacion"])

    if not details_sku_col or not details_qty_emp_col:
        return pd.DataFrame(columns=["month", "quantity"])

    details[details_qty_emp_col] = to_numeric(details[details_qty_emp_col])

    if details_quote_id_col:
        details[details_quote_id_col] = details[details_quote_id_col].astype(str)

    if details_date_col:
        details[details_date_col] = to_datetime_naive(details[details_date_col])

    if quotes_approved_date_col:
        quotes[quotes_approved_date_col] = to_datetime_naive(quotes[quotes_approved_date_col])

    if quotes_status_col:
        approved_quotes = quotes[
            quotes[quotes_status_col].astype(str).str.lower().str.contains("aprobada", na=False)
        ].copy()
    else:
        approved_quotes = quotes.copy()

    if quotes_id_col:
        approved_quotes[quotes_id_col] = approved_quotes[quotes_id_col].astype(str)
        approved_ids = set(approved_quotes[quotes_id_col].astype(str))
    else:
        approved_ids = set()

    valid_details = details.copy()
    if details_quote_id_col and approved_ids:
        valid_details = valid_details[valid_details[details_quote_id_col].isin(approved_ids)].copy()

    valid_details = valid_details[valid_details[details_qty_emp_col] > 0].copy()
    valid_details = valid_details[valid_details[details_sku_col].astype(str) == str(sku)].copy()

    date_series = None
    if details_date_col:
        date_series = valid_details[details_date_col]
    elif details_quote_id_col and quotes_id_col and quotes_approved_date_col:
        date_map = approved_quotes.set_index(quotes_id_col)[quotes_approved_date_col]
        date_series = valid_details[details_quote_id_col].map(date_map)

    if date_series is None:
        return pd.DataFrame(columns=["month", "quantity"])

    valid_details = valid_details.assign(_date=to_datetime_naive(date_series))
    valid_details = valid_details[valid_details["_date"].notna()].copy()
    valid_details = valid_details[valid_details["_date"] >= start].copy()

    if valid_details.empty:
        return pd.DataFrame(columns=["month", "quantity"])

    valid_details = valid_details.assign(month=valid_details["_date"].dt.to_period("M").dt.to_timestamp())
    series = (
        valid_details.groupby("month", dropna=False)[details_qty_emp_col]
        .sum()
        .reset_index(name="quantity")
        .sort_values("month")
    )

    all_months = pd.date_range(start=start, end=today, freq="MS")
    series = series.set_index("month").reindex(all_months, fill_value=0).rename_axis("month").reset_index()
    return series


def weekly_demand_series(
    details: pd.DataFrame,
    quotes: pd.DataFrame,
    sku: str,
    weeks_back: int = 26,
) -> pd.DataFrame:
    today = pd.Timestamp(datetime.now().date())
    start = today - pd.Timedelta(days=int(weeks_back) * 7)

    details = details.copy()
    quotes = quotes.copy()

    details_sku_col = detect_column(details, ["sku", "codigo_interno", "producto", "name"])
    details_qty_emp_col = detect_column(details, ["cantidad_empacada", "qty_empacada"])
    details_quote_id_col = detect_column(details, ["cotizacion_id", "cotizaciones_a_clientes", "cotizacion"])
    details_date_col = detect_column(
        details,
        ["fecha_de_salida", "fecha_salida_valida", "ultima_edicion", "fecha", "last_edited_time"],
    )

    quotes_id_col = detect_column(quotes, ["id", "cotizacion_id"])
    quotes_status_col = detect_column(quotes, ["estado", "status"])
    quotes_approved_date_col = detect_column(quotes, ["fecha_aprobacion", "approved_date", "fecha_de_aprobacion"])

    if not details_sku_col or not details_qty_emp_col:
        return pd.DataFrame(columns=["week", "quantity"])

    details[details_qty_emp_col] = to_numeric(details[details_qty_emp_col])

    if details_quote_id_col:
        details[details_quote_id_col] = details[details_quote_id_col].astype(str)

    if details_date_col:
        details[details_date_col] = to_datetime_naive(details[details_date_col])

    if quotes_approved_date_col:
        quotes[quotes_approved_date_col] = to_datetime_naive(quotes[quotes_approved_date_col])

    if quotes_status_col:
        approved_quotes = quotes[
            quotes[quotes_status_col].astype(str).str.lower().str.contains("aprobada", na=False)
        ].copy()
    else:
        approved_quotes = quotes.copy()

    if quotes_id_col:
        approved_quotes[quotes_id_col] = approved_quotes[quotes_id_col].astype(str)
        approved_ids = set(approved_quotes[quotes_id_col].astype(str))
    else:
        approved_ids = set()

    valid_details = details.copy()
    if details_quote_id_col and approved_ids:
        valid_details = valid_details[valid_details[details_quote_id_col].isin(approved_ids)].copy()

    valid_details = valid_details[valid_details[details_qty_emp_col] > 0].copy()
    valid_details = valid_details[valid_details[details_sku_col].astype(str) == str(sku)].copy()

    date_series = None
    if details_date_col:
        date_series = valid_details[details_date_col]
    elif details_quote_id_col and quotes_id_col and quotes_approved_date_col:
        date_map = approved_quotes.set_index(quotes_id_col)[quotes_approved_date_col]
        date_series = valid_details[details_quote_id_col].map(date_map)

    if date_series is None:
        return pd.DataFrame(columns=["week", "quantity"])

    valid_details = valid_details.assign(_date=to_datetime_naive(date_series))
    valid_details = valid_details[valid_details["_date"].notna()].copy()
    valid_details = valid_details[valid_details["_date"] >= start].copy()

    if valid_details.empty:
        return pd.DataFrame(columns=["week", "quantity"])

    valid_details = valid_details.assign(week=valid_details["_date"].dt.to_period("W-MON").dt.start_time)
    series = (
        valid_details.groupby("week", dropna=False)[details_qty_emp_col]
        .sum()
        .reset_index(name="quantity")
        .sort_values("week")
    )

    all_weeks = pd.date_range(start=start.normalize(), end=today, freq="W-MON")
    series = series.set_index("week").reindex(all_weeks, fill_value=0).rename_axis("week").reset_index()
    return series
