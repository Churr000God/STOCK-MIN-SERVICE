from __future__ import annotations

import math
import re
import threading
import traceback
from pathlib import Path
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
import pandas as pd

from app.calculator import monthly_behavior_series, monthly_demand_series, process_data, weekly_demand_series
from app.config import OUTPUT_DIR, WEBHOOK_DELAY_SECONDS, WEBHOOK_URL
from app.loader import load_all_data
from app.schemas import CategoriesResponse, MonthlyBehaviorResponse, RunResponse
from app.utils import (
    docker_container_create_tunnel,
    docker_container_logs,
    docker_container_remove,
    docker_container_start,
    docker_container_stop,
    docker_find_container_by_name,
    docker_image_pull,
    docker_sock_available,
    save_output,
    send_webhook_get,
)

app = FastAPI(title="RTB Stock Minimum Service", version="1.0.0")


@app.get("/")
def root():
    return {
        "service": "RTB Stock Minimum Service",
        "endpoints": {
            "health": "/health",
            "run_stock_min": "/run-stock-min",
            "ui": "/ui",
            "outputs": "/api/outputs",
            "output_file": "/api/output/{filename}",
            "monthly_sku": "/api/sku/{sku}/monthly",
            "weekly_sku": "/api/sku/{sku}/weekly",
            "monthly_behavior": "/api/behavior/monthly",
            "categories": "/api/categories",
            "date_coverage": "/api/diagnostics/date-coverage",
            "webhook_trigger": "/api/webhook/trigger",
            "tunnel_status": "/api/tunnel/status",
            "tunnel_start": "/api/tunnel/start",
            "tunnel_stop": "/api/tunnel/stop",
        },
    }


def _schedule_webhook():
    if not WEBHOOK_URL:
        return
    delay = max(0, int(WEBHOOK_DELAY_SECONDS))
    t = threading.Timer(delay, lambda: send_webhook_get(WEBHOOK_URL))
    t.daemon = True
    t.start()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/run-stock-min", response_model=RunResponse)
def run_stock_min():
    try:
        data = load_all_data()
        result_df = process_data(
            inventory=data["inventory"],
            details=data["details"],
            quotes=data["quotes"],
            sales=data["sales"],
        )
        output_path = save_output(result_df, OUTPUT_DIR)
        _schedule_webhook()
        return RunResponse(
            success=True,
            message="Proceso ejecutado correctamente",
            output_file=str(output_path),
            rows_processed=int(len(result_df)),
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content=RunResponse(
                success=False,
                message=f"Error ejecutando proceso: {exc}\n{traceback.format_exc()}",
                output_file=None,
                rows_processed=0,
            ).model_dump(),
        )


@app.post("/api/webhook/trigger")
def trigger_webhook():
    if not WEBHOOK_URL:
        raise HTTPException(status_code=400, detail="WEBHOOK_URL no configurado")
    return send_webhook_get(WEBHOOK_URL)


_TUNNEL_CONTAINER_NAME = "stock-min-tunnel"
_TUNNEL_IMAGE = "cloudflare/cloudflared:latest"
_TUNNEL_TARGET_CONTAINER = "stock-min-service"
_TUNNEL_LOCAL_URL = "http://127.0.0.1:8000"
_TRYCLOUDFLARE_RE = re.compile(r"https://[a-z0-9-]+\.trycloudflare\.com")


def _extract_tunnel_url(logs: str) -> str | None:
    matches = _TRYCLOUDFLARE_RE.findall(logs or "")
    return matches[-1] if matches else None


def _tunnel_status_payload() -> dict[str, object]:
    if not docker_sock_available():
        return {
            "available": False,
            "running": False,
            "container": _TUNNEL_CONTAINER_NAME,
            "url": None,
        }

    container = docker_find_container_by_name(_TUNNEL_CONTAINER_NAME)
    if not container:
        return {
            "available": True,
            "running": False,
            "container": _TUNNEL_CONTAINER_NAME,
            "url": None,
        }

    container_id = container.get("Id")
    running = container.get("State") == "running"
    url = None
    if container_id and running:
        url = _extract_tunnel_url(docker_container_logs(container_id, tail=120))

    return {
        "available": True,
        "running": bool(running),
        "container": _TUNNEL_CONTAINER_NAME,
        "url": url,
    }


@app.get("/api/tunnel/status")
def tunnel_status():
    return _tunnel_status_payload()


@app.post("/api/tunnel/start")
def tunnel_start():
    if not docker_sock_available():
        raise HTTPException(status_code=501, detail="Docker socket no disponible en el contenedor")

    container = docker_find_container_by_name(_TUNNEL_CONTAINER_NAME)
    if container and container.get("Id"):
        docker_container_stop(container["Id"], timeout_seconds=5)
        docker_container_remove(container["Id"], force=True)

    docker_image_pull(_TUNNEL_IMAGE)
    container_id = docker_container_create_tunnel(
        name=_TUNNEL_CONTAINER_NAME,
        image=_TUNNEL_IMAGE,
        target_container=_TUNNEL_TARGET_CONTAINER,
        url=_TUNNEL_LOCAL_URL,
    )
    if not container_id:
        raise HTTPException(status_code=500, detail="No se pudo crear el contenedor del túnel")

    ok = docker_container_start(container_id)
    if not ok:
        raise HTTPException(status_code=500, detail="No se pudo iniciar el túnel")

    return _tunnel_status_payload()


@app.post("/api/tunnel/stop")
def tunnel_stop():
    if not docker_sock_available():
        raise HTTPException(status_code=501, detail="Docker socket no disponible en el contenedor")

    container = docker_find_container_by_name(_TUNNEL_CONTAINER_NAME)
    if not container or not container.get("Id"):
        return _tunnel_status_payload()

    docker_container_stop(container["Id"], timeout_seconds=10)
    return _tunnel_status_payload()

@app.get("/api/outputs")
def list_outputs():
    output_dir = Path(OUTPUT_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)
    files = sorted(output_dir.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    return {
        "files": [p.name for p in files],
        "latest": files[0].name if files else None,
    }


@app.get("/api/output/{filename}")
def get_output(filename: str):
    output_dir = Path(OUTPUT_DIR)
    output_path = (output_dir / filename).resolve()
    try:
        output_dir_resolved = output_dir.resolve()
    except FileNotFoundError:
        output_dir_resolved = output_dir

    if output_dir_resolved not in output_path.parents:
        raise HTTPException(status_code=400, detail="Filename inválido")
    if not output_path.exists() or not output_path.is_file():
        raise HTTPException(status_code=404, detail="Archivo no encontrado")

    df = pd.read_csv(output_path, encoding="utf-8-sig")
    raw_rows = df.to_dict(orient="records")

    def json_safe(value):
        if value is None:
            return None
        if isinstance(value, float):
            return value if math.isfinite(value) else None
        if isinstance(value, (pd.Timestamp, pd.Timedelta)):
            return str(value)
        if pd.isna(value):
            return None
        return value

    rows = [{k: json_safe(v) for k, v in row.items()} for row in raw_rows]
    return {
        "filename": output_path.name,
        "rows": rows,
        "columns": list(df.columns),
        "row_count": int(len(df)),
    }


@app.get("/api/sku/{sku}/monthly")
def get_sku_monthly(sku: str, months_back: int = 24):
    months_back = int(months_back)
    if months_back < 1 or months_back > 120:
        raise HTTPException(status_code=400, detail="months_back fuera de rango")

    data = load_all_data()
    series = monthly_demand_series(
        details=data["details"],
        quotes=data["quotes"],
        sku=sku,
        months_back=months_back,
    )

    if series.empty:
        return {"sku": sku, "points": []}

    return {
        "sku": sku,
        "points": [
            {"month": m.strftime("%Y-%m"), "quantity": float(q)}
            for m, q in zip(series["month"], series["quantity"])
        ],
    }


@app.get("/api/sku/{sku}/weekly")
def get_sku_weekly(sku: str, weeks_back: int = 26):
    weeks_back = int(weeks_back)
    if weeks_back < 1 or weeks_back > 520:
        raise HTTPException(status_code=400, detail="weeks_back fuera de rango")

    data = load_all_data()
    series = weekly_demand_series(
        details=data["details"],
        quotes=data["quotes"],
        sku=sku,
        weeks_back=weeks_back,
    )

    if series.empty:
        return {"sku": sku, "points": []}

    return {
        "sku": sku,
        "points": [
            {"week": w.strftime("%Y-%m-%d"), "quantity": float(q)}
            for w, q in zip(series["week"], series["quantity"])
        ],
    }


@app.get("/api/categories", response_model=CategoriesResponse)
def get_categories():
    data = load_all_data()
    inventory = data["inventory"]
    candidates = ["categoria", "category", "familia", "linea", "linea_producto"]
    category_col = next((c for c in candidates if c in inventory.columns), None)
    if not category_col:
        return CategoriesResponse(categories=[])
    cats = (
        inventory[category_col]
        .dropna()
        .astype(str)
        .map(lambda s: s.strip())
        .loc[lambda s: s.str.len() > 0]
        .unique()
        .tolist()
    )
    cats = sorted(cats)
    return CategoriesResponse(categories=cats)


@app.get("/api/behavior/monthly", response_model=MonthlyBehaviorResponse)
def get_monthly_behavior(
    sku: str | None = None,
    category: str | None = None,
    total_months: int = 12,
    history_months: int = 9,
    forecast_months: int = 3,
    harmonics: int = 2,
):
    data = load_all_data()
    try:
        payload = monthly_behavior_series(
            inventory=data["inventory"],
            details=data["details"],
            quotes=data["quotes"],
            sku=sku,
            category=category,
            total_months=int(total_months),
            history_months=int(history_months),
            forecast_months=int(forecast_months),
            harmonics=int(harmonics),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return MonthlyBehaviorResponse.model_validate(payload)



@app.get("/api/diagnostics/date-coverage")
def date_coverage():
    data = load_all_data()

    def dateish_columns(df: pd.DataFrame) -> list[str]:
        needles = ("fecha", "date", "time", "edited", "aprob", "created")
        return [c for c in df.columns if any(n in c.lower() for n in needles)]

    out = {}
    for key in ("details", "quotes", "sales", "inventory"):
        df = data.get(key)
        if df is None:
            continue
        cols = dateish_columns(df)
        out[key] = {
            "row_count": int(len(df)),
            "date_columns": cols,
            "raw_non_null": {c: int(df[c].notna().sum()) for c in cols},
            "parsed_non_null": {
                c: int(pd.to_datetime(df[c], errors="coerce").notna().sum()) for c in cols
            },
            "samples": {c: list(df[c].dropna().astype(str).head(3).values) for c in cols},
        }
    return out


@app.get("/ui", response_class=HTMLResponse)
def ui():
    html = """<!doctype html>
<html lang="es">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>RTB Stock Minimum Service</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
    <style>
      :root { --bg:#0b1020; --panel:#111836; --text:#e8ecff; --muted:#aab3d6; --border:#24305e; --accent:#7aa2ff; }
      body { margin:0; font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial; background: var(--bg); color: var(--text); }
      header { padding: 12px 14px; border-bottom: 1px solid var(--border); display:flex; align-items:center; justify-content:space-between; gap:16px; }
      header h1 { margin:0; font-size: 16px; font-weight: 700; letter-spacing: .2px; }
      main { padding: 12px 14px; display:grid; grid-template-columns: 1fr; gap: 14px; max-width: 100%; margin: 0 auto; }
      .row { display:flex; flex-wrap:wrap; gap:10px; align-items:center; }
      .panel { background: var(--panel); border: 1px solid var(--border); border-radius: 12px; padding: 12px; }
      label { font-size: 12px; color: var(--muted); }
      input, select, button { background:#0f1634; border:1px solid var(--border); color: var(--text); border-radius:10px; padding:10px 12px; font-size: 14px; }
      input { min-width: 0; }
      button { cursor:pointer; }
      button.primary { background: var(--accent); border-color: var(--accent); color:#0b1020; font-weight:700; }
      button:disabled { opacity: .6; cursor:not-allowed; }
      .stat { display:flex; flex-direction:column; gap:4px; padding: 10px 12px; border:1px solid var(--border); border-radius: 10px; background:#0f1634; min-width: 0; flex: 1 1 100%; }
      .stat .v { font-size: 18px; font-weight: 800; }
      .grid2 { display:grid; grid-template-columns: 1fr; gap: 14px; }
      table { width:100%; border-collapse: collapse; font-size: 13px; }
      th, td { text-align:left; padding: 10px 10px; border-bottom: 1px solid var(--border); vertical-align: top; }
      th { position: sticky; top: 0; background: var(--panel); z-index: 1; }
      .muted { color: var(--muted); }
      .pill { display:inline-block; padding: 3px 10px; border-radius: 999px; border:1px solid var(--border); background:#0f1634; }
      .scroll { max-height: 520px; overflow:auto; border-radius: 10px; border:1px solid var(--border); }
      .right { margin-left:auto; }
      canvas { display:block; }

      .controls > div { flex: 1 1 100%; min-width: 0; }
      .controls select, .controls input { width: 100%; }
      .actions { width: 100%; margin-left: 0; }
      .actions button { flex: 1 1 auto; }
      .tunnelRow .stat { flex: 0 0 auto; }
      .helpGrid { display:grid; grid-template-columns: 1fr; gap: 12px; margin-top: 10px; }
      .chartBox { position: relative; width: 100%; }
      .chartBoxSm { height: 210px; }
      .chartBoxLg { height: 260px; margin-top: 12px; }
      details { background:#0f1634; border:1px solid var(--border); border-radius: 10px; padding: 10px 12px; }
      summary { cursor:pointer; font-weight:700; }
      .kpi { display:grid; grid-template-columns: 1fr; gap: 8px; margin-top: 10px; }
      .kpi .item { border:1px solid var(--border); border-radius: 10px; background:#0f1634; padding: 10px 12px; }
      .kpi .item .t { color: var(--muted); font-size: 12px; }
      .kpi .item .d { margin-top: 4px; font-size: 13px; line-height: 1.4; }

      @media (min-width: 1024px) {
        header { padding: 16px 20px; }
        main { padding: 18px 20px; width: 100%; max-width: none; margin: 0; }
        .panel { padding: 14px; }
        .grid2 { grid-template-columns: 1fr 1fr; }
        .controls > div { flex: 1 1 240px; min-width: 220px; }
        .actions { width: auto; margin-left: auto; }
        .helpGrid { grid-template-columns: 1fr 1fr; }
        .statsRow .stat { flex: 1 1 160px; min-width: 160px; }
        .tunnelRow > div { width: auto; }
        .chartBoxSm { height: 240px; }
        .chartBoxLg { height: 320px; }
      }
    </style>
  </head>
  <body>
    <header>
      <h1>RTB Stock Minimum Service</h1>
      <div class="row">
        <a class="muted" href="/docs" target="_blank" rel="noreferrer">API Docs</a>
      </div>
    </header>
    <main>
      <section class="panel">
        <div class="row controls">
          <div>
            <label>Archivo de salida</label><br />
            <select id="outputSelect"></select>
          </div>
          <div>
            <label>Buscar por SKU</label><br />
            <input id="skuSearch" placeholder="Ej: HT31-41" />
          </div>
          <div class="right row actions">
            <button id="reloadBtn">Recargar</button>
            <button id="webhookBtn">Activar webhook</button>
            <button id="runBtn" class="primary">Ejecutar cálculo</button>
          </div>
        </div>
        <div class="row statsRow" style="margin-top:12px;">
          <div class="stat">
            <div class="muted">Rows (filtradas)</div>
            <div class="v" id="rowsCount">0</div>
          </div>
          <div class="stat">
            <div class="muted">Archivo</div>
            <div class="v" id="fileName">-</div>
          </div>
          <div class="stat">
            <div class="muted">SKU exacto</div>
            <div class="v" id="skuExact">-</div>
          </div>
          <div class="stat">
            <div class="muted">Stock mín. sugerido</div>
            <div class="v" id="stockMin">-</div>
          </div>
        </div>
        <div id="status" class="muted" style="margin-top:10px;"></div>
        <div class="row tunnelRow" style="margin-top:12px;">
          <div class="stat" style="min-width: 260px;">
            <div class="muted">Túnel (compartir UI)</div>
            <div class="v" id="tunnelState">-</div>
          </div>
          <div style="flex: 1; min-width: 260px;">
            <label>Link público</label><br />
            <input id="tunnelUrl" readonly placeholder="(vacío)" style="width:100%;" />
          </div>
          <div class="right row actions">
            <button id="tunnelRefreshBtn">Actualizar</button>
            <button id="tunnelStartBtn">Activar</button>
            <button id="tunnelStopBtn">Desactivar</button>
            <button id="tunnelCopyBtn">Copiar link</button>
          </div>
        </div>
      </section>

      <section class="grid2">
        <div class="panel">
          <div class="row">
            <div class="pill">Rotación (conteo)</div>
          </div>
          <div class="chartBox chartBoxSm">
            <canvas id="rotationChart"></canvas>
          </div>
        </div>
        <div class="panel">
          <div class="row">
            <div class="pill">Top 10 stock mínimo sugerido</div>
          </div>
          <div class="chartBox chartBoxSm">
            <canvas id="topStockChart"></canvas>
          </div>
        </div>
      </section>

      <section class="panel">
        <div class="row controls">
          <div class="pill">Comportamiento mensual (SKU)</div>
          <div id="monthlyInfo" class="muted"></div>
          <div class="right row actions">
            <div>
              <label class="muted">Categoría</label><br />
              <select id="categorySelect"></select>
            </div>
            <div>
              <label class="muted">Periodo</label><br />
              <select id="periodSelect">
                <option value="monthly" selected>Mes</option>
                <option value="weekly">Semana</option>
              </select>
            </div>
          </div>
        </div>
        <div class="chartBox chartBoxLg">
          <canvas id="monthlyChart"></canvas>
        </div>
      </section>

      <section class="panel">
        <div class="row">
          <div class="pill">Guía: cómo interpretar y usar los datos</div>
        </div>
        <div class="helpGrid">
          <details>
            <summary>Cómo leer las gráficas</summary>
            <div class="kpi">
              <div class="item">
                <div class="t">Mes</div>
                <div class="d">Línea sólida = datos observados. Línea punteada = estimado (pronóstico).</div>
              </div>
              <div class="item">
                <div class="t">Series mostradas</div>
                <div class="d">SKU (si es exacto), Promedio de categoría y Promedio general. Los promedios se calculan por SKU (promedio de demanda mensual entre SKUs).</div>
              </div>
              <div class="item">
                <div class="t">Por qué el promedio de categoría puede coincidir con el SKU</div>
                <div class="d">Si en esa ventana la categoría tiene 1 SKU activo (o los demás tienen 0), el promedio de categoría puede ser igual al SKU.</div>
              </div>
              <div class="item">
                <div class="t">Pronóstico y error</div>
                <div class="d">El estimado usa un modelo Fourier con estacionalidad anual. El rango aceptable se acota a ±5%–±10% para cada punto pronosticado.</div>
              </div>
            </div>
          </details>
          <details>
            <summary>Cómo usarlo para decisiones</summary>
            <div class="kpi">
              <div class="item">
                <div class="t">Stock mínimo sugerido</div>
                <div class="d">Úsalo como referencia de reorden considerando lead time y rotación. Si la rotación es “Nula”, el SKU requiere revisión manual.</div>
              </div>
              <div class="item">
                <div class="t">SKU exacto</div>
                <div class="d">Para ver la serie del SKU, escribe exactamente el SKU (la UI marca “SKU exacto”). Si no es exacto, se muestran promedios.</div>
              </div>
              <div class="item">
                <div class="t">Categoría</div>
                <div class="d">Si no hay SKU, selecciona una categoría para ver el comportamiento promedio de esa categoría más el promedio general.</div>
              </div>
              <div class="item">
                <div class="t">Semana vs Mes</div>
                <div class="d">Semana: detalle fino de movimiento reciente. Mes: tendencia + comparación contra promedios + pronóstico.</div>
              </div>
            </div>
          </details>
        </div>
      </section>

      <section class="panel">
        <div class="row">
          <div class="pill">Tabla (filtrable por SKU)</div>
        </div>
        <div class="scroll" style="margin-top:12px;">
          <table>
            <thead id="thead"></thead>
            <tbody id="tbody"></tbody>
          </table>
        </div>
      </section>
    </main>

    <script>
      const state = {
        allRows: [],
        filteredRows: [],
        columns: [],
        filename: null,
        charts: { rotation: null, topStock: null, monthly: null },
        monthlySku: null,
        categoriesLoaded: false,
      };

      const el = (id) => document.getElementById(id);
      const setStatus = (msg) => { el("status").textContent = msg || ""; };

      function safeNumber(v) {
        if (v === null || v === undefined) return null;
        const n = Number(v);
        return Number.isFinite(n) ? n : null;
      }

      function renderTable() {
        const thead = el("thead");
        const tbody = el("tbody");
        thead.innerHTML = "";
        tbody.innerHTML = "";

        const cols = state.columns;
        const headRow = document.createElement("tr");
        for (const c of cols) {
          const th = document.createElement("th");
          th.textContent = c;
          headRow.appendChild(th);
        }
        thead.appendChild(headRow);

        const rows = state.filteredRows;
        for (const r of rows) {
          const tr = document.createElement("tr");
          for (const c of cols) {
            const td = document.createElement("td");
            const v = r[c];
            td.textContent = v === null || v === undefined ? "" : String(v);
            tr.appendChild(td);
          }
          tbody.appendChild(tr);
        }
      }

      function applyFilter() {
        const q = (el("skuSearch").value || "").trim().toLowerCase();
        if (!q) {
          state.filteredRows = state.allRows.slice();
        } else {
          state.filteredRows = state.allRows.filter(r => String(r.sku || "").toLowerCase().includes(q));
        }
        el("rowsCount").textContent = String(state.filteredRows.length);
        renderTable();
        renderCharts();
        renderSkuStats();
      }

      function renderSkuStats() {
        const q = (el("skuSearch").value || "").trim();
        const period = el("periodSelect").value || "monthly";
        const exact = q && state.filteredRows.length === 1 && String(state.filteredRows[0].sku || "") === q;
        if (!q) {
          el("skuExact").textContent = "-";
          el("stockMin").textContent = "-";
          if (period === "weekly") {
            el("monthlyInfo").textContent = "Busca un SKU para ver la serie semanal.";
            state.charts.monthly = destroyChart(state.charts.monthly);
            state.monthlySku = null;
          } else {
            loadMonthlyBehavior(null);
          }
          return;
        }
        if (exact) {
          const r = state.filteredRows[0];
          el("skuExact").textContent = String(r.sku || "-");
          const sm = r.stock_minimo_sugerido_auto ?? r.stock_minimo_sugerido ?? null;
          el("stockMin").textContent = sm === null || sm === undefined ? "-" : String(sm);
          if (period === "weekly") loadSeriesWeekly(String(r.sku || ""));
          else loadMonthlyBehavior(String(r.sku || ""));
        } else {
          el("skuExact").textContent = "No exacto";
          el("stockMin").textContent = "-";
          if (period === "weekly") {
            el("monthlyInfo").textContent = "Escribe un SKU exacto para ver la serie semanal.";
            state.charts.monthly = destroyChart(state.charts.monthly);
            state.monthlySku = null;
          } else {
            loadMonthlyBehavior(null);
          }
        }
      }

      function destroyChart(chart) {
        if (chart) chart.destroy();
        return null;
      }

      function renderCharts() {
        const rows = state.filteredRows;
        const rotationCounts = new Map();
        for (const r of rows) {
          const rot = String(r.clasificacion_rotacion_calculada || "N/D");
          rotationCounts.set(rot, (rotationCounts.get(rot) || 0) + 1);
        }
        const rotLabels = Array.from(rotationCounts.keys());
        const rotData = rotLabels.map(l => rotationCounts.get(l));

        state.charts.rotation = destroyChart(state.charts.rotation);
        state.charts.rotation = new Chart(el("rotationChart"), {
          type: "bar",
          data: { labels: rotLabels, datasets: [{ label: "SKUs", data: rotData, backgroundColor: "#7aa2ff" }] },
          options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { y: { beginAtZero: true } } }
        });

        const rowsWithStock = rows
          .map(r => ({ sku: String(r.sku || ""), v: safeNumber(r.stock_minimo_sugerido_auto ?? r.stock_minimo_sugerido ?? null) }))
          .filter(x => x.sku && x.v !== null)
          .sort((a,b) => b.v - a.v)
          .slice(0, 10);

        const topLabels = rowsWithStock.map(x => x.sku);
        const topData = rowsWithStock.map(x => x.v);

        state.charts.topStock = destroyChart(state.charts.topStock);
        state.charts.topStock = new Chart(el("topStockChart"), {
          type: "bar",
          data: { labels: topLabels, datasets: [{ label: "Stock mín", data: topData, backgroundColor: "#a6ffcb" }] },
          options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { y: { beginAtZero: true } } }
        });
      }

      async function loadSeriesWeekly(sku) {
        if (!sku) return;
        const key = `${sku}::weekly`;
        if (state.monthlySku === key) return;
        state.monthlySku = key;
        el("monthlyInfo").textContent = "Cargando serie...";
        try {
          const url = `/api/sku/${encodeURIComponent(sku)}/weekly?weeks_back=26`;
          const data = await fetchJson(url);
          const points = (data.points || []);
          if (!points.length) {
            state.charts.monthly = destroyChart(state.charts.monthly);
            try {
              const diag = await fetchJson("/api/diagnostics/date-coverage");
              const d = diag.details || { row_count: 0, date_columns: [] , non_null: {} };
              const q = diag.quotes || { row_count: 0, date_columns: [] , non_null: {} };
              const parts = [];
              if (!d.date_columns || !d.date_columns.length) {
                parts.push(`detalle_cotizacion: sin columnas de fecha (${d.row_count} filas)`);
              } else {
                parts.push(`detalle_cotizacion: ${d.date_columns.map(c => `${c} parseable ${diag.details.parsed_non_null[c]||0}/${d.row_count}`).join(", ")}`);
              }
              if (!q.date_columns || !q.date_columns.length) {
                parts.push(`cotizaciones_clientes: sin columnas de fecha (${q.row_count} filas)`);
              } else {
                parts.push(`cotizaciones_clientes: ${q.date_columns.map(c => `${c} parseable ${diag.quotes.parsed_non_null[c]||0}/${q.row_count}`).join(", ")}`);
              }
              el("monthlyInfo").textContent = `Sin fechas para construir la serie mensual. ${parts.join(" | ")}`;
            } catch (e) {
              el("monthlyInfo").textContent = "No hay fechas disponibles para construir la serie mensual (revisa columnas de fecha en los CSV).";
            }
            return;
          }

          const labels = points.map(p => p.month || p.week);
          const values = points.map(p => Number(p.quantity || 0));

          state.charts.monthly = destroyChart(state.charts.monthly);
          state.charts.monthly = new Chart(el("monthlyChart"), {
            type: "line",
            data: {
              labels,
              datasets: [{
                label: "Cantidad empacada (semanal)",
                data: values,
                borderColor: "#7aa2ff",
                backgroundColor: "rgba(122,162,255,0.15)",
                fill: true,
                tension: 0.25,
                pointRadius: 2,
              }]
            },
            options: {
              responsive: true,
              maintainAspectRatio: false,
              plugins: { legend: { display: true } },
              scales: { y: { beginAtZero: true } }
            }
          });
          el("monthlyInfo").textContent = `SKU: ${sku} (últimas 26 semanas)`;
        } catch (e) {
          state.charts.monthly = destroyChart(state.charts.monthly);
          el("monthlyInfo").textContent = `Error cargando serie semanal: ${e.message}`;
        }
      }

      function renderMonthlyBehavior(payload) {
        const series = payload.series || [];
        if (!series.length) {
          state.charts.monthly = destroyChart(state.charts.monthly);
          el("monthlyInfo").textContent = "Sin datos para construir la serie mensual.";
          return;
        }

        const labels = (series[0].points || []).map(p => p.month);
        const palette = ["#7aa2ff", "#a6ffcb", "#ffb86b", "#ff6bcb"];

        const datasets = series.map((s, idx) => {
          const points = s.points || [];
          const values = points.map(p => Number(p.quantity || 0));
          const forecastStart = points.findIndex(p => p.kind === "forecast");
          const dashFrom = forecastStart >= 0 ? forecastStart : 10 ** 9;
          const color = palette[idx % palette.length];
          const bg = color.replace("#", "");
          const r = parseInt(bg.slice(0,2), 16), g = parseInt(bg.slice(2,4), 16), b = parseInt(bg.slice(4,6), 16);
          return {
            label: s.label || s.key,
            data: values,
            borderColor: color,
            backgroundColor: `rgba(${r},${g},${b},0.12)`,
            fill: false,
            tension: 0.25,
            pointRadius: 2,
            segment: {
              borderDash: (ctx) => (ctx.p0DataIndex >= dashFrom ? [6,4] : undefined),
            },
          };
        });

        state.charts.monthly = destroyChart(state.charts.monthly);
        state.charts.monthly = new Chart(el("monthlyChart"), {
          type: "line",
          data: { labels, datasets },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: true } },
            scales: { y: { beginAtZero: true } }
          }
        });

        const sku = payload.sku || null;
        const cat = payload.category || null;
        const w0 = labels[0] || "-";
        const w1 = labels[labels.length - 1] || "-";
        const errs = series.map(s => s.error_pct).filter(v => typeof v === "number");
        const err = errs.length ? Math.max(...errs) : null;
        const errTxt = err !== null ? ` | Pronóstico: Fourier ±${Math.round(err * 100)}%` : "";
        if (sku) el("monthlyInfo").textContent = `SKU: ${sku}${cat ? ` | ${cat}` : ""} | Ventana: ${w0} → ${w1}${errTxt}`;
        else el("monthlyInfo").textContent = `Promedio${cat ? ` | ${cat}` : ""} | Ventana: ${w0} → ${w1}${errTxt}`;
      }

      async function loadMonthlyBehavior(sku) {
        const period = el("periodSelect").value || "monthly";
        if (period !== "monthly") return;
        const category = (el("categorySelect").value || "").trim() || null;
        const key = `${sku || ""}::${category || ""}::monthly`;
        if (state.monthlySku === key) return;
        state.monthlySku = key;
        el("monthlyInfo").textContent = "Cargando serie mensual...";
        try {
          const qs = new URLSearchParams();
          if (sku) qs.set("sku", sku);
          if (!sku && category) qs.set("category", category);
          qs.set("total_months", "12");
          qs.set("history_months", "9");
          qs.set("forecast_months", "3");
          qs.set("harmonics", "2");
          const data = await fetchJson(`/api/behavior/monthly?${qs.toString()}`);
          renderMonthlyBehavior(data);
        } catch (e) {
          state.charts.monthly = destroyChart(state.charts.monthly);
          el("monthlyInfo").textContent = `Error cargando serie mensual: ${e.message}`;
        }
      }

      async function fetchJson(url, opts) {
        const res = await fetch(url, opts);
        if (!res.ok) {
          const txt = await res.text();
          throw new Error(txt || `HTTP ${res.status}`);
        }
        return await res.json();
      }

      async function loadOutputsList() {
        const data = await fetchJson("/api/outputs");
        const sel = el("outputSelect");
        sel.innerHTML = "";
        for (const f of data.files) {
          const opt = document.createElement("option");
          opt.value = f;
          opt.textContent = f;
          sel.appendChild(opt);
        }
        if (data.latest) sel.value = data.latest;
        return data.latest;
      }

      async function loadOutput(filename) {
        setStatus("Cargando archivo...");
        const data = await fetchJson(`/api/output/${encodeURIComponent(filename)}`);
        state.filename = data.filename;
        state.columns = data.columns;
        state.allRows = data.rows;
        el("fileName").textContent = data.filename;
        setStatus("");
        applyFilter();
      }

      async function runCalculation() {
        const btn = el("runBtn");
        btn.disabled = true;
        try {
          setStatus("Ejecutando cálculo...");
          const data = await fetchJson("/run-stock-min", { method: "POST" });
          if (!data.success) throw new Error(data.message || "Error");
          await loadOutputsList();
          const latest = (await fetchJson("/api/outputs")).latest;
          if (latest) await loadOutput(latest);
          setStatus("Cálculo ejecutado y archivo cargado. Webhook programado automáticamente.");
        } finally {
          btn.disabled = false;
        }
      }

      async function triggerWebhook() {
        const btn = el("webhookBtn");
        btn.disabled = true;
        try {
          setStatus("Enviando webhook...");
          const data = await fetchJson("/api/webhook/trigger", { method: "POST" });
          const code = data.status_code ?? 0;
          setStatus(`Webhook enviado. Status: ${code}`);
        } catch (e) {
          setStatus(`Error enviando webhook: ${e.message}`);
        } finally {
          btn.disabled = false;
        }
      }

      function copyText(text) {
        if (!text) return false;
        if (navigator.clipboard && window.isSecureContext) {
          navigator.clipboard.writeText(text);
          return true;
        }
        const ta = document.createElement("textarea");
        ta.value = text;
        document.body.appendChild(ta);
        ta.select();
        document.execCommand("copy");
        document.body.removeChild(ta);
        return true;
      }

      function setTunnelUI(data) {
        const available = !!data.available;
        const running = !!data.running;
        const url = data.url || "";
        el("tunnelState").textContent = available ? (running ? "Activo" : "Inactivo") : "No disponible";
        el("tunnelUrl").value = url;
        el("tunnelStartBtn").disabled = !available || running;
        el("tunnelStopBtn").disabled = !available || !running;
        el("tunnelCopyBtn").disabled = !url;
      }

      async function loadTunnelStatus() {
        try {
          const data = await fetchJson("/api/tunnel/status");
          setTunnelUI(data);
        } catch (e) {
          el("tunnelState").textContent = "Error";
        }
      }

      async function startTunnel() {
        const btn = el("tunnelStartBtn");
        btn.disabled = true;
        try {
          setStatus("Activando túnel...");
          const data = await fetchJson("/api/tunnel/start", { method: "POST" });
          setTunnelUI(data);
          setStatus(data.url ? "Túnel activo." : "Túnel iniciado (espera a que aparezca el link).");
        } catch (e) {
          setStatus(`Error activando túnel: ${e.message}`);
        } finally {
          btn.disabled = false;
        }
      }

      async function stopTunnel() {
        const btn = el("tunnelStopBtn");
        btn.disabled = true;
        try {
          setStatus("Desactivando túnel...");
          const data = await fetchJson("/api/tunnel/stop", { method: "POST" });
          setTunnelUI(data);
          setStatus("Túnel desactivado.");
        } catch (e) {
          setStatus(`Error desactivando túnel: ${e.message}`);
        } finally {
          btn.disabled = false;
        }
      }

      function copyTunnelLink() {
        const url = el("tunnelUrl").value || "";
        if (!url) return;
        copyText(url);
        setStatus("Link copiado.");
      }

      el("skuSearch").addEventListener("input", () => applyFilter());
      el("outputSelect").addEventListener("change", async (e) => loadOutput(e.target.value));
      el("reloadBtn").addEventListener("click", async () => {
        const latest = await loadOutputsList();
        const sel = el("outputSelect");
        const chosen = sel.value || latest;
        if (chosen) await loadOutput(chosen);
      });
      el("runBtn").addEventListener("click", async () => runCalculation());
      el("webhookBtn").addEventListener("click", async () => triggerWebhook());
      el("tunnelRefreshBtn").addEventListener("click", async () => loadTunnelStatus());
      el("tunnelStartBtn").addEventListener("click", async () => startTunnel());
      el("tunnelStopBtn").addEventListener("click", async () => stopTunnel());
      el("tunnelCopyBtn").addEventListener("click", () => copyTunnelLink());
      el("periodSelect").addEventListener("change", () => renderSkuStats());
      el("categorySelect").addEventListener("change", () => renderSkuStats());

      async function loadCategories() {
        try {
          const data = await fetchJson("/api/categories");
          const cats = data.categories || [];
          const sel = el("categorySelect");
          sel.innerHTML = "";
          for (const c of cats) {
            const opt = document.createElement("option");
            opt.value = c;
            opt.textContent = c;
            sel.appendChild(opt);
          }
          if (cats.length && !sel.value) sel.value = cats[0];
          state.categoriesLoaded = true;
          renderSkuStats();
        } catch (e) {
          state.categoriesLoaded = true;
        }
      }

      (async () => {
        try {
          const latest = await loadOutputsList();
          const sel = el("outputSelect");
          const chosen = sel.value || latest;
          if (chosen) await loadOutput(chosen);
          else setStatus("No hay archivos de salida todavía. Ejecuta el cálculo.");
          await loadCategories();
          await loadTunnelStatus();
          setInterval(loadTunnelStatus, 5000);
        } catch (e) {
          setStatus(`Error cargando UI: ${e.message}`);
        }
      })();
    </script>
  </body>
</html>"""
    return HTMLResponse(content=html)


def run_cli() -> str:
    data = load_all_data()
    result_df = process_data(
        inventory=data["inventory"],
        details=data["details"],
        quotes=data["quotes"],
        sales=data["sales"],
    )
    output_path = save_output(result_df, OUTPUT_DIR)
    return str(output_path)


if __name__ == "__main__":
    print(run_cli())
