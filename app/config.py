import os
from pathlib import Path

BASE_DIR = Path("/app") if Path("/app").exists() else Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
INPUT_DIR = DATA_DIR / "input"
OUTPUT_DIR = DATA_DIR / "output"

FILES = {
    "inventory": INPUT_DIR / "gestion_inventario.csv",
    "details": INPUT_DIR / "detalle_cotizacion.csv",
    "quotes": INPUT_DIR / "cotizaciones_clientes.csv",
    "sales": INPUT_DIR / "reporte_ventas.csv",
}

ROTATION_FACTORS = {
    "Alta": 1.40,
    "Media": 1.25,
    "Baja": 1.10,
    "Nula": 0.00,
}

DEFAULT_LEAD_TIME_DAYS = 7

ROTATION_RULES = {
    "Alta": {"d90": 10, "d180": 20},
    "Media": {"d90": 3, "d180": 6},
    "Baja": {"d90": 1, "d180": 1},
}

WEBHOOK_URL = os.getenv(
    "WEBHOOK_URL",
    "https://sistemas-rtb.app.n8n.cloud/webhook/d11d8d55-4629-44a0-9979-4ce1649d87b5",
)
WEBHOOK_DELAY_SECONDS = int(os.getenv("WEBHOOK_DELAY_SECONDS", "180"))
