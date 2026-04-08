from typing import Literal
from pydantic import BaseModel


class RunResponse(BaseModel):
    success: bool
    message: str
    output_file: str | None = None
    rows_processed: int = 0


class MonthlyBehaviorPoint(BaseModel):
    month: str
    quantity: float
    kind: Literal["actual", "forecast"]
    low: float | None = None
    high: float | None = None


class MonthlyBehaviorSeries(BaseModel):
    key: str
    label: str
    model: str | None = None
    error_pct: float | None = None
    first_month: str | None = None
    last_month_observed: str | None = None
    points: list[MonthlyBehaviorPoint]


class MonthlyBehaviorResponse(BaseModel):
    sku: str | None = None
    category: str | None = None
    available_categories: list[str] = []
    series: list[MonthlyBehaviorSeries] = []


class CategoriesResponse(BaseModel):
    categories: list[str] = []
