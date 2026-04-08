from __future__ import annotations

import re
import pandas as pd
from app.config import FILES
from app.utils import normalize_columns


_ASSIGNMENT_RE = re.compile(r"^assignments\.(\d+)\.(name|value|type)$")


def _maybe_parse_assignments_export(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_columns(df)

    assignment_cols: dict[int, dict[str, str]] = {}
    passthrough_cols: list[str] = []

    for col in df.columns:
        match = _ASSIGNMENT_RE.match(col)
        if match:
            idx = int(match.group(1))
            kind = match.group(2)
            assignment_cols.setdefault(idx, {})[kind] = col
        else:
            passthrough_cols.append(col)

    if not assignment_cols:
        return df

    ordered_idxs = sorted(assignment_cols.keys())
    records: list[dict[str, object]] = []

    for _, row in df.iterrows():
        record: dict[str, object] = {}

        for col in passthrough_cols:
            value = row.get(col)
            if pd.isna(value):
                continue
            record[col] = value

        for idx in ordered_idxs:
            cols = assignment_cols[idx]
            name_col = cols.get("name")
            value_col = cols.get("value")
            if not name_col or not value_col:
                continue

            raw_name = row.get(name_col)
            if pd.isna(raw_name):
                continue

            key = str(raw_name).strip()
            if not key:
                continue

            raw_value = row.get(value_col)
            if isinstance(raw_value, str):
                record[key] = raw_value.strip()
            else:
                record[key] = raw_value

        records.append(record)

    return normalize_columns(pd.DataFrame.from_records(records))


def load_csv(path) -> pd.DataFrame:
    df = pd.read_csv(path, encoding="utf-8-sig", engine="python")
    return _maybe_parse_assignments_export(df)


def load_all_data() -> dict[str, pd.DataFrame]:
    return {
        "inventory": load_csv(FILES["inventory"]),
        "details": load_csv(FILES["details"]),
        "quotes": load_csv(FILES["quotes"]),
        "sales": load_csv(FILES["sales"]),
    }
