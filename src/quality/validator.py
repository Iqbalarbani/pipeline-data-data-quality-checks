"""Validation runner separating clean records from quarantined rows."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pandas as pd
import pandera.errors as pa_errors

from src.quality.schema import FoodPriceRawSchema

REQUIRED_COLUMNS = ("tanggal", "komoditas", "provinsi", "harga", "satuan", "sumber")


@dataclass(frozen=True)
class ValidationOutcome:
    """Container holding valid rows and quarantined rows with failure reasons."""

    valid_df: pd.DataFrame
    quarantine_df: pd.DataFrame


def _normalize_input_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure expected columns exist and preserve original index for failure mapping."""
    working = df.copy().reset_index(drop=True)
    for col in REQUIRED_COLUMNS:
        if col not in working.columns:
            working[col] = "Rp/kg" if col == "satuan" else None
    working["satuan"] = working["satuan"].fillna("Rp/kg")
    return working


def _serialize_row(row: pd.Series) -> str:
    """Serialize a raw record to JSON for auditability in quarantine."""
    cleaned: dict[str, Any] = {}
    for key, val in row.items():
        if pd.isna(val):
            cleaned[str(key)] = None
        elif isinstance(val, pd.Timestamp):
            cleaned[str(key)] = val.isoformat()
        else:
            cleaned[str(key)] = val
    return json.dumps(cleaned, default=str, ensure_ascii=False)


def validate_food_prices(df: pd.DataFrame, run_id: str) -> ValidationOutcome:
    """Validate incoming DataFrame against FoodPriceRawSchema with lazy failure capture."""
    quarantine_cols = [
        "quarantined_at",
        "run_id",
        "row_index",
        "failure_reason",
        "raw_payload",
    ]
    if df.empty:
        return ValidationOutcome(
            valid_df=pd.DataFrame(columns=list(REQUIRED_COLUMNS)),
            quarantine_df=pd.DataFrame(columns=quarantine_cols),
        )

    working = _normalize_input_columns(df)
    reasons_by_idx: dict[int, list[str]] = {}

    try:
        validated = FoodPriceRawSchema.validate(working, lazy=True)
        valid_df = validated.loc[:, list(REQUIRED_COLUMNS)].copy()
        valid_df["tanggal"] = pd.to_datetime(valid_df["tanggal"]).dt.date
        valid_df["komoditas"] = valid_df["komoditas"].astype(str).str.strip()
        valid_df["provinsi"] = valid_df["provinsi"].astype(str).str.strip()
        valid_df["satuan"] = valid_df["satuan"].astype(str).str.strip()
        valid_df["sumber"] = valid_df["sumber"].astype(str).str.strip()
        valid_df["harga"] = valid_df["harga"].astype(float)
        return ValidationOutcome(
            valid_df=valid_df.reset_index(drop=True),
            quarantine_df=pd.DataFrame(columns=quarantine_cols),
        )
    except pa_errors.SchemaErrors as exc:
        failure_cases = exc.failure_cases
        for _, case in failure_cases.iterrows():
            idx = case.get("index")
            col = case.get("column") or "schema"
            check = case.get("check") or "validation_error"
            bad_val = case.get("failure_case")
            reason = f"{col}: {check} (value={bad_val!r})"
            if idx is None or pd.isna(idx):
                for row_idx in range(len(working)):
                    reasons_by_idx.setdefault(row_idx, []).append(reason)
            else:
                row_idx = int(idx)
                reasons_by_idx.setdefault(row_idx, []).append(reason)

    failed_indices = sorted(reasons_by_idx.keys())
    valid_mask = ~working.index.isin(failed_indices)
    clean_candidates = working.loc[valid_mask].copy()

    if not clean_candidates.empty:
        valid_df = FoodPriceRawSchema.validate(clean_candidates, lazy=False)
        valid_df = valid_df.loc[:, list(REQUIRED_COLUMNS)].copy()
        valid_df["tanggal"] = pd.to_datetime(valid_df["tanggal"]).dt.date
        valid_df["komoditas"] = valid_df["komoditas"].astype(str).str.strip()
        valid_df["provinsi"] = valid_df["provinsi"].astype(str).str.strip()
        valid_df["satuan"] = valid_df["satuan"].astype(str).str.strip()
        valid_df["sumber"] = valid_df["sumber"].astype(str).str.strip()
        valid_df["harga"] = valid_df["harga"].astype(float)
        valid_df = valid_df.reset_index(drop=True)
    else:
        valid_df = pd.DataFrame(columns=list(REQUIRED_COLUMNS))

    now_utc = datetime.now(UTC)
    quarantine_records: list[dict[str, Any]] = []
    for idx in failed_indices:
        unique_reasons = list(dict.fromkeys(reasons_by_idx[idx]))
        quarantine_records.append(
            {
                "quarantined_at": now_utc,
                "run_id": run_id,
                "row_index": idx,
                "failure_reason": "; ".join(unique_reasons),
                "raw_payload": _serialize_row(working.loc[idx]),
            }
        )

    quarantine_df = pd.DataFrame(quarantine_records, columns=quarantine_cols)
    return ValidationOutcome(valid_df=valid_df, quarantine_df=quarantine_df)
