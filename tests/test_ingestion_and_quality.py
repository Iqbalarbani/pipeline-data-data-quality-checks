"""Unit and integration tests for Phase 2 ingestion, Pandera validation, and quarantine."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import httpx
import pandas as pd
import pytest

from src.config import Settings
from src.ingestion.fetcher import fetch_food_prices
from src.ingestion.pipeline import run_ingestion
from src.logging_utils import get_logger
from src.quality.validator import validate_food_prices


def test_validate_food_prices_separates_valid_and_quarantine() -> None:
    """Clean rows pass validation while out-of-range and null key rows go to quarantine."""
    raw_df = pd.DataFrame(
        [
            {
                "tanggal": "2026-03-01",
                "komoditas": "Beras Medium",
                "provinsi": "DKI Jakarta",
                "harga": 14200,
                "satuan": "Rp/kg",
                "sumber": "Panel Harga BPN",
            },
            {
                "tanggal": "2026-03-01",
                "komoditas": "Cabai Rawit Merah",
                "provinsi": "DKI Jakarta",
                "harga": -2500,
                "satuan": "Rp/kg",
                "sumber": "Panel Harga BPN",
            },
            {
                "tanggal": "2026-03-01",
                "komoditas": None,
                "provinsi": "Jawa Barat",
                "harga": 15000,
                "satuan": "Rp/kg",
                "sumber": "Panel Harga BPN",
            },
            {
                "tanggal": "2026-03-01",
                "komoditas": "Bawang Merah",
                "provinsi": "Jawa Timur",
                "harga": 9_500_000,
                "satuan": "Rp/kg",
                "sumber": "Panel Harga BPN",
            },
        ]
    )

    outcome = validate_food_prices(raw_df, run_id="test_run_01")

    assert len(outcome.valid_df) == 1
    assert outcome.valid_df.iloc[0]["komoditas"] == "Beras Medium"

    assert len(outcome.quarantine_df) == 3
    assert list(outcome.quarantine_df["row_index"]) == [1, 2, 3]
    assert "harga" in outcome.quarantine_df.iloc[0]["failure_reason"]
    assert "komoditas" in outcome.quarantine_df.iloc[1]["failure_reason"]
    assert "harga" in outcome.quarantine_df.iloc[2]["failure_reason"]
    payload = json.loads(outcome.quarantine_df.iloc[0]["raw_payload"])
    assert payload["harga"] == -2500


def test_fetch_food_prices_retries_and_falls_back_to_csv(tmp_path: Path) -> None:
    """When public API returns 503 across retries, fetcher falls back to sample CSV."""
    sample_csv = tmp_path / "sample.csv"
    sample_csv.write_text(
        "tanggal,komoditas,provinsi,harga,satuan,sumber\n"
        "2026-03-01,Beras Medium,DI Yogyakarta,13500,Rp/kg,Sample\n",
        encoding="utf-8",
    )

    attempts = {"count": 0}

    def handler(_request: httpx.Request) -> httpx.Response:
        attempts["count"] += 1
        return httpx.Response(503, json={"error": "service unavailable"})

    mock_client = httpx.Client(transport=httpx.MockTransport(handler))
    settings = Settings(
        duckdb_path=tmp_path / "wh.duckdb",
        source_mode="auto",
        source_url="https://example.invalid/prices",
        sample_csv_path=sample_csv,
        max_retries=2,
        backoff_seconds=0.0,
        timeout_seconds=2.0,
        log_level="WARNING",
    )
    logger = get_logger("test.fetcher", level="WARNING")

    result = fetch_food_prices(settings, logger=logger, client=mock_client)

    assert attempts["count"] == 2
    assert result.fallback_triggered is True
    assert result.source_used == f"fallback:{sample_csv}"
    assert len(result.df) == 1


def test_run_ingestion_is_idempotent_and_records_pipeline_runs(tmp_path: Path) -> None:
    """Running ingestion twice on sample data inserts once and skips duplicates on rerun."""
    db_path = tmp_path / "harga_pangan.duckdb"
    settings = Settings(
        duckdb_path=db_path,
        source_mode="sample",
        source_url="https://example.invalid/prices",
        sample_csv_path=Path("data/sample/harga_pangan_sample.csv"),
        max_retries=1,
        backoff_seconds=0.0,
        timeout_seconds=2.0,
        log_level="WARNING",
    )

    first = run_ingestion(settings=settings)
    assert first.status == "SUCCESS"
    assert first.rows_fetched == 183
    assert first.rows_inserted == 180
    assert first.rows_duplicate_skipped == 0
    assert first.rows_quarantined == 3

    second = run_ingestion(settings=settings)
    assert second.status == "SUCCESS"
    assert second.rows_fetched == 183
    assert second.rows_inserted == 0
    assert second.rows_duplicate_skipped == 180
    assert second.rows_quarantined == 3

    with duckdb.connect(str(db_path)) as conn:
        raw_count = conn.execute("SELECT COUNT(*) FROM raw_harga_pangan").fetchone()[0]
        quarantine_count = conn.execute("SELECT COUNT(*) FROM quarantine_harga_pangan").fetchone()[
            0
        ]
        runs_count = conn.execute("SELECT COUNT(*) FROM pipeline_runs").fetchone()[0]

    assert raw_count == 180
    assert quarantine_count == 6
    assert runs_count == 2


def test_run_ingestion_records_failed_status_on_missing_sample(tmp_path: Path) -> None:
    """When ingestion fails, pipeline_runs records FAILED status with error details."""
    db_path = tmp_path / "harga_pangan.duckdb"
    settings = Settings(
        duckdb_path=db_path,
        source_mode="sample",
        source_url="https://example.invalid/prices",
        sample_csv_path=tmp_path / "nonexistent.csv",
        max_retries=1,
        backoff_seconds=0.0,
        timeout_seconds=2.0,
        log_level="WARNING",
    )

    with pytest.raises(FileNotFoundError):
        run_ingestion(settings=settings)

    with duckdb.connect(str(db_path)) as conn:
        row = conn.execute(
            "SELECT status, error_message FROM pipeline_runs ORDER BY started_at DESC LIMIT 1"
        ).fetchone()

    assert row is not None
    assert row[0] == "FAILED"
    assert "nonexistent.csv" in row[1]
