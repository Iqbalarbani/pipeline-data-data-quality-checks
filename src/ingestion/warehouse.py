"""DuckDB storage layer for idempotent raw ingestion, quarantine, and run observability."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pandas as pd


def compute_record_hash(row: pd.Series) -> str:
    """Build a deterministic SHA-256 natural key for idempotent deduplication."""
    tanggal_str = str(row["tanggal"]).strip()
    komoditas_norm = str(row["komoditas"]).strip().lower()
    provinsi_norm = str(row["provinsi"]).strip().lower()
    sumber_norm = str(row["sumber"]).strip().lower()
    raw_key = f"{tanggal_str}|{komoditas_norm}|{provinsi_norm}|{sumber_norm}"
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()


class DuckDBWarehouse:
    """Manages raw, quarantine, and pipeline_runs tables in a single DuckDB file."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> duckdb.DuckDBPyConnection:
        """Open a DuckDB connection and ensure base tables exist."""
        conn = duckdb.connect(str(self.db_path))
        self._init_tables(conn)
        return conn

    @staticmethod
    def _init_tables(conn: duckdb.DuckDBPyConnection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_harga_pangan (
                record_hash VARCHAR PRIMARY KEY,
                tanggal DATE NOT NULL,
                komoditas VARCHAR NOT NULL,
                provinsi VARCHAR NOT NULL,
                harga DOUBLE NOT NULL,
                satuan VARCHAR NOT NULL,
                sumber VARCHAR NOT NULL,
                ingested_at TIMESTAMPTZ NOT NULL,
                run_id VARCHAR NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS quarantine_harga_pangan (
                quarantined_at TIMESTAMPTZ NOT NULL,
                run_id VARCHAR NOT NULL,
                row_index INTEGER,
                failure_reason VARCHAR NOT NULL,
                raw_payload VARCHAR NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id VARCHAR PRIMARY KEY,
                pipeline_name VARCHAR NOT NULL,
                source_used VARCHAR NOT NULL,
                started_at TIMESTAMPTZ NOT NULL,
                finished_at TIMESTAMPTZ NOT NULL,
                rows_fetched INTEGER NOT NULL,
                rows_inserted INTEGER NOT NULL,
                rows_duplicate_skipped INTEGER NOT NULL,
                rows_quarantined INTEGER NOT NULL,
                status VARCHAR NOT NULL,
                error_message VARCHAR
            )
            """
        )

    def insert_raw_idempotent(self, valid_df: pd.DataFrame, run_id: str) -> tuple[int, int]:
        """Append new records into raw_harga_pangan without creating duplicates."""
        if valid_df.empty:
            return 0, 0

        staged = valid_df.copy()
        staged["record_hash"] = staged.apply(compute_record_hash, axis=1)
        staged = staged.drop_duplicates(subset=["record_hash"], keep="last").copy()
        staged["ingested_at"] = datetime.now(UTC)
        staged["run_id"] = run_id

        ordered_cols = [
            "record_hash",
            "tanggal",
            "komoditas",
            "provinsi",
            "harga",
            "satuan",
            "sumber",
            "ingested_at",
            "run_id",
        ]
        staged = staged.loc[:, ordered_cols]

        with self.connect() as conn:
            conn.register("staged_raw_df", staged)
            existing_count_row = conn.execute(
                """
                SELECT COUNT(*) FROM staged_raw_df s
                INNER JOIN raw_harga_pangan r ON s.record_hash = r.record_hash
                """
            ).fetchone()
            duplicate_count = int(existing_count_row[0]) if existing_count_row else 0

            conn.execute(
                """
                INSERT INTO raw_harga_pangan (
                    record_hash, tanggal, komoditas, provinsi, harga,
                    satuan, sumber, ingested_at, run_id
                )
                SELECT
                    s.record_hash, s.tanggal, s.komoditas, s.provinsi, s.harga,
                    s.satuan, s.sumber, s.ingested_at, s.run_id
                FROM staged_raw_df s
                LEFT JOIN raw_harga_pangan r ON s.record_hash = r.record_hash
                WHERE r.record_hash IS NULL
                """
            )
            conn.unregister("staged_raw_df")

        inserted_count = len(staged) - duplicate_count
        return inserted_count, duplicate_count

    def insert_quarantine(self, quarantine_df: pd.DataFrame) -> int:
        """Append rejected rows with failure reasons to quarantine_harga_pangan."""
        if quarantine_df.empty:
            return 0

        with self.connect() as conn:
            conn.register("staged_quarantine_df", quarantine_df)
            conn.execute(
                """
                INSERT INTO quarantine_harga_pangan (
                    quarantined_at, run_id, row_index, failure_reason, raw_payload
                )
                SELECT
                    quarantined_at, run_id, row_index, failure_reason, raw_payload
                FROM staged_quarantine_df
                """
            )
            conn.unregister("staged_quarantine_df")
        return len(quarantine_df)

    def record_pipeline_run(
        self,
        *,
        run_id: str,
        pipeline_name: str,
        source_used: str,
        started_at: datetime,
        finished_at: datetime,
        rows_fetched: int,
        rows_inserted: int,
        rows_duplicate_skipped: int,
        rows_quarantined: int,
        status: str,
        error_message: str | None = None,
    ) -> None:
        """Persist execution metrics to pipeline_runs."""
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO pipeline_runs (
                    run_id, pipeline_name, source_used, started_at, finished_at,
                    rows_fetched, rows_inserted, rows_duplicate_skipped,
                    rows_quarantined, status, error_message
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    run_id,
                    pipeline_name,
                    source_used,
                    started_at,
                    finished_at,
                    rows_fetched,
                    rows_inserted,
                    rows_duplicate_skipped,
                    rows_quarantined,
                    status,
                    error_message,
                ],
            )
