"""End-to-end ingestion entrypoint orchestrating fetch, validation, and DuckDB loading."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

import httpx

from src.config import Settings
from src.ingestion.fetcher import fetch_food_prices
from src.ingestion.warehouse import DuckDBWarehouse
from src.logging_utils import get_logger
from src.quality.validator import validate_food_prices

PIPELINE_NAME = "food_price_ingestion"


@dataclass(frozen=True)
class IngestionSummary:
    """Summary metrics returned after a single ingestion run."""

    run_id: str
    source_used: str
    rows_fetched: int
    rows_inserted: int
    rows_duplicate_skipped: int
    rows_quarantined: int
    status: str


def run_ingestion(
    settings: Settings | None = None,
    client: httpx.Client | None = None,
) -> IngestionSummary:
    """Execute one idempotent ingestion cycle and persist observability metrics."""
    cfg = settings or Settings.from_env()
    logger = get_logger("src.ingestion.pipeline", level=cfg.log_level)
    warehouse = DuckDBWarehouse(cfg.duckdb_path)
    run_id = f"run_{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"
    started_at = datetime.now(UTC)

    logger.info("Starting ingestion run", extra={"run_id": run_id, "mode": cfg.source_mode})

    source_used = "unknown"
    rows_fetched = 0
    rows_inserted = 0
    rows_duplicate_skipped = 0
    rows_quarantined = 0

    try:
        fetch_result = fetch_food_prices(cfg, logger=logger, client=client)
        source_used = fetch_result.source_used
        rows_fetched = len(fetch_result.df)

        outcome = validate_food_prices(fetch_result.df, run_id=run_id)
        rows_inserted, rows_duplicate_skipped = warehouse.insert_raw_idempotent(
            outcome.valid_df, run_id=run_id
        )
        rows_quarantined = warehouse.insert_quarantine(outcome.quarantine_df)

        finished_at = datetime.now(UTC)
        warehouse.record_pipeline_run(
            run_id=run_id,
            pipeline_name=PIPELINE_NAME,
            source_used=source_used,
            started_at=started_at,
            finished_at=finished_at,
            rows_fetched=rows_fetched,
            rows_inserted=rows_inserted,
            rows_duplicate_skipped=rows_duplicate_skipped,
            rows_quarantined=rows_quarantined,
            status="SUCCESS",
        )
        logger.info(
            "Completed ingestion run",
            extra={
                "run_id": run_id,
                "source_used": source_used,
                "rows_fetched": rows_fetched,
                "rows_inserted": rows_inserted,
                "rows_duplicate_skipped": rows_duplicate_skipped,
                "rows_quarantined": rows_quarantined,
            },
        )
        return IngestionSummary(
            run_id=run_id,
            source_used=source_used,
            rows_fetched=rows_fetched,
            rows_inserted=rows_inserted,
            rows_duplicate_skipped=rows_duplicate_skipped,
            rows_quarantined=rows_quarantined,
            status="SUCCESS",
        )
    except Exception as exc:  # noqa: BLE001
        finished_at = datetime.now(UTC)
        warehouse.record_pipeline_run(
            run_id=run_id,
            pipeline_name=PIPELINE_NAME,
            source_used=source_used,
            started_at=started_at,
            finished_at=finished_at,
            rows_fetched=rows_fetched,
            rows_inserted=rows_inserted,
            rows_duplicate_skipped=rows_duplicate_skipped,
            rows_quarantined=rows_quarantined,
            status="FAILED",
            error_message=str(exc),
        )
        logger.error("Ingestion run failed", extra={"run_id": run_id, "error": str(exc)})
        raise


def main() -> None:
    """CLI entrypoint for running ingestion directly."""
    run_ingestion()


if __name__ == "__main__":
    main()
