"""Runtime configuration loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    """Typed application settings for ingestion, warehouse, and observability."""

    duckdb_path: Path
    source_mode: str
    source_url: str
    sample_csv_path: Path
    max_retries: int
    backoff_seconds: float
    timeout_seconds: float
    log_level: str

    @classmethod
    def from_env(cls, env_file: str | Path | None = None) -> Settings:
        """Load settings from optional .env file and environment variables."""
        if env_file is not None:
            load_dotenv(env_file, override=False)
        else:
            load_dotenv(override=False)

        mode = os.getenv("FOOD_PRICE_SOURCE_MODE", "auto").strip().lower()
        if mode not in {"auto", "api", "sample"}:
            mode = "auto"

        return cls(
            duckdb_path=Path(os.getenv("DUCKDB_PATH", "data/warehouse/harga_pangan.duckdb")),
            source_mode=mode,
            source_url=os.getenv(
                "FOOD_PRICE_SOURCE_URL",
                "https://api-panelhargav2.badanpangan.go.id/api/front/harga-pangan-informasi",
            ).strip(),
            sample_csv_path=Path(
                os.getenv("SAMPLE_CSV_PATH", "data/sample/harga_pangan_sample.csv")
            ),
            max_retries=max(1, int(os.getenv("INGEST_MAX_RETRIES", "3"))),
            backoff_seconds=max(0.0, float(os.getenv("INGEST_BACKOFF_SECONDS", "1.0"))),
            timeout_seconds=max(1.0, float(os.getenv("INGEST_TIMEOUT_SECONDS", "10.0"))),
            log_level=os.getenv("LOG_LEVEL", "INFO").strip().upper(),
        )
