"""Public data fetcher with exponential retry and deterministic CSV fallback."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import pandas as pd

from src.config import Settings

DEFAULT_HEADERS = {
    "User-Agent": "harga-pangan-pipeline/0.1 (public-interest-research; respectful-rate-limit)",
    "Accept": "application/json, text/plain, */*",
}


@dataclass(frozen=True)
class FetchResult:
    """Result of a fetch attempt including provenance metadata."""

    df: pd.DataFrame
    source_used: str
    fallback_triggered: bool


def parse_bapanas_payload(payload: dict[str, Any], source_label: str) -> pd.DataFrame:
    """Normalize JSON response from Panel Harga Badan Pangan Nasional into standard columns."""
    records: list[dict[str, Any]] = []
    raw_items = payload.get("data") or payload.get("results") or []
    if isinstance(raw_items, dict):
        raw_items = raw_items.get("data", [])

    for item in raw_items:
        if not isinstance(item, dict):
            continue
        tanggal = item.get("tanggal") or item.get("date") or item.get("created_at")
        komoditas = item.get("komoditas") or item.get("name") or item.get("nama_komoditas")
        provinsi = item.get("provinsi") or item.get("province_name") or item.get("nama_provinsi")
        harga = item.get("harga") or item.get("price") or item.get("rata_rata")
        satuan = item.get("satuan") or item.get("unit") or "Rp/kg"
        if tanggal is None and komoditas is None and harga is None:
            continue
        records.append(
            {
                "tanggal": tanggal,
                "komoditas": komoditas,
                "provinsi": provinsi,
                "harga": harga,
                "satuan": satuan,
                "sumber": source_label,
            }
        )

    return pd.DataFrame(
        records,
        columns=["tanggal", "komoditas", "provinsi", "harga", "satuan", "sumber"],
    )


def fetch_from_api(
    url: str,
    *,
    max_retries: int,
    backoff_seconds: float,
    timeout_seconds: float,
    logger: logging.Logger,
    client: httpx.Client | None = None,
) -> pd.DataFrame:
    """Fetch JSON payload from public endpoint with exponential backoff retries."""
    last_error: Exception | None = None
    owns_client = client is None
    http_client = client or httpx.Client(timeout=timeout_seconds, headers=DEFAULT_HEADERS)

    try:
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(
                    "Requesting public food price endpoint",
                    extra={"url": url, "attempt": attempt, "max_retries": max_retries},
                )
                response = http_client.get(url)
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict):
                    raise ValueError("Unexpected JSON root type; expected object")
                df = parse_bapanas_payload(payload, source_label="Panel Harga BPN")
                if df.empty:
                    raise ValueError("Public endpoint returned zero parseable price records")
                return df
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                logger.warning(
                    "Public endpoint attempt failed",
                    extra={"attempt": attempt, "error": str(exc)},
                )
                if attempt < max_retries and backoff_seconds > 0:
                    sleep_duration = backoff_seconds * (2 ** (attempt - 1))
                    time.sleep(sleep_duration)
    finally:
        if owns_client:
            http_client.close()

    raise RuntimeError(f"Failed to fetch from {url} after {max_retries} attempts: {last_error}")


def load_sample_csv(csv_path: Path, logger: logging.Logger) -> pd.DataFrame:
    """Load fallback CSV dataset from disk."""
    if not csv_path.exists():
        raise FileNotFoundError(f"Sample CSV not found at {csv_path}")
    logger.info("Loading fallback sample dataset", extra={"path": str(csv_path)})
    df = pd.read_csv(csv_path)
    if "sumber" not in df.columns:
        df["sumber"] = "Sample CSV (PIHPS/BPN Reference)"
    return df


def fetch_food_prices(
    settings: Settings,
    logger: logging.Logger,
    client: httpx.Client | None = None,
) -> FetchResult:
    """Retrieve food price records using configured mode with safe fallback."""
    if settings.source_mode == "sample":
        df = load_sample_csv(settings.sample_csv_path, logger)
        return FetchResult(
            df=df,
            source_used=f"sample:{settings.sample_csv_path}",
            fallback_triggered=False,
        )

    try:
        df = fetch_from_api(
            settings.source_url,
            max_retries=settings.max_retries,
            backoff_seconds=settings.backoff_seconds,
            timeout_seconds=settings.timeout_seconds,
            logger=logger,
            client=client,
        )
        return FetchResult(df=df, source_used=settings.source_url, fallback_triggered=False)
    except Exception as exc:  # noqa: BLE001
        if settings.source_mode == "api":
            raise
        logger.warning(
            "Falling back to local sample CSV after public endpoint failure",
            extra={"error": str(exc), "fallback_path": str(settings.sample_csv_path)},
        )
        df = load_sample_csv(settings.sample_csv_path, logger)
        return FetchResult(
            df=df,
            source_used=f"fallback:{settings.sample_csv_path}",
            fallback_triggered=True,
        )
