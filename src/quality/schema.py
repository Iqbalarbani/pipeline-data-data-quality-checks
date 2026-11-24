"""Pandera schema definitions for incoming Indonesian food price records."""

from __future__ import annotations

import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series

MIN_REASONABLE_PRICE_IDR = 500.0
MAX_REASONABLE_PRICE_IDR = 500_000.0


class FoodPriceRawSchema(pa.DataFrameModel):
    """Contract enforced at the ingestion boundary before loading into raw."""

    tanggal: Series[pd.Timestamp] = pa.Field(
        nullable=False,
        coerce=True,
        description="Observation date (YYYY-MM-DD)",
    )
    komoditas: Series[str] = pa.Field(
        nullable=False,
        str_length={"min_value": 2, "max_value": 120},
        description="Food commodity name",
    )
    provinsi: Series[str] = pa.Field(
        nullable=False,
        str_length={"min_value": 2, "max_value": 120},
        description="Indonesian province name",
    )
    harga: Series[float] = pa.Field(
        nullable=False,
        coerce=True,
        ge=MIN_REASONABLE_PRICE_IDR,
        le=MAX_REASONABLE_PRICE_IDR,
        description="Retail price in IDR per kg/liter",
    )
    satuan: Series[str] = pa.Field(
        nullable=True,
        default="Rp/kg",
        description="Measurement unit",
    )
    sumber: Series[str] = pa.Field(
        nullable=False,
        str_length={"min_value": 2, "max_value": 120},
        description="Data source identifier",
    )

    class Config:
        strict = False
        coerce = True

    @pa.check("tanggal", name="valid_observation_year")
    def check_reasonable_year(cls, series: Series[pd.Timestamp]) -> Series[bool]:
        return (series.dt.year >= 2015) & (series.dt.year <= 2035)

    @pa.check("komoditas", name="non_blank_komoditas")
    def check_non_blank_komoditas(cls, series: Series[str]) -> Series[bool]:
        return series.fillna("").str.strip().str.len() >= 2

    @pa.check("provinsi", name="non_blank_provinsi")
    def check_non_blank_provinsi(cls, series: Series[str]) -> Series[bool]:
        return series.fillna("").str.strip().str.len() >= 2
