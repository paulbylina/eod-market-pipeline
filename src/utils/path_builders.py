from pathlib import Path

from src.utils.settings import CURATED_DATA_DIR, QUALITY_DATA_DIR, RAW_DATA_DIR, STAGING_DATA_DIR


def build_raw_output_path(symbol: str, start_date: str, end_date: str) -> Path:
    return RAW_DATA_DIR / f"{symbol}_{start_date}_{end_date}_raw.json"


def build_staging_output_path(symbol: str, start_date: str, end_date: str) -> Path:
    return STAGING_DATA_DIR / f"{symbol}_{start_date}_{end_date}_staging.parquet"


def build_curated_output_path(symbol: str, start_date: str, end_date: str) -> Path:
    return CURATED_DATA_DIR / f"{symbol}_{start_date}_{end_date}_curated.parquet"


def build_validation_failures_output_path(symbol: str, start_date: str, end_date: str) -> Path:
    return QUALITY_DATA_DIR / f"{symbol}_{start_date}_{end_date}_validation_failures.parquet"


def build_validation_warnings_output_path(symbol: str, start_date: str, end_date: str) -> Path:
    return QUALITY_DATA_DIR / f"{symbol}_{start_date}_{end_date}_validation_warnings.parquet"


def build_validation_summary_output_path(symbol: str, start_date: str, end_date: str) -> Path:
    return QUALITY_DATA_DIR / f"{symbol}_{start_date}_{end_date}_validation_summary.parquet"