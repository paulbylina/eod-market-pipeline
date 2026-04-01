# eod-market-pipeline

Production-style end-of-day stock data pipeline using Massive, with ETL, validation, and z-score feature generation.

## Overview

This project is a modular Python pipeline for downloading, standardizing, validating, and transforming end-of-day stock market data.

The current version focuses on:

- pulling daily OHLCV bars from the Massive aggregates API
- storing raw vendor responses
- standardizing data into a consistent staging schema
- validating daily bar integrity
- generating 30-bar volume and close-price z-score features
- writing analytics-ready curated outputs

## Current Features

- Massive API client
- Daily bar ingestion
- Standardization layer
- Validation layer
- Feature generation
- Raw, staging, curated, and quality outputs
- Unit test foundation
- End-to-end pipeline runner

## Project Structure

```text
eod-market-pipeline/
├── config/
├── data/
│   ├── raw/
│   ├── staging/
│   ├── curated/
│   └── quality/
├── docs/
├── logs/
├── notebooks/
├── orchestration/
├── src/
│   ├── features/
│   ├── ingestion/
│   ├── pipelines/
│   ├── standardization/
│   ├── storage/
│   ├── utils/
│   └── validation/
├── tests/
│   ├── data_quality/
│   ├── integration/
│   └── unit/
├── pyproject.toml
└── uv.lock
```

## Pipeline Flow

The current pipeline flow is:

1. fetch raw daily bars from Massive
2. standardize the raw response into a tabular schema
3. validate required fields and OHLCV integrity
4. generate 30-bar rolling features
5. write outputs to raw, staging, curated, and quality layers

## Output Layers
**Raw** - Untouched API response data

**Staging** - Cleaned and standardized daily bars.

**Curated** - Validated and feature-enriched daily bars.

**Quality** - Validation failures, warnings, and summary outputs.

## Current Derived Features
- volume_mean_30d
- volume_std_30d
- volume_zscore_30d
- close_mean_30d
- close_std_30d
- close_price_zscore_30d

## Tech Stack
- Python
- uv
- httpx
- pandas
- numpy
- pandera
- pyarrow
- duckdb
- pydantic
- pytest
- ruff

## Local Setup
**1. Clone the repo**
```bash
git clone git@github.com:YOUR_GITHUB_USERNAME/eod-market-pipeline.git
```
**2. Install dependencies**
```bash
uv sync
```
**3. Create a .env file**
```text
MASSIVE_API_KEY=your_api_key_here
```
or

```bash
cp .env.example .env
```

## Running the Pipeline
Example:
```bash
uv run python -c "from src.pipelines.daily_eod_pipeline import run_daily_eod_pipeline; run_daily_eod_pipeline('AAPL', '2023-10-01', '2024-01-31')"
```

## Running Tests
```bash
uv run pytest
```

## Roadmap
- improve validation coverage
- add stronger unit and integration tests
- support multi-symbol batch runs
- add DuckDB querying layer
- add scheduling/orchestration
- expand feature library
- support intraday data
- expose data through an API and app layer later

## Notes
This project is part of a broader trading development workflow, but this repository is focused specifically on the market data pipeline foundation.