
```
eod-market-pipeline/
├── config/             # runtime settings, environments, symbols, pipeline parameters
├── data/               # local data lake for all pipeline outputs
│   ├── raw/            # untouched data pulled from Polygon
│   ├── staging/        # standardized, typed, intermediate datasets
│   ├── curated/        # validated, feature-ready final datasets
│   └── quality/        # validation reports, failed checks, anomaly outputs
├── docs/               # architecture notes, pipeline docs, diagrams, decisions
├── logs/               # pipeline run logs
├── orchestration/      # scheduling and run orchestration logic
├── src/                # application source code
│   ├── ingestion/      # Polygon download logic
│   ├── standardization/ # schema cleanup, column normalization, type casting
│   ├── validation/     # dataframe and business-rule checks
│   ├── features/       # 30-bar volume and close-price z-score calculations
│   ├── storage/        # Parquet and DuckDB read/write logic
│   ├── pipelines/      # end-to-end pipeline flow definitions
│   └── utils/          # shared helpers used across modules
├── tests/              # automated tests
│   ├── unit/           # small isolated logic tests
│   ├── integration/    # end-to-end or multi-step pipeline tests
│   └── data_quality/   # validation and data-rule test cases
└── notebooks/          # exploration, debugging, and analysis notebooks
```