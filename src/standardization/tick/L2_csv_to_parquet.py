from pathlib import Path
import sys

import polars as pl


REQUIRED_COLUMNS = {
    "ts_utc",
    "event_time",
    "instrument",
    "side",
    "operation",
    "position",
    "price",
    "volume",
    "market_maker",
    "is_reset",
}


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit(
            "Usage: uv run python src/standardization/tick/L2_csv_to_parquet.py <input-csv> <output-parquet>"
        )

    input_path = Path(sys.argv[1])
    output_path = Path(sys.argv[2])

    if not input_path.exists():
        raise SystemExit(f"Input file not found: {input_path}")

    df = pl.read_csv(input_path)

    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise SystemExit(f"Missing required columns: {sorted(missing)}")

    typed = (
        df.with_columns(
            [
                pl.col("ts_utc").str.to_datetime(strict=False, time_zone="UTC"),
                pl.col("event_time").str.to_datetime(strict=False),
                pl.col("position").cast(pl.Int64),
                pl.col("price").cast(pl.Float64),
                pl.col("volume").cast(pl.Int64),
                pl.col("market_maker").cast(pl.Utf8),
                pl.col("is_reset").cast(pl.Boolean),
            ]
        )
        .with_columns(
            [
                pl.col("instrument").str.replace_all(" ", "_").alias("instrument_code"),
                pl.col("ts_utc").dt.date().alias("trade_date"),
            ]
        )
        .select(
            [
                "ts_utc",
                "event_time",
                "instrument",
                "instrument_code",
                "side",
                "operation",
                "position",
                "price",
                "volume",
                "market_maker",
                "is_reset",
                "trade_date",
            ]
        )
        .sort("ts_utc")
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    typed.write_parquet(output_path)

    print(f"Wrote parquet: {output_path}")
    print(f"Rows: {typed.height}")
    print(typed.schema)


if __name__ == "__main__":
    main()
