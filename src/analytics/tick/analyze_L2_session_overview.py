from pathlib import Path
import argparse

import polars as pl


def parse_ninjatrader_utc_expr(col_name: str) -> pl.Expr:
    """
    Parses NinjaTrader/C# ISO timestamps like:
    2026-04-27T12:11:39.1234567Z
    """
    return (
        pl.col(col_name)
        .str.strptime(
            pl.Datetime(time_zone="UTC"),
            format="%Y-%m-%dT%H:%M:%S%.fZ",
            strict=False,
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Analyze raw NinjaTrader Level 2 market depth session."
    )
    parser.add_argument("csv_path", help="Path to raw Level 2 CSV file")
    parser.add_argument(
        "--gap-ms",
        type=int,
        default=1000,
        help="Gap threshold in milliseconds. Default: 1000",
    )
    args = parser.parse_args()

    csv_path = Path(args.csv_path)

    if not csv_path.exists():
        raise FileNotFoundError(f"File not found: {csv_path}")

    df = pl.read_csv(
        csv_path,
        infer_schema_length=10_000,
        schema_overrides={
            "recorded_utc": pl.String,
            "event_time": pl.String,
            "instrument": pl.String,
            "side": pl.String,
            "operation": pl.String,
            "position": pl.Int32,
            "price": pl.Float64,
            "volume": pl.Int64,
            "market_maker": pl.String,
            "is_reset": pl.Boolean,
        },
    )

    df = df.with_columns(
        parse_ninjatrader_utc_expr("recorded_utc").alias("recorded_dt_utc"),
        parse_ninjatrader_utc_expr("event_time").alias("event_dt_utc"),
    )

    print("\n=== BASIC INFO ===")
    print(f"File: {csv_path}")
    print(f"Rows: {df.height:,}")
    print(f"Columns: {df.columns}")

    time_stats = df.select(
        pl.min("recorded_dt_utc").alias("start_utc"),
        pl.max("recorded_dt_utc").alias("end_utc"),
    )

    start_utc = time_stats["start_utc"][0]
    end_utc = time_stats["end_utc"][0]

    duration_seconds = (end_utc - start_utc).total_seconds()
    updates_per_second = df.height / duration_seconds if duration_seconds > 0 else 0

    print("\n=== TIME RANGE ===")
    print(f"Start UTC: {start_utc}")
    print(f"End UTC:   {end_utc}")
    print(f"Duration seconds: {duration_seconds:,.2f}")
    print(f"Updates/sec: {updates_per_second:,.2f}")

    print("\n=== COUNTS BY SIDE ===")
    print(df.group_by("side").len().sort("side"))

    print("\n=== COUNTS BY OPERATION ===")
    print(df.group_by("operation").len().sort("operation"))

    print("\n=== COUNTS BY SIDE + OPERATION ===")
    print(df.group_by(["side", "operation"]).len().sort(["side", "operation"]))

    print("\n=== MAX POSITION BY SIDE ===")
    print(
        df.group_by("side")
        .agg(
            pl.min("position").alias("min_position"),
            pl.max("position").alias("max_position"),
        )
        .sort("side")
    )

    print("\n=== PRICE / VOLUME BY OPERATION ===")
    print(
        df.group_by("operation")
        .agg(
            pl.min("price").alias("min_price"),
            pl.max("price").alias("max_price"),
            pl.min("volume").alias("min_volume"),
            pl.max("volume").alias("max_volume"),
        )
        .sort("operation")
    )

    print("\n=== RESET EVENTS ===")
    print(
        df.group_by("is_reset")
        .len()
        .sort("is_reset")
    )

    gap_df = (
        df.select(
            [
                "recorded_dt_utc",
                "event_dt_utc",
                "side",
                "operation",
                "position",
                "price",
                "volume",
            ]
        )
        .with_columns(
            pl.col("recorded_dt_utc")
            .diff()
            .dt.total_milliseconds()
            .alias("gap_ms")
        )
        .filter(pl.col("gap_ms") >= args.gap_ms)
        .sort("gap_ms", descending=True)
    )

    print(f"\n=== GAPS >= {args.gap_ms} ms ===")
    print(f"Gap count: {gap_df.height:,}")
    print(gap_df.head(20))

    print("\n=== FIRST 10 ROWS ===")
    print(df.head(10))

    print("\n=== LAST 10 ROWS ===")
    print(df.tail(10))


if __name__ == "__main__":
    main()