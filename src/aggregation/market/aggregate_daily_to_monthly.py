from datetime import UTC, datetime

import pandas as pd


MONTHLY_BAR_COLUMNS = [
    "symbol",
    "timeframe",
    "bar_start",
    "bar_end",
    "session_date",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "vwap",
    "trade_count",
    "source",
    "ingested_at",
    "standardized_at",
]


def aggregate_daily_to_monthly(daily_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate validated daily bars into monthly OHLCV bars.

    This first-pass implementation groups by calendar month using session_date and
    uses the actual first/last daily bars present in each grouped month:
      - open  = first open
      - high  = max high
      - low   = min low
      - close = last close
      - volume = sum volume
      - trade_count = sum trade_count
      - vwap = volume-weighted average of daily vwap when available

    The output follows the canonical bar schema.
    """
    if daily_df.empty:
        return pd.DataFrame(columns=MONTHLY_BAR_COLUMNS)

    df = daily_df.copy()

    required_columns = [
        "symbol",
        "bar_start",
        "bar_end",
        "session_date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "vwap",
        "trade_count",
    ]
    missing_columns = [column for column in required_columns if column not in df.columns]
    if missing_columns:
        missing = ", ".join(missing_columns)
        raise ValueError(
            f"Daily input is missing required columns for monthly aggregation: {missing}"
        )

    if "timeframe" in df.columns:
        invalid_timeframes = (
            df.loc[df["timeframe"] != "1d", "timeframe"].dropna().unique()
        )
        if len(invalid_timeframes) > 0:
            invalid = ", ".join(sorted(str(value) for value in invalid_timeframes))
            raise ValueError(
                f"aggregate_daily_to_monthly expects only 1d bars, found: {invalid}"
            )

    df["bar_start"] = pd.to_datetime(df["bar_start"], errors="coerce")
    df["bar_end"] = pd.to_datetime(df["bar_end"], errors="coerce")
    df["session_date"] = pd.to_datetime(df["session_date"], errors="coerce")

    df = df.sort_values(["symbol", "bar_start"]).reset_index(drop=True)

    # Group into calendar months using session_date.
    df["month_bucket"] = df["session_date"].dt.to_period("M")

    df["vwap_x_volume"] = df["vwap"] * df["volume"]

    grouped = df.groupby(["symbol", "month_bucket"], sort=True)

    monthly_df = grouped.agg(
        bar_start=("bar_start", "min"),
        bar_end=("bar_end", "max"),
        session_date=("session_date", "max"),
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        volume=("volume", "sum"),
        vwap_numerator=("vwap_x_volume", "sum"),
        trade_count=("trade_count", "sum"),
    ).reset_index()

    monthly_df["timeframe"] = "1mo"
    monthly_df["date"] = monthly_df["session_date"].dt.normalize()
    monthly_df["source"] = "derived_from_1d"

    monthly_df["vwap"] = monthly_df["vwap_numerator"] / monthly_df["volume"]
    monthly_df.loc[
        monthly_df["volume"].isna() | (monthly_df["volume"] == 0),
        "vwap",
    ] = pd.NA

    ingested_at = datetime.now(UTC)
    standardized_at = datetime.now(UTC)

    monthly_df["ingested_at"] = ingested_at
    monthly_df["standardized_at"] = standardized_at

    monthly_df = monthly_df.drop(columns=["month_bucket", "vwap_numerator"])

    monthly_df["symbol"] = monthly_df["symbol"].astype("string")
    monthly_df["timeframe"] = monthly_df["timeframe"].astype("string")
    monthly_df["source"] = monthly_df["source"].astype("string")

    numeric_float_cols = ["open", "high", "low", "close", "vwap"]
    for col in numeric_float_cols:
        monthly_df[col] = pd.to_numeric(monthly_df[col], errors="coerce")

    monthly_df["volume"] = (
        pd.to_numeric(monthly_df["volume"], errors="coerce").round().astype("Int64")
    )
    monthly_df["trade_count"] = (
        pd.to_numeric(monthly_df["trade_count"], errors="coerce").round().astype("Int64")
    )

    return monthly_df[MONTHLY_BAR_COLUMNS]