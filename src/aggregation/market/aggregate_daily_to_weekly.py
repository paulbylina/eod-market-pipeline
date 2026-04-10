from datetime import UTC, datetime

import pandas as pd


WEEKLY_BAR_COLUMNS = [
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


def aggregate_daily_to_weekly(daily_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate validated daily bars into weekly OHLCV bars.

    This first-pass implementation groups by trading weeks ending on Friday and
    uses the actual first/last daily bars present in each grouped week:
      - open  = first open
      - high  = max high
      - low   = min low
      - close = last close
      - volume = sum volume
      - trade_count = sum trade_count
      - vwap = volume-weighted average of daily vwap when available
    """
    if daily_df.empty:
        return pd.DataFrame(columns=WEEKLY_BAR_COLUMNS)

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
            f"Daily input is missing required columns for weekly aggregation: {missing}"
        )

    if "timeframe" in df.columns:
        invalid_timeframes = df.loc[df["timeframe"] != "1d", "timeframe"].dropna().unique()
        if len(invalid_timeframes) > 0:
            invalid = ", ".join(sorted(str(value) for value in invalid_timeframes))
            raise ValueError(
                f"aggregate_daily_to_weekly expects only 1d bars, found: {invalid}"
            )

    df["bar_start"] = pd.to_datetime(df["bar_start"], errors="coerce")
    df["bar_end"] = pd.to_datetime(df["bar_end"], errors="coerce")
    df["session_date"] = pd.to_datetime(df["session_date"], errors="coerce")

    df = df.sort_values(["symbol", "bar_start"]).reset_index(drop=True)

    # Group into trading weeks ending on Friday.
    df["week_bucket"] = df["bar_start"].dt.to_period("W-FRI")

    # Weighted VWAP numerator.
    df["vwap_x_volume"] = df["vwap"] * df["volume"]

    grouped = df.groupby(["symbol", "week_bucket"], sort=True)

    weekly_df = grouped.agg(
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

    weekly_df["timeframe"] = "1w"
    weekly_df["date"] = weekly_df["session_date"].dt.normalize()
    weekly_df["source"] = "derived_from_1d"

    weekly_df["vwap"] = weekly_df["vwap_numerator"] / weekly_df["volume"]
    weekly_df.loc[weekly_df["volume"].isna() | (weekly_df["volume"] == 0), "vwap"] = pd.NA

    ingested_at = datetime.now(UTC)
    standardized_at = datetime.now(UTC)

    weekly_df["ingested_at"] = ingested_at
    weekly_df["standardized_at"] = standardized_at

    weekly_df = weekly_df.drop(columns=["week_bucket", "vwap_numerator"])

    weekly_df["symbol"] = weekly_df["symbol"].astype("string")
    weekly_df["timeframe"] = weekly_df["timeframe"].astype("string")
    weekly_df["source"] = weekly_df["source"].astype("string")

    numeric_float_cols = ["open", "high", "low", "close", "vwap"]
    for col in numeric_float_cols:
        weekly_df[col] = pd.to_numeric(weekly_df[col], errors="coerce")

    weekly_df["volume"] = (
        pd.to_numeric(weekly_df["volume"], errors="coerce").round().astype("Int64")
    )
    weekly_df["trade_count"] = (
        pd.to_numeric(weekly_df["trade_count"], errors="coerce").round().astype("Int64")
    )

    return weekly_df[WEEKLY_BAR_COLUMNS]