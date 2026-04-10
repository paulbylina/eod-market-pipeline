from datetime import UTC, datetime

import pandas as pd


INTRADAY_BAR_COLUMNS = [
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


SUPPORTED_INTRADAY_DERIVATIONS = {
    "5m": 5,
    "15m": 15,
    "60m": 60,
}


def aggregate_intraday_from_minute(
    minute_df: pd.DataFrame,
    target_timeframe: str,
) -> pd.DataFrame:
    """
    Aggregate 1-minute bars into 5m / 15m / 60m bars.

    Bucketing is done in America/New_York local market time and grouped within
    session_date so bars never roll across overnight boundaries.
    """
    if target_timeframe not in SUPPORTED_INTRADAY_DERIVATIONS:
        supported = ", ".join(sorted(SUPPORTED_INTRADAY_DERIVATIONS))
        raise ValueError(
            f"Unsupported intraday target timeframe '{target_timeframe}'. "
            f"Supported: {supported}"
        )

    if minute_df.empty:
        return pd.DataFrame(columns=INTRADAY_BAR_COLUMNS)

    df = minute_df.copy()

    required_columns = [
        "symbol",
        "timeframe",
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
            f"Minute input is missing required columns for intraday aggregation: {missing}"
        )

    invalid_timeframes = df.loc[df["timeframe"] != "1m", "timeframe"].dropna().unique()
    if len(invalid_timeframes) > 0:
        invalid = ", ".join(sorted(str(value) for value in invalid_timeframes))
        raise ValueError(
            f"aggregate_intraday_from_minute expects only 1m bars, found: {invalid}"
        )

    minutes = SUPPORTED_INTRADAY_DERIVATIONS[target_timeframe]
    bucket_freq = f"{minutes}min"

    df["bar_start"] = pd.to_datetime(df["bar_start"], errors="coerce")
    df["bar_end"] = pd.to_datetime(df["bar_end"], errors="coerce")
    df["session_date"] = pd.to_datetime(df["session_date"], errors="coerce")

    df = df.sort_values(["symbol", "bar_start"]).reset_index(drop=True)

    # bar_start is stored as UTC-based naive timestamp. Localize to UTC first,
    # then convert to New York time for session-aware bucketing.
    df["bar_start_local"] = (
        pd.to_datetime(df["bar_start"], utc=True)
        .dt.tz_convert("America/New_York")
    )
    df["bucket_start_local"] = df["bar_start_local"].dt.floor(bucket_freq)
    df["bucket_end_local"] = df["bucket_start_local"] + pd.Timedelta(minutes=minutes)

    df["vwap_x_volume"] = df["vwap"] * df["volume"]

    grouped = df.groupby(
        ["symbol", "session_date", "bucket_start_local", "bucket_end_local"],
        sort=True,
    )

    intraday_df = grouped.agg(
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        volume=("volume", "sum"),
        vwap_numerator=("vwap_x_volume", "sum"),
        trade_count=("trade_count", "sum"),
    ).reset_index()

    intraday_df["timeframe"] = target_timeframe
    intraday_df["bar_start"] = (
        intraday_df["bucket_start_local"]
        .dt.tz_convert("UTC")
        .dt.tz_localize(None)
    )
    intraday_df["bar_end"] = (
        intraday_df["bucket_end_local"]
        .dt.tz_convert("UTC")
        .dt.tz_localize(None)
    )
    intraday_df["date"] = intraday_df["session_date"].dt.normalize()
    intraday_df["source"] = "derived_from_1m"

    intraday_df["vwap"] = intraday_df["vwap_numerator"] / intraday_df["volume"]
    intraday_df.loc[
        intraday_df["volume"].isna() | (intraday_df["volume"] == 0),
        "vwap",
    ] = pd.NA

    ingested_at = datetime.now(UTC)
    standardized_at = datetime.now(UTC)

    intraday_df["ingested_at"] = ingested_at
    intraday_df["standardized_at"] = standardized_at

    intraday_df = intraday_df.drop(
        columns=["bucket_start_local", "bucket_end_local", "vwap_numerator"]
    )

    intraday_df["symbol"] = intraday_df["symbol"].astype("string")
    intraday_df["timeframe"] = intraday_df["timeframe"].astype("string")
    intraday_df["source"] = intraday_df["source"].astype("string")

    numeric_float_cols = ["open", "high", "low", "close", "vwap"]
    for col in numeric_float_cols:
        intraday_df[col] = pd.to_numeric(intraday_df[col], errors="coerce")

    intraday_df["volume"] = (
        pd.to_numeric(intraday_df["volume"], errors="coerce").round().astype("Int64")
    )
    intraday_df["trade_count"] = (
        pd.to_numeric(intraday_df["trade_count"], errors="coerce").round().astype("Int64")
    )

    return intraday_df[INTRADAY_BAR_COLUMNS]