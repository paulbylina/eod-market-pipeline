from datetime import UTC, datetime

import pandas as pd


def _build_bar_end(bar_start_utc: pd.Series, timeframe: str) -> pd.Series:
    if timeframe == "1m":
        return bar_start_utc + pd.Timedelta(minutes=1)
    if timeframe == "5m":
        return bar_start_utc + pd.Timedelta(minutes=5)
    if timeframe == "15m":
        return bar_start_utc + pd.Timedelta(minutes=15)
    if timeframe == "60m":
        return bar_start_utc + pd.Timedelta(minutes=60)
    if timeframe == "1d":
        return bar_start_utc + pd.Timedelta(days=1)
    if timeframe == "1w":
        return bar_start_utc + pd.Timedelta(weeks=1)
    if timeframe == "1mo":
        return bar_start_utc + pd.DateOffset(months=1)

    return bar_start_utc


def standardize_bars(raw_response: dict, timeframe: str) -> pd.DataFrame:
    """
    Convert a raw Massive aggregates response into a standardized bar DataFrame.

    `bar_start` / `bar_end` are stored as UTC-based naive timestamps.
    `session_date` is stored in America/New_York calendar terms so intraday bars
    remain attached to the correct U.S. equity session date.
    """
    results = raw_response.get("results", [])

    columns = [
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

    if not results:
        return pd.DataFrame(columns=columns)

    df = pd.DataFrame(results).rename(
        columns={
            "o": "open",
            "h": "high",
            "l": "low",
            "c": "close",
            "v": "volume",
            "vw": "vwap",
            "n": "trade_count",
            "t": "timestamp",
        }
    )

    for column in [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "vwap",
        "trade_count",
        "timestamp",
    ]:
        if column not in df.columns:
            df[column] = pd.NA

    bar_start_utc = pd.to_datetime(
        df["timestamp"],
        unit="ms",
        utc=True,
        errors="coerce",
    )

    df["symbol"] = raw_response.get("ticker")
    df["timeframe"] = timeframe
    df["bar_start"] = bar_start_utc.dt.tz_localize(None)
    df["bar_end"] = _build_bar_end(df["bar_start"], timeframe)

    df["session_date"] = (
        bar_start_utc
        .dt.tz_convert("America/New_York")
        .dt.normalize()
        .dt.tz_localize(None)
    )
    df["date"] = df["session_date"]

    df["source"] = "massive"

    ingested_at = datetime.now(UTC)
    standardized_at = datetime.now(UTC)

    df["ingested_at"] = ingested_at
    df["standardized_at"] = standardized_at

    df["symbol"] = df["symbol"].astype("string")
    df["timeframe"] = df["timeframe"].astype("string")
    df["source"] = df["source"].astype("string")

    numeric_float_cols = ["open", "high", "low", "close", "vwap"]
    for col in numeric_float_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").round().astype("Int64")
    df["trade_count"] = (
        pd.to_numeric(df["trade_count"], errors="coerce").round().astype("Int64")
    )

    return df[columns]