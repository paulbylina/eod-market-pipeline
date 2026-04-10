from datetime import UTC, datetime

import pandas as pd


def standardize_bars(raw_response: dict, timeframe: str) -> pd.DataFrame:
    """
    Convert a raw Massive aggregates response into a standardized bar DataFrame.

    This is the canonical bar schema for the platform. A legacy `date` column is
    kept for compatibility with current daily consumers while the platform moves
    toward timeframe-aware keys.
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

    df["symbol"] = raw_response.get("ticker")
    df["timeframe"] = timeframe

    df["bar_start"] = pd.to_datetime(
        df["timestamp"],
        unit="ms",
        utc=True,
        errors="coerce",
    ).dt.tz_localize(None)

    if timeframe == "1d":
        df["bar_end"] = df["bar_start"] + pd.Timedelta(days=1)
    elif timeframe == "1w":
        df["bar_end"] = df["bar_start"] + pd.Timedelta(weeks=1)
    else:
        df["bar_end"] = df["bar_start"]

    df["session_date"] = df["bar_start"].dt.normalize()
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