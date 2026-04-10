from src.ingestion.massive.fetch_bars_chunked import fetch_bars_chunked


def fetch_minute_bars(
    symbol: str,
    start_date: str,
    end_date: str,
    adjusted: bool = True,
    sort: str = "asc",
    limit: int = 50000,
    chunk_days: int = 28,
) -> dict:
    """
    Fetch 1-minute OHLCV aggregate bars for a single stock symbol over a date range.

    Requests are chunked to keep large minute-bar pulls manageable.
    """
    return fetch_bars_chunked(
        symbol=symbol,
        timeframe="1m",
        start_date=start_date,
        end_date=end_date,
        adjusted=adjusted,
        sort=sort,
        limit=limit,
        chunk_days=chunk_days,
    )