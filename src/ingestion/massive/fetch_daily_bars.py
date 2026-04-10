from src.ingestion.massive.fetch_bars import fetch_bars


def fetch_daily_bars(
    symbol: str,
    start_date: str,
    end_date: str,
    adjusted: bool = True,
    sort: str = "asc",
    limit: int = 5000,
) -> dict:
    """
    Backward-compatible wrapper around the generic bar fetcher.
    """
    return fetch_bars(
        symbol=symbol,
        timeframe="1d",
        start_date=start_date,
        end_date=end_date,
        adjusted=adjusted,
        sort=sort,
        limit=limit,
    )