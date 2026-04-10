from collections.abc import Iterator
from datetime import date, timedelta


def chunk_date_range(
    start_date: str,
    end_date: str,
    chunk_days: int,
) -> Iterator[tuple[str, str]]:
    """
    Yield inclusive ISO date chunks across a date range.

    Example:
        2024-01-01 to 2024-03-15 with chunk_days=28
        -> (2024-01-01, 2024-01-28), (2024-01-29, 2024-02-25), ...
    """
    if chunk_days <= 0:
        raise ValueError("chunk_days must be positive")

    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)

    if start > end:
        raise ValueError("start_date must be <= end_date")

    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end)
        yield current.isoformat(), chunk_end.isoformat()
        current = chunk_end + timedelta(days=1)