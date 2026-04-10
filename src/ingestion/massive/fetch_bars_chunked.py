from src.ingestion.massive.fetch_bars import fetch_bars
from src.utils.date_ranges import chunk_date_range
from src.utils.timeframes import get_timeframe_spec


def _deduplicate_results(results: list[dict]) -> list[dict]:
    """
    Deduplicate aggregate rows by timestamp while preserving ascending order.
    """
    deduped_by_timestamp: dict[int, dict] = {}
    passthrough_rows: list[dict] = []

    for row in results:
        timestamp = row.get("t")
        if isinstance(timestamp, int):
            deduped_by_timestamp[timestamp] = row
        else:
            passthrough_rows.append(row)

    ordered_timestamp_rows = [
        deduped_by_timestamp[timestamp]
        for timestamp in sorted(deduped_by_timestamp)
    ]

    return ordered_timestamp_rows + passthrough_rows


def fetch_bars_chunked(
    symbol: str,
    timeframe: str,
    start_date: str,
    end_date: str,
    adjusted: bool = True,
    sort: str = "asc",
    limit: int = 50000,
    chunk_days: int = 28,
) -> dict:
    """
    Fetch source-native bars over multiple date chunks and merge the results.

    This is primarily intended for 1m bars so long ranges do not exceed vendor
    guidance or base-aggregate limits.
    """
    spec = get_timeframe_spec(timeframe)

    if not spec.source_native:
        raise ValueError(
            f"Timeframe '{timeframe}' is configured as derived, not source-native."
        )

    combined_results: list[dict] = []
    chunk_metadata: list[dict] = []

    for chunk_start, chunk_end in chunk_date_range(
        start_date=start_date,
        end_date=end_date,
        chunk_days=chunk_days,
    ):
        response = fetch_bars(
            symbol=symbol,
            timeframe=timeframe,
            start_date=chunk_start,
            end_date=chunk_end,
            adjusted=adjusted,
            sort=sort,
            limit=limit,
        )

        chunk_results = response.get("results", []) or []
        combined_results.extend(chunk_results)

        chunk_metadata.append(
            {
                "start_date": chunk_start,
                "end_date": chunk_end,
                "results_count": len(chunk_results),
                "status": response.get("status"),
            }
        )

    combined_results = _deduplicate_results(combined_results)

    return {
        "ticker": symbol,
        "timeframe": timeframe,
        "adjusted": adjusted,
        "sort": sort,
        "status": "OK",
        "queryCount": len(combined_results),
        "resultsCount": len(combined_results),
        "results": combined_results,
        "chunks": chunk_metadata,
    }