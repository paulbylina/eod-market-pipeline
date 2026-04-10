from src.clients.massive.massive_client import MassiveClient
from src.utils.timeframes import get_timeframe_spec


def fetch_bars(
    symbol: str,
    timeframe: str,
    start_date: str,
    end_date: str,
    adjusted: bool = True,
    sort: str = "asc",
    limit: int = 5000,
) -> dict:
    """
    Fetch OHLCV aggregate bars for a single stock symbol from the Massive API.

    Only source-native timeframes should be fetched directly from the vendor.
    Derived timeframes such as 1w and 1mo should be built from lower-granularity
    curated data in a later aggregation pipeline.
    """
    spec = get_timeframe_spec(timeframe)

    if not spec.source_native:
        raise ValueError(
            f"Timeframe '{timeframe}' is configured as derived, not source-native."
        )

    client = MassiveClient()

    path = (
        f"/v2/aggs/ticker/{symbol}/range/"
        f"{spec.multiplier}/{spec.unit}/{start_date}/{end_date}"
    )

    params = {
        "adjusted": str(adjusted).lower(),
        "sort": sort,
        "limit": limit,
    }

    return client._get(path, params=params)