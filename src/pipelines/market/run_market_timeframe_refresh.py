from collections.abc import Sequence

from src.pipelines.market.run_derived_bars_pipeline import run_derived_bars_pipeline
from src.pipelines.market.run_market_bars_pipeline import run_market_bars_pipeline


SUPPORTED_SOURCE_TIMEFRAMES = {"1d"}
SUPPORTED_DERIVED_TIMEFRAMES = {"1w", "1mo"}


def run_market_timeframe_refresh(
    symbol: str,
    start_date: str,
    end_date: str,
    source_timeframes: Sequence[str] = ("1d",),
    derived_timeframes: Sequence[str] = ("1w", "1mo"),
) -> None:
    """
    Refresh source-native and derived market bar datasets for a symbol/date range.

    Current supported flow:
      - source-native: 1d
      - derived from 1d: 1w, 1mo
    """
    invalid_source = sorted(set(source_timeframes) - SUPPORTED_SOURCE_TIMEFRAMES)
    if invalid_source:
        invalid = ", ".join(invalid_source)
        supported = ", ".join(sorted(SUPPORTED_SOURCE_TIMEFRAMES))
        raise ValueError(
            f"Unsupported source timeframes: {invalid}. Supported: {supported}"
        )

    invalid_derived = sorted(set(derived_timeframes) - SUPPORTED_DERIVED_TIMEFRAMES)
    if invalid_derived:
        invalid = ", ".join(invalid_derived)
        supported = ", ".join(sorted(SUPPORTED_DERIVED_TIMEFRAMES))
        raise ValueError(
            f"Unsupported derived timeframes: {invalid}. Supported: {supported}"
        )

    for timeframe in source_timeframes:
        run_market_bars_pipeline(
            symbol=symbol,
            timeframe=timeframe,
            start_date=start_date,
            end_date=end_date,
        )

    for timeframe in derived_timeframes:
        if timeframe == "1w":
            run_derived_bars_pipeline(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                source_timeframe="1d",
                target_timeframe="1w",
            )
        elif timeframe == "1mo":
            run_derived_bars_pipeline(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                source_timeframe="1d",
                target_timeframe="1mo",
            )