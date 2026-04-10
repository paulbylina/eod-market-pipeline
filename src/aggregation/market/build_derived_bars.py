import pandas as pd

from src.aggregation.market.aggregate_daily_to_monthly import aggregate_daily_to_monthly
from src.aggregation.market.aggregate_daily_to_weekly import aggregate_daily_to_weekly
from src.aggregation.market.aggregate_intraday_from_minute import (
    aggregate_intraday_from_minute,
)


def build_derived_bars(
    source_df: pd.DataFrame,
    source_timeframe: str,
    target_timeframe: str,
) -> pd.DataFrame:
    """
    Dispatch derived-bar construction based on source/target timeframe pair.
    """
    derivation = (source_timeframe, target_timeframe)

    if derivation == ("1d", "1w"):
        return aggregate_daily_to_weekly(source_df)

    if derivation == ("1d", "1mo"):
        return aggregate_daily_to_monthly(source_df)

    if derivation in {("1m", "5m"), ("1m", "15m"), ("1m", "60m")}:
        return aggregate_intraday_from_minute(
            minute_df=source_df,
            target_timeframe=target_timeframe,
        )

    raise ValueError(
        f"No derived-bar builder registered for {source_timeframe}->{target_timeframe}"
    )