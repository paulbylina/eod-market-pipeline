import pandas as pd

from src.standardization.market.standardize_bars import standardize_bars


def standardize_daily_bars(raw_response: dict) -> pd.DataFrame:
    """
    Backward-compatible wrapper around the generic bar standardizer.
    """
    return standardize_bars(raw_response=raw_response, timeframe="1d")