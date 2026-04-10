import pandas as pd

from src.validation.market.validate_bars import validate_bars


def validate_daily_bars(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Backward-compatible wrapper around the generic bar validator.
    """
    return validate_bars(df)