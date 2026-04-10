import pandas as pd


def generate_bar_features(valid_df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate 30-bar rolling volume and close-price statistics plus z-score features.

    Grouping is symbol + timeframe so multiple bar granularities can coexist safely.
    """
    df = valid_df.copy()

    if df.empty:
        return df

    sort_columns = ["symbol", "bar_start"]
    group_columns = ["symbol"]

    if "timeframe" in df.columns:
        sort_columns = ["symbol", "timeframe", "bar_start"]
        group_columns = ["symbol", "timeframe"]

    df = df.sort_values(sort_columns).reset_index(drop=True)

    grouped = df.groupby(group_columns, group_keys=False)

    df["volume_mean_30bar"] = grouped["volume"].transform(
        lambda s: s.rolling(window=30, min_periods=30).mean()
    )
    df["volume_std_30bar"] = grouped["volume"].transform(
        lambda s: s.rolling(window=30, min_periods=30).std()
    )
    df["close_mean_30bar"] = grouped["close"].transform(
        lambda s: s.rolling(window=30, min_periods=30).mean()
    )
    df["close_std_30bar"] = grouped["close"].transform(
        lambda s: s.rolling(window=30, min_periods=30).std()
    )

    df["volume_zscore_30bar"] = (
        (df["volume"] - df["volume_mean_30bar"]) / df["volume_std_30bar"]
    )
    df["close_price_zscore_30bar"] = (
        (df["close"] - df["close_mean_30bar"]) / df["close_std_30bar"]
    )

    df.loc[df["volume_std_30bar"] == 0, "volume_zscore_30bar"] = pd.NA
    df.loc[df["close_std_30bar"] == 0, "close_price_zscore_30bar"] = pd.NA

    return df