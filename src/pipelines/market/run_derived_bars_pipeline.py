import pandas as pd

from src.aggregation.market import (
    aggregate_daily_to_monthly,
    aggregate_daily_to_weekly,
)
from src.features.market import generate_bar_features
from src.storage.write_data import write_dataframe_parquet
from src.storage.write_quality_data import write_quality_dataframe
from src.utils.path_builders import (
    build_market_curated_output_path,
    build_market_staging_output_path,
    build_market_validation_failures_output_path,
    build_market_validation_summary_output_path,
    build_market_validation_warnings_output_path,
)
from src.validation.market import validate_bars


SUPPORTED_DERIVATIONS = {
    ("1d", "1w"),
    ("1d", "1mo"),
}


def run_derived_bars_pipeline(
    symbol: str,
    start_date: str,
    end_date: str,
    source_timeframe: str = "1d",
    target_timeframe: str = "1w",
) -> None:
    """
    Build derived bars from an existing lower-granularity curated dataset.

    Supported derivations:
        1d -> 1w
        1d -> 1mo
    """
    derivation = (source_timeframe, target_timeframe)
    if derivation not in SUPPORTED_DERIVATIONS:
        supported = ", ".join(
            f"{source}->{target}" for source, target in sorted(SUPPORTED_DERIVATIONS)
        )
        raise ValueError(
            f"Unsupported derivation {source_timeframe}->{target_timeframe}. "
            f"Supported derivations: {supported}"
        )

    source_path = build_market_curated_output_path(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        timeframe=source_timeframe,
    )

    if not source_path.exists():
        raise FileNotFoundError(
            f"Source curated dataset does not exist: {source_path}. "
            f"Run the {source_timeframe} source pipeline first."
        )

    source_df = pd.read_parquet(source_path)

    if derivation == ("1d", "1w"):
        aggregated_df = aggregate_daily_to_weekly(source_df)
    elif derivation == ("1d", "1mo"):
        aggregated_df = aggregate_daily_to_monthly(source_df)
    else:
        raise ValueError(f"No aggregation handler implemented for {derivation}")

    valid_df, failures_df, warnings_df, summary_df = validate_bars(aggregated_df)
    featured_df = generate_bar_features(valid_df)

    write_dataframe_parquet(
        aggregated_df,
        build_market_staging_output_path(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            timeframe=target_timeframe,
        ),
    )

    write_dataframe_parquet(
        featured_df,
        build_market_curated_output_path(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            timeframe=target_timeframe,
        ),
    )

    write_quality_dataframe(
        failures_df,
        build_market_validation_failures_output_path(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            timeframe=target_timeframe,
        ),
    )

    write_quality_dataframe(
        warnings_df,
        build_market_validation_warnings_output_path(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            timeframe=target_timeframe,
        ),
    )

    write_quality_dataframe(
        summary_df,
        build_market_validation_summary_output_path(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            timeframe=target_timeframe,
        ),
    )