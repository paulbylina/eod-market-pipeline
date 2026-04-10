from src.features.market import generate_bar_features, generate_daily_features
from src.ingestion.massive import fetch_bars
from src.standardization.market import standardize_bars
from src.storage.write_data import write_dataframe_parquet, write_raw_json
from src.storage.write_quality_data import write_quality_dataframe
from src.utils.path_builders import (
    build_market_curated_output_path,
    build_market_staging_output_path,
    build_market_validation_failures_output_path,
    build_market_validation_summary_output_path,
    build_market_validation_warnings_output_path,
    build_massive_raw_output_path,
)
from src.validation.market import validate_bars


def run_market_bars_pipeline(
    symbol: str,
    timeframe: str,
    start_date: str,
    end_date: str,
) -> None:
    """
    Run the end-to-end source-native bar pipeline for a single symbol, timeframe,
    and date range.
    """
    raw_response = fetch_bars(
        symbol=symbol,
        timeframe=timeframe,
        start_date=start_date,
        end_date=end_date,
    )

    standardized_df = standardize_bars(
        raw_response=raw_response,
        timeframe=timeframe,
    )

    valid_df, failures_df, warnings_df, summary_df = validate_bars(standardized_df)

    if timeframe == "1d":
        featured_df = generate_daily_features(valid_df)
    else:
        featured_df = generate_bar_features(valid_df)

    write_raw_json(
        raw_response,
        build_massive_raw_output_path(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            timeframe=timeframe,
        ),
    )

    write_dataframe_parquet(
        standardized_df,
        build_market_staging_output_path(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            timeframe=timeframe,
        ),
    )

    write_dataframe_parquet(
        featured_df,
        build_market_curated_output_path(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            timeframe=timeframe,
        ),
    )

    write_quality_dataframe(
        failures_df,
        build_market_validation_failures_output_path(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            timeframe=timeframe,
        ),
    )

    write_quality_dataframe(
        warnings_df,
        build_market_validation_warnings_output_path(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            timeframe=timeframe,
        ),
    )

    write_quality_dataframe(
        summary_df,
        build_market_validation_summary_output_path(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            timeframe=timeframe,
        ),
    )