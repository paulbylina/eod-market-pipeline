from pathlib import Path

import pandas as pd

from src.utils.settings import CURATED_DATA_DIR, QUALITY_DATA_DIR


def _extract_symbol_from_curated_filename(
    path: Path,
    start_date: str,
    end_date: str,
) -> str:
    suffix = f"_{start_date}_{end_date}_curated.parquet"
    name = path.name
    if not name.endswith(suffix):
        raise ValueError(f"Unexpected curated filename format: {name}")
    return name.removesuffix(suffix)


def build_daily_gap_audit(
    start_date: str,
    end_date: str,
) -> dict:
    """
    Build a first-pass daily coverage audit from curated 1d parquet files.

    Expected trading days are inferred from the union of all observed daily
    session dates across the symbol set for the requested window. This is a good
    universe-level audit even without an exchange calendar dependency.

    Outputs:
      - by_symbol daily coverage audit parquet
      - missing symbol/date pairs parquet
      - one-row overall summary parquet
    """
    curated_dir = CURATED_DATA_DIR / "market" / "1d"
    output_dir = QUALITY_DATA_DIR / "market" / "run_summary"
    output_dir.mkdir(parents=True, exist_ok=True)

    by_symbol_output_path = output_dir / f"{start_date}_{end_date}_daily_gap_audit_by_symbol.parquet"
    missing_output_path = output_dir / f"{start_date}_{end_date}_daily_gap_audit_missing_dates.parquet"
    overall_output_path = output_dir / f"{start_date}_{end_date}_daily_gap_audit_overall.parquet"

    pattern = f"*_{start_date}_{end_date}_curated.parquet"
    files = sorted(curated_dir.glob(pattern))

    if not files:
        by_symbol_df = pd.DataFrame(
            columns=[
                "symbol",
                "start_date",
                "end_date",
                "expected_bars",
                "actual_bars",
                "missing_bars",
                "coverage_pct",
                "passes_full_coverage",
            ]
        )
        missing_df = pd.DataFrame(columns=["symbol", "missing_date"])
        overall_df = pd.DataFrame(
            [
                {
                    "symbol_count": 0,
                    "expected_dates": 0,
                    "symbols_with_gaps": 0,
                    "total_missing_bars": 0,
                    "average_coverage_pct": 0.0,
                    "median_coverage_pct": 0.0,
                }
            ]
        )

        by_symbol_df.to_parquet(by_symbol_output_path, index=False)
        missing_df.to_parquet(missing_output_path, index=False)
        overall_df.to_parquet(overall_output_path, index=False)

        return {
            "by_symbol_output_path": str(by_symbol_output_path),
            "missing_output_path": str(missing_output_path),
            "overall_output_path": str(overall_output_path),
            "symbol_count": 0,
        }

    symbol_to_dates: dict[str, set[pd.Timestamp]] = {}

    for path in files:
        symbol = _extract_symbol_from_curated_filename(
            path=path,
            start_date=start_date,
            end_date=end_date,
        )

        df = pd.read_parquet(path)

        if df.empty or "session_date" not in df.columns:
            symbol_to_dates[symbol] = set()
            continue

        session_dates = set(
            pd.to_datetime(df["session_date"], errors="coerce")
            .dropna()
            .dt.normalize()
            .tolist()
        )
        symbol_to_dates[symbol] = session_dates

    expected_dates = sorted(set().union(*symbol_to_dates.values()))

    by_symbol_rows: list[dict] = []
    missing_rows: list[dict] = []

    for symbol, actual_dates in sorted(symbol_to_dates.items()):
        missing_dates = [date for date in expected_dates if date not in actual_dates]

        for missing_date in missing_dates:
            missing_rows.append(
                {
                    "symbol": symbol,
                    "missing_date": pd.Timestamp(missing_date),
                }
            )

        expected_count = len(expected_dates)
        actual_count = len(actual_dates)
        missing_count = len(missing_dates)
        coverage_pct = (actual_count / expected_count * 100.0) if expected_count else 0.0

        by_symbol_rows.append(
            {
                "symbol": symbol,
                "start_date": start_date,
                "end_date": end_date,
                "expected_bars": expected_count,
                "actual_bars": actual_count,
                "missing_bars": missing_count,
                "coverage_pct": coverage_pct,
                "passes_full_coverage": missing_count == 0,
            }
        )

    by_symbol_df = pd.DataFrame(by_symbol_rows).sort_values(
        ["passes_full_coverage", "coverage_pct", "symbol"],
        ascending=[True, True, True],
    ).reset_index(drop=True)

    missing_df = pd.DataFrame(
        missing_rows,
        columns=["symbol", "missing_date"],
    ).sort_values(
        ["symbol", "missing_date"]
    ).reset_index(drop=True)

    overall_df = pd.DataFrame(
        [
            {
                "symbol_count": len(by_symbol_df),
                "expected_dates": len(expected_dates),
                "symbols_with_gaps": int((~by_symbol_df["passes_full_coverage"]).sum()),
                "total_missing_bars": int(by_symbol_df["missing_bars"].sum()),
                "average_coverage_pct": float(by_symbol_df["coverage_pct"].mean()),
                "median_coverage_pct": float(by_symbol_df["coverage_pct"].median()),
            }
        ]
    )

    by_symbol_df.to_parquet(by_symbol_output_path, index=False)
    missing_df.to_parquet(missing_output_path, index=False)
    overall_df.to_parquet(overall_output_path, index=False)

    return {
        "by_symbol_output_path": str(by_symbol_output_path),
        "missing_output_path": str(missing_output_path),
        "overall_output_path": str(overall_output_path),
        "symbol_count": len(by_symbol_df),
    }