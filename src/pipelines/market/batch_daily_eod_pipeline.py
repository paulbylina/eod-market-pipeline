from pathlib import Path

from src.pipelines.market.daily_eod_pipeline import run_daily_eod_pipeline
from src.utils.load_symbols import load_symbols


def run_batch_daily_eod_pipeline(symbols_file: Path, start_date: str, end_date: str) -> None:
    symbols = load_symbols(symbols_file)
    failed_symbols: list[str] = []

    for symbol in symbols:
        print(f"Running pipeline for {symbol}...")

        try:
            run_daily_eod_pipeline(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
            )
        except Exception as exc:
            print(f"Failed for {symbol}: {exc}")
            failed_symbols.append(symbol)
            continue

    print("Batch pipeline completed")

    if failed_symbols:
        print(f"Failed symbols count: {len(failed_symbols)}")
        print("Failed symbols:")
        for symbol in failed_symbols:
            print(symbol)