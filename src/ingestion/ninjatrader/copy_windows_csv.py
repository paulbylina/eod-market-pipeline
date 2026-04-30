from pathlib import Path
import argparse
import shutil


def copy_csv(source_path: Path, destination_dir: Path, overwrite: bool) -> Path:
    if not source_path.exists():
        raise FileNotFoundError(f"Source file not found: {source_path}")

    destination_dir.mkdir(parents=True, exist_ok=True)
    destination_path = destination_dir / source_path.name

    if destination_path.exists() and not overwrite:
        print(f"Already exists, skipping: {destination_path}")
        return destination_path

    shutil.copy2(source_path, destination_path)

    print("Copied:")
    print(f"  from: {source_path}")
    print(f"  to:   {destination_path}")

    return destination_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Copy a specific NinjaTrader CSV file from Windows storage into WSL ninja-lake."
    )

    parser.add_argument(
        "--feed-type",
        choices=["L1", "L2"],
        required=True,
        help="Feed type to copy, for example L1 or L2.",
    )

    parser.add_argument(
        "--filename",
        required=True,
        help="CSV filename to copy, for example ES_06-26_L2_20260429_071242.csv.",
    )

    parser.add_argument(
        "--windows-root",
        type=Path,
        default=Path("/mnt/c/REPOSITORY/trading-dev-framework/ninja-lake/raw/ninjatrader"),
        help="Windows-mounted raw NinjaTrader root.",
    )

    parser.add_argument(
        "--wsl-root",
        type=Path,
        default=Path("../ninja-lake/raw/ninjatrader"),
        help="WSL raw NinjaTrader root.",
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite the destination file if it already exists.",
    )

    args = parser.parse_args()

    source_path = args.windows_root / args.feed_type / args.filename
    destination_dir = args.wsl_root / args.feed_type

    copy_csv(source_path, destination_dir, args.overwrite)


if __name__ == "__main__":
    main()
