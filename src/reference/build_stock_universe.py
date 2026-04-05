# Pull the active common-stock universe from Massive and write it to config/symbols.txt

from pathlib import Path

import httpx

from src.utils.settings import (
    CONFIG_DIR,
    MASSIVE_API_KEY,
    MASSIVE_BASE_URL,
    REQUEST_TIMEOUT_SECONDS,
)


def fetch_all_common_stock_symbols() -> list[str]:
    if not MASSIVE_API_KEY:
        raise RuntimeError("MASSIVE_API_KEY is not set")

    base_url = f"{MASSIVE_BASE_URL}/v3/reference/tickers"
    params = {
        "market": "stocks",
        "active": "true",
        "type": "CS",
        "sort": "ticker",
        "order": "asc",
        "limit": 1000,
        "apiKey": MASSIVE_API_KEY,
    }

    symbols: list[str] = []
    next_url: str | None = base_url

    with httpx.Client(timeout=REQUEST_TIMEOUT_SECONDS) as client:
        while next_url is not None:
            response = client.get(
                next_url,
                params=params if next_url == base_url else None,
            )
            response.raise_for_status()
            payload = response.json()

            for item in payload.get("results", []):
                ticker = item.get("ticker")
                if ticker:
                    symbols.append(ticker)

            next_url = payload.get("next_url")
            if next_url and "apiKey=" not in next_url:
                separator = "&" if "?" in next_url else "?"
                next_url = f"{next_url}{separator}apiKey={MASSIVE_API_KEY}"

    return sorted(set(symbols))


def write_symbols_file(symbols: list[str], output_path: Path) -> None:
    output_path.write_text("\n".join(symbols) + "\n", encoding="utf-8")


def main() -> None:
    output_path = CONFIG_DIR / "symbols.txt"
    symbols = fetch_all_common_stock_symbols()
    write_symbols_file(symbols, output_path)

    print(f"Wrote {len(symbols)} symbols to {output_path}")


if __name__ == "__main__":
    main()