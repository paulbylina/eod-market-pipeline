from dataclasses import dataclass


@dataclass(frozen=True)
class TimeframeSpec:
    name: str
    multiplier: int
    unit: str
    source_native: bool
    base_timeframe: str | None = None


TIMEFRAME_SPECS: dict[str, TimeframeSpec] = {
    "1d": TimeframeSpec(
        name="1d",
        multiplier=1,
        unit="day",
        source_native=True,
    ),
    "1w": TimeframeSpec(
        name="1w",
        multiplier=1,
        unit="week",
        source_native=False,
        base_timeframe="1d",
    ),
    "1mo": TimeframeSpec(
        name="1mo",
        multiplier=1,
        unit="month",
        source_native=False,
        base_timeframe="1d",
    ),
}


def get_timeframe_spec(timeframe: str) -> TimeframeSpec:
    try:
        return TIMEFRAME_SPECS[timeframe]
    except KeyError as exc:
        supported = ", ".join(sorted(TIMEFRAME_SPECS))
        raise ValueError(
            f"Unsupported timeframe '{timeframe}'. Supported timeframes: {supported}"
        ) from exc