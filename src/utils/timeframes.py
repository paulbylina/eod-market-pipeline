from dataclasses import dataclass


@dataclass(frozen=True)
class TimeframeSpec:
    name: str
    multiplier: int
    unit: str
    source_native: bool
    base_timeframe: str | None = None


@dataclass(frozen=True)
class DerivationSpec:
    source_timeframe: str
    target_timeframe: str


TIMEFRAME_SPECS: dict[str, TimeframeSpec] = {
    "1m": TimeframeSpec(
        name="1m",
        multiplier=1,
        unit="minute",
        source_native=True,
    ),
    "5m": TimeframeSpec(
        name="5m",
        multiplier=5,
        unit="minute",
        source_native=False,
        base_timeframe="1m",
    ),
    "15m": TimeframeSpec(
        name="15m",
        multiplier=15,
        unit="minute",
        source_native=False,
        base_timeframe="1m",
    ),
    "60m": TimeframeSpec(
        name="60m",
        multiplier=60,
        unit="minute",
        source_native=False,
        base_timeframe="1m",
    ),
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


DERIVATION_SPECS: dict[str, DerivationSpec] = {
    "5m": DerivationSpec(source_timeframe="1m", target_timeframe="5m"),
    "15m": DerivationSpec(source_timeframe="1m", target_timeframe="15m"),
    "60m": DerivationSpec(source_timeframe="1m", target_timeframe="60m"),
    "1w": DerivationSpec(source_timeframe="1d", target_timeframe="1w"),
    "1mo": DerivationSpec(source_timeframe="1d", target_timeframe="1mo"),
}


def get_timeframe_spec(timeframe: str) -> TimeframeSpec:
    try:
        return TIMEFRAME_SPECS[timeframe]
    except KeyError as exc:
        supported = ", ".join(TIMEFRAME_SPECS)
        raise ValueError(
            f"Unsupported timeframe '{timeframe}'. Supported timeframes: {supported}"
        ) from exc


def get_derivation_spec(target_timeframe: str) -> DerivationSpec:
    try:
        return DERIVATION_SPECS[target_timeframe]
    except KeyError as exc:
        supported = ", ".join(DERIVATION_SPECS)
        raise ValueError(
            f"Unsupported derived timeframe '{target_timeframe}'. "
            f"Supported derived timeframes: {supported}"
        ) from exc


def is_source_timeframe(timeframe: str) -> bool:
    return get_timeframe_spec(timeframe).source_native


def is_derived_timeframe(timeframe: str) -> bool:
    return not get_timeframe_spec(timeframe).source_native


def list_source_timeframes() -> tuple[str, ...]:
    return tuple(
        timeframe
        for timeframe, spec in TIMEFRAME_SPECS.items()
        if spec.source_native
    )


def list_derived_timeframes() -> tuple[str, ...]:
    return tuple(DERIVATION_SPECS.keys())