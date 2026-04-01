import os
import pytest
from dotenv import load_dotenv
from fredapi import Fred

@pytest.mark.fred
def test_can_fetch_us_rate_series() -> None:
    load_dotenv()

    api_key = os.getenv("FRED_API_KEY")
    assert api_key is not None
    assert api_key != ""

    fred = Fred(api_key=api_key)

    series = fred.get_series("EFFR")

    assert series is not None
    assert len(series) > 0