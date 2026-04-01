import os
import pytest
from dotenv import load_dotenv

@pytest.mark.fred
def test_fred_api_key_exists() -> None:
    load_dotenv()
    api_key = os.getenv("FRED_API_KEY")

    assert api_key is not None
    assert api_key != ""