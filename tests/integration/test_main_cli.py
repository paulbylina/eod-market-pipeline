import subprocess


def test_main_cli_runs_successfully() -> None:
    result = subprocess.run(
        [
            "uv",
            "run",
            "python",
            "-m",
            "src.main",
            "--symbol",
            "AAPL",
            "--start-date",
            "2023-10-01",
            "--end-date",
            "2024-01-31",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert "Starting daily EOD pipeline for AAPL" in result.stdout
    assert "Pipeline completed successfully." in result.stdout