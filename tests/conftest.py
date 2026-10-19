"""
Pytest configuration and test fixtures.
"""

from pathlib import Path
import pytest
from src.weather_pipeline.config import PipelineConfig


@pytest.fixture
def mock_open_meteo_payload():
    """Sample Open-Meteo REST API JSON response for Faridabad."""
    return {
        "latitude": 28.4089,
        "longitude": 77.3178,
        "generationtime_ms": 0.12,
        "utc_offset_seconds": 19800,
        "timezone": "Asia/Kolkata",
        "timezone_abbreviation": "IST",
        "elevation": 210.0,
        "current": {
            "time": "2026-10-19T14:00",
            "interval": 900,
            "temperature_2m": 31.4,
            "relative_humidity_2m": 42.0,
            "wind_speed_10m": 8.5,
            "weather_code": 1,
            "is_day": 1,
        },
        "hourly": {
            "time": [
                "2026-10-19T12:00",
                "2026-10-19T13:00",
                "2026-10-19T14:00",
            ],
            "temperature_2m": [30.1, 31.0, 31.4],
            "relative_humidity_2m": [45.0, 43.0, 42.0],
            "wind_speed_10m": [7.2, 8.0, 8.5],
            "weather_code": [0, 1, 1],
            "is_day": [1, 1, 1],
        },
    }


@pytest.fixture
def test_config(tmp_path: Path):
    """Custom PipelineConfig pointing to a temporary DuckDB database."""
    test_db = tmp_path / "test_weather.duckdb"
    return PipelineConfig(
        location_name="Faridabad, Haryana, India",
        latitude=28.4089,
        longitude=77.3178,
        duckdb_path=str(test_db),
        dlt_pipeline_name="test_pipeline",
        dlt_dataset_name="test_analytics",
        api_timeout_seconds=5,
        api_max_retries=2,
    )
