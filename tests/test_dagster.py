"""
Tests for Dagster definitions, job configuration, schedules, and asset execution.
"""

from unittest.mock import patch
from dagster import materialize
import pytest

from src.weather_pipeline.config import config
from src.weather_pipeline.dagster_orchestration.definitions import defs
from src.weather_pipeline.dagster_orchestration.assets import (
    fetch_weather_data,
    load_weather_to_duckdb,
    weather_analytics_report,
)


def test_dagster_definitions_structure():
    """Verify Dagster Definitions contains all assets, jobs, and schedules."""
    # Check assets
    asset_keys = {
        asset.key.to_user_string()
        for asset in [fetch_weather_data, load_weather_to_duckdb, weather_analytics_report]
    }
    assert "fetch_weather_data" in asset_keys
    assert "load_weather_to_duckdb" in asset_keys
    assert "weather_analytics_report" in asset_keys

    # Check job
    job = defs.get_job_def("weather_ingestion_job")
    assert job is not None
    assert job.name == "weather_ingestion_job"

    # Check schedule
    schedule = defs.get_schedule_def("hourly_weather_schedule")
    assert schedule is not None
    assert schedule.cron_schedule == "0 * * * *"
    assert schedule.execution_timezone == "Asia/Kolkata"


def test_dagster_asset_execution_mocked(mock_open_meteo_payload, tmp_path):
    """Test full in-process execution of Dagster assets with mocked API data."""
    test_db = tmp_path / "dagster_test.duckdb"

    with patch("src.weather_pipeline.client.OpenMeteoClient.fetch_weather_raw", return_value=mock_open_meteo_payload):
        with patch.object(config, "duckdb_path", str(test_db)):
            result = materialize(
                [fetch_weather_data, load_weather_to_duckdb, weather_analytics_report],
            )
            assert result.success


def test_dagster_job_execution_in_process():
    """Execute the complete Dagster weather ingestion job in-process."""
    job = defs.get_job_def("weather_ingestion_job")
    result = job.execute_in_process()
    assert result.success
