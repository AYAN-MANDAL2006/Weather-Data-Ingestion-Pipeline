"""
Tests for dlt ingestion pipeline and DuckDB destination.
"""

from pathlib import Path
import duckdb
import pytest

from src.weather_pipeline.pipeline import WeatherPipelineRunner
from src.weather_pipeline.config import PipelineConfig


def test_dlt_pipeline_ingestion(tmp_path: Path):
    """Test loading records into an isolated DuckDB instance via dlt."""
    db_file = tmp_path / "test_ingestion.duckdb"
    cfg = PipelineConfig(
        duckdb_path=str(db_file),
        dlt_pipeline_name="test_dlt_pipeline",
        dlt_dataset_name="test_dataset",
    )

    records = [
        {
            "location": "Faridabad, Haryana, India",
            "latitude": 28.4089,
            "longitude": 77.3178,
            "timestamp": "2026-10-19T10:00",
            "temperature_c": 26.5,
            "temperature_f": 79.7,
            "relative_humidity_pct": 55.0,
            "wind_speed_kmh": 12.0,
            "wind_speed_ms": 3.33,
            "weather_code": 0,
            "weather_description": "Clear sky",
            "is_day": 1,
            "ingested_at": "2026-10-19T10:05:00Z",
        },
        {
            "location": "Faridabad, Haryana, India",
            "latitude": 28.4089,
            "longitude": 77.3178,
            "timestamp": "2026-10-19T11:00",
            "temperature_c": 28.0,
            "temperature_f": 82.4,
            "relative_humidity_pct": 50.0,
            "wind_speed_kmh": 14.0,
            "wind_speed_ms": 3.89,
            "weather_code": 1,
            "weather_description": "Mainly clear",
            "is_day": 1,
            "ingested_at": "2026-10-19T11:05:00Z",
        },
    ]

    runner = WeatherPipelineRunner(cfg)
    result = runner.run(custom_records=records, database_path=str(db_file))

    assert result["status"] == "success"
    assert result["record_count"] == 2

    # Verify directly in DuckDB
    conn = duckdb.connect(str(db_file), read_only=True)
    count = conn.execute(f"SELECT COUNT(*) FROM {cfg.dlt_dataset_name}.{cfg.dlt_table_name}").fetchone()[0]
    conn.close()

    assert count == 2


def test_dlt_idempotency_and_merge(tmp_path: Path):
    """Verify that re-inserting same keys merges/deduplicates without inflating row count."""
    db_file = tmp_path / "test_merge.duckdb"
    cfg = PipelineConfig(
        duckdb_path=str(db_file),
        dlt_pipeline_name="test_merge_pipeline",
        dlt_dataset_name="test_dataset",
    )

    initial_records = [
        {
            "location": "Faridabad, Haryana, India",
            "latitude": 28.4089,
            "longitude": 77.3178,
            "timestamp": "2026-10-19T12:00",
            "temperature_c": 29.0,
            "temperature_f": 84.2,
            "relative_humidity_pct": 48.0,
            "wind_speed_kmh": 10.0,
            "wind_speed_ms": 2.78,
            "weather_code": 0,
            "weather_description": "Clear sky",
            "is_day": 1,
            "ingested_at": "2026-10-19T12:05:00Z",
        }
    ]

    runner = WeatherPipelineRunner(cfg)
    runner.run(custom_records=initial_records, database_path=str(db_file))

    # Re-run with the same timestamp but updated temperature
    updated_records = [
        {
            "location": "Faridabad, Haryana, India",
            "latitude": 28.4089,
            "longitude": 77.3178,
            "timestamp": "2026-10-19T12:00",
            "temperature_c": 29.5,  # Updated value
            "temperature_f": 85.1,
            "relative_humidity_pct": 48.0,
            "wind_speed_kmh": 10.0,
            "wind_speed_ms": 2.78,
            "weather_code": 0,
            "weather_description": "Clear sky",
            "is_day": 1,
            "ingested_at": "2026-10-19T12:10:00Z",
        }
    ]
    runner.run(custom_records=updated_records, database_path=str(db_file))

    conn = duckdb.connect(str(db_file), read_only=True)
    count = conn.execute(f"SELECT COUNT(*) FROM {cfg.dlt_dataset_name}.{cfg.dlt_table_name}").fetchone()[0]
    updated_temp = conn.execute(
        f"SELECT temperature_c FROM {cfg.dlt_dataset_name}.{cfg.dlt_table_name} WHERE timestamp = '2026-10-19T12:00'"
    ).fetchone()[0]
    conn.close()

    # Still exactly 1 row, but temperature was updated to 29.5!
    assert count == 1
    assert updated_temp == 29.5
