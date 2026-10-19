"""
Tests for DuckDB SQL analytics queries and reporting.
"""

from pathlib import Path
import duckdb
import pytest

from src.weather_pipeline.analytics import WeatherAnalytics
from src.weather_pipeline.config import PipelineConfig


@pytest.fixture
def analytics_test_db(tmp_path: Path):
    """Create a temporary DuckDB database preloaded with sample weather records."""
    db_file = tmp_path / "analytics_test.duckdb"
    conn = duckdb.connect(str(db_file))

    conn.execute("CREATE SCHEMA IF NOT EXISTS weather_analytics;")
    conn.execute("""
        CREATE TABLE weather_analytics.weather_readings (
            location VARCHAR,
            latitude DOUBLE,
            longitude DOUBLE,
            timestamp VARCHAR,
            temperature_c DOUBLE,
            temperature_f DOUBLE,
            relative_humidity_pct DOUBLE,
            wind_speed_kmh DOUBLE,
            wind_speed_ms DOUBLE,
            weather_code BIGINT,
            weather_description VARCHAR,
            is_day BIGINT,
            ingested_at VARCHAR
        );
    """)

    sample_rows = [
        ("Faridabad, Haryana, India", 28.4089, 77.3178, "2026-10-18T10:00", 25.0, 77.0, 60.0, 10.0, 2.78, 0, "Clear sky", 1, "2026-10-18T10:00:00Z"),
        ("Faridabad, Haryana, India", 28.4089, 77.3178, "2026-10-18T11:00", 27.0, 80.6, 55.0, 12.0, 3.33, 0, "Clear sky", 1, "2026-10-18T11:00:00Z"),
        ("Faridabad, Haryana, India", 28.4089, 77.3178, "2026-10-18T12:00", 29.0, 84.2, 50.0, 15.0, 4.17, 1, "Mainly clear", 1, "2026-10-18T12:00:00Z"),
        ("Faridabad, Haryana, India", 28.4089, 77.3178, "2026-10-19T10:00", 26.0, 78.8, 58.0, 11.0, 3.06, 0, "Clear sky", 1, "2026-10-19T10:00:00Z"),
        ("Faridabad, Haryana, India", 28.4089, 77.3178, "2026-10-19T11:00", 28.0, 82.4, 52.0, 13.0, 3.61, 2, "Partly cloudy", 1, "2026-10-19T11:00:00Z"),
    ]

    for row in sample_rows:
        conn.execute("""
            INSERT INTO weather_analytics.weather_readings VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, row)

    conn.close()
    return str(db_file)


def test_recent_readings_query(analytics_test_db: str):
    """Verify recent readings retrieval."""
    analytics = WeatherAnalytics(database_path=analytics_test_db)
    cols, rows = analytics.get_recent_readings(limit=3)
    assert len(rows) == 3
    assert "timestamp" in cols
    assert rows[0][0] == "2026-10-19T11:00"  # Most recent first


def test_temperature_summary_query(analytics_test_db: str):
    """Verify daily temperature aggregates."""
    analytics = WeatherAnalytics(database_path=analytics_test_db)
    cols, rows = analytics.get_temperature_summary()
    assert len(rows) == 2  # Two distinct dates: 2026-10-18 and 2026-10-19
    # Validate column presence
    assert "avg_temp_c" in cols
    assert "min_temp_c" in cols
    assert "max_temp_c" in cols


def test_humidity_wind_summary_query(analytics_test_db: str):
    """Verify humidity and wind categorization."""
    analytics = WeatherAnalytics(database_path=analytics_test_db)
    cols, rows = analytics.get_humidity_wind_summary()
    assert len(rows) == 2
    assert "avg_humidity_pct" in cols
    assert "humidity_condition" in cols
    assert "max_wind_condition" in cols


def test_historical_trends_query(analytics_test_db: str):
    """Verify historical trends, rolling averages, and delta calculations."""
    analytics = WeatherAnalytics(database_path=analytics_test_db)
    cols, rows = analytics.get_trends_summary()
    assert len(rows) == 5
    assert "rolling_6h_avg_temp_c" in cols
    assert "temp_trend" in cols
    assert "hourly_temp_change_c" in cols


def test_full_analytics_string_report(analytics_test_db: str):
    """Verify combined report generation."""
    analytics = WeatherAnalytics(database_path=analytics_test_db)
    report = analytics.run_all_analytics()
    assert "LATEST WEATHER READINGS" in report
    assert "DAILY TEMPERATURE ANALYTICS" in report
    assert "HUMIDITY & WIND CONDITIONS" in report
    assert "HISTORICAL TRENDS" in report
