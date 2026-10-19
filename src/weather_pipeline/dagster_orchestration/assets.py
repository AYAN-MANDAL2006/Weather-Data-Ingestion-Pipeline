"""
Dagster software-defined assets for weather data ingestion, storage, and analytics.
"""

import logging
from typing import Any
from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    asset,
)

from src.weather_pipeline.client import OpenMeteoClient
from src.weather_pipeline.config import config
from src.weather_pipeline.pipeline import WeatherPipelineRunner
from src.weather_pipeline.analytics import WeatherAnalytics

logger = logging.getLogger(__name__)


@asset(
    group_name="weather_pipeline",
    description="Fetches current and hourly weather data for Faridabad from the Open-Meteo REST API.",
    compute_kind="python",
)
def fetch_weather_data(context: AssetExecutionContext) -> list[dict[str, Any]]:
    """Fetch structured records from Open-Meteo API with retry and validation."""
    context.log.info(f"Connecting to Open-Meteo API for location: {config.location_name}")
    client = OpenMeteoClient(config)
    records = client.get_weather_records()

    if not records:
        context.log.warning("No records were fetched from Open-Meteo API.")
        return []

    latest_time = records[-1]["timestamp"]
    earliest_time = records[0]["timestamp"]

    context.log.info(
        f"Fetched {len(records)} records spanning from {earliest_time} to {latest_time}"
    )

    return records


@asset(
    group_name="weather_pipeline",
    description="Loads structured weather records into DuckDB using dlt with merge deduplication.",
    compute_kind="dlt",
)
def load_weather_to_duckdb(
    context: AssetExecutionContext,
    fetch_weather_data: list[dict[str, Any]],
) -> MaterializeResult:
    """Run dlt pipeline to ingest fetched records into persistent DuckDB storage."""
    context.log.info(
        f"Ingesting {len(fetch_weather_data)} weather records into DuckDB: {config.duckdb_path}"
    )

    runner = WeatherPipelineRunner(config)
    result = runner.run(custom_records=fetch_weather_data)

    context.log.info(f"dlt pipeline execution status: {result['status']}")

    return MaterializeResult(
        metadata={
            "record_count": MetadataValue.int(result["record_count"]),
            "database_path": MetadataValue.path(result["database_path"]),
            "dataset_name": MetadataValue.text(result["dataset_name"]),
            "table_name": MetadataValue.text(result["table_name"]),
            "status": MetadataValue.text(result["status"]),
        }
    )


@asset(
    deps=[load_weather_to_duckdb],
    group_name="weather_pipeline",
    description="Executes DuckDB SQL analytical queries to generate temperature, humidity, and trend summaries.",
    compute_kind="duckdb_sql",
)
def weather_analytics_report(
    context: AssetExecutionContext,
) -> MaterializeResult:
    """Run SQL analytics against DuckDB database and log key metrics."""
    context.log.info("Executing analytical SQL queries against DuckDB...")

    analytics = WeatherAnalytics()
    full_report = analytics.run_all_analytics()

    # Retrieve most recent observation for metadata summary
    cols, recent_rows = analytics.get_recent_readings(limit=1)

    metadata: dict[str, Any] = {
        "report_preview": MetadataValue.md(f"```\n{full_report[:2000]}\n```"),
    }

    if recent_rows:
        latest = recent_rows[0]
        # Columns: timestamp, temperature_c, relative_humidity_pct, wind_speed_kmh, weather_description, ingested_at
        metadata["latest_observation_time"] = MetadataValue.text(str(latest[0]))
        if latest[1] is not None:
            metadata["latest_temperature_c"] = MetadataValue.float(float(latest[1]))
        if latest[2] is not None:
            metadata["latest_humidity_pct"] = MetadataValue.float(float(latest[2]))
        if latest[3] is not None:
            metadata["latest_wind_speed_kmh"] = MetadataValue.float(float(latest[3]))
        if latest[4] is not None:
            metadata["latest_weather_condition"] = MetadataValue.text(str(latest[4]))

    context.log.info("SQL analytics completed successfully.")
    return MaterializeResult(metadata=metadata)
