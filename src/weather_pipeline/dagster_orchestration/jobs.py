"""
Dagster job definitions for Weather Data Ingestion Pipeline.
"""

from dagster import AssetSelection, define_asset_job

# Job that executes the complete ingestion and analytics pipeline
weather_ingestion_job = define_asset_job(
    name="weather_ingestion_job",
    selection=AssetSelection.groups("weather_pipeline"),
    description="End-to-end weather ingestion job: extracts Open-Meteo API data, loads into DuckDB via dlt, and computes SQL analytics.",
)
