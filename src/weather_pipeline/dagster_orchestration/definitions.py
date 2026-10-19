"""
Dagster repository Definitions object uniting assets, jobs, and schedules.
"""

from dagster import Definitions
from src.weather_pipeline.dagster_orchestration.assets import (
    fetch_weather_data,
    load_weather_to_duckdb,
    weather_analytics_report,
)
from src.weather_pipeline.dagster_orchestration.jobs import weather_ingestion_job
from src.weather_pipeline.dagster_orchestration.schedules import hourly_weather_schedule

defs = Definitions(
    assets=[
        fetch_weather_data,
        load_weather_to_duckdb,
        weather_analytics_report,
    ],
    jobs=[
        weather_ingestion_job,
    ],
    schedules=[
        hourly_weather_schedule,
    ],
)
