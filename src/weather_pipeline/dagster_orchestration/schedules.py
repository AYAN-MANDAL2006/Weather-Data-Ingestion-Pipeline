"""
Dagster schedule definitions.
"""

from dagster import ScheduleDefinition
from src.weather_pipeline.dagster_orchestration.jobs import weather_ingestion_job

# Hourly schedule running at minute 0 of every hour
hourly_weather_schedule = ScheduleDefinition(
    name="hourly_weather_schedule",
    job=weather_ingestion_job,
    cron_schedule="0 * * * *",
    execution_timezone="Asia/Kolkata",
    description="Automated hourly trigger for weather data ingestion and analytics update.",
)
