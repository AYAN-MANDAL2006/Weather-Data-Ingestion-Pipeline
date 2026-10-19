"""
Pipeline configuration module.

Reads environment variables with sensible defaults for location,
Open-Meteo API parameters, retry policies, and DuckDB storage paths.
"""

from dataclasses import dataclass
import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env file if available
BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / ".env")


@dataclass
class PipelineConfig:
    """Holds all configuration parameters for the weather ingestion pipeline."""

    # Location settings (Faridabad, Haryana, India)
    location_name: str = os.getenv("WEATHER_LOCATION_NAME", "Faridabad, Haryana, India")
    latitude: float = float(os.getenv("WEATHER_LATITUDE", "28.4089"))
    longitude: float = float(os.getenv("WEATHER_LONGITUDE", "77.3178"))
    timezone: str = os.getenv("WEATHER_TIMEZONE", "Asia/Kolkata")

    # Open-Meteo REST API settings
    api_base_url: str = os.getenv("OPEN_METEO_BASE_URL", "https://api.open-meteo.com/v1/forecast")
    api_timeout_seconds: int = int(os.getenv("API_TIMEOUT_SECONDS", "15"))
    api_max_retries: int = int(os.getenv("API_MAX_RETRIES", "3"))
    api_retry_backoff_factor: float = float(os.getenv("API_RETRY_BACKOFF_FACTOR", "1.5"))

    # Ingestion lookback & lookahead
    # Fetch past 2 days + today's forecast for robust historical tracking
    past_days: int = int(os.getenv("WEATHER_PAST_DAYS", "2"))
    forecast_days: int = int(os.getenv("WEATHER_FORECAST_DAYS", "1"))

    # Database & Storage
    duckdb_path: str = os.getenv("DUCKDB_DATABASE_PATH", str(BASE_DIR / "data" / "weather_data.duckdb"))
    dlt_pipeline_name: str = os.getenv("DLT_PIPELINE_NAME", "weather_ingestion_pipeline")
    dlt_dataset_name: str = os.getenv("DLT_DATASET_NAME", "weather_analytics")
    dlt_table_name: str = "weather_readings"

    # Base workspace directory
    workspace_dir: Path = BASE_DIR


config = PipelineConfig()
