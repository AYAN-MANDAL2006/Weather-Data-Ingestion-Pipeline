"""
Standalone CLI runner for Weather Data Ingestion Pipeline.

Allows manual trigger of ingestion, SQL analytics display, and connectivity verification.
Usage:
    python run_pipeline.py                # Ingest data and display all analytics
    python run_pipeline.py --ingest       # Run ingestion only
    python run_pipeline.py --analytics    # Run SQL analytics reports only
    python run_pipeline.py --verify       # Verify API connectivity and database
"""

import argparse
import logging
import sys
from pathlib import Path

# Ensure src is on Python path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from src.weather_pipeline.config import config
from src.weather_pipeline.client import OpenMeteoClient
from src.weather_pipeline.pipeline import WeatherPipelineRunner
from src.weather_pipeline.analytics import WeatherAnalytics

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("run_pipeline")


def verify_connection() -> bool:
    """Test connection to Open-Meteo REST API."""
    logger.info(f"Testing Open-Meteo API connection for {config.location_name}...")
    try:
        client = OpenMeteoClient()
        raw = client.fetch_weather_raw(past_days=0, forecast_days=1)
        current = raw.get("current", {})
        temp = current.get("temperature_2m")
        logger.info(
            f"API connection successful! Current temperature in {config.location_name}: {temp}°C"
        )
        return True
    except Exception as e:
        logger.error(f"API connectivity test failed: {e}")
        return False


def run_ingestion() -> dict:
    """Run full dlt ingestion into DuckDB."""
    logger.info("Executing weather ingestion pipeline...")
    runner = WeatherPipelineRunner()
    result = runner.run()
    logger.info(
        f"Ingestion finished: status={result['status']}, "
        f"records={result.get('record_count', 0)}, "
        f"database={result.get('database_path')}"
    )
    return result


def run_analytics() -> None:
    """Execute and display SQL analytical summaries."""
    logger.info("Running SQL analytics against DuckDB...")
    try:
        analytics = WeatherAnalytics()
        report = analytics.run_all_analytics()
        print(report)
    except FileNotFoundError as e:
        logger.error(f"Cannot run analytics: {e}")
    except Exception as e:
        logger.error(f"Failed to execute SQL analytics: {e}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Weather Data Ingestion Pipeline - Faridabad, Haryana, India"
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Verify Open-Meteo API connection without modifying database",
    )
    parser.add_argument(
        "--ingest",
        action="store_true",
        help="Run data ingestion pipeline (Open-Meteo -> dlt -> DuckDB)",
    )
    parser.add_argument(
        "--analytics",
        action="store_true",
        help="Run SQL analytics and print summaries",
    )

    args = parser.parse_args()

    # Default action: run both ingestion and analytics
    if not (args.verify or args.ingest or args.analytics):
        logger.info("No specific flag provided; running end-to-end ingestion and analytics.")
        verify_connection()
        run_ingestion()
        run_analytics()
        return

    if args.verify:
        success = verify_connection()
        sys.exit(0 if success else 1)

    if args.ingest:
        run_ingestion()

    if args.analytics:
        run_analytics()


if __name__ == "__main__":
    main()
