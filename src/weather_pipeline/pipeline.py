"""
dlt ingestion pipeline for weather observations into DuckDB.

Extracts weather data via OpenMeteoClient, declares dlt resource with
primary key deduplication and merge disposition, and loads into DuckDB.
"""

from dataclasses import asdict
from datetime import datetime, timezone
import logging
from pathlib import Path
from typing import Any, Iterable, Optional

import dlt
from dlt.common.typing import TDataItem
from dlt.sources import DltResource

from src.weather_pipeline.client import OpenMeteoClient
from src.weather_pipeline.config import config, PipelineConfig

logger = logging.getLogger(__name__)


def create_weather_resource(
    records: Iterable[dict[str, Any]],
    table_name: str = "weather_readings",
) -> DltResource:
    """
    Create a dlt resource with 'merge' write disposition and composite primary key
    on ('location', 'timestamp') to guarantee idempotent deduplication and
    historical observation preservation.
    """
    @dlt.resource(
        name=table_name,
        write_disposition="merge",
        primary_key=["location", "timestamp"],
        columns={
            "location": {"name": "location", "data_type": "text", "nullable": False},
            "latitude": {"name": "latitude", "data_type": "double", "nullable": False},
            "longitude": {"name": "longitude", "data_type": "double", "nullable": False},
            "timestamp": {"name": "timestamp", "data_type": "text", "nullable": False},
            "temperature_c": {"name": "temperature_c", "data_type": "double", "nullable": True},
            "temperature_f": {"name": "temperature_f", "data_type": "double", "nullable": True},
            "relative_humidity_pct": {"name": "relative_humidity_pct", "data_type": "double", "nullable": True},
            "wind_speed_kmh": {"name": "wind_speed_kmh", "data_type": "double", "nullable": True},
            "wind_speed_ms": {"name": "wind_speed_ms", "data_type": "double", "nullable": True},
            "weather_code": {"name": "weather_code", "data_type": "bigint", "nullable": True},
            "weather_description": {"name": "weather_description", "data_type": "text", "nullable": True},
            "is_day": {"name": "is_day", "data_type": "bigint", "nullable": True},
            "ingested_at": {"name": "ingested_at", "data_type": "text", "nullable": False},
        },
    )
    def weather_data_generator() -> Iterable[TDataItem]:
        for record in records:
            yield record

    return weather_data_generator()


class WeatherPipelineRunner:
    """Orchestrates extraction, transformation, and loading into DuckDB via dlt."""

    def __init__(self, cfg: Optional[PipelineConfig] = None):
        self.cfg = cfg or config
        self.client = OpenMeteoClient(self.cfg)

    def run(
        self,
        custom_records: Optional[list[dict[str, Any]]] = None,
        database_path: Optional[str] = None,
    ) -> dict[str, Any]:
        """
        Execute the full ingestion pipeline.

        Args:
            custom_records: Optional list of records (used in tests or manual overrides).
            database_path: Optional path to DuckDB database file.

        Returns:
            Dictionary with execution metadata, load info, and record counts.
        """
        target_db = database_path or self.cfg.duckdb_path
        target_path = Path(target_db).resolve()
        target_path.parent.mkdir(parents=True, exist_ok=True)

        logger.info(f"Target DuckDB database path: {target_path}")

        # 1. Fetch & transform records
        if custom_records is not None:
            records = custom_records
            logger.info(f"Using {len(records)} custom/provided records for ingestion")
        else:
            logger.info("Fetching real weather records from Open-Meteo...")
            records = self.client.get_weather_records()

        if not records:
            logger.warning("No records were retrieved or parsed; aborting dlt load.")
            return {
                "status": "warning",
                "message": "No records fetched",
                "record_count": 0,
                "database_path": str(target_path),
            }

        # 2. Configure dlt pipeline
        # In dlt, destination="duckdb" with credentials pointing to the file path
        pipeline = dlt.pipeline(
            pipeline_name=self.cfg.dlt_pipeline_name,
            destination=dlt.destinations.duckdb(credentials=str(target_path)),
            dataset_name=self.cfg.dlt_dataset_name,
            progress="log",
        )

        # 3. Build resource
        resource = create_weather_resource(
            records=records,
            table_name=self.cfg.dlt_table_name,
        )

        # 4. Run pipeline
        logger.info(f"Starting dlt load of {len(records)} records into DuckDB...")
        load_info = pipeline.run(resource)

        logger.info(f"dlt pipeline completed successfully:\n{load_info}")

        return {
            "status": "success",
            "record_count": len(records),
            "pipeline_name": self.cfg.dlt_pipeline_name,
            "dataset_name": self.cfg.dlt_dataset_name,
            "table_name": self.cfg.dlt_table_name,
            "database_path": str(target_path),
            "load_info": str(load_info),
        }
