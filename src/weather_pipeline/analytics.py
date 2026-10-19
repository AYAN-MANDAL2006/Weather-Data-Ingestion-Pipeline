"""
DuckDB SQL Analytics Runner.

Executes analytical queries against the persistent DuckDB weather database
and formats the results for reports, dashboards, and CLI summaries.
"""

from pathlib import Path
from typing import Any, Optional
import duckdb
from tabulate import tabulate

from src.weather_pipeline.config import config, PipelineConfig


class WeatherAnalytics:
    """Provides analytical reporting on ingested weather data stored in DuckDB."""

    def __init__(self, database_path: Optional[str] = None, cfg: Optional[PipelineConfig] = None):
        self.cfg = cfg or config
        self.db_path = database_path or self.cfg.duckdb_path
        self.sql_dir = self.cfg.workspace_dir / "sql"

    def _get_connection(self, read_only: bool = True) -> duckdb.DuckDBPyConnection:
        """Open a DuckDB connection to the database file."""
        path = Path(self.db_path)
        if not path.exists():
            raise FileNotFoundError(
                f"DuckDB database not found at '{path}'. "
                "Please run the weather ingestion pipeline first."
            )
        return duckdb.connect(database=str(path), read_only=read_only)

    def execute_query(self, sql_query: str) -> tuple[list[str], list[tuple[Any, ...]]]:
        """
        Execute raw SQL query and return (columns, rows).
        """
        conn = self._get_connection(read_only=True)
        try:
            cursor = conn.execute(sql_query)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return columns, rows
        finally:
            conn.close()

    def execute_sql_file(self, sql_filename: str) -> tuple[list[str], list[tuple[Any, ...]]]:
        """
        Read and execute a query from the sql/ directory.
        """
        filepath = self.sql_dir / sql_filename
        if not filepath.exists():
            raise FileNotFoundError(f"SQL file not found: {filepath}")
        with open(filepath, "r", encoding="utf-8") as f:
            sql_text = f.read()
        return self.execute_query(sql_text)

    def format_results_table(self, columns: list[str], rows: list[tuple[Any, ...]], title: Optional[str] = None) -> str:
        """Format SQL results into an aesthetic ASCII table."""
        output = []
        if title:
            output.append("\n" + "=" * 80)
            output.append(f"  {title.upper()}")
            output.append("=" * 80)
        if not rows:
            output.append("No records found.")
        else:
            table_str = tabulate(rows, headers=columns, tablefmt="github")
            output.append(table_str)
        return "\n".join(output)

    def get_recent_readings(self, limit: int = 10) -> tuple[list[str], list[tuple[Any, ...]]]:
        query = f"""
            SELECT
                timestamp,
                temperature_c,
                relative_humidity_pct,
                wind_speed_kmh,
                weather_description,
                ingested_at
            FROM {self.cfg.dlt_dataset_name}.{self.cfg.dlt_table_name}
            ORDER BY timestamp DESC
            LIMIT {limit};
        """
        return self.execute_query(query)

    def get_temperature_summary(self) -> tuple[list[str], list[tuple[Any, ...]]]:
        return self.execute_sql_file("temperature_stats.sql")

    def get_humidity_wind_summary(self) -> tuple[list[str], list[tuple[Any, ...]]]:
        return self.execute_sql_file("humidity_wind_stats.sql")

    def get_trends_summary(self) -> tuple[list[str], list[tuple[Any, ...]]]:
        return self.execute_sql_file("historical_trends.sql")

    def run_all_analytics(self) -> str:
        """Execute all analytical reports and return combined string report."""
        sections = []

        # 1. Recent readings
        cols, rows = self.get_recent_readings(limit=10)
        sections.append(self.format_results_table(cols, rows, "Latest Weather Readings (Faridabad)"))

        # 2. Temperature aggregates
        cols, rows = self.get_temperature_summary()
        sections.append(self.format_results_table(cols, rows, "Daily Temperature Analytics (°C / °F)"))

        # 3. Humidity & Wind
        cols, rows = self.get_humidity_wind_summary()
        sections.append(self.format_results_table(cols, rows, "Humidity & Wind Conditions"))

        # 4. Historical Trends & Moving Averages
        cols, rows = self.get_trends_summary()
        sections.append(self.format_results_table(cols, rows, "Historical Trends & 6h Moving Averages"))

        return "\n\n".join(sections)
