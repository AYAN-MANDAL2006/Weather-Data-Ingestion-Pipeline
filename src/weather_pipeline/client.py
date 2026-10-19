"""
Open-Meteo REST API client.

Handles resilient HTTP requests with exponential retries, response parsing,
error validation, missing-value handling, and transformation into structured records.
"""

from datetime import datetime, timezone
import logging
from typing import Any, Optional
import requests
from urllib3.util import Retry
from requests.adapters import HTTPAdapter

from src.weather_pipeline.config import config, PipelineConfig
from src.weather_pipeline.models import WeatherRecord, get_weather_description

logger = logging.getLogger(__name__)


class WeatherAPIError(Exception):
    """Custom exception raised when weather data retrieval fails."""
    pass


class OpenMeteoClient:
    """Client for interacting with Open-Meteo weather API."""

    def __init__(self, cfg: Optional[PipelineConfig] = None):
        self.cfg = cfg or config
        self.session = self._create_resilient_session()

    def _create_resilient_session(self) -> requests.Session:
        """Create a requests Session configured with retry logic and backoff."""
        session = requests.Session()
        retries = Retry(
            total=self.cfg.api_max_retries,
            backoff_factor=self.cfg.api_retry_backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            raise_on_status=False,
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def fetch_weather_raw(
        self,
        latitude: Optional[float] = None,
        longitude: Optional[float] = None,
        past_days: Optional[int] = None,
        forecast_days: Optional[int] = None,
    ) -> dict[str, Any]:
        """
        Fetch raw JSON payload from Open-Meteo API.

        Raises:
            WeatherAPIError: If the request fails, times out, or returns an error response.
        """
        lat = latitude if latitude is not None else self.cfg.latitude
        lon = longitude if longitude is not None else self.cfg.longitude
        past = past_days if past_days is not None else self.cfg.past_days
        forecast = forecast_days if forecast_days is not None else self.cfg.forecast_days

        params = {
            "latitude": lat,
            "longitude": lon,
            "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,weather_code,is_day",
            "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m,weather_code,is_day",
            "timezone": self.cfg.timezone,
            "past_days": past,
            "forecast_days": forecast,
        }

        logger.info(
            f"Requesting Open-Meteo weather data: lat={lat}, lon={lon}, "
            f"past_days={past}, forecast_days={forecast}, tz={self.cfg.timezone}"
        )

        try:
            response = self.session.get(
                self.cfg.api_base_url,
                params=params,
                timeout=self.cfg.api_timeout_seconds,
            )
        except requests.exceptions.Timeout as e:
            msg = f"Network timeout after {self.cfg.api_timeout_seconds}s connecting to Open-Meteo API"
            logger.error(msg)
            raise WeatherAPIError(msg) from e
        except requests.exceptions.ConnectionError as e:
            msg = f"Network connection error contacting Open-Meteo API: {e}"
            logger.error(msg)
            raise WeatherAPIError(msg) from e
        except requests.exceptions.RequestException as e:
            msg = f"Unexpected request failure contacting Open-Meteo API: {e}"
            logger.error(msg)
            raise WeatherAPIError(msg) from e

        # Validate HTTP status code
        if response.status_code != 200:
            error_reason = f"HTTP {response.status_code}: {response.text[:200]}"
            logger.error(f"Open-Meteo returned error: {error_reason}")
            raise WeatherAPIError(f"Open-Meteo API error - {error_reason}")

        try:
            payload = response.json()
        except ValueError as e:
            msg = f"Invalid JSON payload returned by Open-Meteo: {response.text[:200]}"
            logger.error(msg)
            raise WeatherAPIError(msg) from e

        if payload.get("error"):
            reason = payload.get("reason", "Unknown API error")
            logger.error(f"Open-Meteo payload error: {reason}")
            raise WeatherAPIError(f"Open-Meteo reported error: {reason}")

        return payload

    def parse_and_transform(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Parse raw Open-Meteo response into a list of validated record dictionaries.
        Safely handles missing values, rounds numbers, and computes derived fields.
        """
        latitude = payload.get("latitude", self.cfg.latitude)
        longitude = payload.get("longitude", self.cfg.longitude)
        location = self.cfg.location_name
        ingest_time = datetime.now(timezone.utc).isoformat()

        records_by_timestamp: dict[str, dict[str, Any]] = {}

        # 1. Process hourly series
        hourly = payload.get("hourly", {})
        times = hourly.get("time", [])
        temps = hourly.get("temperature_2m", [])
        humidities = hourly.get("relative_humidity_2m", [])
        wind_speeds = hourly.get("wind_speed_10m", [])
        weather_codes = hourly.get("weather_code", [])
        is_days = hourly.get("is_day", [])

        total_hourly = len(times)
        for idx in range(total_hourly):
            ts = times[idx]
            if not ts:
                continue

            raw_temp = temps[idx] if idx < len(temps) else None
            raw_hum = humidities[idx] if idx < len(humidities) else None
            raw_wind = wind_speeds[idx] if idx < len(wind_speeds) else None
            raw_code = weather_codes[idx] if idx < len(weather_codes) else None
            raw_is_day = is_days[idx] if idx < len(is_days) else None

            # Skip entries where all core weather metrics are missing
            if raw_temp is None and raw_hum is None and raw_wind is None:
                continue

            temp_c = round(float(raw_temp), 2) if raw_temp is not None else None
            temp_f = round((temp_c * 9.0 / 5.0) + 32.0, 2) if temp_c is not None else None
            humidity_pct = round(float(raw_hum), 1) if raw_hum is not None else None
            wind_kmh = round(float(raw_wind), 2) if raw_wind is not None else None
            wind_ms = round(wind_kmh / 3.6, 2) if wind_kmh is not None else None
            wmo_code = int(raw_code) if raw_code is not None else None
            desc = get_weather_description(wmo_code)
            is_day_val = int(raw_is_day) if raw_is_day is not None else None

            # Validate record using Pydantic model
            try:
                record = WeatherRecord(
                    location=location,
                    latitude=float(latitude),
                    longitude=float(longitude),
                    timestamp=str(ts),
                    temperature_c=temp_c,
                    temperature_f=temp_f,
                    relative_humidity_pct=humidity_pct,
                    wind_speed_kmh=wind_kmh,
                    wind_speed_ms=wind_ms,
                    weather_code=wmo_code,
                    weather_description=desc,
                    is_day=is_day_val,
                    ingested_at=ingest_time,
                )
                records_by_timestamp[ts] = record.model_dump()
            except Exception as val_err:
                logger.warning(f"Skipping invalid hourly record at {ts}: {val_err}")

        # 2. Process current observation (if present)
        current = payload.get("current", {})
        curr_ts = current.get("time")
        if curr_ts:
            raw_temp = current.get("temperature_2m")
            raw_hum = current.get("relative_humidity_2m")
            raw_wind = current.get("wind_speed_10m")
            raw_code = current.get("weather_code")
            raw_is_day = current.get("is_day")

            temp_c = round(float(raw_temp), 2) if raw_temp is not None else None
            temp_f = round((temp_c * 9.0 / 5.0) + 32.0, 2) if temp_c is not None else None
            humidity_pct = round(float(raw_hum), 1) if raw_hum is not None else None
            wind_kmh = round(float(raw_wind), 2) if raw_wind is not None else None
            wind_ms = round(wind_kmh / 3.6, 2) if wind_kmh is not None else None
            wmo_code = int(raw_code) if raw_code is not None else None
            desc = get_weather_description(wmo_code)
            is_day_val = int(raw_is_day) if raw_is_day is not None else None

            try:
                record = WeatherRecord(
                    location=location,
                    latitude=float(latitude),
                    longitude=float(longitude),
                    timestamp=str(curr_ts),
                    temperature_c=temp_c,
                    temperature_f=temp_f,
                    relative_humidity_pct=humidity_pct,
                    wind_speed_kmh=wind_kmh,
                    wind_speed_ms=wind_ms,
                    weather_code=wmo_code,
                    weather_description=desc,
                    is_day=is_day_val,
                    ingested_at=ingest_time,
                )
                # Current observation overwrites or adds the slot
                records_by_timestamp[curr_ts] = record.model_dump()
            except Exception as val_err:
                logger.warning(f"Skipping invalid current record at {curr_ts}: {val_err}")

        # Sort chronologically
        sorted_records = [records_by_timestamp[k] for k in sorted(records_by_timestamp.keys())]
        logger.info(f"Successfully transformed {len(sorted_records)} structured weather records")
        return sorted_records

    def get_weather_records(
        self,
        latitude: Optional[float] = None,
        longitude: Optional[float] = None,
        past_days: Optional[int] = None,
        forecast_days: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        """Fetch and transform weather records in a single step."""
        raw = self.fetch_weather_raw(
            latitude=latitude,
            longitude=longitude,
            past_days=past_days,
            forecast_days=forecast_days,
        )
        return self.parse_and_transform(raw)
