"""
Data models and schemas for weather observations.
"""

from datetime import datetime, timezone
from typing import Optional
from pydantic import BaseModel, Field, field_validator


# WMO Weather interpretation codes (WW) mapping
WMO_CODE_DESCRIPTIONS: dict[int, str] = {
    0: "Clear sky",
    1: "Mainly clear",
    2: "Partly cloudy",
    3: "Overcast",
    45: "Fog",
    48: "Depositing rime fog",
    51: "Light drizzle",
    53: "Moderate drizzle",
    55: "Dense drizzle",
    56: "Light freezing drizzle",
    57: "Dense freezing drizzle",
    61: "Slight rain",
    63: "Moderate rain",
    65: "Heavy rain",
    66: "Light freezing rain",
    67: "Heavy freezing rain",
    71: "Slight snow fall",
    73: "Moderate snow fall",
    75: "Heavy snow fall",
    77: "Snow grains",
    80: "Slight rain showers",
    81: "Moderate rain showers",
    82: "Violent rain showers",
    85: "Slight snow showers",
    86: "Heavy snow showers",
    95: "Thunderstorm",
    96: "Thunderstorm with slight hail",
    99: "Thunderstorm with heavy hail",
}


def get_weather_description(code: Optional[int]) -> str:
    """Return human-readable weather description for WMO weather code."""
    if code is None:
        return "Unknown"
    return WMO_CODE_DESCRIPTIONS.get(code, f"Weather code {code}")


class WeatherRecord(BaseModel):
    """Structured weather observation record for storage in DuckDB."""

    location: str = Field(..., description="Name of the geographical location")
    latitude: float = Field(..., description="Latitude coordinate")
    longitude: float = Field(..., description="Longitude coordinate")
    timestamp: str = Field(..., description="Observation timestamp in ISO format (YYYY-MM-DDTHH:MM)")
    temperature_c: Optional[float] = Field(None, description="Temperature in Celsius")
    temperature_f: Optional[float] = Field(None, description="Temperature in Fahrenheit")
    relative_humidity_pct: Optional[float] = Field(None, description="Relative humidity percentage (0-100)")
    wind_speed_kmh: Optional[float] = Field(None, description="Wind speed in kilometers per hour")
    wind_speed_ms: Optional[float] = Field(None, description="Wind speed in meters per second")
    weather_code: Optional[int] = Field(None, description="WMO weather interpretation code")
    weather_description: Optional[str] = Field(None, description="Human readable weather description")
    is_day: Optional[int] = Field(None, description="1 if daytime, 0 if nighttime")
    ingested_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="UTC timestamp when record was ingested",
    )

    @field_validator("temperature_c", mode="after")
    @classmethod
    def validate_temp(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and not (-90.0 <= v <= 65.0):
            raise ValueError(f"Temperature {v}°C is outside realistic Earth atmospheric range (-90 to 65)")
        return v

    @field_validator("relative_humidity_pct", mode="after")
    @classmethod
    def validate_humidity(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and not (0.0 <= v <= 100.0):
            raise ValueError(f"Humidity {v}% must be between 0 and 100")
        return v

    @field_validator("wind_speed_kmh", mode="after")
    @classmethod
    def validate_wind(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and not (0.0 <= v <= 400.0):
            raise ValueError(f"Wind speed {v} km/h must be non-negative and realistic")
        return v
