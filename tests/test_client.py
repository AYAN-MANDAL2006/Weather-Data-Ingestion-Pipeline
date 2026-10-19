"""
Unit and integration tests for Open-Meteo REST API client.
"""

from unittest.mock import MagicMock, patch
import pytest
import requests

from src.weather_pipeline.client import OpenMeteoClient, WeatherAPIError
from src.weather_pipeline.config import PipelineConfig
from src.weather_pipeline.models import get_weather_description, WeatherRecord


def test_wmo_code_mapping():
    """Verify WMO codes map to descriptive names."""
    assert get_weather_description(0) == "Clear sky"
    assert get_weather_description(3) == "Overcast"
    assert get_weather_description(61) == "Slight rain"
    assert get_weather_description(95) == "Thunderstorm"
    assert get_weather_description(None) == "Unknown"
    assert get_weather_description(999) == "Weather code 999"


def test_weather_record_validation():
    """Verify Pydantic validation on realistic atmospheric metrics."""
    record = WeatherRecord(
        location="Faridabad, Haryana, India",
        latitude=28.4089,
        longitude=77.3178,
        timestamp="2026-10-19T14:00",
        temperature_c=25.0,
        temperature_f=77.0,
        relative_humidity_pct=60.0,
        wind_speed_kmh=10.0,
        wind_speed_ms=2.78,
        weather_code=0,
        weather_description="Clear sky",
        is_day=1,
    )
    assert record.location == "Faridabad, Haryana, India"
    assert record.temperature_c == 25.0

    # Test invalid temperature
    with pytest.raises(ValueError):
        WeatherRecord(
            location="Faridabad",
            latitude=28.4,
            longitude=77.3,
            timestamp="2026-10-19T14:00",
            temperature_c=150.0,  # Unrealistic Earth temperature
        )

    # Test invalid humidity
    with pytest.raises(ValueError):
        WeatherRecord(
            location="Faridabad",
            latitude=28.4,
            longitude=77.3,
            timestamp="2026-10-19T14:00",
            relative_humidity_pct=105.0,  # Over 100%
        )


def test_parse_and_transform_valid(mock_open_meteo_payload):
    """Test transformation of Open-Meteo payload into structured records."""
    client = OpenMeteoClient()
    records = client.parse_and_transform(mock_open_meteo_payload)

    assert len(records) == 3
    # First record checks
    rec0 = records[0]
    assert rec0["timestamp"] == "2026-10-19T12:00"
    assert rec0["temperature_c"] == 30.1
    assert rec0["temperature_f"] == round((30.1 * 9.0 / 5.0) + 32.0, 2)
    assert rec0["relative_humidity_pct"] == 45.0
    assert rec0["weather_description"] == "Clear sky"


def test_parse_and_transform_missing_values():
    """Verify graceful handling when hourly slots contain None or incomplete data."""
    payload = {
        "latitude": 28.4089,
        "longitude": 77.3178,
        "hourly": {
            "time": ["2026-10-19T10:00", "2026-10-19T11:00"],
            "temperature_2m": [None, 24.5],
            "relative_humidity_2m": [None, None],
            "wind_speed_10m": [None, 5.0],
            "weather_code": [None, 1],
            "is_day": [None, 1],
        },
    }
    client = OpenMeteoClient()
    records = client.parse_and_transform(payload)

    # 10:00 has all metrics None, so it should be skipped
    # 11:00 has temp=24.5 and wind=5.0, so it should be preserved
    assert len(records) == 1
    assert records[0]["timestamp"] == "2026-10-19T11:00"
    assert records[0]["temperature_c"] == 24.5
    assert records[0]["relative_humidity_pct"] is None


def test_api_network_timeout():
    """Verify WeatherAPIError is raised upon network timeout."""
    client = OpenMeteoClient()
    with patch.object(client.session, "get", side_effect=requests.exceptions.Timeout("Connection timed out")):
        with pytest.raises(WeatherAPIError) as exc_info:
            client.fetch_weather_raw()
        assert "timeout" in str(exc_info.value).lower()


def test_api_http_error_response():
    """Verify WeatherAPIError is raised when Open-Meteo returns non-200 status."""
    client = OpenMeteoClient()
    mock_resp = MagicMock()
    mock_resp.status_code = 500
    mock_resp.text = "Internal Server Error"
    with patch.object(client.session, "get", return_value=mock_resp):
        with pytest.raises(WeatherAPIError) as exc_info:
            client.fetch_weather_raw()
        assert "500" in str(exc_info.value)


def test_api_reported_error_in_json():
    """Verify error reported inside 200 JSON payload is handled."""
    client = OpenMeteoClient()
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"error": True, "reason": "Latitude must be in range of -90 to 90"}
    with patch.object(client.session, "get", return_value=mock_resp):
        with pytest.raises(WeatherAPIError) as exc_info:
            client.fetch_weather_raw()
        assert "Latitude must be in range" in str(exc_info.value)


@pytest.mark.integration
def test_real_open_meteo_live_fetch():
    """Live API integration test against Open-Meteo REST API."""
    client = OpenMeteoClient()
    records = client.get_weather_records(past_days=1, forecast_days=1)
    assert len(records) > 0
    latest = records[-1]
    assert "timestamp" in latest
    assert "temperature_c" in latest
    assert latest["latitude"] == pytest.approx(28.4089, abs=0.1)
