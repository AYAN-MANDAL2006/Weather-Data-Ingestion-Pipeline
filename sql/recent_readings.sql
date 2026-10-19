-- ==============================================================================
-- Query: Recent Weather Observations
-- Description: Retrieves the latest 24 recorded weather observations for Faridabad,
--              ordered chronologically descending (newest first).
-- ==============================================================================

SELECT
    location,
    latitude,
    longitude,
    timestamp AS observation_time,
    temperature_c,
    temperature_f,
    relative_humidity_pct,
    wind_speed_kmh,
    wind_speed_ms,
    weather_description,
    ingested_at
FROM weather_analytics.weather_readings
ORDER BY timestamp DESC
LIMIT 24;
