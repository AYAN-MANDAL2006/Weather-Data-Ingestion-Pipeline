-- ==============================================================================
-- Query: Historical Weather Trends & Rolling Averages
-- Description: Uses SQL window functions to analyze trends, moving averages,
--              and hourly delta changes across the observation timeline.
-- ==============================================================================

WITH ordered_readings AS (
    SELECT
        timestamp,
        location,
        temperature_c,
        relative_humidity_pct,
        wind_speed_kmh,
        weather_description,
        -- Rolling 6-hour moving average for temperature
        ROUND(AVG(temperature_c) OVER (
            ORDER BY timestamp
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ), 2) AS rolling_6h_avg_temp_c,
        -- Rolling 6-hour moving average for humidity
        ROUND(AVG(relative_humidity_pct) OVER (
            ORDER BY timestamp
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ), 2) AS rolling_6h_avg_humidity_pct,
        -- Previous reading temperature to detect warming or cooling
        LAG(temperature_c, 1) OVER (ORDER BY timestamp) AS prev_hour_temp_c
    FROM weather_analytics.weather_readings
    WHERE temperature_c IS NOT NULL
)
SELECT
    timestamp,
    location,
    temperature_c,
    rolling_6h_avg_temp_c,
    ROUND(temperature_c - prev_hour_temp_c, 2) AS hourly_temp_change_c,
    CASE
        WHEN prev_hour_temp_c IS NULL THEN 'Baseline'
        WHEN temperature_c > prev_hour_temp_c THEN 'Warming (+)'
        WHEN temperature_c < prev_hour_temp_c THEN 'Cooling (-)'
        ELSE 'Stable (=)'
    END AS temp_trend,
    relative_humidity_pct,
    rolling_6h_avg_humidity_pct,
    wind_speed_kmh,
    weather_description
FROM ordered_readings
ORDER BY timestamp DESC
LIMIT 48;
