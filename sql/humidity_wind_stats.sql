-- ==============================================================================
-- Query: Humidity & Wind Speed Analytics
-- Description: Computes average humidity, wind speed statistics, wind condition
--              classification, and moisture levels.
-- ==============================================================================

SELECT
    CAST(timestamp AS DATE) AS observation_date,
    COUNT(*) AS total_observations,
    -- Humidity metrics
    ROUND(AVG(relative_humidity_pct), 2) AS avg_humidity_pct,
    ROUND(MIN(relative_humidity_pct), 2) AS min_humidity_pct,
    ROUND(MAX(relative_humidity_pct), 2) AS max_humidity_pct,
    -- Wind speed metrics (km/h and m/s)
    ROUND(AVG(wind_speed_kmh), 2) AS avg_wind_speed_kmh,
    ROUND(MAX(wind_speed_kmh), 2) AS max_wind_speed_kmh,
    ROUND(AVG(wind_speed_ms), 2) AS avg_wind_speed_ms,
    ROUND(MAX(wind_speed_ms), 2) AS max_wind_speed_ms,
    -- Comfort categorization based on humidity
    CASE
        WHEN AVG(relative_humidity_pct) < 30 THEN 'Dry'
        WHEN AVG(relative_humidity_pct) BETWEEN 30 AND 60 THEN 'Comfortable'
        WHEN AVG(relative_humidity_pct) BETWEEN 60 AND 80 THEN 'Humid'
        ELSE 'Very Humid / Muggy'
    END AS humidity_condition,
    -- Wind strength classification (Beaufort scale approximation)
    CASE
        WHEN MAX(wind_speed_kmh) < 5 THEN 'Calm'
        WHEN MAX(wind_speed_kmh) < 20 THEN 'Light to Gentle Breeze'
        WHEN MAX(wind_speed_kmh) < 40 THEN 'Moderate to Fresh Breeze'
        ELSE 'Strong Wind'
    END AS max_wind_condition
FROM weather_analytics.weather_readings
WHERE relative_humidity_pct IS NOT NULL
  AND wind_speed_kmh IS NOT NULL
GROUP BY CAST(timestamp AS DATE)
ORDER BY observation_date DESC;
