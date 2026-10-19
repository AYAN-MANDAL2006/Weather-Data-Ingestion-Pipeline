-- ==============================================================================
-- Query: Temperature Statistics (Overall & Daily Breakdown)
-- Description: Computes average, maximum, and minimum temperature metrics,
--              including temperature range and daily statistics.
-- ==============================================================================

-- Daily temperature aggregates
SELECT
    CAST(timestamp AS DATE) AS observation_date,
    COUNT(*) AS total_readings,
    ROUND(AVG(temperature_c), 2) AS avg_temp_c,
    ROUND(MIN(temperature_c), 2) AS min_temp_c,
    ROUND(MAX(temperature_c), 2) AS max_temp_c,
    ROUND(MAX(temperature_c) - MIN(temperature_c), 2) AS diurnal_temp_range_c,
    ROUND(AVG(temperature_f), 2) AS avg_temp_f,
    ROUND(MIN(temperature_f), 2) AS min_temp_f,
    ROUND(MAX(temperature_f), 2) AS max_temp_f
FROM weather_analytics.weather_readings
WHERE temperature_c IS NOT NULL
GROUP BY CAST(timestamp AS DATE)
ORDER BY observation_date DESC;
