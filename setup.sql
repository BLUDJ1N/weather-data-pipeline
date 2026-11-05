CREATE TABLE IF NOT EXISTS public.weather_hourly (
    city_name TEXT NOT NULL,
    weather_time TIMESTAMPTZ NOT NULL,
    temperature_c REAL NOT NULL,
    precipitation_mm REAL NOT NULL,
    wind_speed_kmh REAL NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (city_name, weather_time)
);

CREATE OR REPLACE VIEW public.weather_daily AS
SELECT
    city_name,
    (weather_time AT TIME ZONE 'Europe/Moscow')::date AS weather_date,
    COUNT(*) AS hours_count,
    ROUND(AVG(temperature_c)::numeric, 2) AS avg_temperature_c,
    ROUND(MIN(temperature_c)::numeric, 2) AS min_temperature_c,
    ROUND(MAX(temperature_c)::numeric, 2) AS max_temperature_c,
    ROUND(SUM(precipitation_mm)::numeric, 2) AS precipitation_sum_mm,
    COUNT(*) FILTER (
        WHERE precipitation_mm > 0
    ) AS hours_with_precipitation,
    ROUND(MAX(wind_speed_kmh)::numeric, 2) AS max_wind_speed_kmh,
    MAX(loaded_at) AS last_loaded_at
FROM public.weather_hourly
GROUP BY
    city_name,
    (weather_time AT TIME ZONE 'Europe/Moscow')::date;
