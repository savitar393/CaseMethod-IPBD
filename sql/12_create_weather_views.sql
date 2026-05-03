CREATE OR REPLACE VIEW v_metabase_weather_region AS
SELECT
    weather_id,
    weather_datetime_utc,
    forecast_date,
    province_name,
    weather_code,
    weather_desc,
    temperature_min,
    temperature_max,
    temperature_avg,
    humidity_min,
    humidity_max,
    humidity_avg,
    wind_direction,
    wind_speed,
    weather_risk_label,
    weather_risk_score,
    source,
    scraped_at
FROM fact_weather_region;


CREATE OR REPLACE VIEW v_metabase_weather_daily AS
SELECT
    forecast_date,
    province_name,
    AVG(temperature_avg) AS avg_temperature,
    MAX(temperature_max) AS max_temperature,
    AVG(humidity_avg) AS avg_humidity,
    AVG(wind_speed) AS avg_wind_speed,
    MAX(weather_risk_score) AS max_weather_risk_score,
    CASE
        WHEN MAX(weather_risk_score) >= 3 THEN 'High'
        WHEN MAX(weather_risk_score) >= 1 THEN 'Medium'
        ELSE 'Low'
    END AS daily_weather_risk_label,
    COUNT(*) AS forecast_record_count
FROM fact_weather_region
GROUP BY
    forecast_date,
    province_name;


CREATE OR REPLACE VIEW v_metabase_price_weather_context AS
SELECT
    p.price_date,
    p.province_name,
    p.commodity_group,
    p.commodity_name,
    p.source AS price_source,
    p.price,
    p.percentage_change,
    p.alert_status,
    w.avg_temperature,
    w.max_temperature,
    w.avg_humidity,
    w.avg_wind_speed,
    w.max_weather_risk_score,
    w.daily_weather_risk_label
FROM v_metabase_food_price p
LEFT JOIN v_metabase_weather_daily w
    ON p.price_date = w.forecast_date
    AND p.province_name = w.province_name;