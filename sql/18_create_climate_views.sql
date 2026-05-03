CREATE OR REPLACE VIEW v_metabase_climate_daily AS
SELECT
    climate_id,
    climate_date,
    DATE_TRUNC('month', climate_date)::DATE AS period_month,
    province_name,
    latitude,
    longitude,
    precipitation_mm,
    temperature_avg_c,
    temperature_max_c,
    temperature_min_c,
    relative_humidity_pct,
    wind_speed_mps,
    dry_day_flag,
    heavy_rain_flag,
    heat_stress_flag,
    climate_risk_score,
    climate_risk_label,
    source,
    scraped_at
FROM fact_climate_region_daily;


CREATE OR REPLACE VIEW v_metabase_climate_monthly AS
SELECT
    DATE_TRUNC('month', climate_date)::DATE AS period_month,
    province_name,
    AVG(latitude) AS latitude,
    AVG(longitude) AS longitude,

    SUM(precipitation_mm) AS monthly_precipitation_mm,
    AVG(precipitation_mm) AS avg_daily_precipitation_mm,
    AVG(temperature_avg_c) AS avg_temperature_c,
    MAX(temperature_max_c) AS max_temperature_c,
    MIN(temperature_min_c) AS min_temperature_c,
    AVG(relative_humidity_pct) AS avg_relative_humidity_pct,
    AVG(wind_speed_mps) AS avg_wind_speed_mps,

    SUM(dry_day_flag) AS dry_days,
    SUM(heavy_rain_flag) AS heavy_rain_days,
    SUM(heat_stress_flag) AS heat_stress_days,
    MAX(climate_risk_score) AS max_climate_risk_score,
    AVG(climate_risk_score) AS avg_climate_risk_score,

    CASE
        WHEN SUM(dry_day_flag) >= 20
          OR SUM(heavy_rain_flag) >= 5
          OR SUM(heat_stress_flag) >= 10
          OR MAX(climate_risk_score) >= 3
            THEN 'High'
        WHEN SUM(dry_day_flag) >= 10
          OR SUM(heavy_rain_flag) >= 2
          OR SUM(heat_stress_flag) >= 5
          OR MAX(climate_risk_score) >= 1
            THEN 'Medium'
        ELSE 'Low'
    END AS monthly_climate_risk_label,

    COUNT(*) AS climate_record_count
FROM fact_climate_region_daily
GROUP BY
    DATE_TRUNC('month', climate_date)::DATE,
    province_name;


CREATE OR REPLACE VIEW v_metabase_price_climate_monthly AS
WITH monthly_price AS (
    SELECT
        DATE_TRUNC('month', price_date)::DATE AS period_month,
        province_name,
        commodity_group,
        commodity_name,
        source,
        AVG(price) AS avg_price,
        MIN(price) AS min_price,
        MAX(price) AS max_price,
        COUNT(*) AS price_record_count
    FROM v_metabase_food_price
    WHERE source = 'PIHPS Grid Backfill'
      AND province_name <> 'Nasional'
    GROUP BY
        DATE_TRUNC('month', price_date)::DATE,
        province_name,
        commodity_group,
        commodity_name,
        source
)

SELECT
    p.period_month,
    p.province_name,
    p.commodity_group,
    p.commodity_name,
    p.source AS price_source,
    p.avg_price,
    p.min_price,
    p.max_price,
    p.price_record_count,

    c.monthly_precipitation_mm,
    c.avg_daily_precipitation_mm,
    c.avg_temperature_c,
    c.max_temperature_c,
    c.min_temperature_c,
    c.avg_relative_humidity_pct,
    c.avg_wind_speed_mps,
    c.dry_days,
    c.heavy_rain_days,
    c.heat_stress_days,
    c.max_climate_risk_score,
    c.avg_climate_risk_score,
    c.monthly_climate_risk_label,
    c.climate_record_count
FROM monthly_price p
LEFT JOIN v_metabase_climate_monthly c
    ON p.period_month = c.period_month
    AND p.province_name = c.province_name;