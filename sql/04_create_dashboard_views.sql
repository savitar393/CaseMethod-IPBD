CREATE OR REPLACE VIEW v_food_price_enriched AS
SELECT
    f.price_date,
    c.commodity_name,
    c.unit,
    r.province_name,
    r.city_name,
    f.price,
    f.source,
    a.previous_price,
    a.percentage_change,
    a.gap_days,
    a.alert_status
FROM fact_food_price f
JOIN dim_commodity c
    ON f.commodity_id = c.commodity_id
JOIN dim_region r
    ON f.region_id = r.region_id
LEFT JOIN fact_price_alert a
    ON f.commodity_id = a.commodity_id
    AND f.region_id = a.region_id
    AND f.price_date = a.price_date
    AND f.source = a.source;


CREATE OR REPLACE VIEW v_food_price_source_summary AS
SELECT
    f.source,
    COUNT(*) AS row_count,
    COUNT(DISTINCT f.price_date) AS date_count,
    COUNT(DISTINCT f.commodity_id) AS commodity_count,
    COUNT(DISTINCT f.region_id) AS region_count,
    MIN(f.price_date) AS min_date,
    MAX(f.price_date) AS max_date
FROM fact_food_price f
GROUP BY f.source;