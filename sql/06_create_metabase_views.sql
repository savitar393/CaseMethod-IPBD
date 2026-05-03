CREATE OR REPLACE VIEW v_metabase_food_price AS
SELECT
    price_date,
    commodity_name,
    commodity_group,
    unit,
    province_name,
    city_name,
    region_level,
    price,
    previous_price,
    percentage_change,
    gap_days,
    alert_status,
    source,
    source_type
FROM v_food_price_analysis;