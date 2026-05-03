CREATE OR REPLACE VIEW v_metabase_latest_commodity_map AS
WITH date_coverage AS (
    SELECT
        price_date,
        COUNT(DISTINCT province_name) AS province_count
    FROM v_metabase_food_price
    WHERE source = 'PIHPS Grid Backfill'
      AND province_name <> 'Nasional'
    GROUP BY price_date
),

latest_full_date AS (
    SELECT MAX(price_date) AS latest_date
    FROM date_coverage
    WHERE province_count >= 30
)

SELECT
    p.price_date,
    p.province_name,
    l.latitude,
    l.longitude,
    p.commodity_name,
    p.commodity_group,
    AVG(p.price)::numeric(14,2) AS avg_price,
    COUNT(*) AS record_count
FROM v_metabase_food_price p
JOIN latest_full_date d
    ON p.price_date = d.latest_date
LEFT JOIN dim_province_location l
    ON p.province_name = l.province_name
WHERE p.source = 'PIHPS Grid Backfill'
  AND p.province_name <> 'Nasional'
GROUP BY
    p.price_date,
    p.province_name,
    l.latitude,
    l.longitude,
    p.commodity_name,
    p.commodity_group;