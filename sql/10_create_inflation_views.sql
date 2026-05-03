CREATE OR REPLACE VIEW v_metabase_inflation_region AS
SELECT
    inflation_id,
    period_month,
    province_name,
    inflation_mtm,
    inflation_yoy,
    cpi,
    source,
    created_at
FROM fact_inflation_region;


CREATE OR REPLACE VIEW v_metabase_price_inflation_monthly AS
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
    WHERE province_name <> 'Nasional'
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
    i.inflation_mtm,
    i.inflation_yoy,
    i.cpi,
    i.source AS inflation_source
FROM monthly_price p
LEFT JOIN fact_inflation_region i
    ON p.period_month = i.period_month
    AND p.province_name = i.province_name;