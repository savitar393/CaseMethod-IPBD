CREATE OR REPLACE VIEW v_metabase_exchange_rate_usd_idr AS
SELECT
    exchange_rate_id,
    rate_date,
    currency_pair,
    rate_mid,
    source,
    scraped_at
FROM fact_exchange_rate_usd_idr;


CREATE OR REPLACE VIEW v_metabase_fuel_price_region AS
SELECT
    fuel_price_id,
    effective_date,
    province_name,
    product_name,
    price_per_liter,
    source,
    scraped_at
FROM fact_fuel_price_region;


CREATE OR REPLACE VIEW v_metabase_latest_fuel_price AS
WITH latest AS (
    SELECT MAX(effective_date) AS latest_date
    FROM fact_fuel_price_region
)
SELECT
    f.effective_date,
    f.province_name,
    f.product_name,
    f.price_per_liter,
    f.source
FROM fact_fuel_price_region f
JOIN latest l
    ON f.effective_date = l.latest_date;


CREATE OR REPLACE VIEW v_metabase_macro_cost_summary AS
SELECT
    'Exchange Rate' AS context_type,
    rate_date AS context_date,
    'Indonesia' AS region_name,
    currency_pair AS indicator_name,
    rate_mid AS indicator_value,
    'IDR per USD' AS unit,
    source
FROM fact_exchange_rate_usd_idr

UNION ALL

SELECT
    'Fuel Price' AS context_type,
    effective_date AS context_date,
    province_name AS region_name,
    product_name AS indicator_name,
    price_per_liter AS indicator_value,
    'IDR per liter' AS unit,
    source
FROM fact_fuel_price_region;