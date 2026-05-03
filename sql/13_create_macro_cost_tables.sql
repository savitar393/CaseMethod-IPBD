CREATE TABLE IF NOT EXISTS fact_exchange_rate_usd_idr (
    exchange_rate_id SERIAL PRIMARY KEY,
    rate_date DATE NOT NULL,
    currency_pair TEXT NOT NULL DEFAULT 'USD/IDR',
    rate_mid NUMERIC(14,2),
    source TEXT NOT NULL,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (rate_date, currency_pair, source)
);


CREATE TABLE IF NOT EXISTS fact_fuel_price_region (
    fuel_price_id SERIAL PRIMARY KEY,
    effective_date DATE NOT NULL,
    province_name TEXT NOT NULL,
    product_name TEXT NOT NULL,
    price_per_liter NUMERIC(14,2),
    source TEXT NOT NULL,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (effective_date, province_name, product_name, source)
);