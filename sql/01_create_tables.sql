-- sql/01_create_tables.sql

\connect panganwatch

CREATE TABLE IF NOT EXISTS dim_commodity (
    commodity_id SERIAL PRIMARY KEY,
    commodity_name TEXT UNIQUE NOT NULL,
    unit TEXT
);

CREATE TABLE IF NOT EXISTS dim_region (
    region_id SERIAL PRIMARY KEY,
    province_name TEXT NOT NULL,
    city_name TEXT,
    UNIQUE (province_name, city_name)
);

CREATE TABLE IF NOT EXISTS fact_food_price (
    price_id SERIAL PRIMARY KEY,
    commodity_id INT NOT NULL REFERENCES dim_commodity(commodity_id),
    region_id INT NOT NULL REFERENCES dim_region(region_id),
    price_date DATE NOT NULL,
    price NUMERIC(12,2) NOT NULL,
    source TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (commodity_id, region_id, price_date, source)
);

CREATE TABLE IF NOT EXISTS fact_price_alert (
    alert_id SERIAL PRIMARY KEY,
    commodity_id INT REFERENCES dim_commodity(commodity_id),
    region_id INT REFERENCES dim_region(region_id),
    price_date DATE NOT NULL,
    source TEXT,
    current_price NUMERIC(12,2),
    previous_price NUMERIC(12,2),
    percentage_change NUMERIC(8,2),
    gap_days INT,
    alert_status TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (commodity_id, region_id, price_date, source)
);

GRANT USAGE, CREATE ON SCHEMA public TO pangan_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO pangan_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO pangan_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO pangan_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT USAGE, SELECT ON SEQUENCES TO pangan_user;