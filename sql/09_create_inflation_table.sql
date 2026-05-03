CREATE TABLE IF NOT EXISTS fact_inflation_region (
    inflation_id SERIAL PRIMARY KEY,
    period_month DATE NOT NULL,
    province_name TEXT NOT NULL,
    inflation_mtm NUMERIC(8,2),
    inflation_yoy NUMERIC(8,2),
    cpi NUMERIC(10,2),
    source TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (period_month, province_name)
);