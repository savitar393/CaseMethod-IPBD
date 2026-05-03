CREATE TABLE IF NOT EXISTS dim_province_location (
    province_name TEXT PRIMARY KEY,
    province_code TEXT,
    map_code TEXT,
    latitude NUMERIC(12,8),
    longitude NUMERIC(12,8),
    source TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);