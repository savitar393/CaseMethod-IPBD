CREATE TABLE IF NOT EXISTS fact_climate_region_daily (
    climate_id SERIAL PRIMARY KEY,
    climate_date DATE NOT NULL,
    province_name TEXT NOT NULL,
    latitude NUMERIC(12,8),
    longitude NUMERIC(12,8),

    precipitation_mm NUMERIC(12,2),
    temperature_avg_c NUMERIC(8,2),
    temperature_max_c NUMERIC(8,2),
    temperature_min_c NUMERIC(8,2),
    relative_humidity_pct NUMERIC(8,2),
    wind_speed_mps NUMERIC(8,2),

    dry_day_flag INT,
    heavy_rain_flag INT,
    heat_stress_flag INT,
    climate_risk_score INT,
    climate_risk_label TEXT,

    source TEXT NOT NULL,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (climate_date, province_name, source)
);