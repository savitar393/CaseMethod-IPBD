DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fact_food_price_unique_source_date_region_commodity'
    ) THEN
        ALTER TABLE fact_food_price
        ADD CONSTRAINT fact_food_price_unique_source_date_region_commodity
        UNIQUE (commodity_id, region_id, price_date, source);
    END IF;
END $$;