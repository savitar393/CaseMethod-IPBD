CREATE OR REPLACE VIEW v_food_price_analysis AS
SELECT
    v.*,
    CASE
        WHEN LOWER(v.commodity_name) LIKE '%beras%' THEN 'Beras'
        WHEN LOWER(v.commodity_name) LIKE '%cabai%' OR LOWER(v.commodity_name) LIKE '%cabe%' THEN 'Cabai'
        WHEN LOWER(v.commodity_name) LIKE '%bawang%' THEN 'Bawang'
        WHEN LOWER(v.commodity_name) LIKE '%daging%'
          OR LOWER(v.commodity_name) LIKE '%ayam%'
          OR LOWER(v.commodity_name) LIKE '%telur%' THEN 'Protein Hewani'
        WHEN LOWER(v.commodity_name) LIKE '%minyak%' THEN 'Minyak Goreng'
        WHEN LOWER(v.commodity_name) LIKE '%gula%' THEN 'Gula'
        WHEN LOWER(v.commodity_name) LIKE '%gas%'
          OR LOWER(v.commodity_name) LIKE '%elpiji%' THEN 'Energi Rumah Tangga'
        WHEN LOWER(v.commodity_name) LIKE '%ikan%' THEN 'Ikan'
        WHEN LOWER(v.commodity_name) LIKE '%garam%'
          OR LOWER(v.commodity_name) LIKE '%tepung%' THEN 'Bahan Pokok Lainnya'
        ELSE 'Lainnya'
    END AS commodity_group,
    CASE
        WHEN v.province_name = 'Nasional' THEN 'Nasional'
        ELSE 'Provinsi/Kota'
    END AS region_level,
    CASE
        WHEN v.source = 'PIHPS Grid Backfill' THEN 'Province Historical'
        WHEN v.source = 'PIHPS Chart History' THEN 'National Historical'
        WHEN v.source IN ('Info Pangan Jakarta', 'Berita Jakarta Info Pangan') THEN 'DKI Jakarta Snapshot'
        ELSE 'Other'
    END AS source_type
FROM v_food_price_enriched v;