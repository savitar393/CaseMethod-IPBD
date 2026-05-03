CREATE OR REPLACE VIEW v_metabase_food_price_map AS
SELECT
    *,
    CASE province_name
        WHEN 'Aceh' THEN 4.6951
        WHEN 'Sumatera Utara' THEN 3.5952
        WHEN 'Sumatera Barat' THEN -0.9471
        WHEN 'Riau' THEN 0.5071
        WHEN 'Kepulauan Riau' THEN 3.9457
        WHEN 'Jambi' THEN -1.6101
        WHEN 'Sumatera Selatan' THEN -3.3194
        WHEN 'Bengkulu' THEN -3.7928
        WHEN 'Lampung' THEN -5.4500
        WHEN 'Kepulauan Bangka Belitung' THEN -2.7411
        WHEN 'DKI Jakarta' THEN -6.2088
        WHEN 'Jawa Barat' THEN -6.9175
        WHEN 'Jawa Tengah' THEN -6.9667
        WHEN 'DI Yogyakarta' THEN -7.7956
        WHEN 'Jawa Timur' THEN -7.2575
        WHEN 'Banten' THEN -6.1200
        WHEN 'Bali' THEN -8.4095
        WHEN 'Nusa Tenggara Barat' THEN -8.6529
        WHEN 'Nusa Tenggara Timur' THEN -8.6574
        WHEN 'Kalimantan Barat' THEN -0.2788
        WHEN 'Kalimantan Tengah' THEN -1.6815
        WHEN 'Kalimantan Selatan' THEN -3.0926
        WHEN 'Kalimantan Timur' THEN -0.5022
        WHEN 'Kalimantan Utara' THEN 3.0731
        WHEN 'Sulawesi Utara' THEN 1.4931
        WHEN 'Sulawesi Tengah' THEN -1.4300
        WHEN 'Sulawesi Selatan' THEN -5.1477
        WHEN 'Sulawesi Tenggara' THEN -4.1449
        WHEN 'Gorontalo' THEN 0.6999
        WHEN 'Sulawesi Barat' THEN -2.8441
        WHEN 'Maluku' THEN -3.2385
        WHEN 'Maluku Utara' THEN 1.5700
        WHEN 'Papua' THEN -4.2699
        WHEN 'Papua Barat' THEN -1.3361
        ELSE NULL
    END AS latitude,
    CASE province_name
        WHEN 'Aceh' THEN 96.7494
        WHEN 'Sumatera Utara' THEN 98.6722
        WHEN 'Sumatera Barat' THEN 100.4172
        WHEN 'Riau' THEN 101.4478
        WHEN 'Kepulauan Riau' THEN 108.1429
        WHEN 'Jambi' THEN 103.6131
        WHEN 'Sumatera Selatan' THEN 103.9144
        WHEN 'Bengkulu' THEN 102.2608
        WHEN 'Lampung' THEN 105.2667
        WHEN 'Kepulauan Bangka Belitung' THEN 106.4406
        WHEN 'DKI Jakarta' THEN 106.8456
        WHEN 'Jawa Barat' THEN 107.6191
        WHEN 'Jawa Tengah' THEN 110.4167
        WHEN 'DI Yogyakarta' THEN 110.3695
        WHEN 'Jawa Timur' THEN 112.7521
        WHEN 'Banten' THEN 106.1503
        WHEN 'Bali' THEN 115.1889
        WHEN 'Nusa Tenggara Barat' THEN 117.3616
        WHEN 'Nusa Tenggara Timur' THEN 121.0794
        WHEN 'Kalimantan Barat' THEN 111.4753
        WHEN 'Kalimantan Tengah' THEN 113.3824
        WHEN 'Kalimantan Selatan' THEN 115.2838
        WHEN 'Kalimantan Timur' THEN 117.1536
        WHEN 'Kalimantan Utara' THEN 116.0414
        WHEN 'Sulawesi Utara' THEN 124.8413
        WHEN 'Sulawesi Tengah' THEN 121.4456
        WHEN 'Sulawesi Selatan' THEN 119.4327
        WHEN 'Sulawesi Tenggara' THEN 122.1746
        WHEN 'Gorontalo' THEN 122.4467
        WHEN 'Sulawesi Barat' THEN 119.2321
        WHEN 'Maluku' THEN 130.1453
        WHEN 'Maluku Utara' THEN 127.8088
        WHEN 'Papua' THEN 138.0804
        WHEN 'Papua Barat' THEN 133.1747
        ELSE NULL
    END AS longitude
FROM v_metabase_food_price
WHERE province_name <> 'Nasional';