CREATE OR REPLACE VIEW v_metabase_food_news AS
SELECT
    news_id,
    title,
    url,
    source_name,
    published_at,
    DATE(published_at) AS published_date,
    query_keyword,
    commodity_group,
    event_category,
    sentiment_label,
    sentiment_score,
    matched_keywords,
    scraped_at
FROM fact_food_news;