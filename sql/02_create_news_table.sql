CREATE TABLE IF NOT EXISTS fact_food_news (
    news_id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    url TEXT,
    source_name TEXT,
    published_at TIMESTAMP,
    query_keyword TEXT,
    commodity_group TEXT,
    event_category TEXT,
    sentiment_label TEXT,
    sentiment_score INT,
    matched_keywords TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (title, published_at, query_keyword)
);