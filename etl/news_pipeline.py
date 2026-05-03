import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text


DATABASE_URL = os.getenv(
    "PANGAN_DATABASE_URL",
    "postgresql+psycopg2://pangan_user:user123@localhost:5434/panganwatch"
)

DATA_DIR = Path(os.getenv("PIPELINE_DATA_DIR", "data"))
NEWS_CSV_PATH = DATA_DIR / "news_food_price.csv"


def get_engine():
    return create_engine(DATABASE_URL)


def load_news_to_postgres():
    if not NEWS_CSV_PATH.exists():
        raise FileNotFoundError(f"News CSV not found: {NEWS_CSV_PATH}")

    df = pd.read_csv(NEWS_CSV_PATH)

    if df.empty:
        print("No news rows to load.")
        return

    df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce")

    engine = get_engine()

    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(
                text("""
                    INSERT INTO fact_food_news
                    (
                        title,
                        url,
                        source_name,
                        published_at,
                        query_keyword,
                        commodity_group,
                        event_category,
                        sentiment_label,
                        sentiment_score,
                        matched_keywords
                    )
                    VALUES
                    (
                        :title,
                        :url,
                        :source_name,
                        :published_at,
                        :query_keyword,
                        :commodity_group,
                        :event_category,
                        :sentiment_label,
                        :sentiment_score,
                        :matched_keywords
                    )
                    ON CONFLICT (title, published_at, query_keyword)
                    DO UPDATE SET
                        url = EXCLUDED.url,
                        source_name = EXCLUDED.source_name,
                        commodity_group = EXCLUDED.commodity_group,
                        event_category = EXCLUDED.event_category,
                        sentiment_label = EXCLUDED.sentiment_label,
                        sentiment_score = EXCLUDED.sentiment_score,
                        matched_keywords = EXCLUDED.matched_keywords
                """),
                {
                    "title": row.get("title"),
                    "url": row.get("url"),
                    "source_name": row.get("source_name"),
                    "published_at": None if pd.isna(row["published_at"]) else row["published_at"].to_pydatetime(),
                    "query_keyword": row.get("query_keyword"),
                    "commodity_group": row.get("commodity_group"),
                    "event_category": row.get("event_category"),
                    "sentiment_label": row.get("sentiment_label"),
                    "sentiment_score": None if pd.isna(row.get("sentiment_score")) else int(row.get("sentiment_score")),
                    "matched_keywords": row.get("matched_keywords"),
                }
            )

    print(f"Loaded {len(df)} news rows into PostgreSQL.")


if __name__ == "__main__":
    load_news_to_postgres()