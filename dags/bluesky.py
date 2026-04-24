import json
import os

import pendulum
import requests

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


BASE_URL = "https://public.api.bsky.app/xrpc/app.bsky.feed.getAuthorFeed"


@dag(
    dag_id="bluesky_to_postgres",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 4, 1, tz="UTC"),
    catchup=False,
    tags=["bluesky", "postgres", "etl"],
)
def bluesky_to_postgres():
    @task()
    def fetch_posts():
        actor = os.environ.get("BLUESKY_ACTOR", "bsky.app")

        response = requests.get(
            BASE_URL,
            params={"actor": actor, "limit": 50},
            timeout=30,
        )
        response.raise_for_status()

        payload = response.json()
        feed_items = payload.get("feed", [])

        rows = []
        for item in feed_items:
            post = item.get("post", {})
            author = post.get("author", {})
            record = post.get("record", {})

            rows.append(
                {
                    "actor_handle": author.get("handle"),
                    "actor_did": author.get("did"),
                    "post_uri": post.get("uri"),
                    "post_cid": post.get("cid"),
                    "text": record.get("text"),
                    "created_at": record.get("createdAt"),
                    "like_count": post.get("likeCount"),
                    "repost_count": post.get("repostCount"),
                    "reply_count": post.get("replyCount"),
                    "quote_count": post.get("quoteCount"),
                    "raw_json": json.dumps(item),
                }
            )

        return rows

    @task()
    def load_to_postgres(rows: list[dict]):
        hook = PostgresHook(postgres_conn_id="bluesky_postgres")

        create_sql = """
        CREATE TABLE IF NOT EXISTS bluesky_posts (
            id BIGSERIAL PRIMARY KEY,
            actor_handle TEXT,
            actor_did TEXT,
            post_uri TEXT NOT NULL UNIQUE,
            post_cid TEXT,
            text TEXT,
            created_at TIMESTAMPTZ,
            like_count INTEGER,
            repost_count INTEGER,
            reply_count INTEGER,
            quote_count INTEGER,
            raw_json JSONB,
            inserted_at TIMESTAMPTZ DEFAULT NOW()
        );
        """

        upsert_sql = """
        INSERT INTO bluesky_posts (
            actor_handle,
            actor_did,
            post_uri,
            post_cid,
            text,
            created_at,
            like_count,
            repost_count,
            reply_count,
            quote_count,
            raw_json
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (post_uri)
        DO UPDATE SET
            actor_handle = EXCLUDED.actor_handle,
            actor_did = EXCLUDED.actor_did,
            post_cid = EXCLUDED.post_cid,
            text = EXCLUDED.text,
            created_at = EXCLUDED.created_at,
            like_count = EXCLUDED.like_count,
            repost_count = EXCLUDED.repost_count,
            reply_count = EXCLUDED.reply_count,
            quote_count = EXCLUDED.quote_count,
            raw_json = EXCLUDED.raw_json;
        """

        hook.run(create_sql)

        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                cur.executemany(
                    upsert_sql,
                    [
                        (
                            row["actor_handle"],
                            row["actor_did"],
                            row["post_uri"],
                            row["post_cid"],
                            row["text"],
                            row["created_at"],
                            row["like_count"],
                            row["repost_count"],
                            row["reply_count"],
                            row["quote_count"],
                            row["raw_json"],
                        )
                        for row in rows
                    ],
                )
            conn.commit()
        finally:
            conn.close()

    load_to_postgres(fetch_posts())


bluesky_to_postgres()