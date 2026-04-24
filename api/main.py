import json
from pathlib import Path
from fastapi import FastAPI

app = FastAPI(title="Wired Articles API")

DATA_PATH = Path("data/wired_articles.json")


@app.get("/")
def root():
    return {"message": "Wired Articles API is running"}


@app.get("/articles")
def get_articles():
    if not DATA_PATH.exists():
        return []

    with open(DATA_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Supports both session format and direct list format.
    if isinstance(data, dict) and "articles" in data:
        return data["articles"]

    if isinstance(data, list):
        return data

    return []