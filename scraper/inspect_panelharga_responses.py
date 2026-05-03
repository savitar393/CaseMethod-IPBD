import json
import os
from pathlib import Path


DATA_DIR = Path(os.getenv("PIPELINE_DATA_DIR", "data"))
RESPONSE_LOG = DATA_DIR / "source_pages" / "panelharga_headers" / "api_responses.json"


def main():
    logs = json.loads(RESPONSE_LOG.read_text(encoding="utf-8"))

    for item in logs:
        url = item.get("url", "")
        status = item.get("status")
        sample = item.get("body_sample", "")

        if "api-panelhargav2" in url:
            print("\nURL:", url)
            print("Status:", status)
            print("Body sample:", sample[:300])


if __name__ == "__main__":
    main()