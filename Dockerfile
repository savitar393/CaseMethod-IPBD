FROM apache/airflow:3.1.8

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libpangocairo-1.0-0 \
    libpango-1.0-0 \
    libcairo2 \
    libgtk-3-0 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    "requests>=2.32.0" \
    "urllib3<3" \
    "charset-normalizer>=3,<4" \
    psycopg2-binary \
    pandas \
    sqlalchemy \
    requests \
    beautifulsoup4 \
    boto3 \
    playwright \
    html5lib \ 
    lxml

RUN python -m playwright install chromium