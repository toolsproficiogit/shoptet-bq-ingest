# Cloud Run container for Shoptet → BigQuery
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8080

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Python deps
COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# App code
COPY app/ ./app/

# IMPORTANT: no config files are copied; multi-pipeline uses remote YAML via CONFIG_URL
CMD ["python", "app/main.py"]
