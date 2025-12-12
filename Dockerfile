# Dockerfile
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# System dependencies (minimal)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app source
COPY app/ app/

# Cloud Run will inject $PORT, default to 8080 for local dev
ENV PORT=8080

# Memory-efficient settings for large CSV processing
ENV BATCH_SIZE=5000 \
    CHUNK_SIZE=1048576 \
    DEDUPE_MODE=auto_dedupe

# Start via gunicorn with optimized worker configuration
# - workers: number of worker processes (Cloud Run auto-scales)
# - threads: threads per worker for I/O-bound operations
# - timeout: request timeout in seconds (increased for large CSVs)
# - max-requests: restart worker after N requests to prevent memory leaks
CMD exec gunicorn -b :$PORT app.main:app \
    --workers 2 \
    --threads 4 \
    --timeout 300 \
    --max-requests 100 \
    --max-requests-jitter 10
