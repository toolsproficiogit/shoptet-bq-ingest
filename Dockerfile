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

# Start via gunicorn, pointing to Flask app in app/main.py:app
CMD exec gunicorn -b :$PORT app.main:app --workers 2 --threads 4 --timeout 120
