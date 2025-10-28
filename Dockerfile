FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY app/ ./app/
COPY config/config.yaml /app/config/config.yaml
ENV PORT=8080
CMD ["python", "app/main.py"]
