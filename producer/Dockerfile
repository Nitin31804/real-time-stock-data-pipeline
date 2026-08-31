FROM python:3.11-slim

LABEL maintainer="stock-pipeline"
LABEL description="Kafka Stock Data Producer"

WORKDIR /app

# Install system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

CMD ["python", "-u", "producer.py"]
