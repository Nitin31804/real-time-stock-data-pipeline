FROM python:3.11-slim

LABEL description="Stock Data Pipeline Dashboard"

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 5000

CMD ["gunicorn", "--worker-class", "gevent", "--bind", "0.0.0.0:5000", "--workers", "2", "--timeout", "120", "app:app"]
