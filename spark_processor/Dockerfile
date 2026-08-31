FROM python:3.11-slim

LABEL description="PySpark Structured Streaming Job — Stock Data Pipeline"

WORKDIR /app

# ── System dependencies: Java + curl ──────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless \
    curl \
    && rm -rf /var/lib/apt/lists/*

# ── Python dependencies ────────────────────────────────────────────────────────
RUN pip install --no-cache-dir \
    pyspark==3.5.3 \
    psycopg2-binary==2.9.9

# ── Download Kafka + JDBC JARs into a known /app/jars directory ───────────────
# (We pass this dir to spark.jars at runtime via streaming_job.py)
RUN mkdir -p /app/jars && \
    curl -fsSL https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
         -o /app/jars/postgresql-42.7.3.jar && \
    curl -fsSL https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar \
         -o /app/jars/spark-sql-kafka-0-10_2.12-3.5.3.jar && \
    curl -fsSL https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
         -o /app/jars/kafka-clients-3.4.1.jar && \
    curl -fsSL https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar \
         -o /app/jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar && \
    curl -fsSL https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
         -o /app/jars/commons-pool2-2.11.1.jar && \
    echo "✅ All JARs downloaded:" && ls -lh /app/jars/

COPY streaming_job.py .

CMD ["python", "-u", "streaming_job.py"]
