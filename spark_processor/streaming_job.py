"""
Real-Time Stock Data Pipeline — PySpark Structured Streaming Job
=================================================================
Reads from Kafka topic: stock.raw-ticks.v1
Computes:
  - 1-minute OHLCV candles (Open, High, Low, Close, Volume)
  - VWAP (Volume-Weighted Average Price)
Writes to PostgreSQL table: stock_ohlcv_1m
DLQ for malformed records: stock.dlq.v1
"""

import glob
import json
import logging
import os
import time
from datetime import datetime, timezone

from prometheus_client import Counter, Histogram, start_http_server
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    length,
    lit,
    max_by,
    min_by,
    struct,
    to_json,
    window,
)
from pyspark.sql.functions import (
    count as spark_count,
)
from pyspark.sql.functions import (
    max as spark_max,
)
from pyspark.sql.functions import (
    min as spark_min,
)
from pyspark.sql.functions import (
    sum as spark_sum,
)
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# ─── Logging ──────────────────────────────────────────────────────────────────
class JsonFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps(
            {
                "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
                "level": record.levelname,
                "service": "spark-processor",
                "message": record.getMessage(),
            }
        )


handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())
logging.basicConfig(level=logging.INFO, handlers=[handler])
logger = logging.getLogger("spark-streaming-job")

# ─── Configuration ────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock.raw-ticks.v1")
KAFKA_DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "stock.dlq.v1")
POSTGRES_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://localhost:5432/stock_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/spark_checkpoints")
SPARK_JARS_DIR = os.getenv("SPARK_JARS", "/app/jars")
CANDLE_WINDOW = os.getenv("CANDLE_WINDOW", "1 minute")
WATERMARK_DELAY = os.getenv("WATERMARK_DELAY", "30 seconds")
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "2 seconds")
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[2]")
METRICS_PORT = int(os.getenv("METRICS_PORT", "9102"))

POSTGRES_PROPS = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}

# Collect all pre-downloaded JARs from /app/jars.
_jars = ",".join(glob.glob(f"{SPARK_JARS_DIR}/*.jar"))
logger.info(f"📦 JARs found: {_jars or 'NONE — check /app/jars'}")

RAW_ROWS_WRITTEN = Counter(
    "stock_pipeline_raw_rows_written_total", "Raw stock ticks written to PostgreSQL"
)
CANDLE_ROWS_WRITTEN = Counter(
    "stock_pipeline_candle_rows_written_total", "Finalized OHLCV candles written"
)
FAILED_RECORDS = Counter(
    "stock_pipeline_failed_records_total", "Invalid records routed to the dead-letter topic"
)
END_TO_END_LATENCY = Histogram(
    "stock_pipeline_end_to_end_latency_seconds",
    "Elapsed time from event timestamp to raw PostgreSQL persistence",
)
POSTGRES_WRITE_SECONDS = Histogram(
    "stock_pipeline_postgres_write_seconds",
    "Time spent writing a micro-batch to PostgreSQL",
    ["table"],
)

# ─── Raw Tick JSON Schema ─────────────────────────────────────────────────────
TICK_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("symbol", StringType(), False),
        StructField("price", DoubleType(), False),
        StructField("volume", LongType(), True),
        StructField("timestamp", TimestampType(), False),
        StructField("exchange", StringType(), True),
        StructField("trade_conditions", ArrayType(StringType()), True),
        StructField("source", StringType(), True),
    ]
)


# ─── Spark Session ────────────────────────────────────────────────────────────
def build_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("StockDataPipelineStreaming")
        .master(SPARK_MASTER)
        .config("spark.jars", _jars)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.kafka.consumer.cache.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created with master=%s", SPARK_MASTER)
    return spark


# ─── PostgreSQL Connection Helper ─────────────────────────────────────────────
def _pg_connect():
    """Open a psycopg2 connection using env-var credentials (no URL parsing)."""
    import psycopg2

    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "stock_db"),
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


# ─── PostgreSQL Writers ───────────────────────────────────────────────────────
def upsert_ohlcv_batch(batch_df: DataFrame, batch_id: int):
    """
    Write an OHLCV micro-batch to PostgreSQL using executemany INSERT ON CONFLICT.
    Direct psycopg2 — no staging tables, no JDBC URL parsing.
    """
    if batch_df.isEmpty():
        return

    rows = batch_df.collect()
    if not rows:
        return

    logger.info(f"📦 Batch {batch_id}: writing {len(rows)} OHLCV rows to PostgreSQL...")

    started = time.perf_counter()
    conn = None
    try:
        conn = _pg_connect()
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO stock_ohlcv_1m
                    (symbol, bucket_start, open, high, low, close, volume, vwap, trade_count, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (symbol, bucket_start)
                DO UPDATE SET
                    open        = EXCLUDED.open,
                    high        = EXCLUDED.high,
                    low         = EXCLUDED.low,
                    close       = EXCLUDED.close,
                    volume      = EXCLUDED.volume,
                    vwap        = EXCLUDED.vwap,
                    trade_count = EXCLUDED.trade_count,
                    updated_at  = NOW();
            """,
                [
                    (
                        r["symbol"],
                        r["bucket_start"],
                        float(r["open"]),
                        float(r["high"]),
                        float(r["low"]),
                        float(r["close"]),
                        int(r["volume"]) if r["volume"] else 0,
                        float(r["vwap"]) if r["vwap"] else None,
                        int(r["trade_count"]) if r["trade_count"] else 0,
                    )
                    for r in rows
                ],
            )
        conn.commit()
        CANDLE_ROWS_WRITTEN.inc(len(rows))
        logger.info(f"✅ Batch {batch_id}: upserted {len(rows)} rows into stock_ohlcv_1m")

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"❌ Batch {batch_id} write failed: {e}")
        raise
    finally:
        if conn:
            conn.close()
        POSTGRES_WRITE_SECONDS.labels(table="stock_ohlcv_1m").observe(time.perf_counter() - started)


def write_raw_ticks_batch(batch_df: DataFrame, batch_id: int):
    """Append raw ticks to the raw_stock_ticks table (audit log)."""
    if batch_df.isEmpty():
        return
    rows = batch_df.collect()
    if not rows:
        return
    started = time.perf_counter()
    conn = None
    try:
        conn = _pg_connect()
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO raw_stock_ticks
                    (event_id, symbol, price, volume, trade_timestamp, exchange, source)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING;
            """,
                [
                    (
                        r["event_id"],
                        r["symbol"],
                        float(r["price"]),
                        int(r["volume"]) if r["volume"] else 0,
                        r["trade_timestamp"],
                        r["exchange"] if r["exchange"] else "SIMULATED",
                        r["source"] if r["source"] else "simulation_gbm",
                    )
                    for r in rows
                ],
            )
        conn.commit()
        RAW_ROWS_WRITTEN.inc(len(rows))
        now = datetime.now(timezone.utc)
        for row in rows:
            timestamp = row["trade_timestamp"]
            if timestamp is not None:
                if timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=timezone.utc)
                END_TO_END_LATENCY.observe(max(0, (now - timestamp).total_seconds()))
        logger.debug(f"Raw ticks batch {batch_id}: appended {len(rows)} rows")
    except Exception as e:
        if conn:
            conn.rollback()
        logger.warning(f"Failed to write raw ticks batch {batch_id}: {e}")
        raise
    finally:
        if conn:
            conn.close()
        POSTGRES_WRITE_SECONDS.labels(table="raw_stock_ticks").observe(
            time.perf_counter() - started
        )


def split_valid_invalid(parsed: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split parsed records with one shared validation predicate."""
    uuid_pattern = (
        "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
    )
    is_valid = (
        col("event_id").isNotNull()
        & col("event_id").rlike(uuid_pattern)
        & col("symbol").isNotNull()
        & col("symbol").rlike("^[A-Z0-9.-]{1,10}$")
        & col("price").isNotNull()
        & (col("price") > 0)
        & col("timestamp").isNotNull()
        & (col("volume").isNull() | (col("volume") >= 0))
        & (col("exchange").isNull() | (length(col("exchange")) <= 20))
        & (col("source").isNull() | (length(col("source")) <= 20))
    )
    return parsed.filter(is_valid), parsed.filter(~is_valid)


def write_dlq_batch(batch_df: DataFrame, batch_id: int):
    """Publish invalid records and account for them in the failure metric."""
    count = batch_df.count()
    if count == 0:
        return
    (
        batch_df.select(
            to_json(struct("*")).alias("value"),
            lit(KAFKA_TOPIC).alias("key"),
        )
        .write.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", KAFKA_DLQ_TOPIC)
        .save()
    )
    FAILED_RECORDS.inc(count)
    logger.warning("Batch %s: routed %s invalid records to %s", batch_id, count, KAFKA_DLQ_TOPIC)


def build_ohlcv_aggregation(valid_ticks: DataFrame) -> DataFrame:
    """Build finalized, event-time ordered one-minute OHLCV candles."""
    order_key = struct(col("timestamp"), col("event_id"))
    return (
        valid_ticks.groupBy(window(col("timestamp"), CANDLE_WINDOW), col("symbol"))
        .agg(
            min_by(col("price"), order_key).alias("open"),
            spark_max("price").alias("high"),
            spark_min("price").alias("low"),
            max_by(col("price"), order_key).alias("close"),
            spark_sum("volume").alias("volume"),
            (
                spark_sum(col("price") * col("volume").cast("double"))
                / spark_sum(col("volume").cast("double"))
            ).alias("vwap"),
            spark_count("*").alias("trade_count"),
        )
        .select(
            col("symbol"),
            col("window.start").alias("bucket_start"),
            col("open"),
            col("high"),
            col("low"),
            col("close"),
            col("volume"),
            col("vwap"),
            col("trade_count"),
        )
    )


# ─── Main Streaming Pipeline ──────────────────────────────────────────────────
def main():
    logger.info("=" * 60)
    logger.info("  PySpark Structured Streaming — Stock Data Pipeline")
    logger.info("=" * 60)
    logger.info(f"  Kafka  : {KAFKA_BOOTSTRAP_SERVERS} → {KAFKA_TOPIC}")
    logger.info(f"  Postgres: {POSTGRES_URL}")
    logger.info("=" * 60)

    start_http_server(METRICS_PORT)
    logger.info("Prometheus metrics listening on port %s", METRICS_PORT)
    spark = build_spark_session()

    # ── 1. Read Stream from Kafka ──────────────────────────────────────────────
    kafka_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("kafka.session.timeout.ms", "30000")
        .option("kafka.heartbeat.interval.ms", "10000")
        .load()
    )

    # ── 2. Parse JSON Payload ─────────────────────────────────────────────────
    parsed = (
        kafka_raw.selectExpr("CAST(value AS STRING) AS json_str", "timestamp AS kafka_timestamp")
        .select(
            from_json(col("json_str"), TICK_SCHEMA).alias("d"),
            col("kafka_timestamp"),
        )
        .select("d.*", "kafka_timestamp")
    )

    # ── 3. Filter Valid Records / DLQ ─────────────────────────────────────────
    valid_ticks, invalid_ticks = split_valid_invalid(parsed)
    valid_ticks = valid_ticks.withWatermark(
        "timestamp", WATERMARK_DELAY
    ).dropDuplicatesWithinWatermark(["event_id"])

    # ── 4. Write Invalid Records to DLQ ───────────────────────────────────────
    (
        invalid_ticks.writeStream.outputMode("append")
        .foreachBatch(write_dlq_batch)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/dlq")
        .start()
    )
    logger.info("🗑️  DLQ stream started")

    # ── 5. Raw Tick Append (Audit Log) ────────────────────────────────────────
    raw_ticks_for_db = valid_ticks.select(
        col("event_id"),
        col("symbol"),
        col("price"),
        col("volume"),
        col("timestamp").alias("trade_timestamp"),
        col("exchange"),
        col("source"),
    )

    (
        raw_ticks_for_db.writeStream.outputMode("append")
        .foreachBatch(write_raw_ticks_batch)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/raw_ticks")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )
    logger.info("📝 Raw tick audit stream started")

    # ── 6. 1-Minute OHLCV + VWAP Aggregation ─────────────────────────────────
    ohlcv_1m = build_ohlcv_aggregation(valid_ticks)

    (
        ohlcv_1m.writeStream.outputMode("append")
        .foreachBatch(upsert_ohlcv_batch)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/ohlcv_1m")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )
    logger.info("📊 1-minute OHLCV aggregation stream started")

    # ── 7. Await Termination ──────────────────────────────────────────────────
    logger.info("🔄 All streaming queries active. Awaiting termination...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
