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

import os
import json
import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    from_json,
    window,
    first,
    last,
    max as spark_max,
    min as spark_min,
    sum as spark_sum,
    count as spark_count,
    lit,
    current_timestamp,
    to_json,
    struct,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    TimestampType,
)

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
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

POSTGRES_PROPS = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}

# Collect all pre-downloaded JARs from /app/jars
import glob as _glob

_jars = ",".join(_glob.glob(f"{SPARK_JARS_DIR}/*.jar"))
logger.info(f"📦 JARs found: {_jars or 'NONE — check /app/jars'}")

# ─── Raw Tick JSON Schema ─────────────────────────────────────────────────────
TICK_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("symbol", StringType(), False),
        StructField("price", DoubleType(), False),
        StructField("volume", LongType(), True),
        StructField("timestamp", TimestampType(), False),
        StructField("exchange", StringType(), True),
        StructField("trade_conditions", StringType(), True),  # array → string in JSON
        StructField("source", StringType(), True),
    ]
)


# ─── Spark Session ────────────────────────────────────────────────────────────
def build_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("StockDataPipelineStreaming")
        .master("local[2]")
        .config("spark.jars", _jars)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.kafka.consumer.cache.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("✅ SparkSession created (local[2] mode)")
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
                    high        = GREATEST(stock_ohlcv_1m.high, EXCLUDED.high),
                    low         = LEAST(stock_ohlcv_1m.low,  EXCLUDED.low),
                    close       = EXCLUDED.close,
                    volume      = stock_ohlcv_1m.volume + EXCLUDED.volume,
                    vwap        = EXCLUDED.vwap,
                    trade_count = stock_ohlcv_1m.trade_count + EXCLUDED.trade_count,
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
        conn.close()
        logger.info(
            f"✅ Batch {batch_id}: upserted {len(rows)} rows into stock_ohlcv_1m"
        )

    except Exception as e:
        logger.error(f"❌ Batch {batch_id} write failed: {e}")
        raise


def write_raw_ticks_batch(batch_df: DataFrame, batch_id: int):
    """Append raw ticks to the raw_stock_ticks table (audit log)."""
    if batch_df.isEmpty():
        return
    rows = batch_df.collect()
    if not rows:
        return
    try:
        conn = _pg_connect()
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO raw_stock_ticks
                    (symbol, price, volume, trade_timestamp, exchange, source)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
            """,
                [
                    (
                        r["symbol"],
                        float(r["price"]),
                        int(r["volume"]) if r["volume"] else 0,
                        r["trade_timestamp"],
                        r["exchange"] if r["exchange"] else "MOCK",
                        r["source"] if r["source"] else "gbm_mock",
                    )
                    for r in rows
                ],
            )
        conn.commit()
        conn.close()
        logger.debug(f"Raw ticks batch {batch_id}: appended {len(rows)} rows")
    except Exception as e:
        logger.warning(f"Failed to write raw ticks batch {batch_id}: {e}")


# ─── Main Streaming Pipeline ──────────────────────────────────────────────────
def main():
    logger.info("=" * 60)
    logger.info("  PySpark Structured Streaming — Stock Data Pipeline")
    logger.info("=" * 60)
    logger.info(f"  Kafka  : {KAFKA_BOOTSTRAP_SERVERS} → {KAFKA_TOPIC}")
    logger.info(f"  Postgres: {POSTGRES_URL}")
    logger.info("=" * 60)

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
        kafka_raw.selectExpr(
            "CAST(value AS STRING) AS json_str", "timestamp AS kafka_timestamp"
        )
        .select(
            from_json(col("json_str"), TICK_SCHEMA).alias("d"),
            col("kafka_timestamp"),
        )
        .select("d.*", "kafka_timestamp")
    )

    # ── 3. Filter Valid Records / DLQ ─────────────────────────────────────────
    valid_ticks = parsed.filter(
        col("symbol").isNotNull()
        & col("price").isNotNull()
        & (col("price") > 0)
        & col("timestamp").isNotNull()
    ).withWatermark("timestamp", "30 seconds")

    invalid_ticks = parsed.filter(
        col("symbol").isNull()
        | col("price").isNull()
        | (col("price") <= 0)
        | col("timestamp").isNull()
    )

    # ── 4. Write Invalid Records to DLQ ───────────────────────────────────────
    dlq_query = (
        invalid_ticks.select(
            to_json(struct("*")).alias("value"),
            lit(KAFKA_TOPIC).alias("key"),
        )
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", KAFKA_DLQ_TOPIC)
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

    raw_query = (
        raw_ticks_for_db.writeStream.outputMode("append")
        .foreachBatch(write_raw_ticks_batch)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/raw_ticks")
        .trigger(processingTime="2 seconds")
        .start()
    )
    logger.info("📝 Raw tick audit stream started")

    # ── 6. 1-Minute OHLCV + VWAP Aggregation ─────────────────────────────────
    ohlcv_1m = (
        valid_ticks.groupBy(
            window(col("timestamp"), "5 seconds"),
            col("symbol"),
        )
        .agg(
            first("price").alias("open"),
            spark_max("price").alias("high"),
            spark_min("price").alias("low"),
            last("price").alias("close"),
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

    ohlcv_query = (
        ohlcv_1m.writeStream.outputMode("update")
        .foreachBatch(upsert_ohlcv_batch)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/ohlcv_1m")
        .trigger(processingTime="2 seconds")
        .start()
    )
    logger.info("📊 1-minute OHLCV aggregation stream started")

    # ── 7. Await Termination ──────────────────────────────────────────────────
    logger.info("🔄 All streaming queries active. Awaiting termination...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
