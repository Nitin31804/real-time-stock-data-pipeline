import json
import os
import time
import uuid
from datetime import datetime, timedelta, timezone

import psycopg2
import pytest
from confluent_kafka import Consumer, Producer

pytestmark = pytest.mark.integration


@pytest.mark.skipif(
    os.getenv("RUN_PIPELINE_INTEGRATION") != "1",
    reason="Set RUN_PIPELINE_INTEGRATION=1 with Docker Compose running",
)
def test_known_ticks_produce_one_correct_finalized_candle():
    symbol = f"E2E{uuid.uuid4().hex[:5]}".upper()
    base = datetime.now(timezone.utc).replace(second=0, microsecond=0) - timedelta(minutes=2)
    producer = Producer({"bootstrap.servers": "localhost:29092"})
    ticks = [
        (5, 100.0, 1),
        (20, 104.0, 2),
        (50, 98.0, 3),
    ]
    first_payload = None
    for seconds, price, volume in ticks:
        payload = {
            "event_id": str(uuid.uuid4()),
            "symbol": symbol,
            "price": price,
            "volume": volume,
            "timestamp": (base + timedelta(seconds=seconds)).isoformat(),
            "exchange": "TEST",
            "trade_conditions": ["INTEGRATION"],
            "source": "integration_test",
        }
        if first_payload is None:
            first_payload = payload
        producer.produce("stock.raw-ticks.v1", key=symbol, value=json.dumps(payload))

    # A retried Kafka delivery must not alter the raw-event count or candle totals.
    producer.produce("stock.raw-ticks.v1", key=symbol, value=json.dumps(first_payload))

    watermark = {
        "event_id": str(uuid.uuid4()),
        "symbol": symbol,
        "price": 99.0,
        "volume": 1,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "exchange": "TEST",
        "trade_conditions": ["WATERMARK"],
        "source": "integration_test",
    }
    producer.produce("stock.raw-ticks.v1", key=symbol, value=json.dumps(watermark))
    producer.flush(10)

    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        with psycopg2.connect(
            host="localhost",
            dbname=os.getenv("POSTGRES_DB", "stock_db"),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT open, high, low, close, volume, trade_count
                    FROM stock_ohlcv_1m
                    WHERE symbol = %s AND bucket_start = %s
                    """,
                    (symbol, base),
                )
                row = cursor.fetchone()
        if row:
            with psycopg2.connect(
                host="localhost",
                dbname=os.getenv("POSTGRES_DB", "stock_db"),
                user=os.getenv("POSTGRES_USER", "postgres"),
                password=os.getenv("POSTGRES_PASSWORD", "postgres"),
            ) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT COUNT(*) FROM raw_stock_ticks WHERE symbol = %s",
                        (symbol,),
                    )
                    raw_count = cursor.fetchone()[0]
            if raw_count != 4:
                time.sleep(2)
                continue
            assert tuple(map(float, row[:4])) == (100.0, 104.0, 98.0, 98.0)
            assert row[4:] == (6, 3)
            return
        time.sleep(2)
    pytest.fail("Timed out waiting for the finalized integration-test candle")


@pytest.mark.skipif(
    os.getenv("RUN_PIPELINE_INTEGRATION") != "1",
    reason="Set RUN_PIPELINE_INTEGRATION=1 with Docker Compose running",
)
def test_malformed_tick_is_routed_to_dead_letter_topic():
    event_id = str(uuid.uuid4())
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:29092",
            "group.id": f"dlq-test-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe(["stock.dlq.v1"])

    producer = Producer({"bootstrap.servers": "localhost:29092"})
    producer.produce(
        "stock.raw-ticks.v1",
        key="BAD",
        value=json.dumps(
            {
                "event_id": event_id,
                "symbol": "BAD",
                "price": -1,
                "volume": 1,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "exchange": "TEST",
                "trade_conditions": ["INTEGRATION"],
                "source": "integration_test",
            }
        ),
    )
    producer.flush(10)

    deadline = time.monotonic() + 90
    try:
        while time.monotonic() < deadline:
            message = consumer.poll(2)
            if message is None or message.error():
                continue
            payload = json.loads(message.value())
            if payload.get("event_id") == event_id:
                assert payload["price"] == -1
                return
    finally:
        consumer.close()
    pytest.fail("Timed out waiting for malformed record on the dead-letter topic")
