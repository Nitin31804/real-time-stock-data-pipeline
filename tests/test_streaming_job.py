from datetime import datetime, timezone

import pytest
import streaming_job
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("stock-pipeline-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_ohlcv_uses_event_time_for_open_and_close(spark):
    rows = [
        (
            "00000000-0000-4000-8000-000000000002",
            "AAPL",
            102.0,
            2,
            datetime(2026, 1, 1, 12, 0, 20, tzinfo=timezone.utc),
        ),
        (
            "00000000-0000-4000-8000-000000000001",
            "AAPL",
            100.0,
            1,
            datetime(2026, 1, 1, 12, 0, 5, tzinfo=timezone.utc),
        ),
        (
            "00000000-0000-4000-8000-000000000003",
            "AAPL",
            98.0,
            3,
            datetime(2026, 1, 1, 12, 0, 50, tzinfo=timezone.utc),
        ),
    ]
    frame = spark.createDataFrame(rows, ["event_id", "symbol", "price", "volume", "timestamp"])

    candle = streaming_job.build_ohlcv_aggregation(frame).collect()[0]

    assert candle.open == 100.0
    assert candle.close == 98.0
    assert candle.high == 102.0
    assert candle.low == 98.0
    assert candle.volume == 6
    assert candle.trade_count == 3
    assert candle.vwap == pytest.approx((100.0 + 204.0 + 294.0) / 6)


def test_validation_routes_bad_records_to_dlq_frame(spark):
    rows = [
        (
            "00000000-0000-4000-8000-000000000001",
            "AAPL",
            100.0,
            1,
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            "TEST",
            "unit_test",
        ),
        (None, "AAPL", 100.0, 1, datetime(2026, 1, 1, tzinfo=timezone.utc), "TEST", "unit_test"),
        (
            "bad-price",
            "MSFT",
            -1.0,
            1,
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            "TEST",
            "unit_test",
        ),
        (
            "00000000-0000-4000-8000-000000000004",
            "MSFT",
            10.0,
            -1,
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            "TEST",
            "unit_test",
        ),
    ]
    frame = spark.createDataFrame(
        rows,
        ["event_id", "symbol", "price", "volume", "timestamp", "exchange", "source"],
    )

    valid, invalid = streaming_job.split_valid_invalid(frame)

    assert valid.count() == 1
    assert invalid.count() == 3
