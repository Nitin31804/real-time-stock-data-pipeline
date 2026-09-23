"""
Real-Time Stock Data Pipeline — Kafka Producer
================================================
Modes:
  1. Finnhub WebSocket (live trade ticks) — activated when FINNHUB_API_KEY is set
  2. GBM Mock Generator — activated automatically when no API key is available

Published to Kafka topic: stock.raw-ticks.v1
Partition key: stock symbol (ensures ordering per symbol)
"""

import json
import logging
import math
import os
import random
import time
import uuid
from datetime import datetime, timezone

import numpy as np
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from prometheus_client import Counter, Gauge, start_http_server


# ─── Logging ──────────────────────────────────────────────────────────────────
class JsonFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps(
            {
                "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
                "level": record.levelname,
                "service": "producer",
                "message": record.getMessage(),
            }
        )


handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())
logging.basicConfig(level=logging.INFO, handlers=[handler])
logger = logging.getLogger("stock-producer")

# ─── Configuration ────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock.raw-ticks.v1")
KAFKA_DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "stock.dlq.v1")
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "").strip()
DATA_MODE = os.getenv("DATA_MODE", "simulation").strip().lower()
STOCK_SYMBOLS = [
    s.strip() for s in os.getenv("STOCK_SYMBOLS", "AAPL,GOOGL,MSFT,AMZN,TSLA").split(",")
]
TICK_INTERVAL_MS = int(os.getenv("TICK_INTERVAL_MS", "100"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "9101"))

TICKS_PUBLISHED = Counter(
    "stock_pipeline_ticks_published_total", "Ticks acknowledged by Kafka", ["source"]
)
PUBLISH_FAILURES = Counter("stock_pipeline_publish_failures_total", "Kafka publish failures")
LAST_TICK_TIMESTAMP = Gauge(
    "stock_pipeline_last_tick_timestamp_seconds", "Unix timestamp of the latest produced tick"
)

# Realistic seed prices for the disclosed GBM simulation
SEED_PRICES = {
    "AAPL": 185.50,
    "GOOGL": 175.20,
    "MSFT": 415.80,
    "AMZN": 195.40,
    "TSLA": 245.30,
    "NVDA": 875.60,
    "META": 505.20,
    "NFLX": 640.90,
    "AMD": 165.40,
    "INTC": 32.50,
}


# ─── Kafka Admin: Create Topics ───────────────────────────────────────────────
def ensure_topics_exist():
    """Create Kafka topics if they don't already exist."""
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    topics_to_create = [
        NewTopic(KAFKA_TOPIC, num_partitions=4, replication_factor=1),
        NewTopic(KAFKA_DLQ_TOPIC, num_partitions=1, replication_factor=1),
    ]
    futures = admin.create_topics(topics_to_create)
    for topic, future in futures.items():
        try:
            future.result()
            logger.info(f"✅ Topic created: {topic}")
        except Exception as e:
            if "already exists" in str(e).lower() or "topic already exists" in str(e).lower():
                logger.info(f"ℹ️  Topic already exists: {topic}")
            else:
                logger.warning(f"⚠️  Could not create topic {topic}: {e}")


# ─── Kafka Producer ───────────────────────────────────────────────────────────
def build_kafka_producer() -> Producer:
    config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 500,
        "compression.type": "snappy",
        "linger.ms": 10,
        "batch.size": 16384,
    }
    return Producer(config)


def delivery_report(err, msg):
    if err is not None:
        PUBLISH_FAILURES.inc()
        logger.error(f"❌ Delivery failed for {msg.key()}: {err}")
        return
    try:
        source = json.loads(msg.value()).get("source", "unknown")
    except (TypeError, json.JSONDecodeError):
        source = "unknown"
    TICKS_PUBLISHED.labels(source=source).inc()


def publish_tick(producer: Producer, tick: dict):
    """Serialize tick to JSON and publish to Kafka, keyed by symbol."""
    try:
        payload = json.dumps(tick, default=str).encode("utf-8")
        producer.produce(
            topic=KAFKA_TOPIC,
            key=tick["symbol"].encode("utf-8"),
            value=payload,
            callback=delivery_report,
        )
        producer.poll(0)  # trigger callbacks without blocking
        LAST_TICK_TIMESTAMP.set(time.time())
    except Exception as e:
        PUBLISH_FAILURES.inc()
        logger.error(f"Publish error: {e}")


# ─── GBM Mock Price Generator ─────────────────────────────────────────────────
class GBMMockGenerator:
    """
    Geometric Brownian Motion price simulator.
    Produces realistic OHLCV tick data for any list of symbols.
    Activates automatically when no Finnhub API key is configured.
    """

    def __init__(self, symbols: list[str], dt: float = 1 / (252 * 6.5 * 3600)):
        self.symbols = symbols
        self.dt = dt  # time step ≈ 1 second in trading-year units
        self.mu = 0.12  # annual drift  ~12%
        self.sigma = 0.25  # annual vol    ~25%
        # Initialise prices from seed dict or random reasonable range
        self.prices = {s: SEED_PRICES.get(s, round(random.uniform(50, 500), 2)) for s in symbols}
        self.volumes = {s: random.randint(50_000, 500_000) for s in symbols}
        logger.info(f"📊 GBM Mock Generator initialised for: {', '.join(symbols)}")
        for sym, price in self.prices.items():
            logger.info(f"   {sym}: seed price = ${price:.2f}")

    def next_tick(self, symbol: str) -> dict:
        """Generate the next price tick using GBM step."""
        S = self.prices[symbol]
        eps = np.random.standard_normal()
        # GBM discrete step
        S_new = S * math.exp(
            (self.mu - 0.5 * self.sigma**2) * self.dt + self.sigma * math.sqrt(self.dt) * eps
        )
        # Clamp to prevent negative or absurd prices
        S_new = max(S_new, 0.01)
        S_new = round(S_new, 4)
        self.prices[symbol] = S_new

        # Randomise tick volume
        vol = max(1, int(np.random.exponential(scale=300)))

        return {
            "event_id": str(uuid.uuid4()),
            "symbol": symbol,
            "price": S_new,
            "volume": vol,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "exchange": "MOCK",
            "trade_conditions": ["SIMULATED"],
            "source": "simulation_gbm",
        }

    def run(self, producer: Producer, interval_ms: int):
        """Continuously generate and publish ticks for all symbols."""
        logger.info(f"🔄 GBM generator running at {interval_ms}ms intervals")
        interval_s = interval_ms / 1000.0
        while True:
            for symbol in self.symbols:
                tick = self.next_tick(symbol)
                publish_tick(producer, tick)
                logger.debug(f"[GBM] {tick['symbol']} → ${tick['price']:.4f} vol={tick['volume']}")
            time.sleep(interval_s)


# ─── Finnhub WebSocket Producer ───────────────────────────────────────────────
class FinnhubProducer:
    """
    Connects to Finnhub WebSocket API and streams live trade ticks.
    Reconnects to Finnhub if the connection drops.
    """

    def __init__(self, api_key: str, symbols: list[str], producer: Producer):
        self.api_key = api_key
        self.symbols = symbols
        self.producer = producer
        self._ws = None
        self._running = False

    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
            if data.get("type") != "trade":
                return
            for trade in data.get("data", []):
                tick = {
                    "event_id": str(uuid.uuid4()),
                    "symbol": trade.get("s", "UNKNOWN"),
                    "price": float(trade.get("p", 0)),
                    "volume": int(trade.get("v", 0)),
                    "timestamp": datetime.fromtimestamp(
                        trade.get("t", time.time() * 1000) / 1000, tz=timezone.utc
                    ).isoformat(),
                    "exchange": "FINNHUB",
                    "trade_conditions": trade.get("c", []),
                    "source": "finnhub",
                }
                publish_tick(self.producer, tick)
                logger.info(
                    f"[Finnhub] {tick['symbol']} → ${tick['price']:.4f} vol={tick['volume']}"
                )
        except Exception as e:
            logger.error(f"Message parse error: {e}")

    def _on_open(self, ws):
        logger.info("🟢 Finnhub WebSocket connected")
        for symbol in self.symbols:
            ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))
            logger.info(f"   Subscribed to: {symbol}")

    def _on_error(self, ws, error):
        logger.error(f"Finnhub WS error: {error}")

    def _on_close(self, ws, code, msg):
        logger.warning(f"Finnhub WS closed (code={code}). Reconnecting in 10s...")
        self._running = False

    def run(self):
        import websocket  # imported here to avoid hard dep when not in finnhub mode

        self._running = True
        while True:
            url = f"wss://ws.finnhub.io?token={self.api_key}"
            self._ws = websocket.WebSocketApp(
                url,
                on_message=self._on_message,
                on_open=self._on_open,
                on_error=self._on_error,
                on_close=self._on_close,
            )
            self._ws.run_forever(ping_interval=30, ping_timeout=10)
            logger.info("Reconnecting to Finnhub in 10 seconds...")
            time.sleep(10)


# ─── Main Entry Point ─────────────────────────────────────────────────────────
def main():
    if DATA_MODE not in {"simulation", "live"}:
        raise ValueError("DATA_MODE must be either 'simulation' or 'live'.")
    if DATA_MODE == "live" and not FINNHUB_API_KEY:
        raise ValueError("DATA_MODE=live requires FINNHUB_API_KEY.")

    start_http_server(METRICS_PORT)
    logger.info("Prometheus metrics listening on port %s", METRICS_PORT)
    logger.info("=" * 60)
    logger.info("  Real-Time Stock Data Pipeline — Kafka Producer")
    logger.info("=" * 60)
    logger.info(f"  Kafka broker  : {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"  Topic         : {KAFKA_TOPIC}")
    logger.info(f"  Symbols       : {', '.join(STOCK_SYMBOLS)}")
    mode = "Finnhub WebSocket" if DATA_MODE == "live" else "GBM Mock Generator"
    logger.info(f"  Mode          : {mode}")
    logger.info("=" * 60)

    # Wait for Kafka to be ready
    logger.info("⏳ Waiting for Kafka to be ready...")
    while True:
        try:
            admin = AdminClient(
                {
                    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
                    "socket.timeout.ms": 5000,
                }
            )
            meta = admin.list_topics(timeout=5)
            if meta:
                logger.info("✅ Kafka is ready.")
                break
        except Exception:
            logger.info("   Kafka not ready yet, retrying in 5s...")
            time.sleep(5)

    ensure_topics_exist()
    producer = build_kafka_producer()

    if DATA_MODE == "live":
        logger.info("🌐 Starting Finnhub WebSocket producer...")
        fh = FinnhubProducer(FINNHUB_API_KEY, STOCK_SYMBOLS, producer)
        fh.run()
    else:
        logger.warning("Simulation mode enabled — publishing generated GBM ticks.")
        gbm = GBMMockGenerator(STOCK_SYMBOLS)
        gbm.run(producer, TICK_INTERVAL_MS)


if __name__ == "__main__":
    main()
