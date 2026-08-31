# 📈 Real-Time Stock Data Pipeline

A production-grade streaming data pipeline for live stock market data built with **Apache Kafka**, **PySpark Structured Streaming**, **PostgreSQL / TimescaleDB**, and a **premium real-time dashboard**.

```
Finnhub WebSocket API / GBM Mock Generator
            ↓
   Python Producer  →  Kafka (stock.raw-ticks.v1)
                              ↓
                    PySpark Structured Streaming
                     (1-min OHLCV + VWAP windows)
                              ↓
                 PostgreSQL + TimescaleDB
                              ↓
              Flask SSE API + TradingView Dashboard
```

---

## 🚀 Quick Start

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (running)
- 8 GB RAM recommended (Kafka + Spark + PostgreSQL)

### 1. Clone & configure
```bash
cd stock-pipeline
cp .env.example .env
# Optional: add your Finnhub API key to .env for live data
# Leave FINNHUB_API_KEY blank to use the built-in GBM mock generator
```

### 2. Launch the pipeline
```bash
docker compose up -d --build
```

Or with `make`:
```bash
make up
```

### 3. Open the dashboard
| Service | URL |
|---|---|
| **📊 Dashboard** | http://localhost:5000 |
| **⚡ Spark UI** | http://localhost:8080 |
| **🐘 PostgreSQL** | localhost:5432 |
| **📨 Kafka** | localhost:29092 |

---

## 🏗️ Project Structure

```
stock-pipeline/
├── docker-compose.yml          # Orchestrates all 7 services
├── .env.example                # Environment variable template
├── Makefile                    # Convenience commands
│
├── producer/                   # Kafka Producer
│   ├── producer.py             # Finnhub WS + GBM fallback
│   ├── requirements.txt
│   └── Dockerfile
│
├── spark_processor/            # PySpark Streaming Job
│   ├── streaming_job.py        # OHLCV aggregation + JDBC write
│   ├── requirements.txt
│   └── Dockerfile
│
├── db/
│   └── init.sql                # Schema: ticks, OHLCV, indicators, health
│
└── dashboard/                  # Flask Web Dashboard
    ├── app.py                  # Flask + SSE + REST API
    ├── requirements.txt
    ├── Dockerfile
    ├── templates/
    │   └── dashboard.html      # Premium dark terminal UI
    └── static/
        ├── css/style.css       # Design system
        └── js/main.js          # TradingView charts + SSE client
```

---

## 🔧 Architecture Deep Dive

### 1. Producer (`producer/`)
- **Primary mode**: Connects to [Finnhub WebSocket API](https://finnhub.io) — set `FINNHUB_API_KEY` in `.env`
- **Fallback mode**: GBM (Geometric Brownian Motion) mock generator — auto-activates when no API key is set
- Publishes JSON tick messages to Kafka topic `stock.raw-ticks.v1`
- Partition key = `symbol` (guarantees ordering per stock)

**Tick message format:**
```json
{
  "event_id": "uuid",
  "symbol":   "AAPL",
  "price":    185.42,
  "volume":   350,
  "timestamp": "2026-07-25T11:22:49.123Z",
  "exchange":  "MOCK",
  "source":    "gbm_mock"
}
```

### 2. Kafka Topics
| Topic | Partitions | Purpose |
|---|---|---|
| `stock.raw-ticks.v1` | 4 | Raw trade ticks (keyed by symbol) |
| `stock.dlq.v1` | 1 | Dead-letter queue for malformed records |

### 3. Spark Streaming Job (`spark_processor/`)
- Reads from `stock.raw-ticks.v1`
- Enforces schema + 30-second watermark for late data
- Routes invalid records to DLQ
- Computes **1-minute OHLCV candles** + **VWAP** using windowed aggregation
- Upserts to PostgreSQL via idempotent staging table pattern
- Triggers every 15 seconds

### 4. Database (`db/`)
| Table | Type | Description |
|---|---|---|
| `raw_stock_ticks` | Hypertable | Individual trade ticks (audit log) |
| `stock_ohlcv_1m` | Hypertable | 1-minute OHLCV candles (primary) |
| `stock_technical_indicators` | Regular | SMA, EMA, RSI, MACD values |
| `pipeline_health` | Regular | Service status heartbeats |

### 5. Dashboard (`dashboard/`)
- **Flask** backend with `ThreadedConnectionPool` for PostgreSQL
- **Server-Sent Events (SSE)** at `/stream/<symbol>` — pushes latest candle every second
- **REST API**: `/api/tickers`, `/api/candles/<symbol>`, `/api/stats/<symbol>`, `/api/health`
- **Frontend**: TradingView Lightweight Charts for hardware-accelerated candlestick rendering
- Real-time RSI, SMA-10, SMA-20 computed on the fly

---

## 🛠️ Useful Commands

```bash
# View logs
make logs                      # All services
make log SERVICE=producer      # Specific service

# Inspect data
make db-check                  # Show latest OHLCV rows
make db-stats                  # Row counts per table
make kafka-topics              # List Kafka topics

# Stop / reset
make down                      # Stop containers (preserves data)
make reset                     # Wipe all data (volumes deleted)
```

---

## 📊 Dashboard Features

| Feature | Description |
|---|---|
| **Candlestick Chart** | 1-minute OHLCV with TradingView Lightweight Charts |
| **Volume Chart** | Synchronized volume histogram (green/red colored) |
| **Live Ticker Panel** | Watchlist with real-time price + % change + flash animations |
| **OHLCV Header** | Open / High / Low / Close / Volume for current bar |
| **Technical Indicators** | VWAP, SMA-10, SMA-20, RSI-14 |
| **RSI Gauge** | Visual needle showing overbought / oversold zones |
| **Pipeline Health** | Live status for all services |
| **Market Clock** | UTC clock synced every second |

---

## 🔑 Environment Variables

| Variable | Default | Description |
|---|---|---|
| `FINNHUB_API_KEY` | _(empty)_ | Finnhub API key (leave blank for mock mode) |
| `KAFKA_TOPIC` | `stock.raw-ticks.v1` | Primary Kafka topic name |
| `POSTGRES_DB` | `stock_db` | PostgreSQL database name |
| `POSTGRES_USER` | `postgres` | PostgreSQL user |
| `POSTGRES_PASSWORD` | `postgres` | PostgreSQL password |
| `STOCK_SYMBOLS` | `AAPL,GOOGL,MSFT,AMZN,TSLA` | Comma-separated watchlist |
| `TICK_INTERVAL_MS` | `500` | Mock generator tick frequency (ms) |

---

## 🩺 Troubleshooting

**Dashboard shows no data?**
> Spark Structured Streaming aggregates on 1-minute windows. Wait 60-90 seconds after startup for the first candle to appear.

**Kafka container unhealthy?**
> Kafka takes 20-30 seconds to initialize. Producer and Spark job retry automatically.

**Spark job fails to connect?**
> Check `docker compose logs spark-job`. The JDBC JARs are pre-downloaded in the Dockerfile. If the build fails, ensure Docker has internet access.

**Out of memory errors?**
> Increase Docker Desktop memory to 8 GB+ in Settings → Resources.
