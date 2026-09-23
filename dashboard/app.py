"""Flask API for the real-time stock data pipeline dashboard."""

import json
import logging
import math
import os
import socket
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import date, datetime, timezone
from decimal import Decimal

import requests
import yfinance as yf
from cachetools import TTLCache, cached
from flask import Flask, Response, g, jsonify, render_template, request
from flask_cors import CORS
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool


class JsonFormatter(logging.Formatter):
    """Emit one machine-readable JSON object per log line."""

    def format(self, record):
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        request_id = getattr(record, "request_id", None)
        if request_id:
            payload["request_id"] = request_id
        return json.dumps(payload, default=str)


handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), handlers=[handler], force=True)
logger = logging.getLogger("stock-dashboard")

app = Flask(__name__)
CORS(app)

DATA_MODE = os.getenv("DATA_MODE", "simulation").strip().lower()
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://prometheus:9090").rstrip("/")
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "database": os.getenv("POSTGRES_DB", "stock_db"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
}

HTTP_REQUESTS = Counter(
    "stock_dashboard_http_requests_total",
    "Dashboard HTTP requests",
    ["method", "route", "status"],
)
HTTP_DURATION = Histogram(
    "stock_dashboard_http_request_duration_seconds",
    "Dashboard HTTP request duration",
    ["method", "route"],
)
PIPELINE_EVENTS_PER_SECOND = Gauge(
    "stock_pipeline_events_per_second", "Ticks persisted per second over the last minute"
)
LATEST_EVENT_AGE = Gauge(
    "stock_pipeline_latest_event_age_seconds", "Age of the newest persisted tick"
)
LATEST_CANDLE_AGE = Gauge(
    "stock_pipeline_latest_candle_age_seconds", "Age of the newest finalized candle"
)

_pool = None
_pool_lock = threading.Lock()
_info_cache = TTLCache(maxsize=100, ttl=900)
_history_cache = TTLCache(maxsize=500, ttl=300)
_financials_cache = TTLCache(maxsize=100, ttl=3600)


def get_pool():
    """Create the database pool only when the first database request arrives."""
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                _pool = ThreadedConnectionPool(1, 10, **DB_CONFIG)
    return _pool


@contextmanager
def db_cursor():
    pool = get_pool()
    connection = pool.getconn()
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            yield cursor
    finally:
        pool.putconn(connection)


def query_all(sql, params=None):
    with db_cursor() as cursor:
        cursor.execute(sql, params)
        return cursor.fetchall()


def query_one(sql, params=None):
    with db_cursor() as cursor:
        cursor.execute(sql, params)
        return cursor.fetchone()


def serialize_value(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def serialize_row(row):
    return {key: serialize_value(value) for key, value in row.items()}


def provider_unavailable(dataset, error):
    logger.warning("%s unavailable: %s", dataset, error)
    return (
        jsonify(
            {
                "error": f"{dataset} are unavailable from the configured provider.",
                "data_source": "unavailable",
            }
        ),
        503,
    )


def finite_float(value):
    try:
        converted = float(value)
    except (TypeError, ValueError):
        return None
    return converted if math.isfinite(converted) else None


def get_yf_ticker(symbol):
    return yf.Ticker(symbol)


@cached(cache=_info_cache)
def get_cached_info(symbol):
    info = get_yf_ticker(symbol).info
    if not isinstance(info, dict) or not info:
        raise ValueError(f"Yahoo Finance returned no company profile for {symbol}")
    return info


@cached(cache=_history_cache)
def get_cached_history(symbol, period, interval):
    frame = get_yf_ticker(symbol).history(
        period=period,
        interval=interval,
        auto_adjust=False,
    )
    if frame is None or frame.empty:
        raise ValueError(f"Yahoo Finance returned no history for {symbol}")
    return frame


@cached(cache=_financials_cache)
def get_cached_financials(symbol):
    frame = get_yf_ticker(symbol).financials
    if frame is None or frame.empty:
        raise ValueError(f"Yahoo Finance returned no financial statements for {symbol}")
    return frame


@app.before_request
def begin_request():
    g.request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
    g.request_started = time.perf_counter()


@app.after_request
def finish_request(response):
    route = request.url_rule.rule if request.url_rule else "unmatched"
    HTTP_REQUESTS.labels(request.method, route, str(response.status_code)).inc()
    HTTP_DURATION.labels(request.method, route).observe(time.perf_counter() - g.request_started)
    response.headers["X-Request-ID"] = g.request_id
    response.headers["X-Data-Mode"] = DATA_MODE
    return response


@app.route("/")
def index():
    return render_template(
        "dashboard.html",
        data_mode=DATA_MODE,
        simulation_mode=DATA_MODE == "simulation",
    )


@app.route("/api/log", methods=["POST"])
def api_log():
    payload = request.get_json(silent=True) or {}
    message = str(payload.get("message", ""))[:500]
    logger.info("browser: %s", message, extra={"request_id": g.request_id})
    return jsonify({"accepted": True})


@app.route("/api/search/<query>")
def api_search(query):
    try:
        response = requests.get(
            "https://query2.finance.yahoo.com/v1/finance/search",
            params={"q": query, "quotesCount": 8, "newsCount": 0},
            headers={"User-Agent": "stock-pipeline-dashboard/1.0"},
            timeout=5,
        )
        response.raise_for_status()
        quotes = response.json().get("quotes", [])
        return jsonify(
            [
                {
                    "symbol": item.get("symbol"),
                    "name": item.get("shortname") or item.get("longname") or item.get("symbol"),
                    "exchange": item.get("exchange"),
                }
                for item in quotes
                if item.get("symbol") and item.get("quoteType") in {"EQUITY", "ETF"}
            ]
        )
    except Exception as error:
        return provider_unavailable("Search results", error)


@app.route("/api/news/<symbol>")
def api_news(symbol):
    try:
        items = get_yf_ticker(symbol.replace(".", "-")).news or []
        results = []
        for item in items[:8]:
            content = item.get("content", item)
            canonical = content.get("canonicalUrl") or {}
            thumbnail = content.get("thumbnail") or {}
            results.append(
                {
                    "title": content.get("title", "Untitled"),
                    "publisher": (content.get("provider") or {}).get(
                        "displayName", "Yahoo Finance"
                    ),
                    "link": canonical.get("url") or content.get("link"),
                    "thumbnail": thumbnail.get("originalUrl") or content.get("thumbnail", ""),
                    "time": content.get("pubDate") or content.get("providerPublishTime"),
                    "data_source": "yahoo_finance",
                }
            )
        return jsonify(results)
    except Exception as error:
        return provider_unavailable("News", error)


@app.route("/api/tickers")
def api_tickers():
    rows = query_all(
        """
        WITH latest_tick AS (
            SELECT DISTINCT ON (symbol)
                   symbol, price, volume, trade_timestamp, exchange, source
            FROM raw_stock_ticks
            ORDER BY symbol, trade_timestamp DESC
        ), latest_candle AS (
            SELECT DISTINCT ON (symbol) symbol, open
            FROM stock_ohlcv_1m
            ORDER BY symbol, bucket_start DESC
        )
        SELECT tick.symbol,
               tick.price AS close,
               tick.volume,
               tick.trade_timestamp,
               tick.exchange,
               tick.source,
               CASE WHEN candle.open > 0
                    THEN ((tick.price - candle.open) / candle.open) * 100
                    ELSE 0 END AS change_pct
        FROM latest_tick tick
        LEFT JOIN latest_candle candle USING (symbol)
        ORDER BY tick.symbol;
        """
    )
    return jsonify([serialize_row(row) for row in rows])


@app.route("/api/candles/<symbol>")
def api_candles(symbol):
    limit = min(max(request.args.get("limit", 120, type=int), 1), 2000)
    rows = query_all(
        """
        SELECT bucket_start, open, high, low, close, volume, vwap, trade_count
        FROM (
            SELECT bucket_start, open, high, low, close, volume, vwap, trade_count
            FROM stock_ohlcv_1m
            WHERE symbol = %s
            ORDER BY bucket_start DESC
            LIMIT %s
        ) recent
        ORDER BY bucket_start ASC;
        """,
        (symbol.upper(), limit),
    )
    candles = [serialize_row(row) for row in rows]
    return jsonify(
        {
            "symbol": symbol.upper(),
            "candles": candles,
            "count": len(candles),
            "data_source": "pipeline",
        }
    )


@app.route("/api/history/<symbol>")
def api_history(symbol):
    period = request.args.get("period", "1mo")
    interval = request.args.get("interval", "1d")
    try:
        frame = get_cached_history(symbol.replace(".", "-"), period, interval)
        candles = [
            {
                "bucket_start": index.isoformat(),
                "open": finite_float(row.get("Open")),
                "high": finite_float(row.get("High")),
                "low": finite_float(row.get("Low")),
                "close": finite_float(row.get("Close")),
                "volume": finite_float(row.get("Volume")),
            }
            for index, row in frame.iterrows()
        ]
        return jsonify(
            {
                "symbol": symbol.upper(),
                "candles": candles,
                "count": len(candles),
                "data_source": "yahoo_finance",
            }
        )
    except Exception as error:
        return provider_unavailable("Historical prices", error)


@app.route("/api/stats/<symbol>")
def api_stats(symbol):
    row = query_one(
        """
        SELECT vwap, volume, high, low, trade_count, bucket_start
        FROM stock_ohlcv_1m
        WHERE symbol = %s
        ORDER BY bucket_start DESC
        LIMIT 1;
        """,
        (symbol.upper(),),
    )
    if not row:
        return jsonify({"error": "No finalized candle found", "data_source": "pipeline"}), 404
    return jsonify({**serialize_row(row), "data_source": "pipeline"})


@app.route("/api/summary_fundamentals/<symbol>")
def api_summary_fundamentals(symbol):
    try:
        info = get_cached_info(symbol.replace(".", "-"))
        recommendation = (
            str(info.get("recommendationKey") or "unavailable").replace("_", " ").title()
        )
        yearly_change = info.get("52WeekChange")
        yearly_performance = (
            f"{float(yearly_change) * 100:.2f}%" if yearly_change is not None else None
        )
        return jsonify(
            {
                "symbol": symbol.upper(),
                "data_source": "yahoo_finance",
                "marketCap": info.get("marketCap"),
                "trailingPE": info.get("trailingPE"),
                "forwardPE": info.get("forwardPE"),
                "fiftyTwoWeekHigh": info.get("fiftyTwoWeekHigh"),
                "fiftyTwoWeekLow": info.get("fiftyTwoWeekLow"),
                "dividendYield": info.get("dividendYield"),
                "recommendation": recommendation,
                "targetMeanPrice": info.get("targetMeanPrice"),
                "analystCount": info.get("numberOfAnalystOpinions"),
                "totalRevenue": info.get("totalRevenue"),
                "netIncomeToCommon": info.get("netIncomeToCommon"),
                "trailingEps": info.get("trailingEps"),
                "beta": info.get("beta"),
                "fiftyDayAverage": info.get("fiftyDayAverage"),
                "fiftyTwoWeekChange": info.get("52WeekChange"),
                "averageVolume": info.get("averageVolume"),
                "performance": [None, None, None, yearly_performance, None],
            }
        )
    except Exception as error:
        return provider_unavailable("Fundamentals", error)


@app.route("/api/financials/<symbol>")
def api_financials(symbol):
    try:
        frame = get_cached_financials(symbol.replace(".", "-"))
        keys = [
            "Total Revenue",
            "Cost Of Revenue",
            "Gross Profit",
            "Operating Expense",
            "Operating Income",
            "Pretax Income",
            "Tax Provision",
            "Net Income",
            "Diluted EPS",
            "Diluted Average Shares",
            "Normalized EBITDA",
        ]
        result = {}
        for column in frame.columns:
            year = str(column)[:4]
            result[year] = {
                key: finite_float(frame.loc[key, column]) if key in frame.index else None
                for key in keys
            }
        return jsonify({"data_source": "yahoo_finance", "periods": result})
    except Exception as error:
        return provider_unavailable("Financial statements", error)


@app.route("/api/company_info/<symbol>")
def api_company_info(symbol):
    try:
        ticker = get_yf_ticker(symbol.replace(".", "-"))
        info = get_cached_info(symbol.replace(".", "-"))
        holders_frame = ticker.institutional_holders
        holders = []
        if holders_frame is not None and not holders_frame.empty:
            for _, row in holders_frame.head(5).iterrows():
                holders.append(
                    {
                        "holder": row.get("Holder", ""),
                        "shares": finite_float(row.get("Shares")),
                        "pct": finite_float(row.get("pctHeld")),
                    }
                )
        return jsonify(
            {
                "name": info.get("shortName") or symbol.upper(),
                "data_source": "yahoo_finance",
                "sector": info.get("sector"),
                "industry": info.get("industry"),
                "description": info.get("longBusinessSummary"),
                "holders": holders,
            }
        )
    except Exception as error:
        return provider_unavailable("Company information", error)


@app.route("/api/consumer-lag")
def api_consumer_lag():
    """Return aggregate Kafka consumer lag from the local Prometheus service."""
    try:
        response = requests.get(
            f"{PROMETHEUS_URL}/api/v1/query",
            params={"query": "sum(kafka_consumergroup_group_topic_sum_lag)"},
            timeout=3,
        )
        response.raise_for_status()
        results = response.json().get("data", {}).get("result", [])
        lag = float(results[0]["value"][1]) if results else None
        return jsonify({"consumer_lag": lag, "data_source": "prometheus"})
    except Exception as error:
        logger.warning("Kafka consumer lag unavailable: %s", error)
        return jsonify({"consumer_lag": None, "data_source": "unavailable"})


@app.route("/api/health")
def api_health():
    services = [{"service": "dashboard", "status": "ok", "message": "Running"}]

    try:
        query_one("SELECT 1 AS alive;")
        services.append({"service": "postgres", "status": "ok", "message": "Connected"})
    except Exception as error:
        services.append({"service": "postgres", "status": "error", "message": str(error)})

    kafka_host = os.getenv("KAFKA_HOST", "kafka")
    kafka_port = int(os.getenv("KAFKA_PORT", "9092"))
    try:
        with socket.create_connection((kafka_host, kafka_port), timeout=2):
            services.append(
                {"service": "kafka", "status": "ok", "message": f"{kafka_host}:{kafka_port}"}
            )
    except OSError:
        services.append({"service": "kafka", "status": "pending", "message": "Unreachable"})

    try:
        row = query_one("SELECT MAX(updated_at) AS last_write FROM stock_ohlcv_1m;")
        last = row.get("last_write") if row else None
        age = (datetime.now(timezone.utc) - last).total_seconds() if last else None
        status = "ok" if age is not None and age < 180 else "pending"
        message = f"last candle {age:.1f}s ago" if age is not None else "No candles"
        services.append({"service": "spark", "status": status, "message": message})
    except Exception as error:
        services.append({"service": "spark", "status": "error", "message": str(error)})

    try:
        row = query_one("SELECT MAX(trade_timestamp) AS last_update FROM raw_stock_ticks;")
        last = row.get("last_update") if row else None
        age = (datetime.now(timezone.utc) - last).total_seconds() if last else None
        status = "ok" if age is not None and age < 90 else "pending"
        message = f"last tick {age:.1f}s ago" if age is not None else "No ticks"
        services.append({"service": "producer", "status": status, "message": message})
    except Exception as error:
        services.append({"service": "producer", "status": "error", "message": str(error)})

    overall = "ok" if all(item["status"] == "ok" for item in services) else "degraded"
    return jsonify({"status": overall, "data_mode": DATA_MODE, "services": services})


@app.route("/metrics")
def metrics():
    try:
        tick_stats = query_one(
            """
            SELECT COUNT(*) FILTER (WHERE trade_timestamp >= NOW() - INTERVAL '60 seconds') AS recent_events,
                   EXTRACT(EPOCH FROM (NOW() - MAX(trade_timestamp))) AS latest_event_age
            FROM raw_stock_ticks;
            """
        )
        candle_stats = query_one(
            """
            SELECT EXTRACT(EPOCH FROM (NOW() - MAX(bucket_end))) AS latest_candle_age
            FROM stock_ohlcv_1m;
            """
        )
        PIPELINE_EVENTS_PER_SECOND.set(float(tick_stats.get("recent_events") or 0) / 60)
        LATEST_EVENT_AGE.set(float(tick_stats.get("latest_event_age") or 0))
        LATEST_CANDLE_AGE.set(float(candle_stats.get("latest_candle_age") or 0))
    except Exception as error:
        logger.warning("Could not refresh database-backed metrics: %s", error)
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)


@app.route("/stream/<symbol>")
def stream(symbol):
    normalized_symbol = symbol.upper()

    def events():
        last_bucket = None
        while True:
            try:
                row = query_one(
                    """
                    SELECT bucket_start, open, high, low, close, volume, vwap, trade_count
                    FROM stock_ohlcv_1m
                    WHERE symbol = %s
                    ORDER BY bucket_start DESC
                    LIMIT 1;
                    """,
                    (normalized_symbol,),
                )
                if row and row["bucket_start"] != last_bucket:
                    last_bucket = row["bucket_start"]
                    payload = {"symbol": normalized_symbol, **serialize_row(row)}
                    yield f"data: {json.dumps(payload)}\n\n"
                else:
                    yield ": heartbeat\n\n"
            except Exception as error:
                yield f"data: {json.dumps({'error': str(error)})}\n\n"
            time.sleep(2)

    return Response(events(), mimetype="text/event-stream")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)
