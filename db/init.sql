-- ============================================================
-- Real-Time Stock Data Pipeline — PostgreSQL Init Script
-- ============================================================

-- ── 1. Raw Stock Ticks ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_stock_ticks (
    event_id        UUID            DEFAULT gen_random_uuid() NOT NULL,
    symbol          VARCHAR(10)     NOT NULL,
    price           NUMERIC(14, 4)  NOT NULL,
    volume          BIGINT          NOT NULL DEFAULT 0,
    trade_timestamp TIMESTAMPTZ     NOT NULL,
    exchange        VARCHAR(20),
    source          VARCHAR(20)     DEFAULT 'gbm_mock',
    created_at      TIMESTAMPTZ     DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ticks_symbol_time
    ON raw_stock_ticks (symbol, trade_timestamp DESC);

-- ── 2. 1-Minute OHLCV Candles ───────────────────────────────
CREATE TABLE IF NOT EXISTS stock_ohlcv_1m (
    symbol          VARCHAR(10)     NOT NULL,
    bucket_start    TIMESTAMPTZ     NOT NULL,
    open            NUMERIC(14, 4)  NOT NULL,
    high            NUMERIC(14, 4)  NOT NULL,
    low             NUMERIC(14, 4)  NOT NULL,
    close           NUMERIC(14, 4)  NOT NULL,
    volume          BIGINT          NOT NULL DEFAULT 0,
    vwap            NUMERIC(14, 4),
    trade_count     INTEGER         DEFAULT 0,
    updated_at      TIMESTAMPTZ     DEFAULT NOW(),
    PRIMARY KEY (symbol, bucket_start)
);

CREATE INDEX IF NOT EXISTS idx_ohlcv_1m_symbol_time
    ON stock_ohlcv_1m (symbol, bucket_start DESC);

-- ── 3. Technical Indicators ─────────────────────────────────
CREATE TABLE IF NOT EXISTS stock_technical_indicators (
    symbol          VARCHAR(10)     NOT NULL,
    timestamp       TIMESTAMPTZ     NOT NULL,
    sma_20          NUMERIC(14, 4),
    ema_12          NUMERIC(14, 4),
    ema_26          NUMERIC(14, 4),
    rsi_14          NUMERIC(6, 2),
    macd            NUMERIC(14, 4),
    macd_signal     NUMERIC(14, 4),
    bb_upper        NUMERIC(14, 4),
    bb_lower        NUMERIC(14, 4),
    PRIMARY KEY (symbol, timestamp)
);

-- ── 4. Pipeline Health Log ──────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline_health (
    id              SERIAL PRIMARY KEY,
    service         VARCHAR(50)     NOT NULL,
    status          VARCHAR(20)     NOT NULL DEFAULT 'ok',
    message         TEXT,
    recorded_at     TIMESTAMPTZ     DEFAULT NOW()
);

INSERT INTO pipeline_health (service, status, message) VALUES
    ('postgres',  'ok',      'Database initialized'),
    ('kafka',     'pending', 'Waiting for Kafka connection'),
    ('spark',     'pending', 'Waiting for Spark job'),
    ('producer',  'pending', 'Waiting for producer start'),
    ('dashboard', 'pending', 'Waiting for dashboard startup');
