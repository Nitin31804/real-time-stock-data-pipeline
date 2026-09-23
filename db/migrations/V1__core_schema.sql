CREATE TABLE raw_stock_ticks (
    event_id        UUID            PRIMARY KEY,
    symbol          VARCHAR(10)     NOT NULL,
    price           NUMERIC(14, 4)  NOT NULL CHECK (price > 0),
    volume          BIGINT          NOT NULL DEFAULT 0 CHECK (volume >= 0),
    trade_timestamp TIMESTAMPTZ     NOT NULL,
    exchange        VARCHAR(20),
    source          VARCHAR(20)     NOT NULL DEFAULT 'simulation_gbm',
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_ticks_symbol_time
    ON raw_stock_ticks (symbol, trade_timestamp DESC);

CREATE TABLE stock_ohlcv_1m (
    symbol          VARCHAR(10)     NOT NULL,
    bucket_start    TIMESTAMPTZ     NOT NULL,
    open            NUMERIC(14, 4)  NOT NULL,
    high            NUMERIC(14, 4)  NOT NULL,
    low             NUMERIC(14, 4)  NOT NULL,
    close           NUMERIC(14, 4)  NOT NULL,
    volume          BIGINT          NOT NULL DEFAULT 0 CHECK (volume >= 0),
    vwap            NUMERIC(14, 4),
    trade_count     INTEGER         NOT NULL DEFAULT 0 CHECK (trade_count >= 0),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (symbol, bucket_start),
    CHECK (high >= low),
    CHECK (open BETWEEN low AND high),
    CHECK (close BETWEEN low AND high)
);

CREATE INDEX idx_ohlcv_1m_symbol_time
    ON stock_ohlcv_1m (symbol, bucket_start DESC);

CREATE TABLE pipeline_health (
    service         VARCHAR(50)     PRIMARY KEY,
    status          VARCHAR(20)     NOT NULL DEFAULT 'pending',
    message         TEXT,
    recorded_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

INSERT INTO pipeline_health (service, status, message) VALUES
    ('postgres', 'ok', 'Database initialized'),
    ('kafka', 'pending', 'Waiting for Kafka connection'),
    ('spark', 'pending', 'Waiting for Spark job'),
    ('producer', 'pending', 'Waiting for producer start'),
    ('dashboard', 'pending', 'Waiting for dashboard startup');
