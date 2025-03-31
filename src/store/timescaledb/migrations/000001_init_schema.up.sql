CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE contracts (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    sec_type TEXT NOT NULL,
    exchange TEXT NOT NULL,
    currency TEXT NOT NULL,
    multiplier NUMERIC NOT NULL,
    expiration NUMERIC,
    option_right TEXT,
    strike NUMERIC,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (symbol, sec_type, exchange, currency, multiplier, expiration, option_right, strike)
);

CREATE INDEX idx_contracts_symbol ON contracts(symbol);
CREATE INDEX idx_contracts_expiration ON contracts(expiration);
CREATE INDEX idx_contracts_sec_type ON contracts(sec_type);

CREATE TABLE historical_data (
    time TIMESTAMPTZ NOT NULL,
    contract_id INTEGER NOT NULL REFERENCES contracts(id) ON DELETE CASCADE,
    open NUMERIC NOT NULL,
    high NUMERIC NOT NULL,
    low NUMERIC NOT NULL,
    close NUMERIC NOT NULL,
    volume NUMERIC NOT NULL,
    spread NUMERIC,
    PRIMARY KEY (time, contract_id)
);

SELECT create_hypertable('historical_data', 'time');

CREATE TABLE trades (
    time TIMESTAMPTZ NOT NULL,
    contract_id INTEGER NOT NULL REFERENCES contracts(id) ON DELETE CASCADE,
    price NUMERIC NOT NULL,
    quantity NUMERIC NOT NULL,
    side TEXT CHECK (side IS NULL OR side IN ('B', 'A')),
    type TEXT CHECK (type IS NULL OR type IN ('MARKET', 'LIMIT', 'STOP', 'STOP_LIMIT', 'TRAIL')),
    trade_id BIGINT,
    PRIMARY KEY (time, contract_id)
);

SELECT create_hypertable('trades', 'time');

CREATE TABLE orderbook_snapshots (
    time TIMESTAMPTZ NOT NULL,
    contract_id INTEGER NOT NULL REFERENCES contracts(id) ON DELETE CASCADE,
    side TEXT NOT NULL CHECK (side IN ('bid', 'ask')),
    price NUMERIC NOT NULL,
    quantity NUMERIC NOT NULL,
    orders INTEGER,
    level INTEGER NOT NULL,
    PRIMARY KEY (time, contract_id, side, level)
);

SELECT create_hypertable('orderbook_snapshots', 'time');

CREATE TABLE news_categories (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    parent_id INTEGER REFERENCES news_categories(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO news_categories (name) VALUES 
    ('Economic'), 
    ('Corporate'), 
    ('Political'),
    ('Market');

INSERT INTO news_categories (name, parent_id) VALUES 
    ('Earnings', (SELECT id FROM news_categories WHERE name = 'Corporate')),
    ('Mergers', (SELECT id FROM news_categories WHERE name = 'Corporate')),
    ('FederalReserve', (SELECT id FROM news_categories WHERE name = 'Economic')),
    ('Legislation', (SELECT id FROM news_categories WHERE name = 'Political')),
    ('EconomicIndicators', (SELECT id FROM news_categories WHERE name = 'Economic'));

CREATE TABLE news_items (
    time TIMESTAMPTZ NOT NULL,
    news_id BIGINT NOT NULL,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    source TEXT NOT NULL,
    url TEXT,
    category_id INTEGER NOT NULL REFERENCES news_categories(id),
    content_hash BIGINT,
    PRIMARY KEY (time, news_id)
);

CREATE INDEX idx_news_content_hash ON news_items(content_hash, category_id);
SELECT create_hypertable('news_items', 'time');

CREATE TABLE news_contract_relations (
    id BIGSERIAL PRIMARY KEY,
    time TIMESTAMPTZ NOT NULL,
    news_id BIGINT NOT NULL,
    contract_id INTEGER NOT NULL REFERENCES contracts(id) ON DELETE CASCADE,
    relevance_score NUMERIC,
    FOREIGN KEY (time, news_id) REFERENCES news_items(time, news_id) ON DELETE CASCADE
);

CREATE INDEX idx_news_contract_relations ON news_contract_relations(contract_id, news_id);

ALTER TABLE historical_data SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'contract_id'
);

ALTER TABLE trades SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'contract_id'
);

ALTER TABLE orderbook_snapshots SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'contract_id'
);

ALTER TABLE news_items SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'category_id'
);

SELECT add_compression_policy('historical_data', INTERVAL '7 days');
SELECT add_compression_policy('trades', INTERVAL '7 days');
SELECT add_compression_policy('orderbook_snapshots', INTERVAL '3 days');
SELECT add_compression_policy('news_items', INTERVAL '30 days');

SELECT add_retention_policy('trades', INTERVAL '90 days');

CREATE VIEW vw_daily_ohlcv AS
SELECT
    contract_id,
    time_bucket('1 day', time) AS day,
    first(open, time) AS open,
    max(high) AS high,
    min(low) AS low,
    last(close, time) AS close,
    sum(volume) AS volume
FROM historical_data
GROUP BY contract_id, day
ORDER BY contract_id, day;