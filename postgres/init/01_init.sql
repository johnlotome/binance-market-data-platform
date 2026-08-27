CREATE TABLE IF NOT EXISTS crypto_prices (
    symbol VARCHAR(20) PRIMARY KEY,
    coin_id VARCHAR(20) NOT NULL,
    name VARCHAR(20) NOT NULL,
    current_price NUMERIC(18, 8),
    price_change_24h NUMERIC(18, 8),
    price_change_percent_24h NUMERIC(10, 4),
    total_volume NUMERIC(24, 8),
    quote_volume NUMERIC(24, 8),
    high_price_24h NUMERIC(18, 8),
    low_price_24h NUMERIC(18, 8),
    trade_count BIGINT,
    last_updated TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL
);
ALTER TABLE crypto_prices REPLICA IDENTITY FULL;
DROP PUBLICATION IF EXISTS dbz_publication;
CREATE PUBLICATION dbz_publication FOR TABLE crypto_prices;