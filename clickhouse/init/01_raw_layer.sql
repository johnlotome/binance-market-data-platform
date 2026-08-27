CREATE DATABASE IF NOT EXISTS raw;
CREATE DATABASE IF NOT EXISTS staging;
CREATE DATABASE IF NOT EXISTS marts;
-- Kafka engine table: a "view" over the Kafka topic, not a stored table.
-- ClickHouse polls this topic as a consumer group.
CREATE TABLE IF NOT EXISTS raw.crypto_prices_kafka (
    symbol String,
    coin_id String,
    name String,
    current_price Nullable(Float64),
    price_change_24h Nullable(Float64),
    price_change_percent_24h Nullable(Float64),
    total_volume Nullable(Float64),
    quote_volume Nullable(Float64),
    high_price_24h Nullable(Float64),
    low_price_24h Nullable(Float64),
    trade_count Nullable(Int64),
    last_updated DateTime64(6),
    ingested_at DateTime64(6),
    __deleted String,
    __op String,
    __source_ts_ms Int64
) ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka:9092',
date_time_input_format = 'best_effort',
kafka_topic_list = 'pg.public.crypto_prices',
kafka_group_name = 'clickhouse_crypto_prices_consumer',
kafka_format = 'JSONEachRow',
kafka_num_consumers = 1,
kafka_skip_broken_messages = 5;
-- Durable target table. ReplacingMergeTree keyed on coin_id, versioned by
-- __source_ts_ms so late/duplicate events resolve to the latest state.
-- Deletes surface as __op = 'd' and are filtered out downstream rather than
-- physically removed, to preserve the change history for auditability.
CREATE TABLE IF NOT EXISTS raw.crypto_prices (
    symbol String,
    coin_id String,
    name String,
    current_price Nullable(Float64),
    price_change_24h Nullable(Float64),
    price_change_percent_24h Nullable(Float64),
    total_volume Nullable(Float64),
    quote_volume Nullable(Float64),
    high_price_24h Nullable(Float64),
    low_price_24h Nullable(Float64),
    trade_count Nullable(Int64),
    last_updated DateTime64(6),
    ingested_at DateTime64(6),
    __deleted String,
    __op String,
    __source_ts_ms Int64
) ENGINE = ReplacingMergeTree(__source_ts_ms)
ORDER BY coin_id;
CREATE MATERIALIZED VIEW IF NOT EXISTS raw.crypto_prices_mv TO raw.crypto_prices AS
SELECT symbol,
    coin_id,
    name,
    current_price,
    price_change_24h,
    price_change_percent_24h,
    total_volume,
    quote_volume,
    high_price_24h,
    low_price_24h,
    trade_count,
    last_updated,
    ingested_at,
    __deleted,
    __op,
    __source_ts_ms
FROM raw.crypto_prices_kafka;