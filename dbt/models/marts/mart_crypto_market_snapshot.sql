{{ config(
    materialized = 'table',
    engine = 'ReplacingMergeTree(last_updated)',
    order_by = ['coin_id'],
    schema = 'marts'
) }}
SELECT coin_id,
    symbol,
    name,
    current_price,
    price_change_24h,
    price_change_percent_24h,
    (high_price_24h - low_price_24h) AS price_spread_24h,
    CASE
        WHEN current_price > 0 THEN ((high_price_24h - low_price_24h) / current_price) * 100
        ELSE 0
    END AS volatility_percent_24h,
    total_volume,
    quote_volume,
    CASE
        WHEN trade_count > 0 THEN quote_volume / trade_count
        ELSE 0
    END AS avg_trade_size,
    CASE
        WHEN price_change_percent_24h > 0 THEN 'up'
        WHEN price_change_percent_24h < 0 THEN 'down'
        ELSE 'flat'
    END AS trend_24h,
    trade_count,
    last_updated,
    dateDiff('second', last_updated, loaded_at) AS ingestion_lag_seconds
FROM {{ ref('stg_crypto_prices') }}