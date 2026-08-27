{{ config(
    materialized = 'view',
    schema = 'staging'
    )
}} WITH source AS (
    -- ClickHouse ReplacingMergeTree requires FINAL or argMax to guarantee deduplication on reads
    SELECT UPPER(symbol) AS symbol,
        LOWER(coin_id) AS coin_id,
        name,
        CAST(COALESCE(current_price, 0) AS Decimal128(8)) AS current_price,
        CAST(COALESCE(price_change_24h, 0) AS Decimal128(8)) AS price_change_24h,
        CAST(
            COALESCE(price_change_percent_24h, 0) AS Decimal64(4)
        ) AS price_change_percent_24h,
        CAST(COALESCE(total_volume, 0) AS Decimal256(8)) AS total_volume,
        CAST(COALESCE(quote_volume, 0) AS Decimal256(8)) AS quote_volume,
        CAST(COALESCE(high_price_24h, 0) AS Decimal128(8)) AS high_price_24h,
        CAST(COALESCE(low_price_24h, 0) AS Decimal128(8)) AS low_price_24h,
        COALESCE(trade_count, 0) AS trade_count,
        last_updated,
        ingested_at AS loaded_at,
        __op,
        __source_ts_ms,
        ROW_NUMBER() OVER (
            PARTITION BY coin_id
            ORDER BY __source_ts_ms DESC
        ) AS rn
    FROM {{ source('raw', 'crypto_prices') }}
)
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
    loaded_at
FROM source
WHERE rn = 1
    AND __op != 'd'