SELECT count(*) AS symbol_count
FROM {{ ref('mart_crypto_market_snapshot') }}
HAVING count(*) != 8