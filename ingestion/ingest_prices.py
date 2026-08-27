"""
Pulls current market data for a fixed list of coins from Binance's public
REST API and upserts into Postgres. Upserting (not append-only inserts) is
deliberate: it produces INSERT events for new coins and UPDATE events for
price refreshes, which is what makes the downstream Debezium CDC demo
meaningful rather than trivial.

"""

from dotenv import load_dotenv

load_dotenv()

import json
import logging
import os
from datetime import datetime, timezone

import psycopg2
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BINANCE_URL = "https://api.binance.com/api/v3/ticker/24hr"

# Symbols
SYMBOLS = os.environ.get(
    "SYMBOLS",
    "BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,XRPUSDT,ADAUSDT,DOGEUSDT,TRXUSDT",
).split(",")

PG_DSN = os.environ.get(
    "PG_DSN",
    "host=postgres port=5432 dbname=sourcedb user=app password=app",
)

UPSERT_SQL = """
INSERT INTO crypto_prices (
    symbol, coin_id, name, current_price, price_change_24h,
    price_change_percent_24h, total_volume, quote_volume,
    high_price_24h, low_price_24h, trade_count, last_updated, ingested_at
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (symbol) DO UPDATE SET
    coin_id                  = EXCLUDED.coin_id,
    name                     = EXCLUDED.name,
    current_price            = EXCLUDED.current_price,
    price_change_24h         = EXCLUDED.price_change_24h,
    price_change_percent_24h = EXCLUDED.price_change_percent_24h,
    total_volume             = EXCLUDED.total_volume,
    quote_volume             = EXCLUDED.quote_volume,
    high_price_24h           = EXCLUDED.high_price_24h,
    low_price_24h            = EXCLUDED.low_price_24h,
    trade_count              = EXCLUDED.trade_count,
    last_updated             = EXCLUDED.last_updated,
    ingested_at              = EXCLUDED.ingested_at;
"""


def fetch_prices() -> list[dict]:
    """Fetch 24hr ticker data for defined symbols from Binance API."""
    params = {"symbols": json.dumps(SYMBOLS)}
    resp = requests.get(BINANCE_URL, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


def upsert(rows: list[dict]) -> int:
    """Upsert raw ticker records into PostgreSQL target table."""
    now = datetime.now(timezone.utc)
    conn = psycopg2.connect(PG_DSN)

    try:
        with conn, conn.cursor() as cur:
            for r in rows:
                symbol = r["symbol"]

                # Derive clean base coin metadata (e.g. BTCUSDT -> btc / BTC)
                base_coin = symbol[:-4] if symbol.endswith("USDT") else symbol
                coin_id = base_coin.lower()
                name = base_coin.upper()

                # Convert Binance closeTime (unix ms timestamp) to Python UTC datetime
                last_updated = datetime.fromtimestamp(
                    r.get("closeTime", 0) / 1000.0, tz=timezone.utc
                )

                cur.execute(
                    UPSERT_SQL,
                    (
                        symbol,
                        coin_id,
                        name,
                        float(r.get("lastPrice", 0)),
                        float(r.get("priceChange", 0)),
                        float(r.get("priceChangePercent", 0)),
                        float(r.get("volume", 0)),
                        float(r.get("quoteVolume", 0)),
                        float(r.get("highPrice", 0)),
                        float(r.get("lowPrice", 0)),
                        int(r.get("count", 0)),
                        last_updated,
                        now,
                    ),
                )
        return len(rows)
    finally:
        conn.close()


def run() -> None:
    rows = fetch_prices()
    n = upsert(rows)
    logger.info("Successfully upserted %d coin price records into Postgres", n)


if __name__ == "__main__":
    run()
