"""Unit tests for ingestion logic, mocking both the API and the DB so CI needs no live services."""
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from ingestion.ingest_prices import fetch_prices, upsert

SAMPLE_RESPONSE = [
    {
        "symbol": "BTCUSDT",
        "priceChange": 824.0,
        "priceChangePercent": 1.046,
        "lastPrice": 79596.0,
        "highPrice": 80520.0,
        "lowPrice": 77632.58,
        "volume": 15065.57131,
        "quoteVolume": 1187148900.9705715,
        "closeTime": 1787828022753,
        "count": 4383542,
    }
]


@patch("ingestion.ingest_prices.requests.get")
def test_fetch_prices_parses_response(mock_get):
    mock_get.return_value = MagicMock(status_code=200, json=lambda: SAMPLE_RESPONSE)
    mock_get.return_value.raise_for_status = lambda: None
    result = fetch_prices()
    assert result == SAMPLE_RESPONSE


@patch("ingestion.ingest_prices.psycopg2.connect")
def test_upsert_executes_one_statement_per_row(mock_connect):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_conn
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    count = upsert(SAMPLE_RESPONSE)

    assert count == 1
    assert mock_cursor.execute.call_count == 1


    args, _ = mock_cursor.execute.call_args
    sql, params = args

    assert params[0] == "BTCUSDT"
    assert params[1] == "btc"
    assert params[2] == "BTC"
    assert params[3] == 79596.0
    assert params[4] == 824.0
    assert params[5] == 1.046
    assert params[6] == 15065.57131
    assert params[7] == 1187148900.9705715
    assert params[8] == 80520.0
    assert params[9] == 77632.58
    assert params[10] == 4383542
    assert params[11] == datetime.fromtimestamp(1787828022753 / 1000.0, tz=timezone.utc)
    assert isinstance(params[12], datetime)


@pytest.mark.parametrize(
    "raw_symbol, expected_coin_id, expected_name",
    [
        ("BTCUSDT", "btc", "BTC"),
        ("ETHUSDT", "eth", "ETH"),
        ("SOLUSDT", "sol", "SOL"),
        ("BTCUSD", "btcusd", "BTCUSD"),
    ],
)
@patch("ingestion.ingest_prices.psycopg2.connect")
def test_symbol_parsing_logic(mock_connect, raw_symbol, expected_coin_id, expected_name):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_conn
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    mock_row = {
        "symbol": raw_symbol,
        "priceChange": 0.0,
        "priceChangePercent": 0.0,
        "lastPrice": 100.0,
        "highPrice": 105.0,
        "lowPrice": 95.0,
        "volume": 1000.0,
        "quoteVolume": 100000.0,
        "closeTime": 1787828022753,
        "count": 500,
    }

    upsert([mock_row])

    _, params = mock_cursor.execute.call_args
    sql_args = params if isinstance(params, tuple) else mock_cursor.execute.call_args[0][1]

    assert sql_args[0] == raw_symbol
    assert sql_args[1] == expected_coin_id
    assert sql_args[2] == expected_name