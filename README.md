# binance-market-data-platform


curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @debezium/pg-connector.json

CONNECT_URL=http://localhost:8083 ./register-connector.sh


curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium/pg-connector.json


curl -s http://localhost:8083/connectors/pg-crypto-prices-connector/status | python3 -m json.tool


docker exec -it postgres psql -U app -d sourcedb -c "SELECT slot_name, active, plugin FROM pg_replication_slots;"


docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list

❯ docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic pg.public.crypto_prices --from-beginning --max-messages 20
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic pg.public.crypto_prices --from-beginning --max-messages 3
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic pg.public.crypto_prices --max-messages 8
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic pg.public.crypto_prices


SELECT pg_drop_replication_slot('dbz_slot_crypto_prices');

curl -X DELETE http://localhost:8083/connectors/pg-crypto-prices-connector
curl -s http://localhost:8083/connectors
curl -s http://localhost:8083/connectors/pg-crypto-prices-connector/status | python3 -m json.tool

curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @debezium/pg-connector.json

# use for updating configs -
curl -X PUT http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium/pg-connector.json


  docker exec -it postgres psql -U app -d sourcedb -c "DELETE FROM crypto_prices WHERE symbol = 'TRXUSDT';"





docker compose stop clickhouse
docker compose rm -f clickhouse
docker volume rm binance-market-data-platform_ch_data
docker compose up -d clickhouse



docker logs clickhouse --tail 100

docker exec -it clickhouse clickhouse-client
docker exec -it clickhouse clickhouse-client --user default --password ""

SHOW DATABASES;


SELECT count() FROM raw.crypto_prices;
SELECT symbol, current_price, op, __source_ts_ms FROM raw.crypto_prices ORDER BY __source_ts_ms DESC LIMIT 8;



SSELECT database, table, consumer_id, assignments.topic, assignments.partition_id,
       assignments.current_offset, exceptions.text, num_messages_read, last_poll_time
FROM system.kafka_consumers FORMAT Vertical;


SHOW TABLES FROM raw;



docker exec -it clickhouse tail -100 /var/log/clickhouse-server/clickhouse-server.err.log


docker exec -it clickhouse tail -f /var/log/clickhouse-server/clickhouse-server.log | grep -i "crypto_prices"



cd dbt
dbt run --profiles-dir .

CLICKHOUSE_HOST=localhost dbt run --profiles-dir .


SELECT * FROM staging_marts.mart_crypto_market_snapshot;



CLICKHOUSE_HOST=localhost CLICKHOUSE_PASSWORD=pass dbt test --profiles-dir .


