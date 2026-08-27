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
