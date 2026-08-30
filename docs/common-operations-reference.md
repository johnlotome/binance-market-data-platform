## Common Operations Reference

### Debezium / Kafka Connect

**Register the connector:**
```bash
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @debezium/pg-connector.json
```

**Register via the init script (used automatically by `connect-init` on `docker compose up`):**
```bash
CONNECT_URL=http://localhost:8083 ./register-connector.sh
```

**Check connector status:**
```bash
curl -s http://localhost:8083/connectors/pg-crypto-prices-connector/status | python3 -m json.tool
```

**List all registered connectors:**
```bash
curl -s http://localhost:8083/connectors
```

**Delete and re-register (used when updating connector config):**
```bash
curl -X DELETE http://localhost:8083/connectors/pg-crypto-prices-connector
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @debezium/pg-connector.json
```

### Postgres

**Check replication slot status (confirms Debezium is attached to the WAL):**
```bash
docker exec -it postgres psql -U app -d sourcedb -c "SELECT slot_name, active, plugin FROM pg_replication_slots;"
```

**Manually drop a replication slot (only needed after fully deleting a connector and wanting a clean re-snapshot):**
```sql
SELECT pg_drop_replication_slot('dbz_slot_crypto_prices');
```

**Trigger a delete event for testing CDC delete handling:**
```bash
docker exec -it postgres psql -U app -d sourcedb -c "DELETE FROM crypto_prices WHERE symbol = 'TRXUSDT';"
```

**Check what's holding port 5432 if the container fails to bind:**
```bash
docker ps | grep postgres
sudo lsof -i :5432
```

### Kafka

**List topics:**
```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list
```

**Consume messages from the CDC topic (from the beginning, capped):**
```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic pg.public.crypto_prices --from-beginning --max-messages 20
```

**Consume only new messages going forward (no `--from-beginning`):**
```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic pg.public.crypto_prices --max-messages 8
```

**Stream continuously:**
```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic pg.public.crypto_prices
```

### ClickHouse

**Recreate the ClickHouse container cleanly (needed after any config/env change that only applies on first boot):**
```bash
docker compose stop clickhouse
docker compose rm -f clickhouse
docker volume rm binance-market-data-platform_ch_data
docker compose up -d clickhouse
```

**Open an interactive client:**
```bash
docker exec -it clickhouse clickhouse-client
# or with explicit credentials:
docker exec -it clickhouse clickhouse-client --user default --password ""
```

**Check databases/tables:**
```sql
SHOW DATABASES;
SHOW TABLES FROM raw;
```

**Check row counts and recent data:**
```sql
SELECT count() FROM raw.crypto_prices;
SELECT symbol, current_price, op, __source_ts_ms FROM raw.crypto_prices ORDER BY __source_ts_ms DESC LIMIT 8;
```

**Force full deduplication at read time (proves `ReplacingMergeTree` versioning works):**
```sql
SELECT symbol, count() FROM raw.crypto_prices FINAL GROUP BY symbol;
```

**Check Kafka consumer status inside ClickHouse (diagnoses stalled/broken ingestion):**
```sql
SELECT database, table, consumer_id, assignments.topic, assignments.partition_id,
       assignments.current_offset, exceptions.text, num_messages_read, last_poll_time
FROM system.kafka_consumers FORMAT Vertical;
```

**Check server logs for errors:**
```bash
docker exec -it clickhouse tail -100 /var/log/clickhouse-server/clickhouse-server.err.log
docker exec -it clickhouse tail -f /var/log/clickhouse-server/clickhouse-server.log | grep -i "crypto_prices"
```

### dbt

**Run models (from the `dbt/` directory, against a locally-run host shell):**
```bash
cd dbt
CLICKHOUSE_HOST=localhost dbt run --profiles-dir .
```

**Run tests:**
```bash
CLICKHOUSE_HOST=localhost CLICKHOUSE_PASSWORD=pass dbt test --profiles-dir .
```

**Query the mart directly:**
```sql
SELECT * FROM staging_marts.mart_crypto_market_snapshot;
```

**Run inside the Airflow container (production path — uses the isolated dbt venv and container-local target/log paths to avoid host/container file-ownership conflicts on the bind-mounted volume):**
```bash
docker compose exec airflow bash -c "cd /opt/airflow/project/dbt && /home/airflow/dbt-venv/bin/dbt run --profiles-dir . --target-path /tmp/dbt-target --log-path /tmp/dbt-logs"
```

### Airflow

**Build and bring up:**
```bash
docker compose build airflow airflow-init
docker compose up -d
docker compose ps
```

**Check DAG parsing / import errors:**
```bash
docker compose exec airflow airflow dags list-import-errors
docker compose exec airflow airflow dags reserialize
```

**Check for a specific DAG in the logs:**
```bash
docker compose logs airflow | grep -i "crypto_prices_pipeline\|broken\|error" | tail -30
```

**Trigger a manual DAG run:**
```bash
docker compose exec airflow airflow dags trigger crypto_prices_pipeline
```

**Retrieve the auto-generated admin password (Airflow 3.x `standalone` mode):**
```bash
docker compose exec airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

**Check `airflow-init`'s one-shot DB migration log:**
```bash
docker compose logs airflow-init
```

### CI/CD

**Set up the workflow file:**
```bash
mkdir -p .github/workflows
nano .github/workflows/ci.yml
```