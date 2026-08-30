# Crypto Prices CDC Pipeline

An end-to-end pipeline: Binance public REST API → PostgreSQL (OLTP) →
Debezium CDC → Kafka → ClickHouse (OLAP) → dbt (staging → marts), orchestrated
by Airflow, containerized with Docker Compose, tested via GitHub Actions CI/CD,
and monitored with Prometheus + Grafana.

## Architecture

```
Binance REST API --(poll every 5 min via Airflow)--> Postgres (crypto_prices)
                                                        |
                                              Debezium (pgoutput CDC)
                                                        |
                                                      Kafka
                                                        |
                                      ClickHouse raw.crypto_prices_kafka
                                          (Kafka engine table)
                                                        |
                                        Materialized View -> raw.crypto_prices
                                          (ReplacingMergeTree, versioned)
                                                        |
                                        dbt: staging.stg_crypto_prices
                                                        |
                                  dbt: marts.mart_crypto_market_snapshot
```

**Why upserts, not append-only ingestion?** CDC on an append-only table is a
trivial "every row is an insert" case. Polling with an upsert produces real
`INSERT` events for new coins and `UPDATE` events for price refreshes, so the
Debezium/Kafka/ClickHouse path actually has to handle change events, not just
bulk-load once.

**Why ClickHouse Kafka engine + materialized view over Debezium Server's
native ClickHouse sink?** It's the more battle-tested, widely-documented
pattern, and keeps ClickHouse ingestion entirely inside ClickHouse's own
config rather than depending on an experimental Debezium sink connector.

**ClickHouse design choices:**
- `ReplacingMergeTree(__source_ts_ms)` on `raw.crypto_prices`, ordered by
  `coin_id` — deduplicates on merge using Debezium's source commit timestamp
  as the version, so out-of-order or duplicate CDC events resolve correctly
  regardless of Kafka delivery order.
- Staging dedups explicitly with `ROW_NUMBER() OVER (PARTITION BY coin_id
  ORDER BY __source_ts_ms DESC) WHERE rn = 1` rather than relying on `FINAL`
  alone — `FINAL` forces a full scan across parts on every read, while
  explicit window-function dedup can still benefit from ClickHouse's sorting
  key and projections as data volume grows.
- CDC deletes are soft-filtered (`__op != 'd'`) in staging rather than
  physically removed from the raw layer, preserving the full change history
  for auditability.

See [`docs/design-report.md`](docs/design-report.md) for the full rationale
behind these choices, the orchestrator switch from Prefect to Airflow, the
Airflow 2→3 migration, the isolated dbt venv, and a real ClickHouse bug found
and fixed along the way.

## Prerequisites

- Docker & Docker Compose v2
- ~6GB RAM available to Docker (Kafka + Zookeeper + Connect + ClickHouse +
  Airflow together are not lightweight)

## Running it

```bash
docker compose up -d
```

This starts everything with one command: Postgres (+ `postgres-exporter`),
Zookeeper, Kafka, Kafka Connect (with Debezium + JMX Prometheus exporter), a
one-shot `connect-init` container that registers the CDC connector,
ClickHouse, `statsd-exporter`, Airflow (standalone: webserver + scheduler +
triggerer), Prometheus, and Grafana.

First boot takes a few minutes (Kafka Connect plugin loading, Airflow DB
migration). Check status:

```bash
docker compose ps
```

## Validating the pipeline end-to-end

1. **Trigger an ingestion cycle** (Airflow otherwise runs this every 5 min on
   its own schedule via the `crypto_prices_pipeline` DAG):
   ```bash
   docker compose exec airflow python /opt/airflow/project/ingestion/ingest_prices.py
   ```
2. **Confirm rows landed in Postgres:**
   ```bash
   docker compose exec postgres psql -U app -d sourcedb -c "SELECT coin_id, current_price FROM crypto_prices LIMIT 5;"
   ```
3. **Confirm the Debezium connector is running:**
   ```bash
   curl -s http://localhost:8083/connectors/pg-crypto-prices-connector/status | jq
   ```
4. **Confirm rows propagated to ClickHouse** (give CDC ~15-30s to flow through
   Kafka):
   ```bash
   curl -s "http://localhost:8123/?query=SELECT coin_id,current_price,__op FROM raw.crypto_prices FINAL LIMIT 5 FORMAT PrettyCompact"
   ```
5. **Run dbt to build staging + marts:**
   ```bash
   docker compose exec airflow bash -c "cd /opt/airflow/project/dbt && /home/airflow/dbt-venv/bin/dbt build --profiles-dir . --target-path /tmp/dbt-target --log-path /tmp/dbt-logs"
   ```
6. **Query the mart:**
   ```bash
   curl -s "http://localhost:8123/?query=SELECT * FROM marts.mart_crypto_market_snapshot FORMAT PrettyCompact"
   ```

To watch the full loop happen automatically, just wait — the Airflow DAG
(`crypto_prices_pipeline`) runs ingest → wait-for-CDC → `dbt run` → `dbt test`
every 5 minutes on its own.

## Accessing each component

| Component | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / auto-generated (see below) |
| Kafka Connect REST API | http://localhost:8083 | none |
| ClickHouse HTTP interface | http://localhost:8123 | default / (see `.env`) |
| Prometheus | http://localhost:9090 | none |
| Grafana | http://localhost:3000 | admin / admin |

Airflow 3's `standalone` mode uses the `SimpleAuthManager`, which generates a
random admin password on first init instead of a fixed one. Retrieve it with:

```bash
docker compose exec airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

(or grep it from `docker compose logs airflow` — it's printed once on first
boot). See [`docs/common-operations-reference.md`](docs/common-operations-reference.md)
for this and other day-to-day commands across every component.

## Data source authentication

Binance's `/api/v3/ticker/24hr` endpoint is used unauthenticated (public
market data, no API key required). The symbol list defaults to
`BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,XRPUSDT,ADAUSDT,DOGEUSDT,TRXUSDT` and is
configurable via the `SYMBOLS` env var. See `ingestion/ingest_prices.py`.

> **Note:** Binance blocks requests from US-hosted IPs. This works fine on a
> personal machine or most cloud regions, but fails from GitHub Actions
> runners — see [CI/CD](#cicd) below for how that's handled.

## Observability

Prometheus scrapes (`monitoring/prometheus/prometheus.yml`):
- **Kafka Connect / Debezium** — a custom `kafka-connect` image bundles the
  Prometheus JMX Java agent (not just the raw `JMXPORT` env var, which isn't
  independently scrapeable) exposing connector/task status and CDC
  throughput/backlog on port 9404. The single highest-value metric here is
  `kafka_connect_source_task_metrics_source_record_active_count_avg` — the
  earliest signal that Debezium is falling behind Postgres's WAL.
- **ClickHouse** native Prometheus endpoint (port 9363, enabled via an
  explicit `<prometheus>` block in server config) — query performance, merge
  activity, resource usage.
- **Postgres** via `postgres-exporter` (port 9187) — connection counts and
  replication slot lag (the slot Debezium reads from).
- **Airflow** via `statsd-exporter` (port 9102) — DAG/task run metrics.

Grafana auto-provisions the Prometheus datasource and a dashboard on startup
(`monitoring/grafana/provisioning/`). The committed dashboard
(`dashboards/json/pipeline-dashboard.json`) covers infra health/endpoint
status, CDC pipeline health (Kafka Connect status, CDC backlog, CDC
records/sec), Postgres/ClickHouse key metrics, and Prometheus scrape
performance — no alerting rules are configured yet.

## CI/CD (`.github/workflows/ci.yml`)

On every push/PR:
1. **Lint & unit test:** `ruff check` and `pytest` (mocking the Binance API
   and DB — no live services needed).
2. **Integration test:** brings up the entire docker-compose stack. Because
   GitHub Actions runners are US-hosted and Binance blocks US IPs, this stage
   seeds Postgres directly with a SQL `INSERT` for all 8 symbols instead of
   calling the live Binance API, then lets the rest of the pipeline (CDC →
   Kafka → ClickHouse → dbt) run for real against that seeded data, and runs
   `dbt build` against the live stack. This proves the whole downstream
   pipeline works end-to-end; it does not exercise the live Binance API call
   itself, which is covered separately by the mocked unit tests.

Unit tests cover response parsing, the upsert SQL/params, and symbol-parsing
logic (`tests/test_ingest_prices.py`). dbt runs 7 data quality tests across
staging and marts — `not_null`/`unique` on key columns, an `accepted_values`
check on `trend_24h`, and a custom test asserting the mart always has exactly
8 rows. The pipeline was also manually verified against all four Debezium CDC
event types (snapshot, insert, update, delete) and a direct before/after
`FINAL` comparison proving `ReplacingMergeTree` dedup actually works. See
[`docs/design-report.md` § 10](docs/design-report.md#10-testing-and-verification)
for the full write-up.

## Scaling considerations

- **Ingestion volume:** the poll-and-upsert pattern works well for a small,
  fixed symbol list. At larger scale (thousands of instruments), this would
  move to a paginated/dynamic symbol list and a dedicated staging table per
  batch rather than row-by-row upserts.
- **Kafka partitioning:** the CDC topic currently runs with default
  partitioning. At higher write volume, partitioning by a symbol/`coin_id`
  hash would preserve per-key ordering while enabling parallel consumption.
- **ClickHouse:** `ReplacingMergeTree` dedup is eventually-consistent between
  merges. At high ingest rates, `OPTIMIZE ... FINAL` on a schedule (or
  switching hot-path reads to `argMax()` aggregation instead of forced
  dedup reads) avoids the read-time cost of full-scan deduplication.
- **Orchestration:** the Airflow DAG currently uses a fixed `sleep(15)` to
  wait for CDC propagation before running dbt — a data-freshness sensor
  against ClickHouse (e.g., poll until it reflects the latest Postgres write)
  would be more robust than a fixed delay.
- **Debezium:** a single connector task is sufficient at this volume; at scale
  this would move to multiple tasks partitioned by table, plus monitoring
  replication slot lag on Postgres to catch a stuck consumer before WAL
  buildup becomes an issue.

## References

- [Binance Spot API — `/api/v3/ticker/24hr`](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints) — the source endpoint polled by `ingestion/ingest_prices.py`.
- [Debezium PostgreSQL connector docs](https://debezium.io/documentation/reference/stable/connectors/postgresql.html) — `pgoutput` plugin, publication/replication slot setup, the `ExtractNewRecordState` SMT used for the `unwrap` transform.
- [Apache Kafka documentation](https://kafka.apache.org/documentation/) — topics, consumer groups, and the Kafka Connect framework Debezium runs on.
- [ClickHouse documentation](https://clickhouse.com/docs) — general reference; specifically the [`Kafka` table engine](https://clickhouse.com/docs/en/engines/table-engines/integrations/kafka) and [`ReplacingMergeTree`](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/replacingmergetree) used in `clickhouse/init/01_raw_layer.sql`.
- [dbt documentation](https://docs.getdbt.com/) and the [dbt-clickhouse adapter](https://github.com/ClickHouse/dbt-clickhouse) used for the staging/marts models.
- [Apache Airflow documentation](https://airflow.apache.org/docs/) — DAG authoring, the Airflow 3 migration guide (relevant to the `schedule_interval` → `schedule` rename), and the `SimpleAuthManager` used by `airflow standalone`.
- [Prometheus documentation](https://prometheus.io/docs/) and [Grafana documentation](https://grafana.com/docs/) — scrape config and dashboard provisioning under `monitoring/`.
- [Docker Compose documentation](https://docs.docker.com/compose/) — the multi-service setup in `docker-compose.yml`.

See also [`docs/design-report.md`](docs/design-report.md) for the project's
own design rationale and [`docs/common-operations-reference.md`](docs/common-operations-reference.md)
for day-to-day operational commands.
