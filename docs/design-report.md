# Design Report

## 1. Architecture & data flow

An Airflow DAG polls Binance's public REST API (`GET /api/v3/ticker/24hr`,
unauthenticated) every 5 minutes for a fixed list of 8 symbols and upserts
the results into Postgres (`crypto_prices`, keyed on `symbol`). Debezium
streams Postgres's WAL (via `pgoutput`) into a Kafka topic
(`pg.public.crypto_prices`) as CDC events. That topic runs continuously and
independently of the DAG — it's not an Airflow task, it's a standing
connector. ClickHouse consumes the topic through a `Kafka` engine table
(`raw.crypto_prices_kafka`), and a materialized view pushes each row into a
durable `ReplacingMergeTree` table (`raw.crypto_prices`). dbt then builds a
staging model that types and deduplicates that raw layer, and a marts model
on top of staging that adds analytics fields for downstream consumption.

Upserting into Postgres (rather than append-only inserts) is deliberate: it
produces real `INSERT` events for new coins and `UPDATE` events for price
refreshes, so the CDC path has to handle actual change events instead of a
trivial bulk-load-once case.

## 2. ClickHouse schema decisions

**Versioning column:** `raw.crypto_prices` is a `ReplacingMergeTree(__source_ts_ms)`
ordered by `coin_id`. `__source_ts_ms` is Debezium's *source* commit
timestamp — the time the change was committed in Postgres — not `ingested_at`
(the ingestion script's wall-clock write time) and not any consumer-side
"synced at" timestamp. Kafka doesn't guarantee delivery order across
retries/rebalances, and consumer-side timestamps drift with clock skew and
processing delay between services. Versioning on the source's own commit
time means that no matter when or in what order a duplicate or out-of-order
event physically lands in ClickHouse, the row with the true latest change in
Postgres always wins on merge.

**Dedup strategy — `ROW_NUMBER()` in staging, not `FINAL` alone:** the
staging model dedupes explicitly with
`ROW_NUMBER() OVER (PARTITION BY coin_id ORDER BY __source_ts_ms DESC) WHERE rn = 1`
rather than relying only on ClickHouse's `FINAL` modifier. `FINAL` forces a
full scan across all parts to resolve duplicates implicitly at query time,
on every table scan, with no control over when or how that resolution
happens. The window-function approach still has to see every row in a
`coin_id` partition before it can rank them, so it isn't free — but it moves
dedup into an explicit step in the query plan that I control, rather than a
blanket behavior forced onto every read of the table. At higher volume, that
control point is where I'd swap in `argMax()`-style aggregation or
time-sliced reads instead of paying `FINAL`'s full-scan cost on every query.

**dbt models:**
- `stg_crypto_prices` (staging, view): casts raw Kafka/CDC types to proper
  numeric types, applies the `ROW_NUMBER()` dedup, and drops CDC delete
  tombstones (`__op = 'd'`).
- `mart_crypto_market_snapshot` (marts, table): one row per coin, adds
  derived analytics — price spread, volatility %, trend direction, average
  trade size, and ingestion lag — on top of the deduplicated staging output.

## 3. Orchestrator choice

Prefect was the original plan — it's lighter weight than Airflow, with far
fewer moving parts than Airflow's scheduler/webserver/triggerer/metadata-DB
ecosystem for a pipeline this size. The switch to Airflow was a deliberate
alignment choice: Airflow is the orchestrator named in the target job
description's stack, so building on it directly demonstrates the relevant
experience rather than the closest lightweight substitute.

Development started on Airflow 2.9.3 and was upgraded to 3.x mid-project.
Airflow 3 consolidates and simplifies several previously-separate services
and is the current direction of the project, so building against it is more
representative of what a team would actually run today.

The 2→3 migration had one real breaking change: the DAG's `schedule_interval`
parameter was renamed to `schedule` in Airflow 3. It surfaced as a DAG import
error in the webserver rather than a runtime failure, which made it quick to
localize — once diagnosed, the fix was a one-line parameter rename in
`pipeline_dag.py`.

## 4. The isolated dbt environment

Installing `dbt-clickhouse` directly into the Airflow image caused a
`protobuf` version conflict: Airflow's bundled `protobuf` version didn't
satisfy what `dbt-adapters`/`dbt-common` required, and the two couldn't
coexist in one Python environment. The fix was to build a separate Python
venv inside the Airflow container (`/home/airflow/dbt-venv`) used only for
dbt, and invoke it via its full binary path from `BashOperator` commands
rather than relying on `dbt` being on `PATH`.

dbt's `--target-path` and `--log-path` are also redirected to `/tmp` inside
the container instead of the default `dbt/target-path` under the bind-mounted
`/opt/airflow/project/dbt`. The project directory is bind-mounted from the
host, so files dbt writes there would be owned by the host user, while the
container runs as the `airflow` user — a UID mismatch that causes permission
errors on subsequent runs. Writing dbt's ephemeral output to a container-local
path sidesteps that entirely. The trade-off is losing dbt's partial-parse
cache across container restarts, since that cache now lives in `/tmp` and
gets wiped whenever the container recreates.

## 5. A real ClickHouse bug worth including

Early on, the materialized view's `SELECT` aliased the CDC timestamp column
as `__source_ts_ms AS source_ts_ms` (single underscore), while the target
table's actual column was `__source_ts_ms` (double underscore, matching
Debezium's field name). Because `MATERIALIZED VIEW ... TO target` matches
the `SELECT` output to the target table by column name, the mismatched alias
meant the real `__source_ts_ms` column never received a mapped value — it
silently stayed at its default (`0`) instead of raising any error, since
ClickHouse just treated it as an unpopulated column rather than a broken
one. It only became visible when the `ReplacingMergeTree` version column
turned out to be `0` for every row, so merges weren't actually picking the
latest state. The fix was removing the alias so the `SELECT` and target
column names matched exactly — a reminder that materialized views into a
named target table are matched by name, not position, and a silent
mismatch there fails quietly rather than loudly.

## 6. Observability

- **Kafka Connect / Debezium JMX exporter:** a custom `kafka-connect`
  Dockerfile builds on `debezium/connect:2.6` and adds the Prometheus JMX
  Java agent (`jmx_prometheus_javaagent`), wired in via `KAFKA_OPTS`, rather
  than just setting the plain `JMXPORT` env var — that env var only opens a
  raw JMX port, which isn't a scrapeable Prometheus HTTP endpoint. This is
  what surfaces connector/task status and CDC throughput/backlog metrics.
- **ClickHouse native `/metrics`:** required an explicit `<prometheus>`
  block added to server config (`clickhouse/config/prometheus.xml`) — it's
  off by default. Monitors query execution, merge activity, and general
  ClickHouse resource usage.
- **postgres-exporter:** monitors Postgres connection counts and — most
  relevantly here — replication slot lag on the slot Debezium reads from,
  the earliest signal of the source side falling behind.
- **statsd-exporter:** receives Airflow's StatsD metrics (task/DAG duration,
  success/failure counts) and exposes them for Prometheus to scrape.

The single most important metric for this pipeline is the CDC backlog —
`kafka_connect_source_task_metrics_source_record_active_count_avg` — because
it's the earliest warning that Debezium is falling behind the Postgres WAL.
Everything downstream (Kafka, ClickHouse, dbt) depends on CDC keeping up;
if this metric climbs, every other "the pipeline is healthy" signal further
downstream is measuring stale data without yet showing any error.

## 7. CI/CD

GitHub Actions runners are US-hosted, and Binance blocks requests from US
IPs (a 451/error response) — so the real ingestion step that works
everywhere on a personal machine or most cloud regions fails specifically in
CI, for reasons unrelated to the pipeline's own correctness. Rather than
skip integration testing, CI seeds Postgres directly with an `INSERT` for
all 8 symbols, then lets the rest of the pipeline — CDC, Kafka, ClickHouse,
dbt — run for real against that seeded data.

This proves the entire downstream pipeline (CDC propagation, ClickHouse
ingestion, dedup, dbt staging/marts, dbt tests) works end-to-end against
live infrastructure. It does not exercise the actual Binance API call itself
— that path is covered separately by unit tests that mock the HTTP response.

## 8. Known limitations / what I'd change with more time

- The DAG waits on a fixed 15-second `sleep` for CDC propagation before
  running dbt, rather than a real freshness check (e.g., polling ClickHouse
  until it reflects the latest Postgres write). Fine at this volume, but not
  a robust pattern.
- No Airbyte or Great Expectations — hand-rolled ingestion and dbt schema
  tests are enough at this scale, but wouldn't be a production-grade data
  quality/ingestion setup at real volume.
- The Grafana dashboard covers infra health (endpoint status, CDC backlog,
  Postgres/ClickHouse metrics, scrape performance) but has no alerting rules
  configured — it's currently something a human has to look at, not
  something that pages anyone.
- The dashboard is committed as a static JSON file rather than managed
  through Grafana's API or a Terraform provider, so dashboard changes aren't
  version-controlled in a way that diffs cleanly or can be applied
  idempotently.
- The symbol list is a fixed, hardcoded set of 8 (`SYMBOLS` env var); a
  production version would pull a paginated/dynamic instrument list instead.

## 9. Scaling considerations

- **Kafka:** currently default partitioning on the CDC topic; at higher
  write volume, partitioning by a symbol/`coin_id` hash would preserve
  per-key ordering while enabling parallel consumption.
- **ClickHouse:** `ReplacingMergeTree` dedup is eventually consistent
  between merges. At higher ingest rates, scheduled `OPTIMIZE ... FINAL`
  runs, or switching hot-path reads to `argMax()`-style aggregation instead
  of forcing `FINAL`/window-function dedup on every read, keeps query cost
  down.
- **Ingestion:** the fixed `SYMBOLS` list works for a handful of instruments;
  at real scale this would move to a paginated or dynamically fetched symbol
  list rather than a hardcoded env var.
- **Debezium:** a single connector/task is sufficient at this volume. At
  scale, this would move to multiple tasks partitioned by table, with
  replication slot lag monitoring (already built into the Postgres exporter
  panel) as the trigger for noticing a stuck or overloaded consumer before
  WAL buildup becomes a problem.

## 10. Testing and verification

**CDC event-type coverage.** Most CDC demos only ever prove the insert-only
case, which is the trivial one — an append-only feed doesn't actually
exercise anything Debezium is for. This pipeline was verified against all
four Debezium op codes on the live connector: `r` (read/snapshot, observed
on initial connector registration before any live changes), `c` (create,
triggered by inserting a symbol not yet in the table), `u` (update,
triggered by re-running ingestion against existing rows, which hits the
`ON CONFLICT ... DO UPDATE` path), and `d` (delete, triggered by a manual
`DELETE FROM crypto_prices WHERE symbol = 'TRXUSDT'`). The delete confirmed
`delete.handling.mode: rewrite` actually behaves as documented: a full
record with `__deleted: true` followed by a Kafka tombstone, rather than
just a bare tombstone.

**Dedup verification.** Compiling without error isn't the same as the
versioning logic being correct, so the `ReplacingMergeTree` behavior was
checked directly rather than assumed: `SELECT symbol, count() FROM
raw.crypto_prices GROUP BY symbol` returned 3 rows per symbol (unmerged
parts still on disk from repeated upserts), while the same query with
`FINAL` returned exactly 1 row per symbol, 8 total. That side-by-side is the
actual proof that `__source_ts_ms` versioning resolves duplicates correctly
on read, not just that the DDL parsed.

**dbt data quality tests.** 7 tests pass across staging and marts:
`not_null`/`unique` on `coin_id` and `symbol` in both layers, `not_null` on
`current_price` and `last_updated`, an `accepted_values` check on
`trend_24h` (`up`/`down`/`flat`), and a custom singular test,
`assert_eight_symbols`, that fails if the mart doesn't contain exactly 8
rows. That row-count test matters specifically because it catches the same
class of bug as the materialized-view alias issue in section 5 — silent
missing or unpopulated data that no schema test on an individual column
would catch, since the column itself is still present and typed correctly.

**CI as repeatable proof, not one-off manual testing.** The event-type and
dedup checks above were run by hand once to validate the design. The GitHub
Actions integration test re-runs the equivalent of that verification
automatically on every push: bring up the full stack, seed data, assert
rows exist in ClickHouse, then run `dbt build` (models + tests) against the
live stack. The point of wiring it into CI is that this isn't a claim made
once and left to rot — every future change to the repo has to clear the
same bar.
