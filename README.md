# keep-ingestion

Alert intake for Keep. Authenticates provider webhooks, resolves the destination
tenant, and publishes to Kafka for `keep-event-handler` to process.

This is the public front door: Appchi and every other sender POST here, and
`GET /settings/webhook` on `keep-api-gateway` hands out this host's URL. It was
split out of the gateway so that a database incident, a schema migration or a
UI-driven gateway deploy cannot stop alerts being accepted — an alert a sender
gives up retrying is gone for good, unlike a UI request a user simply repeats.

It serves three routes and nothing else. No UI endpoints, no provider framework,
no Elasticsearch, no Alembic.

| Route | Notes |
|---|---|
| `POST /alerts/event` | The Appchi route. Typed body: `AlertDto \| list[AlertDto] \| dict` |
| `POST /alerts/event/{provider_type}` | Body passed through **unparsed** |
| `GET /alerts/event/netdata` | Netdata HMAC challenge, unauthenticated |

## Ports

One port. The intake routes, both probes and the Prometheus scrape are served by
the same app — unlike `keep-event-handler`, which splits health (8092) and
metrics (8094) because its consumer has no HTTP app of its own.

| Port | Serves |
|---|---|
| 8080 | everything: `/alerts/event*`, `/healthcheck`, `/readyz`, `/metrics` |

### Probes

| Path | Probe | Meaning |
|---|---|---|
| `/healthcheck` | **liveness** | Unconditional 200. Deliberately checks nothing — checking dependencies here would restart every replica at once on a Postgres blip |
| `/readyz` | **startupProbe** | Database reachable, the schema this service reads is present, Kafka producer connected |

Wire `/readyz` to the **startupProbe**, as the gateway does. It is not a
readiness probe: a shared-dependency check going false on every replica at once
would empty the Service mid-rollout.

`/readyz` doubles as the producer's reconnect trigger (`attempt_reconnect=True`),
so a pod that cannot reach the brokers keeps retrying while reporting NotReady,
rather than accepting alerts and silently diverting them to a topic nothing
consumes. Size the startupProbe budget accordingly — `failureThreshold ×
periodSeconds` has to cover a broker outage you are willing to wait out.

## Database — least privilege

**This service never writes.** Its role needs `SELECT` on exactly three tables:

```sql
CREATE ROLE keep_ingestion_ro LOGIN PASSWORD '...';
GRANT CONNECT ON DATABASE keep TO keep_ingestion_ro;
GRANT USAGE  ON SCHEMA public  TO keep_ingestion_ro;
GRANT SELECT ON tenant, tenantapikey, operator TO keep_ingestion_ro;
```

- `tenant`, `tenantapikey` — API-key verification
- `operator` — operator → tenant routing (VENA-5596 Epic 5)

Deliberately **not** granted: `alembic_version`. The service tolerates not being
able to read it and reports the head as unreadable in `/readyz`. The one
exception is `KEEP_SCHEMA_EXPECTED_REVISION` — pinning a revision requires
`GRANT SELECT ON alembic_version` as well, and without it `/readyz` reports
NotReady and says so rather than ignoring the pin.

`KEEP_APIKEY_TRACK_LAST_USED` defaults to **false** here for the same reason:
stamping `tenantapikey.last_used` on every request would require write access and
defeat the boundary.

## Schema ownership

**`keep-api-gateway` is the single owner of the database schema.** This image
ships no `alembic.ini` and no migrations directory. On startup it *waits* for the
gateway's `alembic upgrade head` to settle, the same pattern `keep-event-handler`
and `keep-workflows` already follow, with `SKIP_DB_CREATION=true` as a second
guard.

The wait retries with capped backoff, indefinitely, rather than exiting. A
gateway still migrating must make this service slow to start, not dead — the
startupProbe budget decides when to give up on the pod.

**Deploy ordering is unchanged: image first, chart with it or after it, never the
chart first.**

## Configuration

Required:

| Variable | Notes |
|---|---|
| `DATABASE_CONNECTION_STRING` | The `SELECT`-only role above |
| `KAFKA_BOOTSTRAP_SERVERS` | Same brokers as `keep-event-handler` |
| `SKIP_DB_CREATION` | `true` — set in the Dockerfile, do not override |

Kafka:

| Variable | Default | Notes |
|---|---|---|
| `KAFKA_TOPIC` | `keep-events` | Must match what the consumer reads |
| `KAFKA_DLQ_TOPIC` | `keep-events-dlq` | **Nothing consumes this.** An alert here is retained, never ingested |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | `SASL_*` / `SSL` variants read the `KAFKA_SASL_*` and `KAFKA_SSL_*` vars |
| `KAFKA_DLQ_BOOTSTRAP_SERVERS` | main brokers | Same cluster by default, so a broker outage takes the DLQ with it |

Behaviour:

| Variable | Default | Notes |
|---|---|---|
| `KEEP_ALERT_RETRY_AFTER` | `5` | `Retry-After` on every rejected publish |
| `KEEP_ALERT_DLQ_ACCEPT` | `false` | `true` answers 202 for a DLQ'd alert. Only for senders that must never see an error — the loss stays visible in the metric |
| `KEEP_READYZ_REQUIRE_PRODUCER` | `true` | Set false during a Kafka incident, or no pod can finish starting |
| `KEEP_READYZ_CHECK_TIMEOUT` | `2` | Per-check bound. Checks run in sequence, so worst case is twice this — keep under the probe's `timeoutSeconds` |
| `KEEP_SCHEMA_REQUIRED_TABLES` | `tenant,tenantapikey,operator` | What the startup wait polls for |
| `KEEP_SCHEMA_EXPECTED_REVISION` | unset | Pin a revision; needs `SELECT` on `alembic_version` |
| `PROMETHEUS_MULTIPROC_DIR` | `/tmp/prometheus` | Set in the Dockerfile. Must be writable |

## Metrics

Scraped from `/metrics` on 8080.

| Metric | Labels |
|---|---|
| `keep_alert_ingestion_total` | `source`, `status` (`success` \| `dlq`) |
| `keep_alert_ingestion_error_total` | `source`, `error_type` |

`keep_alert_ingestion_total` is the producer half of the zero-loss check:
reconcile it against `keep_events_in_total{event_type="alert"}` on
`keep-event-handler`. **Compare deltas over a window, not absolute totals** — the
counters reset independently whenever either pod restarts, so a constant offset
between them is normal and only a *widening* gap means alerts are being lost.

## Scaling

Scale on request rate, independently of `keep-api-gateway`, which scales on UI
query load. That independence is the point of the split. The service is stateless
apart from the Kafka producer's connection pool.

## Running locally

Port 8083, not 8082 — `WORKFLOWS_API_URL` already defaults to 8082.

```bash
cd keep-event-handler && docker compose -f docker-compose.infra.yml up -d  # kafka + postgres
cd keep-api-gateway   && poetry run alembic upgrade head                   # gateway owns the schema
cd keep-api-gateway   && poetry run uvicorn src.main:app --port 8080
cd keep-ingestion     && poetry run uvicorn src.main:app --port 8083       # waits for the schema
cd keep-event-handler && poetry run python -m src.consumer_main
```

`uvicorn` does **not** run migrations: `on_starting` is a gunicorn hook, so a
local uvicorn run skips it. Run `alembic upgrade head` in the gateway explicitly,
as above, or the schema will be empty.

```bash
curl -XPOST localhost:8083/alerts/event \
  -H 'X-API-KEY: ...' -H 'Content-Type: application/json' -d @sample.json
# -> 202 {"task_name":"kafka-async-task","sink":"main"}
```

## Tests

```bash
poetry run pytest tests/
```
