# The reference pipeline — replicant as a durable Postgres→Postgres replicator

One `docker compose up` brings up the whole reference stack:

```
Postgres 18 (source, wal_level=logical + example_pub) ──pgoutput──> [this app]
                                                                     ├─ idempotent orders replica (upsert by PK)
                                                                     └─ durable commit-LSN checkpoint
                                                                  ──> Postgres 18 (destination)
```

This is a **go-forward change replicator**: it streams changes that commit from
the moment the pipeline starts. Pre-existing source rows are **not** backfilled
— `snapshot: true` is the one-flag alternative for that (it additionally
requires `handle_snapshot/2` + `handle_snapshot_complete/1` on the sink; see
the repo README's "Start modes").

All credentials are throwaway local-example values, not secrets. Ports bind to
127.0.0.1 only (15432 source / 15433 destination).

## Run it

```bash
cd examples/replication_pipeline
docker compose up -d --build          # source + destination + pipeline
```

First boot is a full OTP-release build — a few minutes. Watch it flow:

```bash
# write a row on the source
docker compose exec source-pg psql -U postgres -d example_src \
  -c "INSERT INTO orders (id, note) VALUES (1, 'hello')"

# see it on the destination, with its checkpoint
docker compose exec dest-pg psql -U postgres -d example_dst \
  -c "SELECT * FROM orders; SELECT commit_lsn FROM pipeline_checkpoint"
```

Teardown: `docker compose down -v`.

## What each piece teaches

| Piece | The lesson |
| --- | --- |
| `ReplicationPipeline.Sink` | at-least-once delivery made effect-once by the commit-LSN watermark: data + checkpoint in ONE destination transaction, and the `commit_lsn <= checkpoint` skip IS the dedup (Critical Rule 3) |
| `ReplicationPipeline.Sink` (receipts) | the value-free `cdc_receipts` ledger (commit_lsn/schema/table/op, never a value) exists exactly when its transaction took effect — re-delivery is skipped whole |
| `ReplicationPipeline.Sink` (session identity) | the first connect BINDS the source's `{system_identifier, database}` into the checkpoint row; every later connect COMPARES — a rebuilt source container HALTS the pipeline instead of silently resuming a different database (ADR-0007) |
| `ReplicationPipeline.Sink` (upsert) | unchanged-TOAST columns are OMITTED from the upsert SET — the sentinel never appears in `record` (Critical Rule 4) |
| `docker-compose.yml` (source flags) | `wal_level=logical` is the load-bearing flag; the publication is the operator's SQL and a missing one fails closed |

The demo is intentionally minimal (one table, `id` PK). Real deployments
extend the sink per table — the seam is the point, not the schema.

## CI

The `reference-example` CI job builds this stack and drives the sequence above
— insert, replica + checkpoint assertions — so the example can never silently
rot out of sync with replicant's public sink API.
