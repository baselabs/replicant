# replicant examples

Runnable, minimal consumers that show how to integrate replicant end-to-end.
They are **not** part of the Hex package and **not** part of the library's test
suite — the `reference-example` CI job builds and exercises them as the public
sink API's canary.

## `replication_pipeline/` — the reference stack (docker)

The deployment shape consumers actually build, as one `docker compose up`:
Postgres source (`wal_level=logical` + publication) → replicant as an OTP
release in one container → Postgres destination, with an idempotent
orders replica and a durable commit-LSN checkpoint. See
[replication_pipeline/README.md](replication_pipeline/README.md).

The interactive feature tour (snapshot/backfill, logical-decoding messages) lives
in the repo's [getting-started Livebook](../notebooks/getting_started.livemd);
the unchanged-TOAST sentinel is exercised right here — the stack's CI canary
forces the sentinel and asserts the destination value survives.
