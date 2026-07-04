# Replicant

A framework-agnostic Elixir CDC consumer for Postgres logical replication
(`pgoutput`), delivering committed row changes to a pluggable **sink** with
**zero data loss**: the replication slot advances only after the sink has
durably persisted the transaction.

Replicant is **tenant-blind and classification-blind** — the reliable CDC
consumer sibling to [`arcadic`](https://github.com/baselabs/arcadic) and
`ash_age`. Multitenancy, classification, and Ash resources live one layer up,
in a future `ash_replicant` sink adapter.

> **Status:** this release ships the **offline core** — decode, assemble,
> validate, redact. Live streaming (the `Postgrex.ReplicationConnection` that
> owns the replication slot and acks only after the sink commits) is the next
> slice; it is not in this version. See "This is the offline core" below.

## Highlights

- **Sink-owned, transaction-granularity exactly-once** — the unit of delivery
  and of the watermark is the *transaction*, keyed by its single `commit_lsn`
  (every row in a pgoutput proto-v1 transaction shares one commit LSN). A sink
  skips any transaction whose `commit_lsn <= checkpoint` and upserts rows by
  table PK; that is at-least-once plus an idempotent sink, which is the only
  honest way to reach exactly-once without two-phase commit.
- **Value-free errors, logs, and telemetry** — every row value is assumed to
  be PII or a secret. Decode failures are caught and scrubbed into a
  `Replicant.Error` that never carries raw WAL bytes; telemetry metadata is
  allowlisted to LSNs, table names, counts, durations, and error classes.
- **Identifier-validated SQL** — slot and publication names pass through
  `Replicant.Identifier.validate/1` (a strict Postgres-identifier allowlist)
  before they reach SQL, closing the raw-interpolation surface in the
  upstream parser this library vendors from.
- **TOAST-sentinel aware** — an UPDATE that doesn't touch a TOASTed column
  sends a sentinel, not the value. Replicant surfaces it as a first-class
  `unchanged: [col]` list on `Replicant.Change`, so a sink knows exactly which
  columns to leave untouched on upsert, instead of overwriting them with a
  placeholder.
- **Fail-closed on destructive schema drift** — a replica-identity change or a
  dropped column is classified `:destructive` and halts, rather than silently
  emitting incomplete or misattributed rows.
- **Column names stay strings** — never `String.to_atom`, so a wide or
  attacker-influenced schema cannot exhaust the atom table.

## LSN representation

A Postgres LSN is exposed as a single `non_neg_integer` — the 64-bit value
`(xlog_file <<< 32) ||| xlog_offset` — so that ordinary integer comparison is
correct WAL ordering, and the same value feeds the wire-level standby status
update:

```elixir
Replicant.lsn_to_string(0x16E3778)
#=> "0/16E3778"
```

Use `Replicant.lsn_to_string/1` for display; LSNs are WAL positions, not row
data, so they are permitted in telemetry metadata. The exactly-once watermark
check is plain integer comparison: `txn.commit_lsn <= checkpoint`.

## This is the offline core

This release ships the decode / assemble / validate / redact core:

- the vendored `pgoutput` byte parser (credited in `NOTICE`),
- the type-aware assembler that groups decoded messages into
  `Replicant.Transaction`s by `commit_lsn`,
- the identifier-validated SQL builder for slot/publication management, and
- the pluggable `Replicant.Sink` behaviour that a consumer implements to
  receive assembled transactions.

Live streaming — the `Postgrex.ReplicationConnection` that owns the
replication slot, feeds it decoded WAL, and acknowledges progress only after
the sink has durably committed — is not part of this release. Nothing here
opens a replication connection or talks to a live server.

## The 5 critical rules (see `AGENTS.md` for the full text)

1. **No row value in an error, log, or telemetry event.**
2. **Validate identifiers** before they reach SQL.
3. **Exactly-once is at-least-once + a transaction-watermark-idempotent
   sink** — never claim a naked exactly-once.
4. **Unchanged TOAST is a sentinel, not a value** — never overwrite it.
5. **Stay tenant-blind** — multitenancy and classification live in
   `ash_replicant`, never here.

## Installation

```elixir
def deps do
  [
    {:replicant, "~> 0.1"}
  ]
end
```

## Usage sketch (offline core)

The public surface today covers the offline pipeline pieces — decode an
already-captured `pgoutput` message, assemble transactions, and validate
identifiers before building slot/publication SQL:

```elixir
# Decode a single pgoutput protocol message (never raises; redacts on failure).
{:ok, message} = Replicant.Decoder.decode(bytes)

# Validate an identifier before it reaches SQL.
:ok = Replicant.Identifier.validate("my_publication")

# Compare LSNs as plain integers — this is the exactly-once watermark check.
Replicant.lsn_to_string(transaction.commit_lsn)
#=> "0/16E3778"
```

A sink implements `Replicant.Sink` to receive assembled
`Replicant.Transaction`s once live streaming ships:

```elixir
defmodule MyApp.Sink do
  @behaviour Replicant.Sink

  @impl true
  def handle_transaction(%Replicant.Transaction{} = txn) do
    # Persist txn.changes durably, keyed by txn.commit_lsn, before returning.
    :ok
  end
end
```

## Development

```bash
mix deps.get
mix test
mix quality   # format --check-formatted + credo --strict + dialyzer
```

Contributor and agent working rules — including the redaction,
identifier-validation, and tenant-blind invariants — live in
[`AGENTS.md`](AGENTS.md).

## Credits

- [**walex**](https://github.com/cpursley/walex) — the `pgoutput` byte parser,
  OID-to-type database, type caster, and array parser this library vendors
  from (MIT). See `NOTICE` for the full attribution chain (cainophile,
  Supabase Realtime, epgsql).
- The `postgrex`/`ash_postgres` split that inspired `arcadic` and
  `ash_arcadic` also shapes the `replicant`/`ash_replicant` layering.

## License

MIT — see [LICENSE](LICENSE). Third-party attributions in [NOTICE](NOTICE).
