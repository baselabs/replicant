# ADR-0001: Logical-decoding message delivery guarantees (transactional vs non-transactional)

**Status:** Accepted
**Date:** 2026-07-14
**Deciders:** replicant maintainer (feature A2; spec `2026-07-11-replicant-messages-multipub-design.md` §7)

## Context

Postgres `pg_logical_emit_message/3` writes logical-decoding messages into WAL, surfaced by
pgoutput as `'M'` frames. They come in two flavors:

- **Transactional** (`flags = 1`): emitted inside a transaction, bracketed by `Begin`/`Commit`
  (or `StreamCommit` for proto-v2 streamed txns). They have a commit boundary.
- **Non-transactional** (`flags = 0`): emitted standalone, written to WAL immediately with **no
  Begin/Commit bracket and no commit LSN of a surrounding transaction**.

`replicant`'s flagship guarantee is **effect-once** on the sink-owned atomic path: a
`handle_transaction/1` that persists data + checkpoint in one DB transaction, deduped by
`commit_lsn`. Messages needed a delivery contract that does not silently violate that guarantee
or silently lose data. A message can be consumed for two very different jobs — a transactional
**outbox** row that must be exactly-once with its data, or a **heartbeat** emitted outside any
transaction — so one uniform guarantee cannot serve both honestly.

## Decision

Adopt a **split guarantee keyed on the message's `transactional?` flag**, opt-in via a top-level
`messages: true` config:

- **Transactional messages ride `%Transaction{messages: [...]}`** and inherit the transaction
  path's **effect-once** dedup (the txn's `commit_lsn`). They are delivered atomically with the
  transaction's row changes. (`decoder/messages.ex`, `assembler.ex` v1 + streamed clauses;
  `transaction.ex`.)
- **Non-transactional messages route to a new optional `c:handle_message/2`** and are
  **at-least-once — NO dedup key; duplicates are possible on reconnect** (documented in the
  `handle_message/2` docstring — Critical Rule 3, guarantee honesty). (`sink.ex`, `config.ex`.)
- **Fail-closed opt-in:** `messages: true` requires the sink to implement `handle_message/2`
  (`Sink.supports_messages?/1`); otherwise the pipeline is rejected at START with
  `:messages_unsupported` — never silently dropping messages later. (`config.ex` `fetch_messages`.)

Two durability seams prevent a non-transactional message from causing **silent loss** by acking
the slot past undelivered/undurable data:

- **§8.1 idle-ack seam** — a non-txn message in flight bumps `last_commit_lsn`, so the idle-ack
  keepalive path (`idle?/1`) refuses to advance the slot to `wal_end` until the message is
  durably delivered (`{:sink_committed, msg_lsn}` advances `checkpoint_lsn`).
  (`connection.ex` `track_txn/2` non-txn clause.)
- **§8.4 batch-boundary seam** — when a lib-batch or sink-owned batch is OPEN, the message
  signals `{:flush_before_message}`: the AssemblerServer **flushes + acks the batch first**
  (durability-before-ack), then re-dispatches the message, which now finds no open batch and
  delivers. (`assembler.ex` non-txn Message clause; `assembler_server.ex` `dispatch/3`.)

## Options Considered

### Option A: Both flavors at-least-once (uniform, simplest)
| Dimension | Assessment |
|-----------|------------|
| Complexity | Low |
| Honesty | Poor — silently downgrades transactional outbox from the effect-once the txn path already provides |

**Cons:** A transactional outbox message emitted with its data would lose exactly-once semantics
the surrounding transaction already guarantees. Rejected.

### Option B: Both flavors effect-once (uniform, strongest-sounding)
**Cons:** Impossible for non-transactional messages — they carry no commit boundary and no
dedup key, so there is nothing to dedup against. Claiming effect-once here would be a false
guarantee. Rejected.

### Option C: Split guarantee keyed on `transactional?` (adopted)
| Dimension | Assessment |
|-----------|------------|
| Complexity | Medium (two delivery paths + two durability seams) |
| Honesty | High — each flavor gets the strongest guarantee its structure actually supports, truthfully documented |

## Trade-off Analysis

The split trades one extra delivery path and two composition seams for **honest** guarantees.
The alternative "one guarantee" designs each require lying about one flavor. The durability
seams add real complexity precisely at the composition of a message with the deferral modes
(lib-batch, sink-owned batch, streaming) — this is the library's known
`cross-mode-composition-blindspot` class, so the seams are load-bearing and must be tested at
those compositions.

## Consequences

- **Easier:** Transactional-outbox consumers get exactly-once delivery of the message with its
  data. Non-txn heartbeat consumers get simple standalone delivery.
- **Harder:** Non-transactional message consumers **must be idempotent** — duplicates are
  possible on reconnect (by design; there is no dedup key). Every durability-signal / batch path
  must preserve the §8.1 and §8.4 seams or a message can ack past undurable data (silent loss).
- **Revisit if:** a future need for exactly-once non-transactional messages arises — it would
  require a consumer-supplied dedup key, a product change requiring its own ADR/supersession.

### Ordinal — the shared per-txn interleaving hint

Within a delivered `%Transaction{}`, `changes` and `messages` are each in commit order, and the
library never uses `Message.ordinal` for its own delivery ordering — it is a hint for consumers
who choose to interleave the two lists. Both the v1 and the **proto-v2 streamed** paths assign
`ordinal` from a **single shared per-transaction counter** incremented per change AND per
transactional message (stamped at accumulation/attach and preserved through replay/spill), so a
message emitted between two changes sorts strictly between them. An aborted (rolled-back) streamed
change and an interleaved message each occupy a slot, so surviving `ordinal`s may have gaps —
sorting the `changes` ∪ `messages` union by `ordinal` still yields commit-emission order. (Earlier
the streamed message used the change-buffer length at attach, which could collide with a following
change's replay ordinal — resolved 2026-07-14.)

### Telemetry (§10)

`[:replicant, :message, :received]` fires for BOTH message kinds — non-transactional at
`handle_message/2` delivery (`transactional: false`) and each transactional message when its
transaction is durably delivered (`transactional: true`) — carrying only `commit_lsn` + `byte_size`
+ the `transactional` boolean (never `prefix`/`content`, Rule 1).

## Verification (closeout 2026-07-14)

Both seams and both flavors are proven at runtime against live PG16 by
`test/integration/messages_test.exs` (transactional effect-once dup=0 across a crash; non-txn
at-least-once via `handle_message/2`; the §8.4 batch-boundary flush loss=0 in lib-batch and
sink-owned batch). Real captured `'M'` bytes (both flavors) corroborate the decoder in
`test/replicant/decoder/conformance_test.exs`.
