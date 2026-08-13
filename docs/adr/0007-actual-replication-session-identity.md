# ADR 0007: Actual replication-session identity precedes checkpoint lookup

## Status

Accepted for 1.0.0.

## Context

A durable CDC checkpoint is safe only for the PostgreSQL source that produced
it. Connection configuration, DNS, a load balancer, or a separate preflight
connection cannot prove which system and database the replication socket
actually reached. Across failover or routing changes, using a checkpoint from a
different source can skip unrelated WAL or replay against the wrong dataset.

Replicant previously read the sink or library checkpoint before its first
server command. That ordering made authoritative source binding impossible.

## Decision

Every connect and reconnect issues `IDENTIFY_SYSTEM` as the first command on the
same `Postgrex.ReplicationConnection` that later issues `START_REPLICATION`.
Replicant normalizes the one-row result into `Replicant.SessionIdentity` and
synchronously invokes the optional sink callback `handle_session_identity/2`.
Its context contains the configured slot and normalized publication list.

Only `:ok` accepts the session. Any other return, raise, throw, exit, or malformed
protocol result halts fail-closed with a fixed structural reason. The callback
runs before a sink-owned checkpoint read, checkpoint-store read, snapshot, or
stream command. Generic sinks may omit the callback; sinks that bind durable
state to a source implement it and reject drift.

## Consequences

- Source-aware sinks receive authoritative system/database identity without a
  second connection or a time-of-check/time-of-use gap.
- Reconnects revalidate identity before resuming from any checkpoint.
- Existing transaction, batch, message, and snapshot callback arities remain
  unchanged.
- Replicant does not persist source identity; storage and compatibility policy
  remain sink responsibilities.
- The coordinated AshReplicant 1.0.0 release will require Replicant 1.x because
  its checkpoint contract depends on this pre-check.
