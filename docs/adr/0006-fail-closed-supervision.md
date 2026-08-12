# ADR-0006: Fail-closed supervision — `:one_for_all` Pipeline + `:temporary` child

**Status:** Accepted
**Date:** 2026-08-12
**Deciders:** replicant maintainer (1.0 hardening D8; records the Plan-2 supervision decision that shipped in 0.1.0)

## Context

A CDC consumer that loses data silently is worse than one that halts loudly. Two supervision
guarantees are load-bearing for the zero-loss / fail-closed posture:

1. A **fail-closed halt must be PERMANENT** — a destructive schema change, a slot invalidation, a
   sustained sink-lag backlog, or a sink write fault must NOT be auto-retried into a livelock or
   a silent partial delivery. The operator must restart with intent (drop the slot, fix the sink,
   etc.) before delivery resumes.
2. A **fresh `Connection` resuming from the durable checkpoint must never be paired with a stale
   in-memory buffer** — a half-received transaction, an open batch, or a pending snapshot window
   held over from a crashed assembler would corrupt the exactly-once invariant on resume.

## Decision

- **The per-pipeline supervisor is `:one_for_all`** over the pipeline children —
  `[AssemblerServer, Connection]` in sink-owned mode, `[CheckpointStore, AssemblerServer,
  Connection]` in lib mode (CheckpointStore + AssemblerServer start before the Connection, so
  the Connection has somewhere to cast to before it streams). Any child crashing tears the
  WHOLE set down and re-derives from the durable watermark — the AssemblerServer's volatile
  buffers (in-flight transaction, open batch, snapshot window) are explicitly discarded on
  reconnect (`{:seed_lib_checkpoint}`, `{:reset_batch}`, `{:reset_streams}`,
  `{:reset_snapshot_window}`). (`lib/replicant/pipeline.ex`.)
- **The DynamicSupervisor pipeline child is `:temporary`** — a halted pipeline is never restarted
  by the supervisor; restart happens only via an explicit operator `Replicant.start_link/1`.
  (`lib/replicant/supervisor.ex`.)
- **A halt tears the pipeline down from an unlinked spawned process.** `Replicant.Supervisor.halt/2`
  does NOT self-crash the caller (a crash exit would race a `:one_for_all` restart); it spawns an
  unlinked process that performs the `DynamicSupervisor.terminate_child`, so the halting process
  returns normally and the pipeline is gone for good. The `reason` argument is **discarded**
  (Critical Rule 1) — the only outward signal is value-free telemetry. (`lib/replicant/supervisor.ex`,
  `lib/replicant/assembler_server.ex`.)

## Options Considered

### Option A: `:one_for_one` over AssemblerServer + Connection
**Cons:** A crashed Connection would restart fresh while a stale AssemblerServer buffer survived
— re-pairing a fresh checkpoint read with a half-received transaction buffer. This is a
silent-corruption window for exactly-once. Rejected.

### Option B: `:permanent` / `:transient` DynamicSupervisor child (auto-restart on halt)
**Cons:** A fail-closed halt that auto-restarts is a livelock (the same fault trips immediately on
restart), and worse it can mask a destructive condition (e.g. re-creating an invalidated slot) into
a silent partial delivery. Rejected — a halt must be permanent until the operator intervenes.

### Option C: Self-crash the pipeline from the halting process
**Cons:** A `Process.exit(self, :kill)` or `Supervisor.stop(self)` from within the pipeline races
the `:one_for_all`/DynamicSupervisor restart semantics and can deadlock or double-restart. Rejected
in favor of teardown-from-an-unlinked-process.

### Option D: `:one_for_all` + `:temporary` + external teardown (adopted)
| Dimension | Assessment |
|-----------|------------|
| Complexity | Low — standard OTP strategies, one external-teardown helper |
| Correctness | High — no stale-buffer pairing; halts are permanent; no restart race |
| Operability | Trade: every halt requires operator action (by design) |

## Trade-off Analysis

The adopted design makes "halt" mean halt: the pipeline is gone, the durable watermark is
unchanged, and the operator restarts with intent. The `:one_for_all` scoping is the single
mechanism that prevents the fresh-Connection-plus-stale-buffer corruption class. The
external-teardown trick is the one non-obvious bit — it exists precisely so the halting process
does not race its own supervisor.

## Consequences

- **Easier:** Every fail-closed condition (slot invalidation, destructive schema change, sink write
  fault, checkpoint-store exhaustion, §4 sink-too-slow, spill-exhausted, command-error watchdog,
  publication-missing, data_gap) is a permanent halt with a single, consistent teardown path; the
  exactly-once invariant cannot be corrupted by a stale buffer on resume.
- **Harder:** Every halt is an operator action — there is no auto-recovery from a fail-closed
  condition (by design). The external-teardown mechanism is comment-load-bearing; a future edit
  that self-crashes the pipeline reintroduces the restart race.
- **Revisit if:** proto-v4 parallel streaming (concurrent apply of interleaved streams) is added —
  it would break the AssemblerServer's "serial apply in commit order" baseline and require a
  worker pool with an ordered-commit barrier, a rework of this supervision design's core
  invariant. The current Sink behaviour is unaffected (a parallel-apply txn still delivers as one
  `%Transaction{}`), so this ADR does not block that future addition at the contract layer.
