defmodule Replicant.Sink do
  @moduledoc """
  The pluggable sink contract. A sink durably persists a transaction
  AND its checkpoint atomically, returns the commit LSN, and is idempotent on the
  transaction-granularity watermark (skip any `txn.commit_lsn <= checkpoint`;
  upsert rows by table PK).

  ## Callbacks

  | callback | required | purpose |
  |---|---|---|
  | `c:checkpoint/0` | in sink-owned mode | last durably-persisted commit LSN (`nil` = never). In **lib mode** (`:checkpoint_store` configured) the library owns the checkpoint and this callback is not required. |
  | `c:handle_transaction/1` | **yes** | persist the txn + checkpoint atomically, return `{:ok, lsn}` |
  | `c:handle_batch/1` | in batch-delivery mode | deliver N txns + checkpoint atomically |
  | `c:handle_schema_change/2` | no | accept/decline a `SchemaChange`; default halts destructive |
  | `c:sink_kind/0` | no | `:state_mirror` (default) or `:append_log` |
  | `c:handle_snapshot/2` | no | persist a snapshot batch; redo-safe reset on `first_for_table?` |
  | `c:handle_snapshot_complete/1` | no | snapshot handoff commit; persist `checkpoint := snapshot_lsn` |
  | `c:snapshot_progress/0` | in incremental-snapshot mode | most recent persisted `context.progress` resume token (`nil` = no backfill in progress / never started) |

  Every callback is an `@optional_callback`, so any sink compiles clean under
  `--warnings-as-errors` regardless of which it implements — the behaviour imposes no
  compile-time obligation. Which callbacks are actually REQUIRED is enforced at start by
  `Config`, per mode: `handle_transaction/1` (default sink-owned) or `handle_batch/1`
  (batch-delivery mode), plus `checkpoint/0` in sink-owned mode. The
  Assembler dispatches the optional ones via `function_exported?/3`, providing the
  documented default. The snapshot callbacks gate in two pairs: `supports_snapshot?/1`
  gates `snapshot: true` on `handle_snapshot/2` + `handle_snapshot_complete/1` BOTH being
  present; `supports_incremental_snapshot?/1` gates `snapshot: [mode: :incremental]` on
  `handle_snapshot/2` + `snapshot_progress/0` (`handle_snapshot_complete/1` is NOT
  required for incremental).

  ## Lib mode (non-transactional sinks)

  When the pipeline is configured with `:checkpoint_store`, the library owns the
  checkpoint: it reads it on connect and writes it (to a Postgres table) AFTER
  `handle_transaction/1` returns `{:ok, _}`, then advances the ack. A lib-mode sink
  therefore implements ONLY `handle_transaction/1` (persisting DATA; it still returns
  `{:ok, lsn}` — the value is ignored: the library owns the checkpoint in lib mode) and need not
  implement `checkpoint/0`. `Config` enforces `checkpoint/0` presence at start for
  sink-owned mode. The guarantee is at-least-once, dup bounded to one transaction,
  never loss — NOT effect-once (a non-transactional sink cannot dedup).

  ## Batched checkpointing (lib mode)

  When `:checkpoint_store` carries a `:batch` option, the library writes the checkpoint and
  advances the slot ack once per BATCH of up to `max_transactions` transactions (or every
  `max_delay_ms`, or when the batch's WAL span reaches an auto-derived lag-safety cap) — but
  `handle_transaction/1` is still called PER TRANSACTION and the contract is unchanged. This
  amortizes the per-transaction checkpoint-store round-trip. The trade-off is honest: a crash
  or graceful stop mid-batch re-delivers up to one batch (dup ≤ `max_transactions`), never loss.
  Absent `:batch`, checkpointing stays per-transaction (dup ≤ one transaction).

  ## Batched delivery (sink-owned mode)

  When the top-level `batch_delivery` config is set, delivery routes through `c:handle_batch/1`
  instead of `c:handle_transaction/1`: the library accumulates committed transactions and hands
  the sink a batch to persist (rows + checkpoint) in one atomic write. Because the write is
  atomic, effect-once is PRESERVED (dup = 0), stronger than lib-mode batched checkpointing's
  dup ≤ `max_transactions`. `batch_delivery` is sink-owned only (mutually exclusive with
  `:checkpoint_store`) and requires the sink to implement `c:handle_batch/1`
  (`supports_batch?/1`); a sink missing it is rejected at start (`:batch_unsupported`).
  """

  @doc "Last durably-persisted commit LSN, for resume AND as the dedup watermark."
  @callback checkpoint() :: {:ok, Replicant.lsn() | nil} | {:error, term()}

  @doc """
  Persist the whole transaction AND its checkpoint atomically; return the commit
  LSN. Idempotency: skip if `txn.commit_lsn <= checkpoint()`; upsert rows by PK.
  """
  @callback handle_transaction(Replicant.Transaction.t()) ::
              {:ok, Replicant.lsn()} | {:error, term()}

  @doc """
  Optional. Deliver a BATCH of committed transactions as ONE atomic unit. Enabled by
  the top-level `batch_delivery` config (sink-owned mode only). `transactions` arrives in
  ascending `commit_lsn` order; persist ALL of their rows AND `checkpoint := the last (highest)
  commit_lsn` in ONE atomic database transaction (or, at minimum, checkpoint-after-all-persist),
  returning that LSN. Idempotency is unchanged: skip any `commit_lsn <= checkpoint`; upsert rows
  by PK.

  HARD OBLIGATION: the data + checkpoint write is atomic. The effect-once guarantee (dup = 0
  across a mid-batch teardown) rests on it, exactly as `handle_snapshot/2`'s redo-safety rests on
  `first_for_table?`. A non-`{:ok, _}` return (or a raise/throw/exit) halts the pipeline
  fail-closed; the batch is discarded un-acked and re-delivered on resume (deduped to zero net
  effect by the idempotent sink).

  The arity is frozen at 1 for 1.0: unlike `handle_snapshot/2` / `handle_message/2`, NO context
  arg is provided — the batch's high-water LSN is `List.last(transactions).commit_lsn`, and each
  transaction carries its own `commit_lsn`. Adding a context arg later is a 2.0 callback change.
  """
  @callback handle_batch([Replicant.Transaction.t()]) ::
              {:ok, Replicant.lsn()} | {:error, term()}

  @doc """
  Optional. Deliver a NON-TRANSACTIONAL logical-decoding message (`pg_logical_emit_message` with
  `transactional => false`). Such a message arrives STANDALONE (no Begin/Commit bracket) and has
  no PK or `commit_lsn` dedup key, so the guarantee is **at-least-once, duplicates possible** on a
  reconnect between the `{:ok}` return and the checkpoint advancing — mirroring the lib-mode
  `handle_transaction/1` downgrade. A transactional message (`transactional => true`) does NOT
  route here: it rides `%Transaction.messages` and inherits the txn path's effect-once.

  On `:ok` / `{:ok, _}` the library advances the checkpoint to the message's LSN; a non-`:ok`
  return (or a raise/throw/exit) halts the pipeline fail-closed. `context` carries `%{lsn: lsn}`.
  Enabled by the top-level `messages: true` config; a sink missing this callback is rejected at
  start (`:messages_unsupported`).
  """
  @callback handle_message(Replicant.Decoder.Messages.Message.t(), context) ::
              :ok | {:ok, term()} | {:error, term()}
            when context: %{required(:lsn) => Replicant.lsn()}

  @doc """
  Optional. When a sink does not implement this, the Assembler (Task 13) applies
  the default: an `:additive` change auto-applies; a `:destructive` change halts
  the pipeline fail-closed. Implement it to accept (`:ok`) or decline (`{:error, _}`)
  a `Replicant.SchemaChange`.
  """
  @callback handle_schema_change(Replicant.SchemaChange.t(), map()) :: :ok | {:error, term()}

  @doc "Optional (default `:state_mirror`). `:append_log` sinks receive appends."
  @callback sink_kind() :: :state_mirror | :append_log

  @doc """
  Optional. Persist a batch of snapshot (backfill) rows for `context.table` — each a
  `%Replicant.Change{op: :snapshot}` — upserting by table PK.

  This callback serves BOTH snapshot modes with mode-conditional obligations
  (spec §6.1 — normative):

  | Obligation | v1 (`snapshot: true`) | incremental (`snapshot: [mode: :incremental]`) |
  |---|---|---|
  | Non-`:ok` return / raise | aborts + re-runs the WHOLE snapshot | halts fail-closed; resume re-delivers from the last durable chunk |
  | `first_for_table?` | true on the first batch per table, EVERY attempt — clear the table's prior SNAPSHOT-ORIGIN mirror rows, and MUST NOT clear rows the live stream (`handle_transaction/1`) applied | true on a table's first chunk of a FRESH run AND on every PK-less-table redo attempt — same snapshot-origin-only clearing obligation |
  | Checkpoint | never advance here | never advance here |
  | `context.progress` | absent | sink-owned mode MUST persist the token atomically with the chunk (effect-once); ignoring it degrades resume to restart-from-zero (safe, dup-only) |
  | `context.backfill_complete?` | absent | true ONLY on the dedicated final call (`changes: []`), delivered at-least-once until durable |
  | Completion | `c:handle_snapshot_complete/1` follows | `c:handle_snapshot_complete/1` is NEVER called (it would REGRESS the stream-advanced checkpoint) |

  In BOTH modes, the library guarantees at least one call per publication table (even a
  zero-row table), so `first_for_table?` always fires and the redo-safety reset always
  happens. A `:state_mirror` sink MUST distinguish snapshot-origin rows from stream-origin
  rows for that reset: `first_for_table?` clears only the SNAPSHOT-loaded rows and must
  PRESERVE rows the live stream (`c:handle_transaction/1`) applied — under frontier ordering
  a stream update can land BEFORE the first chunk closes, and a blanket clear would lose it.
  `context.snapshot_lsn` is the snapshot's consistent point.
  """
  @callback handle_snapshot([Replicant.Change.t()], context) :: :ok | {:error, term()}
            when context: %{
                   required(:snapshot_lsn) => Replicant.lsn(),
                   required(:table) => String.t(),
                   required(:first_for_table?) => boolean(),
                   optional(:progress) => binary() | nil,
                   optional(:backfill_complete?) => boolean()
                 }

  @doc """
  Optional. The snapshot handoff commit, called once after all batches. Durably persist
  `checkpoint := snapshot_lsn` and return it. Until this succeeds the checkpoint stays
  `nil`, so a crash before it re-runs the whole snapshot. A non-`{:ok, _}`
  return (or a raise/throw/exit) halts the pipeline fail-closed.
  """
  @callback handle_snapshot_complete(snapshot_lsn :: Replicant.lsn()) ::
              {:ok, Replicant.lsn()} | {:error, term()}

  @doc """
  Optional (REQUIRED for sink-owned incremental snapshot — `Config` gates on it).
  Return the most recent `context.progress` token persisted atomically with a chunk
  (`nil` = no backfill in progress / never started). Read ONCE at pipeline start
  for resume. A read fault halts fail-closed — the resume decision is not
  dedup-recoverable (spec §6.1/§8).
  """
  @callback snapshot_progress() :: {:ok, binary() | nil} | {:error, term()}

  @optional_callbacks [
    checkpoint: 0,
    handle_transaction: 1,
    handle_batch: 1,
    handle_message: 2,
    handle_schema_change: 2,
    sink_kind: 0,
    handle_snapshot: 2,
    handle_snapshot_complete: 1,
    snapshot_progress: 0
  ]

  @doc """
  The sink's kind, defaulting to `:state_mirror` when `c:sink_kind/0` is not
  implemented. (Plan 2's start guard refuses a `:state_mirror` sink from an empty
  checkpoint without `go_forward_only: true`.)
  """
  @spec sink_kind(module()) :: :state_mirror | :append_log
  def sink_kind(module) do
    if function_exported?(module, :sink_kind, 0) do
      # Coerce ANY unrecognized return to the strict :state_mirror default. sink_kind
      # gates the go-forward start guard; treating a typo'd/invalid kind as the laxer
      # :append_log would fail OPEN on a safety check and let an empty state-mirror
      # sink partial-deliver. Only an explicit :append_log bypasses the guard.
      case module.sink_kind() do
        :append_log -> :append_log
        _other -> :state_mirror
      end
    else
      :state_mirror
    end
  end

  @doc """
  True when `module` implements BOTH snapshot callbacks — the config gate for
  `snapshot: true`. A partial implementation is rejected at start
  (`:snapshot_unsupported`) rather than half-running a backfill.
  """
  @spec supports_snapshot?(module()) :: boolean()
  def supports_snapshot?(module) do
    function_exported?(module, :handle_snapshot, 2) and
      function_exported?(module, :handle_snapshot_complete, 1)
  end

  @doc """
  True when `module` implements `handle_snapshot/2` AND `snapshot_progress/0` — the
  config gate for sink-owned `snapshot: [mode: :incremental]` (spec §6.1).
  `handle_snapshot_complete/1` is deliberately NOT required: incremental never calls
  it. A partial sink is rejected at start (`:snapshot_unsupported`), fail-closed.
  """
  @spec supports_incremental_snapshot?(module()) :: boolean()
  def supports_incremental_snapshot?(module) do
    function_exported?(module, :handle_snapshot, 2) and
      function_exported?(module, :snapshot_progress, 0)
  end

  @doc """
  True when `module` implements `handle_batch/1` — the config gate for `batch_delivery`
  A `batch_delivery` config whose sink is missing this callback is rejected at
  start (`:batch_unsupported`) rather than silently falling back to per-transaction delivery.
  """
  @spec supports_batch?(module()) :: boolean()
  def supports_batch?(module) do
    function_exported?(module, :handle_batch, 1)
  end

  @doc """
  True when `module` implements `handle_message/2` — the config gate for `messages: true`
  (spec §6.3 / A2). A `messages: true` config whose sink is missing this callback is rejected
  at start (`:messages_unsupported`) rather than silently dropping non-transactional messages.
  """
  @spec supports_messages?(module()) :: boolean()
  def supports_messages?(module) do
    function_exported?(module, :handle_message, 2)
  end
end
