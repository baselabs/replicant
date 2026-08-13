defmodule Replicant.Assembler do
  @moduledoc """
  Assembles a stream of decoded pgoutput messages into a `Replicant.Transaction`
  and applies the sink synchronously per transaction (spec §4/§6).

  The state machine:
    * `Begin`      — open a transaction buffer.
    * `Relation`   — cache the relation; diff against the cached one and emit/halt
                     on a schema change (spec §9). `:additive` auto-applies;
                     `:destructive` halts fail-closed (or delegates to the sink's
                     optional `handle_schema_change/2`).
    * `Type`       — cache the type name.
    * `Insert/Update/Delete/Truncate` — accumulate a `Change`, casting values via
                     `Replicant.Casting.Types.cast_record/2` and extracting
                     unchanged-TOAST sentinels into `change.unchanged` (spec §7).
                     A row/truncate for a relation that was never cached halts
                     fail-closed (we cannot identify the table — never emit a
                     table-less change that would be checkpointed as success).
    * `Commit`     — attach the transaction-granularity commit LSN; pre-skip if
                     `commit_lsn <= checkpoint()` (spec §2); else call
                     `sink.handle_transaction/1` synchronously and return the LSN.

  The Connection (Plan 2) drives this; the Assembler never blocks the keepalive
  path itself — that is Plan 2's process-structure concern.

  ## Telemetry (spec §10, Assembler-owned events)
  Emits the value-free events whose lifetime lives in the Assembler:
  `[:replicant, :transaction, :assembled]`, `[:replicant, :sink, :committed|:failed]`,
  `[:replicant, :schema_change, :additive|:halted]` — metadata scrubbed through the
  `Replicant.Telemetry` allowlist. The `:connection`/`:checkpoint` events are the
  Connection's (Plan 2).

  ## Value-free boundary
  `handle_message/2` wraps its body in `rescue` AND `catch`: a raise from the
  casting path (a malformed numeric/bytea in `Types.cast_record/2`) is scrubbed
  into `{:halt, %Error{reason: :decode_failure}}`; a sink raise/throw/exit is
  scrubbed by `apply_sink/2` into `{:halt, %Error{reason: :sink_failed}}` — never
  inspecting a throw/exit reason, which (e.g. a `GenServer.call` timeout carrying
  the transaction) can embed row values (Critical Rule 1). A row/commit message
  arriving before any `Begin` halts as `:config_invalid` rather than crashing.
  """

  alias Replicant.{
    Casting.Types,
    Change,
    Decoder.Messages,
    Error,
    SchemaChange,
    Spill,
    Telemetry,
    Transaction
  }

  alias Messages.{
    Begin,
    Commit,
    Delete,
    Insert,
    Message,
    Origin,
    Relation,
    StreamAbort,
    StreamCommit,
    StreamStart,
    StreamStop,
    Truncate,
    Type,
    Update
  }

  @type lsn :: Replicant.lsn()

  @type t :: %__MODULE__{
          sink: module(),
          relations: %{non_neg_integer() => Relation.t()},
          projected: %{non_neg_integer() => [Change.Column.t()]},
          txn: buffer() | nil,
          ordinal: non_neg_integer(),
          mode: :sink_owned | :lib,
          checkpoint_writer: (lsn() -> :ok | {:error, term()}) | nil,
          lib_checkpoint: lsn() | nil,
          slot_name: String.t() | nil,
          batch: keyword() | nil,
          batch_count: non_neg_integer(),
          batch_txns: [Transaction.t()],
          batch_spill_paths: [String.t()],
          last_buffered_changes: [Change.t()] | :spilled,
          pending_lsn: lsn() | nil,
          stream_floor: lsn() | nil,
          stream_txns: %{non_neg_integer() => stream_buf()},
          current_stream_xid: non_neg_integer() | nil,
          max_concurrent_txns: pos_integer() | nil,
          spill: keyword() | nil,
          max_inflight_lag: pos_integer() | nil,
          spilled_total: non_neg_integer(),
          spill_fault: Error.t() | nil
        }

  @type buffer :: %{
          begin_lsn: lsn() | nil,
          xid: non_neg_integer() | nil,
          changes: [Change.t()],
          messages: [Message.t()],
          byte_size: non_neg_integer()
        }

  @type stream_buf :: %{
          changes: [{non_neg_integer(), Change.t()}],
          messages: [Message.t()],
          byte_size: non_neg_integer(),
          resident_bytes: non_neg_integer(),
          spilled_bytes: non_neg_integer(),
          spilled_by_subxid: %{non_neg_integer() => non_neg_integer()},
          aborted: MapSet.t(non_neg_integer()),
          spill: Replicant.Spill.handle() | nil,
          # Shared per-txn ordinal counter (spec §5, mirrors the v1 `asm.ordinal`): incremented per
          # accumulated change AND per attached transactional message, so changes and messages carry
          # ONE ascending numbering space and a consumer can interleave `changes` + `messages` by
          # `ordinal`. Stamped at accumulation/attach and PRESERVED at replay (never re-numbered).
          seq: non_neg_integer()
        }

  defstruct [
    :sink,
    :txn,
    relations: %{},
    projected: %{},
    ordinal: 0,
    mode: :sink_owned,
    checkpoint_writer: nil,
    lib_checkpoint: nil,
    slot_name: nil,
    batch: nil,
    batch_count: 0,
    batch_txns: [],
    batch_spill_paths: [],
    last_buffered_changes: [],
    pending_lsn: nil,
    stream_floor: nil,
    stream_txns: %{},
    current_stream_xid: nil,
    max_concurrent_txns: nil,
    spill: nil,
    max_inflight_lag: nil,
    spilled_total: 0,
    spill_fault: nil
  ]

  @doc """
  Create an assembler bound to `sink` (a module implementing `Replicant.Sink`).
  `opts` selects the checkpoint mode:

    * (default) sink-owned — the sink returns its own checkpoint; `checkpoint/0`
      is the watermark read live per Commit;
    * `mode: :lib` — the library owns the checkpoint: `:checkpoint_writer` (a
      `(lsn -> :ok | {:error, _})`) persists it after the sink, `:lib_checkpoint`
      seeds the in-memory watermark used by the pre-skip, and `:slot_name` labels
      the value-free `[:replicant, :checkpoint_store, :failed]` telemetry (spec §10).
  """
  @spec new(module(), keyword()) :: t()
  def new(sink, opts \\ []) do
    %__MODULE__{
      sink: sink,
      mode: Keyword.get(opts, :mode, :sink_owned),
      checkpoint_writer: Keyword.get(opts, :checkpoint_writer),
      lib_checkpoint: Keyword.get(opts, :lib_checkpoint),
      slot_name: Keyword.get(opts, :slot_name),
      batch: Keyword.get(opts, :batch),
      max_concurrent_txns: Keyword.get(opts, :max_concurrent_txns),
      spill: Keyword.get(opts, :spill),
      max_inflight_lag: Keyword.get(opts, :max_inflight_lag)
    }
  end

  @doc """
  Accumulate the raw WAL payload byte-size of the message about to be handled into
  the open transaction buffer, for the `byte_size` metadata on
  `[:replicant, :transaction, :assembled]` (spec §10). `Replicant.AssemblerServer`
  calls this with `byte_size(payload)` before each `handle_message/2`. It is a
  no-op before any `Begin` (bytes arriving outside a transaction — a lone
  Relation/Type/Origin — have no buffer to attribute to), so a stray pre-Begin
  payload never crashes here.
  """
  @spec observe_bytes(t(), non_neg_integer()) :: t()
  # A streamed segment's bytes route to its stream buffer. The `is_map_key/2` guard makes
  # this clause NEVER `Map.fetch!`-raise on an absent buffer — `observe_bytes/2` runs OUTSIDE
  # `handle_message/2`'s value-free rescue (it is called by `AssemblerServer.handle_cast`), so
  # a raise here would be an unguarded GenServer crash, not a value-free halt. With no open
  # stream (or an absent buffer) this clause is skipped and the `txn:` clauses below apply.
  def observe_bytes(%__MODULE__{current_stream_xid: xid, stream_txns: streams} = asm, bytes)
      when is_integer(xid) and is_integer(bytes) and bytes >= 0 and is_map_key(streams, xid) do
    buf = Map.fetch!(streams, xid)
    buf = %{buf | byte_size: buf.byte_size + bytes, resident_bytes: buf.resident_bytes + bytes}
    asm = %{asm | stream_txns: Map.put(streams, xid, buf)}

    case __MODULE__.Streaming.maybe_spill(asm) do
      {:ok, asm} -> asm
      {:halt, err, asm} -> %{asm | spill_fault: err}
    end
  end

  def observe_bytes(%__MODULE__{txn: nil} = asm, _bytes), do: asm

  def observe_bytes(%__MODULE__{txn: buffer} = asm, bytes)
      when is_integer(bytes) and bytes >= 0 do
    %{asm | txn: %{buffer | byte_size: buffer.byte_size + bytes}}
  end

  @doc """
  Handle one decoded message. Returns:
    * `{:ok, t()}` — accumulated, no boundary crossed.
    * `{:transaction, Transaction.t(), lsn(), t()}` — Commit, sink committed.
    * `{:skipped, lsn(), t()}` — Commit but `commit_lsn <= checkpoint` (watermark skip).
    * `{:buffered, t()}` — lib+batch: applied to the sink, checkpoint pending (no ack yet).
    * `{:flush, reason, t()}` — lib+batch: the count/span cap tripped; the AssemblerServer must flush.
    * `{:schema_change, SchemaChange.t(), t()}` — additive schema change applied.
    * `{:halt, SchemaChange.t() | term(), t()}` — destructive schema change, sink
      failure, an unidentifiable-relation row, or a value-bearing raise (e.g.
      casting a malformed numeric) — fail-closed, value-free.

  `handle_message/2` is itself the value-free boundary for the casting path: the
  vendored `Types.cast_record/2` raises `ArgumentError` on malformed numerics /
  bytea (a corrupted stream), and those exceptions embed row bytes. The public
  wrapper scrubs any such raise into `{:halt, %Error{reason: :decode_failure}}`
  and any stray throw/exit (defense-in-depth) likewise, never inspecting the
  reason (Critical Rule 1) — symmetric to `Replicant.Decoder.decode/1`.
  """
  @spec handle_message(t(), struct()) ::
          {:ok, t()}
          | {:transaction, Transaction.t(), lsn(), t()}
          | {:skipped, lsn(), t()}
          | {:schema_change, SchemaChange.t(), t()}
          | {:buffered, t()}
          | {:flush, atom(), t()}
          | {:flush_before_message, atom(), t(), Message.t()}
          | {:message_delivered, lsn(), t()}
          | {:halt, term(), t()}
  def handle_message(asm, message) do
    do_handle_message(asm, message)
  rescue
    exception -> {:halt, Error.decode_failure(exception), asm}
  catch
    # A throw/exit escaping the casting/dispatch path must not breach the value-free
    # boundary. Never inspect the reason — it can embed row values (Critical Rule 1).
    _kind, _reason -> {:halt, %Error{reason: :decode_failure}, asm}
  end

  defp do_handle_message(%__MODULE__{} = asm, %Begin{final_lsn: lsn, xid: xid}) do
    {:ok,
     %{
       asm
       | txn: %{begin_lsn: lsn, xid: xid, changes: [], messages: [], byte_size: 0},
         ordinal: 0
     }}
  end

  # --- streaming reassembly (spec §5) ---
  #
  # These clauses MUST precede the `txn: nil` "row before Begin" guard below: a streamed
  # change has NO open `txn` buffer, so the guard would otherwise mis-halt it as a "row before
  # Begin". They match only STREAMED traffic — StreamStart/StreamStop, and Insert/Update/Delete/
  # Truncate guarded on `is_integer(xid)` (a non-streamed message carries `xid: nil` and skips
  # these to reach the v1 clauses unchanged). Streamed Relation/Type messages fall through to the
  # existing v1 clauses (relation schema is cached globally, not per-stream — correct).

  defp do_handle_message(%__MODULE__{} = asm, %StreamStart{} = msg),
    do: __MODULE__.Streaming.handle_stream_start(asm, msg)

  defp do_handle_message(%__MODULE__{} = asm, %StreamStop{} = msg),
    do: __MODULE__.Streaming.handle_stream_stop(asm, msg)

  # StreamCommit: delegate to Streaming (which checks a recorded `spill_fault` FIRST, then routes
  # the buffer to empty-suppression / in-memory replay / lazy-Reader delivery — spec §5/§7/§8).
  defp do_handle_message(%__MODULE__{} = asm, %StreamCommit{} = msg),
    do: __MODULE__.Streaming.handle_stream_commit(asm, msg)

  # StreamAbort (whole-transaction or subtransaction/savepoint): delegate to Streaming, which folds
  # the top==sub whole-abort and the sub abort into one handler (spec §5).
  defp do_handle_message(%__MODULE__{} = asm, %StreamAbort{} = msg),
    do: __MODULE__.Streaming.handle_stream_abort(asm, msg)

  defp do_handle_message(%__MODULE__{} = asm, %Insert{
         xid: sx,
         relation_id: rid,
         tuple_data: tuple
       })
       when is_integer(sx) do
    stream_accumulate(asm, :insert, rid, tuple, nil, sx)
  end

  defp do_handle_message(%__MODULE__{} = asm, %Update{
         xid: sx,
         relation_id: rid,
         tuple_data: tuple,
         old_tuple_data: old_tuple,
         changed_key_tuple_data: key_tuple
       })
       when is_integer(sx) do
    stream_accumulate(asm, :update, rid, tuple, old_spec(old_tuple, key_tuple), sx)
  end

  defp do_handle_message(%__MODULE__{} = asm, %Delete{
         xid: sx,
         relation_id: rid,
         old_tuple_data: old_tuple,
         changed_key_tuple_data: key_tuple
       })
       when is_integer(sx) do
    stream_accumulate(asm, :delete, rid, nil, old_spec(old_tuple, key_tuple), sx)
  end

  defp do_handle_message(%__MODULE__{} = asm, %Truncate{xid: sx, truncated_relations: rids})
       when is_integer(sx) do
    stream_accumulate_truncate(asm, rids, sx)
  end

  # (a) A STREAMED transactional message (spec §7.1) arrives inside an open stream segment. The
  # `current_stream_xid: top when is_integer(top)` guard keeps this clause disjoint from the v1 (b)/(c)
  # message clauses below (a v1 message has no open stream) and ahead of the `txn: nil` row-before-
  # Begin guard — order preserved, only the body moved to Streaming.handle_streamed_message/2.
  defp do_handle_message(
         %__MODULE__{current_stream_xid: top} = asm,
         %Message{transactional?: true} = msg
       )
       when is_integer(top),
       do: __MODULE__.Streaming.handle_streamed_message(asm, msg)

  # A row/commit message that arrives before any Begin is malformed → fail-closed
  # (without this guard the nil-buffer crash is caught by the outer rescue and
  # miscategorised as :decode_failure). Relation/Type/Origin may legitimately
  # precede a Begin, so they are NOT guarded here.
  defp do_handle_message(%__MODULE__{txn: nil} = asm, msg)
       when is_struct(msg, Insert) or is_struct(msg, Update) or is_struct(msg, Delete) or
              is_struct(msg, Truncate) or is_struct(msg, Commit) do
    {:halt, %Error{reason: :config_invalid, shape: "row/commit message before Begin"}, asm}
  end

  defp do_handle_message(%__MODULE__{} = asm, %Relation{} = rel) do
    handle_relation(asm, rel)
  end

  defp do_handle_message(%__MODULE__{} = asm, %Type{} = _type) do
    # Types map to OIDs already resolved at decode time via OidDatabase; cache is
    # informational only. No-op for v1.
    {:ok, asm}
  end

  defp do_handle_message(%__MODULE__{} = asm, %Origin{} = _origin) do
    {:ok, asm}
  end

  defp do_handle_message(%__MODULE__{} = asm, %Insert{relation_id: rid, tuple_data: tuple}) do
    accumulate(asm, :insert, rid, tuple, nil)
  end

  defp do_handle_message(%__MODULE__{} = asm, %Update{
         relation_id: rid,
         tuple_data: tuple,
         old_tuple_data: old_tuple,
         changed_key_tuple_data: key_tuple
       }) do
    accumulate(asm, :update, rid, tuple, old_spec(old_tuple, key_tuple))
  end

  defp do_handle_message(%__MODULE__{} = asm, %Delete{
         relation_id: rid,
         old_tuple_data: old_tuple,
         changed_key_tuple_data: key_tuple
       }) do
    accumulate(asm, :delete, rid, nil, old_spec(old_tuple, key_tuple))
  end

  defp do_handle_message(
         %__MODULE__{txn: buffer, relations: relations, ordinal: ordinal} = asm,
         %Truncate{
           truncated_relations: rids
         }
       ) do
    if Enum.all?(rids, &Map.has_key?(relations, &1)) do
      # Each truncated relation gets its own monotonic ordinal so a truncate never
      # collides with a following change's ordinal in the same transaction.
      {changes, next_ordinal} =
        Enum.map_reduce(rids, ordinal, fn rid, ord ->
          rel = Map.fetch!(relations, rid)

          change = %Change{
            op: :truncate,
            schema: rel.namespace,
            table: rel.name,
            commit_lsn: buffer.begin_lsn,
            ordinal: ord
          }

          {change, ord + 1}
        end)

      {:ok,
       %{
         asm
         | txn: %{buffer | changes: Enum.reverse(changes, buffer.changes)},
           ordinal: next_ordinal
       }}
    else
      {:halt, %Error{reason: :config_invalid, shape: "truncate for uncached relation"}, asm}
    end
  end

  defp do_handle_message(%__MODULE__{txn: buffer} = asm, %Commit{
         lsn: commit_lsn,
         commit_timestamp: ts
       }) do
    txn = %Transaction{
      commit_lsn: commit_lsn || buffer.begin_lsn,
      commit_timestamp: ts,
      changes: Enum.reverse(buffer.changes),
      messages: Enum.reverse(buffer.messages)
    }

    Telemetry.event(
      [:replicant, :transaction, :assembled],
      %{},
      %{
        change_count: length(txn.changes),
        commit_lsn: txn.commit_lsn,
        byte_size: buffer.byte_size,
        lag_ms: lag_ms(txn.commit_timestamp)
      }
    )

    if skip?(asm, txn) do
      {:skipped, txn.commit_lsn, reset(asm)}
    else
      apply_sink(asm, txn)
    end
  end

  # (b) A v1 transactional message (spec §7.1) attaches to the open transaction's `messages` list
  # (newest-first; reversed to commit order at Commit) with the buffer's current ordinal, then
  # advances it — mirroring a v1 change. Disjoint from (a): a v1 message has no open stream
  # (`current_stream_xid: nil`) and `xid: nil`. The `txn: buffer when buffer != nil` guard keeps it
  # disjoint from (c) (a malformed transactional message before Begin).
  defp do_handle_message(
         %__MODULE__{txn: buffer, ordinal: ordinal} = asm,
         %Message{
           transactional?: true,
           lsn: lsn,
           prefix: prefix,
           content: content
         }
       )
       when buffer != nil do
    msg = %Message{
      transactional?: true,
      lsn: lsn,
      prefix: prefix,
      content: content,
      ordinal: ordinal
    }

    {:ok, %{asm | txn: %{buffer | messages: [msg | buffer.messages]}, ordinal: ordinal + 1}}
  end

  # (c) A malformed v1 transactional message arriving before any Begin (txn: nil). The `xid: nil`
  # guard is LOAD-BEARING: it keeps this clause disjoint from (a), which matches a streamed
  # transactional message carrying `xid` and `txn: nil`. Halt fail-closed value-free (spec §8).
  defp do_handle_message(%__MODULE__{txn: nil} = asm, %Message{transactional?: true, xid: nil}),
    do: {:halt, %Error{reason: :config_invalid, shape: "transactional message before Begin"}, asm}

  # (d) A NON-transactional message (spec §7.1) delivers standalone via handle_message/2. §8.4
  # batch-boundary: when a batch is open (lib-batch OR sink-owned batch), the buffered txns' WAL is
  # not yet durable-checkpointed; delivering+acking the message's LSN now would advance the slot past
  # it → loss on a crash-before-flush. So signal {:flush_before_message, :message_boundary, asm, msg}:
  # the AssemblerServer (Task 9) flushes+acks the batch FIRST, THEN re-dispatches the message — the
  # second pass finds no open batch and reaches deliver_message. loss=0 (§8.4).
  defp do_handle_message(%__MODULE__{} = asm, %Message{transactional?: false} = msg) do
    if batch_pending?(asm) do
      {:flush_before_message, :message_boundary, asm, msg}
    else
      deliver_message(asm, msg)
    end
  end

  defp do_handle_message(%__MODULE__{} = asm, _unsupported) do
    # Any message the decoder could not classify has already been turned into an
    # error at the boundary; reaching here means an unhandled but decodable shape.
    {:halt, %Error{reason: :unsupported_message}, asm}
  end

  # --- streamed-commit delivery (spec §5/§7) — moved to `Replicant.Assembler.Streaming` ---
  # deliver_or_skip_stream/replay_resident/deliver_spilled_stream_commit/deliver_stream_commit/
  # suppress_empty_stream_commit are pure functions on the shared `%Assembler{}` struct. The
  # StreamCommit clause above delegates to `__MODULE__.Streaming.deliver_or_skip_stream/5`; they call
  # back into Core's `skip?/2` + `apply_sink/3` (the dispatch + Rule-1 scrub seam) for the watermark
  # check and delivery. The `[:transaction, :assembled]` event now emits from Streaming (streamed
  # txns) AND Core (v1 Commit) — both value-free; a consistency item, not a leak.

  # --- relation + schema change ---

  defp handle_relation(%__MODULE__{relations: relations} = asm, %Relation{} = rel) do
    case Map.get(relations, rel.id) do
      nil ->
        {:ok, cache_relation(asm, rel)}

      cached ->
        case SchemaChange.classify(cached, rel) do
          nil ->
            {:ok, cache_relation(asm, rel)}

          %SchemaChange{kind: :additive} = sc ->
            Telemetry.event([:replicant, :schema_change, :additive], %{}, %{
              table: sc.table,
              kind: :additive
            })

            {:schema_change, sc, cache_relation(asm, rel)}

          %SchemaChange{kind: :destructive} = sc ->
            delegate_or_halt(asm, sc, rel)
        end
    end
  end

  defp delegate_or_halt(asm, %SchemaChange{} = sc, rel) do
    if function_exported?(asm.sink, :handle_schema_change, 2) do
      case asm.sink.handle_schema_change(sc, %{relation: rel}) do
        :ok ->
          Telemetry.event([:replicant, :schema_change, :additive], %{}, %{
            table: sc.table,
            kind: :additive
          })

          {:schema_change, sc, cache_relation(asm, rel)}

        {:error, _reason} ->
          Telemetry.event([:replicant, :schema_change, :halted], %{}, %{
            table: sc.table,
            kind: :destructive
          })

          {:halt, sc, asm}
      end
    else
      Telemetry.event([:replicant, :schema_change, :halted], %{}, %{
        table: sc.table,
        kind: :destructive
      })

      {:halt, sc, asm}
    end
  end

  # --- change building ---

  # A row for a relation that was never cached cannot be identified (nil table,
  # empty record). Halting fail-closed prevents silently checkpointing dropped
  # data as success (a well-formed pgoutput stream always sends Relation first).
  defp accumulate(
         %__MODULE__{txn: buffer, relations: relations, ordinal: ordinal} = asm,
         op,
         rid,
         new_tuple,
         old_spec
       ) do
    case Map.get(relations, rid) do
      nil ->
        {:halt, %Error{reason: :config_invalid, shape: "row for uncached relation"}, asm}

      %Relation{} = rel ->
        projected = Map.fetch!(asm.projected, rid)
        change = build_change(op, rel, projected, new_tuple, old_spec, buffer.begin_lsn, ordinal)
        {:ok, %{asm | txn: %{buffer | changes: [change | buffer.changes]}, ordinal: ordinal + 1}}
    end
  end

  # --- streamed change accumulation (spec §5) ---

  # Append a streamed row change to its top-level xid's buffer, tagged with the change's own
  # (sub)transaction id (spec §5) — commit_lsn/ordinal are stamped at StreamCommit replay
  # (streamed changes have no LSN before commit). A change outside a StreamStart segment, or for
  # an uncached relation, halts fail-closed.
  defp stream_accumulate(%__MODULE__{current_stream_xid: nil} = asm, _op, _rid, _new, _old, _sx),
    do:
      {:halt, %Error{reason: :config_invalid, shape: "streamed change outside a stream segment"},
       asm}

  defp stream_accumulate(
         %__MODULE__{current_stream_xid: top, relations: relations} = asm,
         op,
         rid,
         new_tuple,
         old_spec,
         subxid
       ) do
    case Map.get(relations, rid) do
      nil ->
        {:halt, %Error{reason: :config_invalid, shape: "streamed row for uncached relation"}, asm}

      %Relation{} = rel ->
        projected = Map.fetch!(asm.projected, rid)
        buf = Map.fetch!(asm.stream_txns, top)

        # Stamp the shared per-txn ordinal at accumulation (mirrors the v1 counter) and preserve it
        # at replay, so it interleaves correctly with any transactional message's ordinal.
        change = build_change(op, rel, projected, new_tuple, old_spec, nil, buf.seq)
        buf = %{buf | changes: [{subxid, change} | buf.changes], seq: buf.seq + 1}
        {:ok, %{asm | stream_txns: Map.put(asm.stream_txns, top, buf)}}
    end
  end

  defp stream_accumulate_truncate(%__MODULE__{current_stream_xid: nil} = asm, _rids, _sx),
    do:
      {:halt,
       %Error{reason: :config_invalid, shape: "streamed truncate outside a stream segment"}, asm}

  defp stream_accumulate_truncate(
         %__MODULE__{current_stream_xid: top, relations: relations} = asm,
         rids,
         subxid
       ) do
    if Enum.all?(rids, &Map.has_key?(relations, &1)) do
      buf = Map.fetch!(asm.stream_txns, top)

      # Each truncated relation gets its own ascending ordinal from the shared per-txn counter
      # (preserved at replay), so a truncate never collides with a following change or message.
      {tagged, next_seq} =
        Enum.map_reduce(rids, buf.seq, fn rid, ord ->
          rel = Map.fetch!(relations, rid)

          {{subxid,
            %Change{
              op: :truncate,
              schema: rel.namespace,
              table: rel.name,
              commit_lsn: nil,
              ordinal: ord
            }}, ord + 1}
        end)

      buf = %{buf | changes: Enum.reverse(tagged, buf.changes), seq: next_seq}
      {:ok, %{asm | stream_txns: Map.put(asm.stream_txns, top, buf)}}
    else
      {:halt, %Error{reason: :config_invalid, shape: "streamed truncate for uncached relation"},
       asm}
    end
  end

  # --- aggregate-resident spill (spec §5) — moved to `Replicant.Assembler.Streaming` ---
  # maybe_spill/spill_largest/flush_buffer_tail/ensure_spill_open/append_tail_or_discard/append_tail
  # are pure functions on the shared `%Assembler{}` struct. `observe_bytes/2` above delegates to
  # `Streaming.maybe_spill/1`; the sink-dispatch + Rule-1 scrub cluster stays here in Core.

  # `old_spec` captures BOTH the old tuple and whether it is key-only:
  #   * a full old tuple (REPLICA IDENTITY FULL, byte 'O') → `{tuple, false}`
  #   * a changed-key tuple (DEFAULT/USING INDEX, byte 'K') → `{tuple, true}`
  # so `old_record` stays key-only under non-FULL identity (spec §7).
  defp old_spec(nil, nil), do: nil
  defp old_spec(nil, key_tuple), do: {key_tuple, true}
  defp old_spec(old_tuple, _key_tuple), do: {old_tuple, false}

  defp build_change(
         op,
         %Relation{} = rel,
         projected_columns,
         new_tuple,
         old_spec,
         commit_lsn,
         ordinal
       ) do
    columns = rel.columns || []
    {record, unchanged} = materialize(new_tuple, columns)
    old_record = materialize_old(old_spec, columns)

    %Change{
      op: op,
      schema: rel.namespace,
      table: rel.name,
      record: record,
      old_record: old_record,
      unchanged: unchanged,
      columns: projected_columns,
      commit_lsn: commit_lsn,
      ordinal: ordinal
    }
  end

  # Cache a relation AND its pre-projected `Change.Column` list together. The column
  # metadata is relation-invariant, so a bulk transaction (N rows of one relation)
  # reuses one shared projection instead of rebuilding it per row.
  defp cache_relation(%__MODULE__{relations: rels, projected: proj} = asm, %Relation{} = rel) do
    %{
      asm
      | relations: Map.put(rels, rel.id, rel),
        projected: Map.put(proj, rel.id, project(rel))
    }
  end

  defp project(%Relation{columns: columns}), do: Enum.map(columns || [], &to_change_column/1)

  defp to_change_column(%Relation.Column{name: n, type: t, flags: f, type_modifier: m}) do
    %Change.Column{name: n, type: t, flags: f, type_modifier: m}
  end

  # Materialise a tuple against column metadata: returns {record_map, unchanged_list}.
  # `:unchanged_toast` sentinels are extracted into `unchanged` (absent from record);
  # `nil` is kept in record as NULL (a value); other values are cast by column type.
  defp materialize(nil, _columns), do: {nil, []}

  defp materialize(tuple, columns) do
    tuple
    |> Tuple.to_list()
    |> Enum.zip(columns)
    |> Enum.reduce({%{}, []}, fn
      {:unchanged_toast, col}, {record, unchanged} ->
        {record, [col.name | unchanged]}

      {value, col}, {record, unchanged} ->
        {Map.put(record, col.name, cast_value(value, col.type)), unchanged}
    end)
    |> then(fn {record, unchanged} -> {record, Enum.reverse(unchanged)} end)
  end

  # Build `old_record`. A key-only tuple (DEFAULT/USING INDEX identity) sends the
  # key columns' values and NULL placeholders for non-key columns; those
  # placeholders are dropped so `old_record` is key-only (spec §7) and a real NULL
  # in a key column is not confused with a non-key placeholder.
  defp materialize_old(nil, _columns), do: nil

  defp materialize_old({tuple, key_only}, columns) do
    {record, _unchanged} = materialize(tuple, columns)
    key_names = key_column_names(columns)

    # Filter to the key columns ONLY when the relation actually declares them (a
    # key-only tuple always corresponds to >= 1 key column in valid pgoutput). If a
    # malformed relation flags no keys, keep the full record rather than emptying
    # old_record — an empty old_record is strictly less useful than the raw tuple.
    if key_only and record != nil and key_names != [] do
      Map.take(record, key_names)
    else
      record
    end
  end

  defp key_column_names(columns) do
    for %Relation.Column{name: name, flags: flags} <- columns,
        is_list(flags) and :key in flags,
        do: name
  end

  defp cast_value(nil, _type), do: nil
  defp cast_value(value, type) when is_binary(value), do: Types.cast_record(value, type)

  # --- sink apply ---

  # Reads the sink's checkpoint. A raise or exit from `checkpoint/0` (e.g. a
  # GenServer.call to a dead checkpoint-store process) is caught and reported as a
  # "no checkpoint" read — the Commit path then applies (fail-open, dup-safe by the
  # §6 idempotency contract) rather than letting the throw/exit breach the boundary.
  defp checkpoint(sink) do
    sink.checkpoint()
  rescue
    _ -> {:ok, nil}
  catch
    _kind, _reason -> {:ok, nil}
  end

  # Lib mode: compare against the IN-MEMORY watermark (seeded from the store,
  # advanced on each write) — no per-transaction store round-trip.
  def skip?(%__MODULE__{mode: :lib, lib_checkpoint: cp}, %Transaction{commit_lsn: lsn}),
    do: is_integer(cp) and lsn <= cp

  # Sink-owned: read the sink's checkpoint live. Reproduces the prior Commit-path
  # semantics exactly — skip only on {:ok, non-nil, >= commit_lsn}; {:ok, nil}
  # (never persisted), {:ok, older_lsn} (newer txn), a {:error, _}, or a
  # raised/exited read all fail-OPEN (dup-safe by the §6 sink idempotency
  # contract: a re-dispatched already-persisted txn is deduped by the sink, and a
  # real store outage makes handle_transaction fail → halt).
  def skip?(%__MODULE__{sink: sink}, %Transaction{commit_lsn: lsn}) do
    case checkpoint(sink) do
      {:ok, cp} -> not is_nil(cp) and lsn <= cp
      _ -> false
    end
  end

  # `spill_path` (default nil) is the on-disk spill file backing a lazy-Reader `%Transaction{}`: for a
  # non-spilled txn it is nil and no file op happens (existing callers behave identically). For a
  # spilled txn delivered per-txn / lib+batch, the file is deleted AFTER the sink call returns for
  # ALL outcomes (in deliver_now, regardless of the result tuple — {:ok} durable OR fault-halt, spec
  # §8). Sink-owned batch buffers the txn (with its Reader) and keeps the file until flush/reset (Task
  # 8), so it RECORDS `spill_path` on `batch_spill_paths` (via buffer_for_delivery/3) for a single
  # delete at flush_sink_batch / reset_batch — it never reaches deliver_now, so no double-delete.
  def apply_sink(%__MODULE__{} = asm, %Transaction{} = txn, spill_path \\ nil) do
    if __MODULE__.Batch.sink_owned_batching?(asm),
      do: __MODULE__.Batch.buffer_for_delivery(asm, txn, spill_path),
      else: deliver_now(asm, txn, spill_path)
  end

  # Per-transaction delivery (sink-owned non-batch, and lib mode, incl. lib+batch which buffers
  # AFTER the sink returns {:ok, _}). Prior apply_sink/2 body plus spill-path threading: `spill_path`
  # is nil for a non-spilled txn (no file op) or the spilled file to delete after the sink call
  # returns — for ALL outcomes, so a fault-halt cannot orphan it (spec §8).
  defp deliver_now(%__MODULE__{sink: sink} = asm, %Transaction{} = txn, spill_path) do
    start_mono = System.monotonic_time(:millisecond)

    result =
      try do
        sink.handle_transaction(txn)
      rescue
        e -> {:sink_raised, e}
      catch
        kind, reason -> {:sink_caught, kind, reason}
      end

    duration = System.monotonic_time(:millisecond) - start_mono

    # The spilled file is dead once the sink call returns — on {:ok} the data is durable; on a fault
    # the pipeline halts and a restart re-streams a FRESH file. Delete it for ALL outcomes (spec §8)
    # so a fault-halt can't orphan a 0600 spill file: at StreamCommit the buffer is already out of
    # `stream_txns` and a per-txn / lib+batch path never records it on `batch_spill_paths`, so the
    # halt's reset_streams |> reset_batch cannot reach it. Best-effort/value-free (no-op on a gone or
    # already-faulted file — safe for the %Spill.Error{} Reader-fault branch). nil for a non-spilled
    # txn (no file op). Sink-owned batch never reaches deliver_now (apply_sink routes it to
    # buffer_for_delivery, which owns the path via batch_spill_paths) — no double-delete.
    if spill_path, do: Spill.rm(spill_path)

    case result do
      {:ok, _lsn} ->
        # The txn (and any transactional messages riding it) is durably delivered — emit the
        # per-message [:message, :received] telemetry (spec §10), symmetric to deliver_message's
        # non-txn emit. lib+batch defers only the CHECKPOINT to flush, not delivery, so this is the
        # correct point for lib/per-txn/lib+batch (sink-owned batch_delivery emits at flush_sink_batch).
        emit_txn_messages_received(txn)

        # lib+batch: buffer the durable txn and defer the store write/ack to the batch
        # flush (spec §7). Per-txn (sink-owned or lib-non-batch): checkpoint-after-persist.
        if __MODULE__.Batch.batching?(asm),
          do: __MODULE__.Batch.buffer_txn(asm, txn, duration),
          else: commit_txn(asm, txn, duration)

      {:sink_raised, %Spill.Error{}} ->
        # The lazy Reader raised a value-free Spill.Error while the sink forced its enumeration (a
        # disk/decode fault mid-read, spec §5). Distinguish it from a sink fault: halt
        # :spill_io_failed (NOT :sink_failed), keeping the fixed reason only — never the exception's
        # message or any row byte (Critical Rule 1).
        Telemetry.event([:replicant, :sink, :failed], %{duration: duration}, %{
          reason: :spill_io_failed
        })

        {:halt, %Error{reason: :spill_io_failed}, asm}

      {:error, _reason} ->
        sink_failed(asm, duration)

      {:sink_raised, e} ->
        # Scrub the sink exception value-free (Critical Rule 1). Reason is
        # :sink_failed (NOT :decode_failure) so a sink fault is distinguishable
        # from a casting fault; only the exception module name is kept for triage.
        sink_failed(asm, duration, safe_shape(e))

      {:sink_caught, _kind, _reason} ->
        # A throw/exit reason may embed row values (a GenServer.call timeout
        # carrying the transaction). Do NOT inspect it — scrub value-free.
        sink_failed(asm, duration)
    end
  end

  # Emit [:replicant, :message, :received] with `transactional: true` for each transactional message
  # riding a DURABLY-delivered txn (spec §10) — the transactional-kind complement to deliver_message's
  # non-txn `transactional: false` emit, so both message kinds are observable on the SAME event tagged
  # by kind. Value-free (Critical Rule 1): only `commit_lsn` (the txn's) + `byte_size` + the boolean —
  # never `prefix`/`content`. `txn.messages` is an in-memory list (never the lazy spill Reader), so
  # iterating it is safe even for a spilled txn.
  defp emit_txn_messages_received(%Transaction{commit_lsn: lsn, messages: messages}) do
    Enum.each(messages, fn %Message{content: content} ->
      Telemetry.event([:replicant, :message, :received], %{}, %{
        commit_lsn: lsn,
        byte_size: byte_size(content || ""),
        transactional: true
      })
    end)
  end

  # Deliver a NON-transactional message standalone via handle_message/2 (spec §7.1, A2 Task 8). This
  # is the value-free Rule 1 boundary at the sink-call site — symmetric to deliver_now/apply_sink for
  # handle_transaction: a raise (the exception message can embed the message `content`) is caught as
  # :sink_raised and the reason is NEVER inspected; a throw/exit likewise as :sink_caught. On :ok /
  # {:ok, _} emit the value-free [:message, :received] event and return {:message_delivered, lsn, asm}
  # (the AssemblerServer, Task 9, advances the ack to `lsn` — durability-before-ack). Any other return
  # halts fail-closed value-free with %Error{reason: :sink_failed} (carries NO value).
  defp deliver_message(%__MODULE__{} = asm, %Message{lsn: msg_lsn} = msg) do
    result =
      try do
        asm.sink.handle_message(msg, %{lsn: msg_lsn})
      rescue
        _ -> :sink_raised
      catch
        _kind, _reason -> :sink_caught
      end

    case result do
      res when res == :ok or (is_tuple(res) and elem(res, 0) == :ok) ->
        Telemetry.event([:replicant, :message, :received], %{}, %{
          commit_lsn: msg_lsn,
          byte_size: byte_size(msg.content || ""),
          transactional: false
        })

        {:message_delivered, msg_lsn, asm}

      _other ->
        Telemetry.event([:replicant, :sink, :failed], %{}, %{reason: :sink_failed})
        {:halt, %Error{reason: :sink_failed}, asm}
    end
  end

  # Per-transaction commit (sink-owned or lib-non-batch): persist the checkpoint to the store
  # BEFORE announcing "committed" (checkpoint-after-persist). A write fault halts fail-closed —
  # never ack without a durable checkpoint. Sink-owned: write_checkpoint/2 is a no-op.
  defp commit_txn(%__MODULE__{} = asm, %Transaction{} = txn, duration) do
    case write_checkpoint(asm, txn.commit_lsn) do
      :ok ->
        # Ack the KNOWN durable position — `txn.commit_lsn`, the LSN just committed —
        # NOT the sink's returned value. A misbehaving sink returning a higher LSN
        # would otherwise advance the slot past un-persisted WAL (loss). Per §6 a
        # correct sink returns exactly `txn.commit_lsn`, so this is contract-equivalent
        # for a correct sink and fail-safe against a buggy one.
        Telemetry.event([:replicant, :sink, :committed], %{duration: duration}, %{
          commit_lsn: txn.commit_lsn
        })

        {:transaction, txn, txn.commit_lsn, advance(asm, txn.commit_lsn)}

      {:error, _} ->
        # Triage fidelity: label a checkpoint-store WRITE fault as a checkpoint_store
        # failure (not :sink_failed) — the sink persisted fine; the store write did not.
        Telemetry.event([:replicant, :checkpoint_store, :failed], %{duration: duration}, %{
          slot_name: asm.slot_name,
          reason: :checkpoint_store_failed
        })

        {:halt, %Error{reason: :checkpoint_store_failed}, asm}
    end
  end

  # Sink-owned: the sink already wrote its own checkpoint atomically. Lib mode:
  # write via the injected writer. A writer raise/exit (e.g. a dead
  # CheckpointStore) is caught value-free and categorised :checkpoint_store_failed
  # (NOT :decode_failure) — never inspecting the reason (Critical Rule 1).
  defp write_checkpoint(%__MODULE__{mode: :sink_owned}, _lsn), do: :ok

  defp write_checkpoint(%__MODULE__{mode: :lib, checkpoint_writer: writer}, lsn)
       when is_function(writer, 1) do
    writer.(lsn)
  rescue
    _ -> {:error, :checkpoint_store_failed}
  catch
    _kind, _reason -> {:error, :checkpoint_store_failed}
  end

  # --- batched checkpointing (spec §7) — buffering + flush moved to `Replicant.Assembler.Batch` ---
  # Core↔Batch call cycle (runtime-resolved in Elixir; both `@moduledoc false`): `apply_sink/3` routes
  # sink-owned batch to `Batch.buffer_for_delivery/3`; `deliver_now/3` routes lib+batch to
  # `Batch.buffer_txn/3`; `flush_batch/2` (public, the AssemblerServer entry) delegates to
  # `Batch.flush_sink_batch/2`/`Batch.flush_lib_batch/2`. Batch calls Core's `reset/1` + `span_base/1`.
  # The Rule-1 batch-flush scrub moves with the flush functions; every other Rule-1 scrub site stays.

  @doc """
  The LAST lib+batch `buffer_txn`'s changes (a `%Change{}` list) so the AssemblerServer can DROP-
  FILTER colliding snapshot-chunk rows, or `:spilled` when that txn's `changes` was a lazy spill-
  backed Enumerable and unenumerable (server taints every open table → re-read instead, spec §2/§4).
  """
  @spec last_buffered_changes(t()) :: [Change.t()] | :spilled
  def last_buffered_changes(%__MODULE__{last_buffered_changes: changes}), do: changes

  # The span-cap base (spec §7): the un-checkpointed window's start = the higher of the durable
  # watermark and the per-stream floor, mirroring the §4 lag floor. On a fresh slot lib_checkpoint
  # is 0 so the floor dominates; after the first flush the watermark dominates.
  def span_base(%__MODULE__{lib_checkpoint: cp, stream_floor: floor}),
    do: max(cp || 0, floor || 0)

  @doc """
  Flush the open batch (spec §7): write ONE store checkpoint at the batch's highest committed
  LSN via the injected `checkpoint_writer` (which carries the bounded retry), advance the
  in-memory watermark to it (keeping `lib_checkpoint` == the durable store checkpoint, spec §9),
  and reset the accumulators. Emits `[:replicant, :checkpoint_store, :batch_flushed]` on success
  (value-free: slot_name + counts + the trigger `reason`). A write fault returns
  `{:error, %Error{}, t()}` so the AssemblerServer halts fail-closed WITHOUT advancing — the whole
  batch re-delivers on restart (dup ≤ batch, never loss). `:empty` when no batch is open (a stale
  flush-timer fire — the pending_lsn is nil).
  """
  @spec flush_batch(t(), atom()) :: {:ok, lsn(), t()} | {:error, Error.t(), t()} | :empty
  def flush_batch(%__MODULE__{pending_lsn: nil}, _reason), do: :empty

  def flush_batch(%__MODULE__{} = asm, reason) do
    if __MODULE__.Batch.sink_owned_batching?(asm),
      do: flush_sink_batch(asm, reason),
      else: flush_lib_batch(asm, reason)
  end

  # Sink-owned batch delivery (spec §6): deliver the whole buffered batch as ONE atomic
  # handle_batch/1 call. On {:ok, _} advance the span base to the batch's highest LSN and reset
  # the buffer; the AssemblerServer then acks pending_lsn (§9 durability-before-ack). A sink
  # fault (return, raise, throw, exit) is scrubbed value-free (Critical Rule 1) — an N-txn
  # throw/exit reason can embed any buffered row (a GenServer.call timeout carrying the txn), so
  # the reason is NEVER inspected; only an exception module name is kept, exactly as apply_sink
  # does for handle_transaction (assembler.ex:504-536).
  defp flush_sink_batch(%__MODULE__{pending_lsn: lsn} = asm, reason) do
    start_mono = System.monotonic_time(:millisecond)
    txns = Enum.reverse(asm.batch_txns)

    result =
      try do
        asm.sink.handle_batch(txns)
      rescue
        e -> {:sink_raised, e}
      catch
        kind, reason -> {:sink_caught, kind, reason}
      end

    duration = System.monotonic_time(:millisecond) - start_mono

    case result do
      {:ok, _returned} ->
        # The batch (and every transactional message riding its txns) is durably delivered — emit
        # the per-message [:message, :received] telemetry (spec §10), as deliver_now does for the
        # non-batch modes. Sink-owned batch_delivery defers DELIVERY to handle_batch, so this is the
        # message's delivery point for this mode.
        Enum.each(txns, &emit_txn_messages_received/1)

        Telemetry.event([:replicant, :sink, :batch_committed], %{duration: duration}, %{
          change_count: asm.batch_count,
          commit_lsn: lsn,
          reason: reason
        })

        # The batch is durable (handle_batch returned {:ok, _} having re-read each Reader from its
        # spill file); delete the migrated spill files exactly once and clear the list. A non-spilled
        # batch has an empty list → a harmless no-op.
        Enum.each(asm.batch_spill_paths, &Spill.rm/1)

        {:ok, lsn,
         %{
           asm
           | batch_count: 0,
             pending_lsn: nil,
             batch_txns: [],
             batch_spill_paths: [],
             lib_checkpoint: max(asm.lib_checkpoint || 0, lsn)
         }}

      {:error, _reason} ->
        sink_batch_failed(asm, duration)

      {:sink_raised, e} ->
        sink_batch_failed(asm, duration, safe_shape(e))

      {:sink_caught, _kind, _reason} ->
        sink_batch_failed(asm, duration)

      # A non-conforming return (not {:ok,_}/{:error,_}) must NOT raise CaseClauseError here:
      # this runs via do_flush, OUTSIDE handle_message/2's value-free rescue, so the raised
      # term (a buffered row) would leak into the OTP crash log. Halt fail-closed value-free,
      # never inspecting the returned term (Critical Rule 1; spec §8 unexpected-return row).
      _unexpected ->
        sink_batch_failed(asm, duration)
    end
  end

  defp sink_batch_failed(asm, duration, shape \\ nil) do
    Telemetry.event([:replicant, :sink, :failed], %{duration: duration}, %{reason: :sink_failed})

    # Delete the migrated spill files on the fault branch too (CV1): a flush fault halts fail-closed
    # and the batch re-streams from the durable checkpoint (fresh files) on restart, so the buffered
    # spill files must not orphan (cleartext row values at rest). Mirrors the {:ok} branch's cleanup
    # + deliver_now's fault cleanup (dab7f2f); clearing the list keeps a later reset_batch idempotent.
    Enum.each(asm.batch_spill_paths, &Spill.rm/1)
    {:error, %Error{reason: :sink_failed, shape: shape}, %{asm | batch_spill_paths: []}}
  end

  defp flush_lib_batch(%__MODULE__{pending_lsn: lsn} = asm, reason) do
    case write_checkpoint(asm, lsn) do
      :ok ->
        Telemetry.event([:replicant, :checkpoint_store, :batch_flushed], %{}, %{
          slot_name: asm.slot_name,
          change_count: asm.batch_count,
          byte_size: lsn - span_base(asm),
          reason: reason
        })

        # lib+batch NEVER records a spill path (a spilled lib+batch txn is deleted by deliver_now
        # after the sink returns durable — only the CHECKPOINT is batched). The list is therefore
        # always [] here; the delete loop is a no-op kept for symmetry with flush_sink_batch.
        Enum.each(asm.batch_spill_paths, &Spill.rm/1)

        {:ok, lsn,
         %{
           asm
           | batch_count: 0,
             pending_lsn: nil,
             batch_spill_paths: [],
             last_buffered_changes: [],
             lib_checkpoint: max(asm.lib_checkpoint || 0, lsn)
         }}

      {:error, _store_reason} ->
        Telemetry.event([:replicant, :checkpoint_store, :failed], %{}, %{
          slot_name: asm.slot_name,
          reason: :checkpoint_store_failed
        })

        # Scrub to the fixed value-free atom (Critical Rule 1), matching the per-txn `commit_txn`
        # path — the writer type is `{:error, term()}`, so a value-bearing term must never cross
        # into %Error{}. The specific reason is not consumed downstream (Supervisor.halt ignores it).
        {:error, %Error{reason: :checkpoint_store_failed}, asm}
    end
  end

  @doc "True when a batch is open (buffered, not yet flushed) — the flush-timer relevance guard (spec §9)."
  @spec batch_pending?(t()) :: boolean()
  def batch_pending?(%__MODULE__{pending_lsn: lsn}), do: is_integer(lsn)

  @doc """
  Discard any open batch — clear the accumulators without checkpointing (spec §9). Used on a
  mid-stream Connection reconnect re-seed: the un-checkpointed batch re-streams from the durable
  checkpoint and re-buffers as a FRESH batch, so a transient reconnect matches the crash/stop→resume
  dup model (bounds dup to one batch per reconnect; stale accumulators would misalign flush
  boundaries). Never writes a checkpoint — loss=0 holds by re-delivery.
  """
  @spec reset_batch(t()) :: t()
  def reset_batch(%__MODULE__{batch_spill_paths: paths} = asm) do
    # A discarded sink-owned batch re-streams from the durable checkpoint, so any migrated spill
    # file must not survive (spec §5 reset cleanup). Delete the recorded files and clear the list.
    Enum.each(paths, &Spill.rm/1)

    %{
      asm
      | batch_count: 0,
        pending_lsn: nil,
        batch_txns: [],
        batch_spill_paths: [],
        last_buffered_changes: []
    }
  end

  @doc """
  Discard all in-progress streamed transactions (on reconnect); loss=0 by re-stream (spec §9). Any
  open spill file is deleted first (a reconnect re-streams from the durable checkpoint, so a stale
  spill file must not survive — spec §5 reset cleanup), and `spilled_total` resets to 0.
  """
  @spec reset_streams(t()) :: t()
  def reset_streams(%__MODULE__{stream_txns: streams} = asm) do
    Enum.each(streams, fn {_xid, buf} -> buf.spill && Spill.discard(buf.spill) end)
    %{asm | stream_txns: %{}, current_stream_xid: nil, spilled_total: 0}
  end

  @doc """
  Record the per-stream floor (the Connection's first-frame `wal_end` — where PG actually began
  streaming). It is the cold-start component of the span-cap base `max(lib_checkpoint, stream_floor)`
  (spec §7): on a fresh slot lib_checkpoint is 0, so measuring the batch's lag from the stream floor
  (not absolute 0) is what keeps a large-absolute first-txn LSN from spuriously span-flushing.
  """
  @spec put_stream_floor(t(), lsn()) :: t()
  def put_stream_floor(%__MODULE__{} = asm, floor) when is_integer(floor),
    do: %{asm | stream_floor: floor}

  # Advance the in-memory lib watermark on a durable commit (lib mode) and reset
  # the buffer. Sink-owned: a bare buffer reset (the watermark lives in the sink).
  defp advance(%__MODULE__{mode: :lib} = asm, lsn),
    do: %{reset(asm) | lib_checkpoint: max(asm.lib_checkpoint || 0, lsn)}

  defp advance(asm, _lsn), do: reset(asm)

  defp sink_failed(asm, duration, shape \\ nil) do
    Telemetry.event([:replicant, :sink, :failed], %{duration: duration}, %{reason: :sink_failed})
    {:halt, %Error{reason: :sink_failed, shape: shape}, asm}
  end

  defp safe_shape(%{__struct__: mod}), do: inspect(mod)
  defp safe_shape(_), do: nil

  # Live lag from the transaction's commit timestamp to now, clamped ≥ 0 (clock
  # skew must not surface a negative). A nil timestamp → 0. Uses runtime `now`, so
  # no fixed-date time-bomb; it is a WAL position/time gauge, never a row value.
  def lag_ms(nil), do: 0

  def lag_ms(%DateTime{} = commit_timestamp) do
    max(0, System.os_time(:millisecond) - DateTime.to_unix(commit_timestamp, :millisecond))
  end

  def reset(asm), do: %{asm | txn: nil, ordinal: 0}
end
