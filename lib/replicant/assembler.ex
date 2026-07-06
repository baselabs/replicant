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
    Telemetry,
    Transaction
  }

  alias Messages.{
    Begin,
    Commit,
    Delete,
    Insert,
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
          pending_lsn: lsn() | nil,
          stream_floor: lsn() | nil,
          stream_txns: %{non_neg_integer() => stream_buf()},
          current_stream_xid: non_neg_integer() | nil,
          max_concurrent_txns: pos_integer() | nil
        }

  @type buffer :: %{
          begin_lsn: lsn() | nil,
          xid: non_neg_integer() | nil,
          changes: [Change.t()],
          byte_size: non_neg_integer()
        }

  @type stream_buf :: %{
          changes: [{non_neg_integer(), Change.t()}],
          byte_size: non_neg_integer()
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
    pending_lsn: nil,
    stream_floor: nil,
    stream_txns: %{},
    current_stream_xid: nil,
    max_concurrent_txns: nil
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
      max_concurrent_txns: Keyword.get(opts, :max_concurrent_txns)
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
    %{asm | stream_txns: Map.put(streams, xid, %{buf | byte_size: buf.byte_size + bytes})}
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
    {:ok, %{asm | txn: %{begin_lsn: lsn, xid: xid, changes: [], byte_size: 0}, ordinal: 0}}
  end

  # --- streaming reassembly (spec §5) ---
  #
  # These clauses MUST precede the `txn: nil` "row before Begin" guard below: a streamed
  # change has NO open `txn` buffer, so the guard would otherwise mis-halt it as a "row before
  # Begin". They match only STREAMED traffic — StreamStart/StreamStop, and Insert/Update/Delete/
  # Truncate guarded on `is_integer(xid)` (a non-streamed message carries `xid: nil` and skips
  # these to reach the v1 clauses unchanged). Streamed Relation/Type messages fall through to the
  # existing v1 clauses (relation schema is cached globally, not per-stream — correct).

  defp do_handle_message(%__MODULE__{} = asm, %StreamStart{xid: xid}) do
    cond do
      Map.has_key?(asm.stream_txns, xid) ->
        {:ok, %{asm | current_stream_xid: xid}}

      map_size(asm.stream_txns) >= (asm.max_concurrent_txns || 64) ->
        {:halt,
         %Error{reason: :config_invalid, shape: "too many concurrent streamed transactions"}, asm}

      true ->
        {:ok,
         %{
           asm
           | current_stream_xid: xid,
             stream_txns: Map.put(asm.stream_txns, xid, %{changes: [], byte_size: 0})
         }}
    end
  end

  defp do_handle_message(%__MODULE__{} = asm, %StreamStop{}) do
    {:ok, %{asm | current_stream_xid: nil}}
  end

  # StreamCommit: replay the buffered streamed changes into a complete %Transaction{}, stamping the
  # transaction-granularity commit_lsn and ascending ordinals AT REPLAY (streamed changes have no
  # LSN before commit — spec §5). Storage is newest-first, so `Enum.reverse` restores commit order;
  # subxid tags are dropped here (sub-aborts already filtered the buffer at StreamAbort time). An
  # EMPTY streamed txn (no published changes) is suppressed here to stay v1-indistinguishable at the
  # sink (spec §7). Otherwise pre-skip if at/below the watermark (effect-once, §2) else deliver via
  # the shared apply_sink/2 — structurally mirroring the v1 Commit clause's stamp+skip?+deliver path.
  defp do_handle_message(%__MODULE__{} = asm, %StreamCommit{
         xid: top,
         commit_lsn: lsn,
         commit_timestamp: ts
       }) do
    case Map.fetch(asm.stream_txns, top) do
      :error ->
        {:halt, %Error{reason: :config_invalid, shape: "stream commit for unknown transaction"},
         asm}

      {:ok, buf} ->
        changes =
          buf.changes
          |> Enum.reverse()
          |> Enum.with_index()
          |> Enum.map(fn {{_subxid, change}, ordinal} ->
            %{change | commit_lsn: lsn, ordinal: ordinal}
          end)

        asm = %{asm | stream_txns: Map.delete(asm.stream_txns, top), current_stream_xid: nil}

        if changes == [] do
          # An EMPTY streamed transaction (e.g. a spilled transaction with no changes for the
          # publication) is emitted by pgoutput v2 as StreamStart/Stop/Commit, but SKIPPED entirely
          # by v1 (PG15+ never sends an empty non-streamed Commit). Suppress delivery so a streamed
          # transaction is indistinguishable from a non-streamed one at the sink (spec §7); emit
          # neither `:assembled` nor `:stream:committed` (consistent with v1, which never sees the
          # empty txn — a "committed" signal for a txn the sink never sees would mislead). Still
          # return `{:skipped, lsn, asm}` so the Connection advances the slot to `lsn` and the txn is
          # not re-streamed (loss=0 — an empty txn carries no effect).
          {:skipped, lsn, asm}
        else
          deliver_stream_commit(asm, lsn, ts, buf.byte_size, changes)
        end
    end
  end

  # Whole-transaction abort (subxid == top): discard the buffer, deliver nothing.
  defp do_handle_message(%__MODULE__{} = asm, %StreamAbort{xid: top, subxid: sub})
       when top == sub do
    if Map.has_key?(asm.stream_txns, top) do
      Telemetry.event([:replicant, :stream, :aborted], %{}, %{reason: :stream_abort})
      cleared = if asm.current_stream_xid == top, do: nil, else: asm.current_stream_xid
      {:ok, %{asm | stream_txns: Map.delete(asm.stream_txns, top), current_stream_xid: cleared}}
    else
      {:halt, %Error{reason: :config_invalid, shape: "stream abort for unknown transaction"}, asm}
    end
  end

  # Subtransaction (savepoint) abort: drop the aborted subxid's changes, keep the rest in order.
  defp do_handle_message(%__MODULE__{} = asm, %StreamAbort{xid: top, subxid: sub}) do
    case Map.fetch(asm.stream_txns, top) do
      :error ->
        {:halt, %Error{reason: :config_invalid, shape: "stream abort for unknown transaction"},
         asm}

      {:ok, buf} ->
        kept = Enum.reject(buf.changes, fn {subxid, _change} -> subxid == sub end)
        {:ok, %{asm | stream_txns: Map.put(asm.stream_txns, top, %{buf | changes: kept})}}
    end
  end

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
      changes: Enum.reverse(buffer.changes)
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

  defp do_handle_message(%__MODULE__{} = asm, _unsupported) do
    # Any message the decoder could not classify has already been turned into an
    # error at the boundary; reaching here means an unhandled but decodable shape.
    {:halt, %Error{reason: :unsupported_message}, asm}
  end

  # The non-empty StreamCommit delivery path: emit the assembled + stream:committed telemetry, then
  # pre-skip at/below the watermark (effect-once, §2) else deliver via the shared apply_sink/2 —
  # extracted verbatim from the StreamCommit clause to keep it within credo's nesting depth. An empty
  # streamed txn never reaches here (it is suppressed in the StreamCommit clause, spec §7).
  defp deliver_stream_commit(asm, lsn, ts, byte_size, changes) do
    txn = %Transaction{commit_lsn: lsn, commit_timestamp: ts, changes: changes}

    Telemetry.event([:replicant, :transaction, :assembled], %{}, %{
      change_count: length(changes),
      commit_lsn: lsn,
      byte_size: byte_size,
      lag_ms: lag_ms(ts)
    })

    Telemetry.event([:replicant, :stream, :committed], %{}, %{
      change_count: length(changes),
      commit_lsn: lsn,
      byte_size: byte_size
    })

    if skip?(asm, txn) do
      {:skipped, lsn, asm}
    else
      apply_sink(asm, txn)
    end
  end

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
        change = build_change(op, rel, projected, new_tuple, old_spec, nil, 0)
        buf = Map.fetch!(asm.stream_txns, top)
        buf = %{buf | changes: [{subxid, change} | buf.changes]}
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
      tagged =
        Enum.map(rids, fn rid ->
          rel = Map.fetch!(relations, rid)

          {subxid,
           %Change{
             op: :truncate,
             schema: rel.namespace,
             table: rel.name,
             commit_lsn: nil,
             ordinal: 0
           }}
        end)

      buf = Map.fetch!(asm.stream_txns, top)
      buf = %{buf | changes: Enum.reverse(tagged, buf.changes)}
      {:ok, %{asm | stream_txns: Map.put(asm.stream_txns, top, buf)}}
    else
      {:halt, %Error{reason: :config_invalid, shape: "streamed truncate for uncached relation"},
       asm}
    end
  end

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
  defp skip?(%__MODULE__{mode: :lib, lib_checkpoint: cp}, %Transaction{commit_lsn: lsn}),
    do: is_integer(cp) and lsn <= cp

  # Sink-owned: read the sink's checkpoint live. Reproduces the prior Commit-path
  # semantics exactly — skip only on {:ok, non-nil, >= commit_lsn}; {:ok, nil}
  # (never persisted), {:ok, older_lsn} (newer txn), a {:error, _}, or a
  # raised/exited read all fail-OPEN (dup-safe by the §6 sink idempotency
  # contract: a re-dispatched already-persisted txn is deduped by the sink, and a
  # real store outage makes handle_transaction fail → halt).
  defp skip?(%__MODULE__{sink: sink}, %Transaction{commit_lsn: lsn}) do
    case checkpoint(sink) do
      {:ok, cp} -> not is_nil(cp) and lsn <= cp
      _ -> false
    end
  end

  defp apply_sink(%__MODULE__{} = asm, %Transaction{} = txn) do
    if sink_owned_batching?(asm),
      do: buffer_for_delivery(asm, txn),
      else: deliver_now(asm, txn)
  end

  # Per-transaction delivery (sink-owned non-batch, and lib mode, incl. lib+batch which buffers
  # AFTER the sink returns {:ok, _}). Verbatim prior apply_sink/2 body — unchanged.
  defp deliver_now(%__MODULE__{sink: sink} = asm, %Transaction{} = txn) do
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

    case result do
      {:ok, _lsn} ->
        # lib+batch: buffer the durable txn and defer the store write/ack to the batch
        # flush (spec §7). Per-txn (sink-owned or lib-non-batch): checkpoint-after-persist.
        if batching?(asm),
          do: buffer_txn(asm, txn, duration),
          else: commit_txn(asm, txn, duration)

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

  # --- batched checkpointing (spec §7) ---

  defp batching?(%__MODULE__{mode: :lib, batch: batch}), do: is_list(batch)
  defp batching?(_asm), do: false

  defp sink_owned_batching?(%__MODULE__{mode: :sink_owned, batch: batch}), do: is_list(batch)
  defp sink_owned_batching?(_asm), do: false

  # The sink persisted the txn's DATA per-txn (announce [:sink, :committed] now); DEFER the
  # store checkpoint write + ack to the batch flush. Accumulate into the open batch and signal
  # a flush when the count OR the LSN-span cap trips. Per spec §4/§7/decision-log #7 the span is
  # `pending_lsn − max(lib_checkpoint, stream_floor)` — the batch's contribution to the §4 in-flight
  # lag, mirroring the Connection's own lag floor (`max(checkpoint_lsn, stream_floor_lsn)`,
  # connection.ex:499). `stream_floor` (the stream's start position, from the Connection's first
  # frame) makes this robust on a fresh slot where lib_checkpoint is 0 but stream LSNs are
  # large-absolute — without it the first txn would spuriously span-flush. The max_delay_ms timer is
  # the AssemblerServer's third trigger. `lib_checkpoint` is NOT advanced here — only at flush (§9).
  defp buffer_txn(%__MODULE__{} = asm, %Transaction{commit_lsn: lsn} = _txn, duration) do
    Telemetry.event([:replicant, :sink, :committed], %{duration: duration}, %{commit_lsn: lsn})

    count = asm.batch_count + 1
    asm = %{reset(asm) | batch_count: count, pending_lsn: lsn}

    cond do
      count >= Keyword.fetch!(asm.batch, :max_transactions) -> {:flush, :max_transactions, asm}
      lsn - span_base(asm) >= Keyword.fetch!(asm.batch, :max_span) -> {:flush, :max_span, asm}
      true -> {:buffered, asm}
    end
  end

  # Sink-owned batch delivery (spec §6): buffer the committed txn (NO sink call) and signal a
  # flush when the count OR the LSN-span cap trips (span base = max(lib_checkpoint, stream_floor),
  # spec §7 — identical to lib-batch). lib_checkpoint is NOT advanced here — only at flush (§9).
  # No [:sink, :committed] event: the txn is not delivered until flush_batch calls handle_batch.
  # Stored newest-first (prepend); flush_sink_batch reverses to ascending commit-LSN order.
  defp buffer_for_delivery(%__MODULE__{} = asm, %Transaction{commit_lsn: lsn} = txn) do
    count = asm.batch_count + 1

    asm = %{
      reset(asm)
      | batch_count: count,
        pending_lsn: lsn,
        batch_txns: [txn | asm.batch_txns]
    }

    cond do
      count >= Keyword.fetch!(asm.batch, :max_transactions) -> {:flush, :max_transactions, asm}
      lsn - span_base(asm) >= Keyword.fetch!(asm.batch, :max_span) -> {:flush, :max_span, asm}
      true -> {:buffered, asm}
    end
  end

  # The span-cap base (spec §7): the un-checkpointed window's start = the higher of the durable
  # watermark and the per-stream floor, mirroring the §4 lag floor. On a fresh slot lib_checkpoint
  # is 0 so the floor dominates; after the first flush the watermark dominates.
  defp span_base(%__MODULE__{lib_checkpoint: cp, stream_floor: floor}),
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
    if sink_owned_batching?(asm),
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
        Telemetry.event([:replicant, :sink, :batch_committed], %{duration: duration}, %{
          change_count: asm.batch_count,
          commit_lsn: lsn,
          reason: reason
        })

        {:ok, lsn,
         %{
           asm
           | batch_count: 0,
             pending_lsn: nil,
             batch_txns: [],
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
    {:error, %Error{reason: :sink_failed, shape: shape}, asm}
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

        {:ok, lsn,
         %{
           asm
           | batch_count: 0,
             pending_lsn: nil,
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
  def reset_batch(%__MODULE__{} = asm),
    do: %{asm | batch_count: 0, pending_lsn: nil, batch_txns: []}

  @doc "Discard all in-progress streamed transactions (on reconnect); loss=0 by re-stream (spec §9)."
  @spec reset_streams(t()) :: t()
  def reset_streams(%__MODULE__{} = asm),
    do: %{asm | stream_txns: %{}, current_stream_xid: nil}

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
  defp lag_ms(nil), do: 0

  defp lag_ms(%DateTime{} = commit_timestamp) do
    max(0, System.os_time(:millisecond) - DateTime.to_unix(commit_timestamp, :millisecond))
  end

  defp reset(asm), do: %{asm | txn: nil, ordinal: 0}
end
