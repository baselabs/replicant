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
  scrubbed by `apply_sink/3` into `{:halt, %Error{reason: :sink_failed}}` — never
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

  alias Messages.{Begin, Commit, Delete, Insert, Origin, Relation, Truncate, Type, Update}

  @type lsn :: Replicant.lsn()

  @type t :: %__MODULE__{
          sink: module(),
          relations: %{non_neg_integer() => Relation.t()},
          projected: %{non_neg_integer() => [Change.Column.t()]},
          txn: buffer() | nil,
          ordinal: non_neg_integer()
        }

  @type buffer :: %{
          begin_lsn: lsn() | nil,
          xid: non_neg_integer() | nil,
          changes: [Change.t()],
          byte_size: non_neg_integer()
        }

  defstruct [:sink, :txn, relations: %{}, projected: %{}, ordinal: 0]

  @doc "Create a new assembler bound to `sink` (a module implementing `Replicant.Sink`)."
  @spec new(module()) :: t()
  def new(sink), do: %__MODULE__{sink: sink}

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

    case checkpoint(asm.sink) do
      {:ok, checkpoint} when not is_nil(checkpoint) and txn.commit_lsn <= checkpoint ->
        {:skipped, txn.commit_lsn, reset(asm)}

      _ ->
        # {:ok, nil} (never persisted → go forward), {:ok, older_lsn} (newer txn), OR
        # {:error, _} / a raised/exited checkpoint read → apply. Fail-OPEN on a
        # checkpoint fault is dup-safe by the §6 sink idempotency contract: a
        # re-dispatched already-persisted txn is deduped by the sink (skip
        # commit_lsn <= its own checkpoint; upsert by PK), and a real store outage
        # makes handle_transaction fail → halt.
        apply_sink(asm.sink, txn, asm)
    end
  end

  defp do_handle_message(%__MODULE__{} = asm, _unsupported) do
    # Any message the decoder could not classify has already been turned into an
    # error at the boundary; reaching here means an unhandled but decodable shape.
    {:halt, %Error{reason: :unsupported_message}, asm}
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

  defp apply_sink(sink, %Transaction{} = txn, asm) do
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
      {:ok, lsn} ->
        Telemetry.event([:replicant, :sink, :committed], %{duration: duration}, %{commit_lsn: lsn})

        {:transaction, txn, lsn, reset(asm)}

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
