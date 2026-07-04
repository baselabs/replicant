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
  `handle_message/2` wraps its body in a rescue: a raise from the casting path
  (a malformed numeric/bytea in `Types.cast_record/2`) or from the sink apply is
  scrubbed into `{:halt, %Error{reason: :decode_failure}}` — symmetric to
  `Replicant.Decoder.decode/1`. A row/commit message arriving before any `Begin`
  halts as `:config_invalid` rather than crashing.
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
          txn: buffer() | nil,
          ordinal: non_neg_integer()
        }

  @type buffer :: %{
          begin_lsn: lsn() | nil,
          xid: non_neg_integer() | nil,
          changes: [Change.t()]
        }

  defstruct [:sink, :txn, relations: %{}, ordinal: 0]

  @doc "Create a new assembler bound to `sink` (a module implementing `Replicant.Sink`)."
  @spec new(module()) :: t()
  def new(sink), do: %__MODULE__{sink: sink}

  @doc """
  Handle one decoded message. Returns:
    * `{:ok, t()}` — accumulated, no boundary crossed.
    * `{:transaction, Transaction.t(), lsn(), t()}` — Commit, sink committed.
    * `{:skipped, lsn(), t()}` — Commit but `commit_lsn <= checkpoint` (watermark skip).
    * `{:schema_change, SchemaChange.t(), t()}` — additive schema change applied.
    * `{:halt, SchemaChange.t() | term(), t()}` — destructive schema change, sink
      failure, or a value-bearing raise (e.g. casting a malformed numeric) —
      fail-closed, value-free.

  `handle_message/2` is itself the value-free boundary for the casting path: the
  vendored `Types.cast_record/2` raises `ArgumentError` on malformed numerics /
  bytea (a corrupted stream), and those exceptions embed row bytes. The public
  wrapper scrubs any such raise into `{:halt, %Error{reason: :decode_failure}}`
  (Critical Rule 1) — symmetric to `Replicant.Decoder.decode/1`.
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
  end

  defp do_handle_message(%__MODULE__{} = asm, %Begin{final_lsn: lsn, xid: xid}) do
    {:ok, %{asm | txn: %{begin_lsn: lsn, xid: xid, changes: []}, ordinal: 0}}
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

  defp do_handle_message(
         %__MODULE__{txn: buffer, relations: relations, ordinal: ordinal} = asm,
         %Insert{
           relation_id: rid,
           tuple_data: tuple
         }
       ) do
    change = build_change(:insert, relations, rid, tuple, nil, buffer.begin_lsn, ordinal)
    {:ok, %{asm | txn: %{buffer | changes: [change | buffer.changes]}, ordinal: ordinal + 1}}
  end

  defp do_handle_message(
         %__MODULE__{txn: buffer, relations: relations, ordinal: ordinal} = asm,
         %Update{
           relation_id: rid,
           tuple_data: tuple,
           old_tuple_data: old_tuple,
           changed_key_tuple_data: key_tuple
         }
       ) do
    old = old_tuple || key_tuple
    change = build_change(:update, relations, rid, tuple, old, buffer.begin_lsn, ordinal)
    {:ok, %{asm | txn: %{buffer | changes: [change | buffer.changes]}, ordinal: ordinal + 1}}
  end

  defp do_handle_message(
         %__MODULE__{txn: buffer, relations: relations, ordinal: ordinal} = asm,
         %Delete{
           relation_id: rid,
           old_tuple_data: old_tuple,
           changed_key_tuple_data: key_tuple
         }
       ) do
    old = old_tuple || key_tuple
    change = build_change(:delete, relations, rid, nil, old, buffer.begin_lsn, ordinal)
    {:ok, %{asm | txn: %{buffer | changes: [change | buffer.changes]}, ordinal: ordinal + 1}}
  end

  defp do_handle_message(%__MODULE__{txn: buffer, ordinal: ordinal} = asm, %Truncate{
         truncated_relations: rids
       }) do
    changes =
      Enum.map(rids, fn rid ->
        rel = Map.get(asm.relations, rid)

        %Change{
          op: :truncate,
          schema: rel && rel.namespace,
          table: rel && rel.name,
          commit_lsn: buffer.begin_lsn,
          ordinal: ordinal
        }
      end)

    {:ok, %{asm | txn: %{buffer | changes: Enum.reverse(changes, buffer.changes)}}}
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
      %{change_count: length(txn.changes), commit_lsn: txn.commit_lsn}
    )

    case checkpoint(asm.sink) do
      {:ok, checkpoint} when not is_nil(checkpoint) and txn.commit_lsn <= checkpoint ->
        {:skipped, txn.commit_lsn, reset(asm)}

      _ ->
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
        {:ok, %{asm | relations: Map.put(relations, rel.id, rel)}}

      cached ->
        case SchemaChange.classify(cached, rel) do
          nil ->
            {:ok, %{asm | relations: Map.put(relations, rel.id, rel)}}

          %SchemaChange{kind: :additive} = sc ->
            Telemetry.event([:replicant, :schema_change, :additive], %{}, %{
              table: sc.table,
              kind: :additive
            })

            {:schema_change, sc, %{asm | relations: Map.put(relations, rel.id, rel)}}

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

          {:schema_change, sc, %{asm | relations: Map.put(asm.relations, rel.id, rel)}}

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

  defp build_change(op, relations, rid, new_tuple, old_tuple, commit_lsn, ordinal) do
    rel = Map.get(relations, rid)
    columns = (rel && rel.columns) || []
    {record, unchanged} = materialize(new_tuple, columns)
    {old_record, _} = materialize(old_tuple, columns)

    %Change{
      op: op,
      schema: rel && rel.namespace,
      table: rel && rel.name,
      record: record,
      old_record: old_record,
      unchanged: unchanged,
      columns: Enum.map(columns, &to_change_column/1),
      commit_lsn: commit_lsn,
      ordinal: ordinal
    }
  end

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

  defp cast_value(nil, _type), do: nil
  defp cast_value(value, type) when is_binary(value), do: Types.cast_record(value, type)

  # --- sink apply ---

  defp checkpoint(sink) do
    sink.checkpoint()
  rescue
    _ -> {:ok, nil}
  end

  defp apply_sink(sink, %Transaction{} = txn, asm) do
    start_mono = System.monotonic_time(:millisecond)

    result =
      try do
        sink.handle_transaction(txn)
      rescue
        e ->
          Telemetry.event([:replicant, :sink, :failed], %{}, %{reason: :sink_failed})
          {:rescued, e}
      end

    duration = System.monotonic_time(:millisecond) - start_mono

    case result do
      {:ok, lsn} ->
        Telemetry.event([:replicant, :sink, :committed], %{duration: duration}, %{commit_lsn: lsn})

        {:transaction, txn, lsn, reset(asm)}

      {:rescued, e} ->
        # Scrub the sink exception value-free (Critical Rule 1) — symmetric to the
        # decode boundary. `apply_sink` catches its own raise so the outer
        # `handle_message` wrapper's :decode_failure is reserved for casting raises.
        {:halt, Error.decode_failure(e), asm}

      {:error, _reason} ->
        Telemetry.event([:replicant, :sink, :failed], %{duration: duration}, %{
          reason: :sink_failed
        })

        {:halt, %Error{reason: :sink_failed}, asm}
    end
  end

  defp reset(asm), do: %{asm | txn: nil, ordinal: 0}
end
