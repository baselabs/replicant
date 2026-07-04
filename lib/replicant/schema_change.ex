defmodule Replicant.SchemaChange do
  @moduledoc """
  Classification of a `Relation` diff (spec §9): **additive** (new column —
  auto-apply) or **destructive** (dropped column, type change, replica-identity
  change — delegate to the sink or halt fail-closed). A `Truncate` is delivered
  as a change, not a schema change.

  The Assembler diffs each incoming `Relation` against the cached one and calls
  `classify/2`. `nil` means "no schema-relevant change" (e.g. only column order
  shifted without drops/type/identity changes — treated as additive-safe).

  ## Known limitation
  A change to *which* columns form the replica identity — expressed via per-column
  `:key` flags rather than the `replica_identity` enum (e.g. a `REPLICA IDENTITY
  USING INDEX` swap to a different unique index, or a primary-key change that keeps
  the same column names) — is **not** detected when the enum value itself is
  unchanged. Such changes are rare and usually accompany a column drop/add (which
  IS caught). If your workload alters key-column sets without dropping/adding
  columns, gate it upstream. (Plan 2 may extend `classify/2` to diff the
  `:key`-flagged column set.)
  """

  alias Replicant.Decoder.Messages.Relation

  @type kind :: :additive | :destructive
  @type change :: :column_added | :column_dropped | :type_changed | :replica_identity_changed

  @type t :: %__MODULE__{
          kind: kind(),
          change: change(),
          schema: String.t() | nil,
          table: String.t() | nil,
          detail: String.t() | nil
        }

  defstruct [:kind, :change, :schema, :table, :detail]

  @doc """
  Diff `old` and `new` `Relation` messages. Returns `%SchemaChange{}` or `nil`
  when nothing schema-relevant changed. A replica-identity change dominates
  (checked first) because it alters the meaning of `old_record` for every
  subsequent change.
  """
  @spec classify(Relation.t() | nil, Relation.t()) :: t() | nil
  def classify(%Relation{} = old, %Relation{columns: new_cols} = new) do
    if old.replica_identity != new.replica_identity do
      %__MODULE__{
        kind: :destructive,
        change: :replica_identity_changed,
        schema: new.namespace,
        table: new.name,
        detail: "replica_identity #{old.replica_identity} -> #{new.replica_identity}"
      }
    else
      diff_columns(old.columns, new_cols, new)
    end
  end

  def classify(nil, %Relation{} = new),
    do: classify(%Relation{columns: [], replica_identity: nil}, new)

  defp diff_columns(old_cols, new_cols, new) do
    old_by_name = column_index(old_cols)
    new_by_name = column_index(new_cols)

    old_names = MapSet.new(Map.keys(old_by_name))
    new_names = MapSet.new(Map.keys(new_by_name))

    added = MapSet.difference(new_names, old_names)
    dropped = MapSet.difference(old_names, new_names)

    # A destructive type change is EITHER a type-name change (int4 -> int8) OR a
    # type_modifier change (varchar(100) -> varchar(10), same "varchar" name but a
    # narrowing atttypmod that can truncate data). Comparing only the name would
    # let a narrowing ALTER COLUMN TYPE proceed unhalted — so compare the pair.
    type_changed =
      Enum.find_value(new_cols, fn col ->
        case Map.fetch(old_by_name, col.name) do
          {:ok, %{type: old_type, type_modifier: old_mod}}
          when old_type != col.type or old_mod != col.type_modifier ->
            {col.name, "#{old_type}/#{inspect(old_mod)}",
             "#{col.type}/#{inspect(col.type_modifier)}"}

          _ ->
            nil
        end
      end)

    cond do
      MapSet.size(dropped) > 0 ->
        %__MODULE__{
          kind: :destructive,
          change: :column_dropped,
          schema: new.namespace,
          table: new.name,
          detail: "dropped: #{Enum.join(MapSet.to_list(dropped), ",")}"
        }

      type_changed ->
        {name, from_t, to_t} = type_changed

        %__MODULE__{
          kind: :destructive,
          change: :type_changed,
          schema: new.namespace,
          table: new.name,
          detail: "#{name}: #{from_t} -> #{to_t}"
        }

      MapSet.size(added) > 0 ->
        %__MODULE__{
          kind: :additive,
          change: :column_added,
          schema: new.namespace,
          table: new.name,
          detail: "added: #{Enum.join(MapSet.to_list(added), ",")}"
        }

      true ->
        nil
    end
  end

  defp column_index(columns) do
    Map.new(columns, fn %Relation.Column{name: name} = col -> {name, col} end)
  end
end
