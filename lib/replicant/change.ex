defmodule Replicant.Change do
  @moduledoc """
  One row change within a `Replicant.Transaction`.

  ## Unchanged TOAST (spec §7)
  An UPDATE that does not touch a TOASTed column sends a sentinel, not the value.
  The sentinel is surfaced as a first-class `unchanged: [col]` list — it **never**
  appears in `record`. A sink upserting the row must leave those columns
  untouched. For sinks needing the full row on every UPDATE, document
  `REPLICA IDENTITY FULL` upstream.

  ## Replica identity (spec §7)
  Under the default identity, a DELETE and an UPDATE's `old_record` carry only
  key columns. `old_record` is key-only unless the table is `REPLICA IDENTITY
  FULL`.

  ## String keys (spec §10)
  Column names in `record` / `old_record` are **binaries**, never atoms — a wide
  or attacker-influenced schema must not exhaust the atom table.
  """

  @type op :: :insert | :update | :delete | :truncate

  @type t :: %__MODULE__{
          op: op(),
          schema: String.t() | nil,
          table: String.t() | nil,
          record: %{String.t() => term()} | nil,
          old_record: %{String.t() => term()} | nil,
          unchanged: [String.t()],
          columns: [Replicant.Change.Column.t()],
          commit_lsn: Replicant.lsn() | nil,
          ordinal: non_neg_integer()
        }

  defstruct [
    :op,
    :schema,
    :table,
    :record,
    :old_record,
    unchanged: [],
    columns: [],
    commit_lsn: nil,
    ordinal: 0
  ]

  defmodule Column do
    @moduledoc "Per-column metadata carried on a `Change` (mirrors the Relation message)."

    @type t :: %__MODULE__{
            name: String.t() | nil,
            type: String.t() | nil,
            flags: [atom()],
            type_modifier: non_neg_integer() | nil
          }

    defstruct [:name, :type, :type_modifier, flags: []]
  end
end
