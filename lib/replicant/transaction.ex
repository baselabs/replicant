defmodule Replicant.Transaction do
  @moduledoc """
  A decoded, committed transaction: an ordered list of `Replicant.Change` rows
  plus the **transaction-granularity commit LSN** (spec §2: every row in a
  pgoutput proto-v1 transaction shares one commit LSN — a per-row LSN would
  collapse an N-row transaction into one row).
  """

  @type t :: %__MODULE__{
          commit_lsn: Replicant.lsn() | nil,
          commit_timestamp: DateTime.t() | nil,
          changes: [Replicant.Change.t()]
        }

  defstruct [:commit_lsn, :commit_timestamp, changes: []]
end
