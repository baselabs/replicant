defmodule Replicant.Transaction do
  @moduledoc """
  A decoded, committed transaction: an ordered list of `Replicant.Change` rows
  plus the **transaction-granularity commit LSN** (every row in a
  pgoutput proto-v1 transaction shares one commit LSN — a per-row LSN would
  collapse an N-row transaction into one row).

  A completed transaction's `changes` is ordinarily a `List`; for a SPILLED
  streamed transaction it is a lazy, single-pass, disk-backed
  `Enumerable` (`Replicant.Spill.Reader`) valid only during the delivery call.
  Iterate with `Enum`/`Stream`; never call `length/1`, `Enum.to_list/1`, or
  re-iterate (any of which forces the whole transaction back into RAM), and do
  not retain it past the `handle_transaction/1` / `handle_batch/1` call.
  """

  @type t :: %__MODULE__{
          commit_lsn: Replicant.lsn() | nil,
          commit_timestamp: DateTime.t() | nil,
          changes: [Replicant.Change.t()] | Replicant.Spill.Reader.t(),
          messages: [Replicant.Decoder.Messages.Message.t()]
        }

  defstruct [:commit_lsn, :commit_timestamp, changes: [], messages: []]
end
