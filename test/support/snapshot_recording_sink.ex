defmodule Replicant.Test.SnapshotRecordingSink do
  @moduledoc """
  A snapshot-capable recording sink for the plugin-decoder snapshot legs: records
  every snapshot change and every streamed transaction in arrival order, keeps the
  watermark `handle_snapshot_complete/1` hands it, and returns it from `checkpoint/0`
  so the handoff streams strictly after the snapshot. Legs run sequentially;
  `reset/0` clears the record before a leg.
  """

  @behaviour Replicant.Sink

  use Agent

  def start_link(_opts \\ []) do
    case Agent.start_link(fn -> initial() end, name: __MODULE__) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
    end
  end

  def reset, do: Agent.update(__MODULE__, fn _ -> initial() end)

  def recorded, do: Agent.get(__MODULE__, & &1)

  @impl true
  def checkpoint, do: {:ok, Agent.get(__MODULE__, & &1.watermark)}

  @impl true
  def handle_transaction(%Replicant.Transaction{} = txn) do
    Agent.update(__MODULE__, fn state -> %{state | txns: state.txns ++ [txn]} end)
    {:ok, txn.commit_lsn}
  end

  @impl true
  def handle_snapshot(changes, _context) do
    Agent.update(__MODULE__, fn state -> %{state | snapshot: state.snapshot ++ changes} end)
    :ok
  end

  @impl true
  def handle_snapshot_complete(lsn) do
    Agent.update(__MODULE__, fn state -> %{state | watermark: lsn, complete?: true} end)
    {:ok, lsn}
  end

  defp initial, do: %{snapshot: [], txns: [], watermark: nil, complete?: false}
end
