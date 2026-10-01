defmodule Replicant.Test.ParitySink do
  @moduledoc """
  A recording sink for the decoder-parity harness (ADR-0009): records the FULL
  `%Replicant.Transaction{}` structs every pipeline delivers, globally in arrival
  order. Legs run SEQUENTIALLY, so a harness leg snapshots `mark/0` before starting
  and reads `since/1` after stopping — no cross-pipeline tagging needed (sink
  callbacks run in the AssemblerServer's process, not the test's, so a per-leg
  process-local tag would be invisible to them).
  """

  @behaviour Replicant.Sink

  use Agent

  def start_link(_opts \\ []) do
    case Agent.start_link(fn -> %{txns: []} end, name: __MODULE__) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
    end
  end

  @impl true
  def checkpoint, do: {:ok, nil}

  @impl true
  def handle_transaction(%Replicant.Transaction{} = txn) do
    Agent.update(__MODULE__, fn %{txns: txns} -> %{txns: [txn | txns]} end)
    {:ok, txn.commit_lsn}
  end

  @doc "The current record count (a leg's snapshot point)."
  def mark, do: Agent.get(__MODULE__, fn %{txns: txns} -> length(txns) end)

  @doc "The transactions recorded AFTER `mark`, in arrival order."
  def since(mark) do
    Agent.get(__MODULE__, fn %{txns: txns} ->
      txns |> Enum.reverse() |> Enum.drop(mark)
    end)
  end
end
