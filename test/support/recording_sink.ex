defmodule Replicant.Test.RecordingSink do
  @moduledoc """
  A minimal test sink: implements ONLY the two mandatory callbacks. Used to prove
  the behaviour's optional callbacks do not emit "missing callback" warnings under
  `--warnings-as-errors`, and to record what the Assembler dispatches.
  """
  @behaviour Replicant.Sink

  use Agent

  def start_link(_opts \\ []) do
    case Agent.start_link(fn -> [] end, name: __MODULE__) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
    end
  end

  @impl Replicant.Sink
  def checkpoint do
    {:ok, Process.get({__MODULE__, :checkpoint})}
  end

  @impl Replicant.Sink
  def handle_transaction(%Replicant.Transaction{} = txn) do
    Agent.update(__MODULE__, fn seen -> [{txn.commit_lsn, txn.changes} | seen] end)
    {:ok, txn.commit_lsn}
  end

  def seen, do: Agent.get(__MODULE__, &Enum.reverse/1)
  def reset, do: Agent.update(__MODULE__, fn _ -> [] end)
end
