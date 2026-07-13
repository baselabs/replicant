defmodule Replicant.Test.RecordingSink do
  @moduledoc """
  A minimal test sink: implements ONLY the two mandatory callbacks. Used to prove
  the behaviour's optional callbacks do not emit "missing callback" warnings under
  `--warnings-as-errors`, and to record what the Assembler dispatches.
  """
  @behaviour Replicant.Sink

  use Agent

  def start_link(_opts \\ []) do
    case Agent.start_link(fn -> %{txns: [], messages: []} end, name: __MODULE__) do
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
    Agent.update(__MODULE__, fn %{txns: txns} = state ->
      %{state | txns: [{txn.commit_lsn, txn.changes} | txns]}
    end)

    {:ok, txn.commit_lsn}
  end

  # Mirror the handle_transaction/1 recording pattern so a non-transactional Message
  # delivered via handle_message/2 is observable in tests (A2 Task 8). Records each
  # delivered %Message{} newest-first; `seen_messages/0` reverses to arrival order.
  # Stored separately from txns so `seen/0`'s `{lsn, changes}` shape stays stable.
  @impl Replicant.Sink
  def handle_message(%Replicant.Decoder.Messages.Message{} = msg, _ctx) do
    Agent.update(__MODULE__, fn %{messages: messages} = state ->
      %{state | messages: [msg | messages]}
    end)

    {:ok, msg.lsn}
  end

  def seen, do: Agent.get(__MODULE__, fn %{txns: txns} -> Enum.reverse(txns) end)
  def seen_messages, do: Agent.get(__MODULE__, fn %{messages: m} -> Enum.reverse(m) end)

  def reset, do: Agent.update(__MODULE__, fn _ -> %{txns: [], messages: []} end)
end
