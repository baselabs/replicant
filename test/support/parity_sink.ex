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
    case Agent.start_link(fn -> %{txns: [], messages: []} end, name: __MODULE__) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
    end
  end

  @impl true
  def checkpoint, do: {:ok, nil}

  @impl true
  def handle_transaction(%Replicant.Transaction{} = txn) do
    # PRESERVE the map (update, not replace) — replacing would drop the :messages
    # key and the next handle_message/2 would KeyError (repair-review finding)
    Agent.update(__MODULE__, fn state -> %{state | txns: [txn | state.txns]} end)
    {:ok, txn.commit_lsn}
  end

  # Non-transactional logical-decoding messages route here (ADR-0001); the sink
  # records them so a `messages: true` leg (the parity marquee's 15+ pgoutput
  # reference) passes the config capability gate and its deliveries stay observable.
  # The fixture's message is TRANSACTIONAL (rides %Transaction.messages), so this
  # stays empty there — it exists for the contract, not the fixture.
  @impl true
  def handle_message(%Replicant.Decoder.Messages.Message{} = message, _context) do
    # recorded SEPARATELY from txns — `since/1` must keep returning only
    # %Transaction{} structs (legs flat_map &1.changes over them)
    Agent.update(__MODULE__, fn state -> %{state | messages: [message | state.messages]} end)
    :ok
  end

  @doc "The non-transactional messages recorded so far (arrival order)."
  def recorded_messages, do: Agent.get(__MODULE__, fn %{messages: ms} -> Enum.reverse(ms) end)

  @doc "The current record count (a leg's snapshot point)."
  def mark, do: Agent.get(__MODULE__, fn %{txns: txns} -> length(txns) end)

  @doc "The transactions recorded AFTER `mark`, in arrival order."
  def since(mark) do
    Agent.get(__MODULE__, fn %{txns: txns} ->
      txns |> Enum.reverse() |> Enum.drop(mark)
    end)
  end
end
