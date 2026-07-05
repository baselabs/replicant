defmodule Replicant.AssemblerServer do
  @moduledoc """
  The serial process shell over the pure `Replicant.Assembler` (spec §4). Receives
  **decoded** pgoutput messages from `Replicant.Connection` (which decodes behind
  the value-free boundary and never applies the sink), assembles transactions, and
  applies the sink **synchronously** — blocking THIS process, off the Connection's
  keepalive path. On a durable sink commit (or a watermark skip) it messages the
  Connection so the ack advances asynchronously; on a fail-closed condition
  (destructive schema change, sink WRITE fault, an unidentifiable-relation row) it
  halts the whole pipeline permanently (spec §6/§9).

  It is a single serial `GenServer` (not a Task per transaction) so transactions
  apply strictly in commit order — the correctness baseline of synchronous
  per-transaction delivery.
  """
  use GenServer

  alias Replicant.Assembler

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    slot_name = Keyword.fetch!(opts, :slot_name)
    GenServer.start_link(__MODULE__, opts, name: via(slot_name))
  end

  @doc "The Registry via-name a pipeline's AssemblerServer registers under."
  @spec via(String.t()) :: {:via, module(), term()}
  def via(slot_name), do: {:via, Registry, {Replicant.Registry, {slot_name, :assembler}}}

  @impl true
  def init(opts) do
    slot_name = Keyword.fetch!(opts, :slot_name)
    sink = Keyword.fetch!(opts, :sink)
    asm = build_assembler(slot_name, sink, Keyword.get(opts, :checkpoint_store))
    {:ok, %{slot_name: slot_name, asm: asm, halted: false}}
  end

  # Lib mode: bind the writer to the pipeline's CheckpointStore (the watermark is
  # seeded later by the Connection's {:seed_lib_checkpoint, _} cast, from the SAME
  # store read it does on connect — one read, deterministic ordering before any
  # Commit). No store I/O in init (fast boot; the CheckpointStore's own non-sync
  # connect owns resilience). A lib-mode assembler is NEVER built without a writer.
  defp build_assembler(slot_name, sink, store) when is_list(store) do
    max_retries =
      Keyword.get(store, :max_retries, Replicant.CheckpointStore.default_max_retries())

    backoff =
      Keyword.get(store, :retry_backoff_ms, Replicant.CheckpointStore.default_retry_backoff_ms())

    writer = fn lsn ->
      write_with_retry(store_write(slot_name, lsn), slot_name, max_retries, backoff, 0)
    end

    Assembler.new(sink, mode: :lib, checkpoint_writer: writer, slot_name: slot_name)
  end

  defp build_assembler(_slot_name, sink, nil), do: Assembler.new(sink)

  # The store write as a 0-arity thunk (so the retry loop can re-invoke it).
  defp store_write(slot_name, lsn) do
    fn -> Replicant.CheckpointStore.write(Replicant.CheckpointStore.via(slot_name), lsn) end
  end

  @doc false
  # Bounded sleep-retry for the mid-stream checkpoint write (spec §4). BLOCKS the serial
  # applier: it must NOT advance to the next transaction before this one's checkpoint is
  # durable (dup-bound-of-one). A PERMANENT fault returns immediately (halt-now); a transient
  # fault retries up to `max_retries` with a `retry_backoff_ms` sleep, then returns {:error, _}
  # (→ `Assembler.apply_sink` halts). The self-driven halt is intentionally delayed until
  # retries exhaust (up to `backoff × max_retries`); an external supervisor `:shutdown` still
  # preempts the sleep (this non-trapping GenServer cannot delay its own teardown), so pipeline
  # shutdown is never blocked.
  #
  # The `write_fun` contract is exactly `CheckpointStore.write/2`'s: `:ok | {:error, Error.t()}`
  # (the store scrubs every Postgrex/DBConnection fault to a value-free `%Replicant.Error{}`
  # before it returns). The spec is narrowed to that shape — not `{:error, term()}` — so the
  # two-clause `case` below is TOTAL over the writer's actual return domain (dialyzer proves
  # no other shape reaches it), rather than relying on a runtime catch-all.
  @spec write_with_retry(
          (-> :ok | {:error, Replicant.Error.t()}),
          String.t(),
          non_neg_integer(),
          pos_integer(),
          non_neg_integer()
        ) ::
          :ok | {:error, atom()}
  def write_with_retry(write_fun, slot_name, max_retries, backoff, attempt) do
    case write_fun.() do
      :ok ->
        :ok

      {:error, %Replicant.Error{reason: reason}} ->
        cond do
          Replicant.CheckpointStore.permanent_reason?(reason) ->
            {:error, reason}

          Replicant.CheckpointStore.retry_decision(attempt, max_retries) == :retry ->
            Replicant.CheckpointStore.emit_retrying(slot_name, attempt + 1, max_retries)
            Process.sleep(backoff)
            write_with_retry(write_fun, slot_name, max_retries, backoff, attempt + 1)

          true ->
            {:error, reason}
        end
    end
  end

  # Post-halt: drop WAL. The pipeline teardown (Supervisor.halt) is in flight and
  # will terminate this process; reprocessing here would be wasted and unsafe.
  @impl true
  def handle_cast({:message, _message, _bytes, _from}, %{halted: true} = state) do
    {:noreply, state}
  end

  def handle_cast({:message, message, bytes, from}, state) do
    asm = Assembler.observe_bytes(state.asm, bytes)
    dispatch(Assembler.handle_message(asm, message), from, state)
  end

  # The Connection seeds the lib-mode watermark from its connect-time store read,
  # before streaming. A no-op in sink-owned mode (the assembler ignores
  # lib_checkpoint there).
  def handle_cast({:seed_lib_checkpoint, lsn}, %{asm: asm} = state) when is_integer(lsn) do
    {:noreply, %{state | asm: %{asm | lib_checkpoint: lsn}}}
  end

  defp dispatch({:ok, asm}, _from, state), do: {:noreply, %{state | asm: asm}}

  defp dispatch({:transaction, _txn, lsn, asm}, from, state) do
    # The sink durably persisted the txn + checkpoint; tell the Connection to
    # advance the ack to `lsn` asynchronously (never on the Connection's own path).
    send(from, {:sink_committed, lsn})
    {:noreply, %{state | asm: asm}}
  end

  defp dispatch({:skipped, lsn, asm}, from, state) do
    # Watermark skip (commit_lsn <= sink checkpoint): the txn already landed, but
    # the ack must still advance to `lsn` so the slot moves past re-delivered data.
    send(from, {:sink_committed, lsn})
    {:noreply, %{state | asm: asm}}
  end

  defp dispatch({:schema_change, _sc, asm}, _from, state) do
    # Additive schema change auto-applied mid-stream; no commit boundary, no ack.
    {:noreply, %{state | asm: asm}}
  end

  defp dispatch({:halt, reason, _asm}, _from, state) do
    # Fail-closed: destructive schema change / sink write fault / unidentifiable
    # relation. `reason` is already value-free (a %SchemaChange{} or a value-free
    # %Error{} from the assembler's boundary). Terminate the whole pipeline
    # permanently; mark halted so no further WAL is processed in the teardown
    # window. Do NOT self-crash (a crash exit would race :one_for_all restart
    # before the DynamicSupervisor terminates the pipeline).
    Replicant.Supervisor.halt(state.slot_name, reason)
    {:noreply, %{state | halted: true}}
  end
end
