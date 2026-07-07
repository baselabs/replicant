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

    asm =
      build_assembler(
        slot_name,
        sink,
        Keyword.get(opts, :checkpoint_store),
        Keyword.get(opts, :batch),
        Keyword.get(opts, :streaming),
        Keyword.get(opts, :max_inflight_lag)
      )

    {:ok, %{slot_name: slot_name, asm: asm, halted: false, conn_pid: nil, batch_timer: nil}}
  end

  # Lib mode: bind the writer to the pipeline's CheckpointStore (the watermark is
  # seeded later by the Connection's {:seed_lib_checkpoint, _} cast, from the SAME
  # store read it does on connect — one read, deterministic ordering before any
  # Commit). No store I/O in init (fast boot; the CheckpointStore's own non-sync
  # connect owns resilience). A lib-mode assembler is NEVER built without a writer.
  defp build_assembler(slot_name, sink, store, batch, streaming, max_inflight_lag)
       when is_list(store) do
    max_retries =
      Keyword.get(store, :max_retries, Replicant.CheckpointStore.default_max_retries())

    backoff =
      Keyword.get(store, :retry_backoff_ms, Replicant.CheckpointStore.default_retry_backoff_ms())

    writer = fn lsn ->
      write_with_retry(store_write(slot_name, lsn), slot_name, max_retries, backoff, 0)
    end

    Assembler.new(sink,
      mode: :lib,
      checkpoint_writer: writer,
      slot_name: slot_name,
      batch: batch,
      max_concurrent_txns: stream_limit(streaming),
      spill: stream_spill(streaming),
      max_inflight_lag: max_inflight_lag
    )
  end

  # Sink-owned mode still passes `slot_name` — spill names its per-stream subdir after the slot
  # (spec §5), so the assembler needs it even without a checkpoint writer.
  defp build_assembler(slot_name, sink, nil, batch, streaming, max_inflight_lag),
    do:
      Assembler.new(sink,
        slot_name: slot_name,
        batch: batch,
        max_concurrent_txns: stream_limit(streaming),
        spill: stream_spill(streaming),
        max_inflight_lag: max_inflight_lag
      )

  # The per-stream concurrency cap (spec §7): `nil` when streaming is not configured (the assembler
  # falls back to its own default), else the caller's `:max_concurrent_txns`.
  defp stream_limit(nil), do: nil

  defp stream_limit(streaming) when is_list(streaming),
    do: Keyword.get(streaming, :max_concurrent_txns)

  # The nested spill config (spec §5): `nil` when streaming (or its `:spill`) is absent (spill
  # disabled), else the caller's `[dir: _, max_spill_bytes: _]`.
  defp stream_spill(nil), do: nil
  defp stream_spill(streaming) when is_list(streaming), do: Keyword.get(streaming, :spill)

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
    state = %{state | conn_pid: from}
    before = state.asm.spilled_total
    asm = Assembler.observe_bytes(state.asm, bytes)
    # A spill flushed a stream buffer's tail to disk: signal the Connection so it can extend its
    # in-flight lag window past the resident-only accounting (spec §5). Value-free — carries only the
    # cumulative spilled byte count, never a row value. Plain send → the Connection's handle_info,
    # matching the {:sink_committed, _} dispatch idiom (Task 10 handles it there).
    if asm.spilled_total != before, do: send(from, {:spilled_bytes, asm.spilled_total})
    dispatch(Assembler.handle_message(asm, message), from, %{state | asm: asm})
  end

  # The Connection seeds the lib-mode watermark from its connect-time store read, before streaming.
  # This fires on EVERY (re)connect. Re-seed the watermark AND discard any open in-memory batch:
  # on a mid-stream reconnect the un-checkpointed batch re-streams from the durable checkpoint and
  # re-buffers as a FRESH batch, so a transient reconnect matches the crash/stop→resume dup model
  # (bounds dup to one batch per reconnect; a surviving stale batch would misalign flush boundaries
  # and compound dup under flapping — CV1 closeout). Cancel the now-stale flush timer. A no-op in
  # sink-owned mode / on the initial connect (no batch open). Never checkpoints — loss=0 by re-delivery.
  def handle_cast({:seed_lib_checkpoint, lsn}, %{asm: asm} = state) when is_integer(lsn) do
    asm = %{Assembler.reset_batch(asm) | lib_checkpoint: lsn}
    {:noreply, cancel_batch_timer(%{state | asm: asm})}
  end

  # Sink-owned batch mode: on every (re)connect the Connection casts {:reset_batch} (start_streaming
  # + snapshot handoff), discarding any open in-memory batch and canceling the flush timer — so a
  # transient reconnect matches the crash/stop→resume model (un-delivered txns re-stream and
  # re-buffer as a FRESH batch; effect-once holds, the durable sink checkpoint gates every ack).
  # lib_checkpoint (the span base) is left intact — it equals the last delivered batch's LSN.
  def handle_cast({:reset_batch}, %{asm: asm} = state) do
    {:noreply, cancel_batch_timer(%{state | asm: Assembler.reset_batch(asm)})}
  end

  # Streaming: on every (re)connect the Connection casts {:reset_streams}, discarding any
  # in-progress streamed transactions so a transient reconnect re-streams them from the durable
  # checkpoint (effect-once by spec §9; loss=0 by re-stream). A no-op when no stream is open.
  def handle_cast({:reset_streams}, %{asm: asm} = state) do
    {:noreply, %{state | asm: Assembler.reset_streams(asm)}}
  end

  # The Connection reports the per-stream floor (its first XLogData frame's wal_end — where PG began
  # streaming) once per (re)connect. It is the cold-start component of the batch span-cap base
  # `max(lib_checkpoint, stream_floor)` (spec §7). A no-op in sink-owned mode.
  def handle_cast({:stream_floor, floor}, %{asm: asm} = state) when is_integer(floor) do
    {:noreply, %{state | asm: Assembler.put_stream_floor(asm, floor)}}
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

  defp dispatch({:buffered, asm}, _from, state) do
    # Batch still open, no ack yet; arm the flush timer if this txn just opened the batch.
    {:noreply, ensure_batch_timer(%{state | asm: asm})}
  end

  defp dispatch({:flush, reason, asm}, _from, state) do
    do_flush(%{state | asm: asm}, reason)
  end

  defp dispatch({:halt, reason, halted_asm}, _from, state) do
    # Fail-closed: destructive schema change / sink write fault / unidentifiable relation.
    # `reason` is already value-free. Terminate the whole pipeline permanently; cancel the
    # flush timer and mark halted so a stale :batch_timeout cannot drive a store write during
    # teardown (spec §9). Do NOT self-crash (a crash exit would race :one_for_all restart).
    Replicant.Supervisor.halt(state.slot_name, reason)
    # Discard any open spill files: a halt tears the pipeline down without a reset cast, so the
    # halted assembler's stream/batch spill files would leak on disk otherwise (spec §5 cleanup).
    # The returned assembler is dropped — this process is terminating; we run the resets purely
    # for the file-delete side effect.
    _ = halted_asm |> Replicant.Assembler.reset_streams() |> Replicant.Assembler.reset_batch()
    {:noreply, %{cancel_batch_timer(state) | halted: true}}
  end

  # The batch flush-timer fired. Guard on BOTH the terminal `halted` state AND a still-open
  # batch: a stale timer (the batch already flushed by a count/span trigger, or the pipeline
  # halted mid-retry) must be a no-op and never drive a store write during teardown (spec §9;
  # connection.ex stale-timer precedent + the replicant-otp-async-lifetime-hygiene rule).
  @impl true
  def handle_info(:batch_timeout, %{halted: true} = state), do: {:noreply, state}

  def handle_info(:batch_timeout, state) do
    if Assembler.batch_pending?(state.asm) do
      do_flush(state, :max_delay_ms)
    else
      {:noreply, %{state | batch_timer: nil}}
    end
  end

  def handle_info(_other, state), do: {:noreply, state}

  # Flush the open batch: write ONE checkpoint at the batch's highest LSN and ack it. A write
  # fault halts fail-closed (buffer discarded, no ack). `:empty` (a stale timer with no open
  # batch) is a no-op. `conn_pid` is the Connection captured from the last {:message, ...} cast —
  # a flush is reachable ONLY after a `{:buffered}` dispatch opened the batch (from that same cast),
  # so `conn_pid` is always a live pid here, never nil.
  defp do_flush(state, reason) do
    state = cancel_batch_timer(state)

    case Assembler.flush_batch(state.asm, reason) do
      {:ok, lsn, asm} ->
        send(state.conn_pid, {:sink_committed, lsn})
        {:noreply, %{state | asm: asm}}

      {:error, error, asm} ->
        Replicant.Supervisor.halt(state.slot_name, error)
        {:noreply, %{state | asm: asm, halted: true}}

      :empty ->
        {:noreply, state}
    end
  end

  # Arm the max_delay_ms flush timer once per batch (on the txn that OPENS the batch). Bound to
  # the batch state: canceled on every flush, and its fire is guarded (see handle_info) so a
  # stale timer never flushes a recovered/absent batch (spec §9; replicant-otp-async-lifetime).
  defp ensure_batch_timer(%{batch_timer: nil, asm: asm} = state) do
    delay = Keyword.fetch!(asm.batch, :max_delay_ms)
    %{state | batch_timer: Process.send_after(self(), :batch_timeout, delay)}
  end

  defp ensure_batch_timer(state), do: state

  defp cancel_batch_timer(%{batch_timer: nil} = state), do: state

  defp cancel_batch_timer(%{batch_timer: ref} = state) do
    Process.cancel_timer(ref)
    %{state | batch_timer: nil}
  end
end
