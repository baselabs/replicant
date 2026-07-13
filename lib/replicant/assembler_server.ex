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

  alias Replicant.{Assembler, Telemetry, Transaction}

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    slot_name = Keyword.fetch!(opts, :slot_name)
    GenServer.start_link(__MODULE__, opts, name: via(slot_name))
  end

  @doc "The Registry via-name a pipeline's AssemblerServer registers under."
  @spec via(String.t()) :: {:via, module(), term()}
  def via(slot_name), do: {:via, Registry, {Replicant.Registry, {slot_name, :assembler}}}

  @doc """
  Open the drop-set tracking window for a table BEFORE the reader captures LW
  (spec §2 R1). The reply is DEFERRED while the assembler-local in-flight estimate
  (frontier − last applied LSN) exceeds max_inflight_lag ÷ 2 — the pacing gate
  (spec §4 R2): stream drain gets priority, chunks fill idle capacity.

  Replies `{:ok, epoch}` — the window GENERATION the reader now operates under. The
  keyless reader threads this back to `finish_snapshot_table/3` so the barrier can
  reject a stale generation (a reconnect reset bumped the epoch since the reader
  opened → its batches were WIPED, not applied — spec §4/§6.4, the 85672f1
  sender-tags-epoch discipline). Replies `{:error, :table_discarded}` if the table was
  contention-discarded (reader re-reads); `{:error, :window_reset}` if a reconnect
  released the deferred open.
  """
  @spec open_snapshot_window(GenServer.server(), String.t()) ::
          {:ok, non_neg_integer()} | {:error, :window_reset | :table_discarded}
  def open_snapshot_window(server, qualified),
    do: GenServer.call(server, {:open_snapshot_window, qualified}, :infinity)

  @doc """
  Hand a read chunk to the applier. The reply is DEFERRED while max_pending_chunks are buffered.
  `{:error, :window_reset}` on a reconnect; `{:error, :table_discarded}` on a contention discard
  (drop-cap breach / a PK-less table's concurrent write) — the reader re-reads from durable progress.
  """
  @spec deliver_snapshot_chunk(GenServer.server(), Replicant.SnapshotWindow.chunk()) ::
          :ok | {:error, :window_reset} | {:error, :table_discarded}
  def deliver_snapshot_chunk(server, chunk),
    do: GenServer.call(server, {:deliver_snapshot_chunk, chunk}, :infinity)

  @doc """
  A PK-less table's whole-read BARRIER (spec §6.4): after the reader streams all of a keyless
  table's provisional batches, it calls this to block until those batches have APPLIED (converged
  past any late contention) — replying `:ok` — or the table was contention-discarded — replying
  `{:error, :table_discarded}` so the reader redoes the whole table. Without this barrier a
  single-batch keyless read can complete before a late-arriving concurrent write discards its
  still-pending batch, silently losing the rows.

  `epoch` is the window GENERATION the reader captured at `open_snapshot_window/2`. If a reconnect
  reset re-seated the window (bumped the epoch) since the reader opened this table, its provisional
  batches were WIPED (not applied): the barrier MUST reply `{:error, :window_reset}` — never a
  spurious `:ok` (which would mark a never-delivered table done → data loss). The stale-generation
  check comes FIRST because a reset also clears `discarded`/`pending`, so a wiped table otherwise
  reads as clean. `{:error, :window_reset}` also fires when a reconnect releases a deferred barrier.
  """
  @spec finish_snapshot_table(GenServer.server(), String.t(), non_neg_integer()) ::
          :ok | {:error, :window_reset} | {:error, :table_discarded}
  def finish_snapshot_table(server, qualified, epoch),
    do: GenServer.call(server, {:finish_snapshot_table, qualified, epoch}, :infinity)

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

    snapshot_window =
      case Keyword.get(opts, :snapshot_window) do
        nil ->
          nil

        kw when is_list(kw) ->
          Replicant.SnapshotWindow.new(
            epoch: 0,
            drop_cap: 10 * Keyword.fetch!(kw, :chunk_rows),
            max_pending: Keyword.fetch!(kw, :max_pending_chunks)
          )
      end

    {:ok,
     %{
       slot_name: slot_name,
       asm: asm,
       halted: false,
       conn_pid: nil,
       batch_timer: nil,
       window: snapshot_window,
       floor_lsn: 0,
       last_applied: 0,
       deferred_deliver: nil,
       deferred_open: nil,
       deferred_drain: nil
     }}
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

    # Append the change (handle_message) BEFORE accounting its WAL bytes + running the spill trigger
    # (observe_bytes → maybe_spill): a change that crosses the RAM bound must be in the buffer when
    # maybe_spill flushes, else it stays resident + unaccounted and a single row > max_inflight_lag
    # never spills (CV2). Only a non-terminal {:ok, asm} carries a live buffer to account.
    #
    # Then signal the Connection its NET spilled total (up on a fresh spill, DOWN when a spilled txn
    # commits/aborts and frees disk) so the §4 in-flight-lag numerator can subtract spilled bytes
    # (spec §5 — they are on disk, not RAM, so a legitimately-spilling txn does not trip the halt).
    # Casting the NET post-dispatch total (not the observe delta) keeps the numerator from stranding
    # stale-high after a large spilled txn commits. A plain send → the Connection's handle_info,
    # matching the {:sink_committed, _} dispatch idiom.
    before = state.asm.spilled_total
    result = account_after_handle(Assembler.handle_message(state.asm, message), bytes)
    {:noreply, new_state} = disp = dispatch(result, from, state)

    if new_state.asm.spilled_total != before,
      do: send(from, {:spilled_bytes, new_state.asm.spilled_total})

    disp
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

  # --- incremental snapshot window (spec §2/§4) ---

  # Epoch-tagged frontier (keepalive/XLogData wal_end forwarded by the Connection during an active
  # backfill). Stale epochs are ignored INSIDE SnapshotWindow.set_frontier/3, then drain any chunk
  # the advanced frontier just closed.
  def handle_cast({:snapshot_frontier, epoch, lsn}, %{window: %{} = w} = state)
      when is_integer(lsn) do
    state = %{state | window: Replicant.SnapshotWindow.set_frontier(w, epoch, lsn)}
    {:noreply, state} = apply_ready_chunks(state)
    {:noreply, state}
  end

  def handle_cast({:snapshot_frontier, _epoch, _lsn}, state), do: {:noreply, state}

  # Reconnect re-seed (spec §4): discard pending + tracking, adopt the new epoch, and RELEASE any
  # deferred reader call with {:error, :window_reset} so the reader restarts its loop from durable
  # progress (never left hanging across a reconnect — OTP async-lifetime hygiene).
  def handle_cast({:reset_snapshot_window, epoch}, %{window: %{} = w} = state) do
    release_deferred(state, {:error, :window_reset})

    {:noreply,
     %{
       state
       | window: Replicant.SnapshotWindow.reset(w, epoch),
         deferred_deliver: nil,
         deferred_open: nil,
         deferred_drain: nil
     }}
  end

  def handle_cast({:reset_snapshot_window, _epoch}, state), do: {:noreply, state}

  # The backfill floor (slot-creation consistent_point) — rides ctx.snapshot_lsn on every chunk.
  def handle_cast({:snapshot_floor, lsn}, state) when is_integer(lsn),
    do: {:noreply, %{state | floor_lsn: lsn}}

  @impl true
  def handle_call({:open_snapshot_window, qualified}, from, %{window: %{} = w} = state) do
    cond do
      Replicant.SnapshotWindow.discarded?(w, qualified) ->
        # The table was contention-discarded before the reader (re)opened it — tell it to re-read
        # and clear the flag so the re-read's first chunk is admitted.
        {:reply, {:error, :table_discarded},
         %{state | window: Replicant.SnapshotWindow.clear_discarded(w, qualified)}}

      paced?(state) ->
        {:noreply, %{state | deferred_open: {from, qualified}}}

      true ->
        {:reply, {:ok, w.epoch},
         %{state | window: Replicant.SnapshotWindow.open_window(w, qualified)}}
    end
  end

  def handle_call({:deliver_snapshot_chunk, chunk}, from, %{window: %{} = w} = state) do
    if Replicant.SnapshotWindow.discarded?(w, chunk.qualified) do
      # Contention discarded this table since the reader last delivered for it: re-read from durable
      # progress. Clear the flag so the re-read's first chunk is admitted.
      {:reply, {:error, :table_discarded},
       %{state | window: Replicant.SnapshotWindow.clear_discarded(w, chunk.qualified)}}
    else
      case Replicant.SnapshotWindow.add_chunk(w, chunk) do
        {w, :ok} ->
          # Accepted under the cap: drain any newly-closed chunks, then reply :ok.
          {:noreply, state} = apply_ready_chunks(%{state | window: w})
          {:reply, :ok, state}

        {w, :at_capacity} ->
          {:noreply, %{state | window: w, deferred_deliver: {from, chunk}}}
      end
    end
  end

  # PK-less whole-read barrier (spec §6.4): reply once the table's buffered chunks have drained
  # (all applied — :ok) or it was contention-discarded ({:error, :table_discarded}); DEFER while
  # chunks are still pending. This closes the single-batch keyless race where a late concurrent
  # write discards a still-pending batch after the reader already streamed it.
  #
  # STALE-GENERATION CHECK FIRST (spec §4/§6.4): if a reconnect reset re-seated the window (bumped
  # the epoch) since the reader opened this table under `reader_epoch`, its provisional batches were
  # WIPED by the reset — reply {:error, :window_reset} so the reader re-reads, NEVER a spurious :ok.
  # A reset ALSO clears discarded/pending, so a wiped table would otherwise read as clean and pass
  # the barrier. Epoch is monotone (reset only increases it), so `e != reader_epoch` ⟺ a reset since
  # open. Capturing the epoch at the reader's OPEN (not at barrier entry) is what makes this catch
  # the race: the reset lands BEFORE the barrier call is processed, so a barrier-entry capture would
  # already read the post-reset epoch and miss it.
  def handle_call(
        {:finish_snapshot_table, qualified, reader_epoch},
        from,
        %{window: %{epoch: e} = w} = state
      ) do
    cond do
      e != reader_epoch ->
        {:reply, {:error, :window_reset}, state}

      Replicant.SnapshotWindow.discarded?(w, qualified) ->
        {:reply, {:error, :table_discarded},
         %{state | window: Replicant.SnapshotWindow.clear_discarded(w, qualified)}}

      Replicant.SnapshotWindow.table_pending?(w, qualified) ->
        {:noreply, %{state | deferred_drain: {from, qualified, reader_epoch}}}

      true ->
        {:reply, :ok, state}
    end
  end

  # Account WAL bytes + run the spill trigger AFTER the change is appended (CV2). Only a non-terminal
  # {:ok, asm} carries a resident buffer worth accounting; every terminal result already delivered or
  # deleted its buffer, so its trivial frame bytes are skipped.
  defp account_after_handle({:ok, asm}, bytes), do: {:ok, Assembler.observe_bytes(asm, bytes)}
  defp account_after_handle(result, _bytes), do: result

  defp dispatch({:ok, asm}, _from, state), do: {:noreply, %{state | asm: asm}}

  defp dispatch({:transaction, txn, lsn, asm}, from, state) do
    # The sink durably persisted the txn + checkpoint; tell the Connection to
    # advance the ack to `lsn` asynchronously (never on the Connection's own path).
    # First feed the committed txn's PKs into any open snapshot window (drop-set
    # tracking) and drain newly-closed chunks — a no-op when incremental is off.
    state = track_window(%{state | asm: asm}, txn_changes(txn), lsn)
    send(from, {:sink_committed, lsn})
    {:noreply, state}
  end

  defp dispatch({:skipped, lsn, asm}, from, state) do
    # Watermark skip (commit_lsn <= sink checkpoint): the txn already landed, but
    # the ack must still advance to `lsn` so the slot moves past re-delivered data.
    # A skipped txn carries no NEW rows for the drop-set (its data predates the
    # checkpoint <= LW, so the chunk already contains it) — advance the frontier only.
    state = track_window(%{state | asm: asm}, [], lsn)
    send(from, {:sink_committed, lsn})
    {:noreply, state}
  end

  # A2 (Task 9, spec §7.1): a NON-transactional pg_logical_emit_message delivered standalone via
  # handle_message/2. The sink durably persisted it (no commit boundary, no transaction-watermark
  # dedup — at-least-once per the handle_message/2 doc); ack `lsn` to the Connection exactly as a
  # committed txn does. track_window advances the applied frontier (a non-txn message carries no
  # row PKs for the drop-set, so [] — same as the skipped path); a no-op when incremental is off.
  defp dispatch({:message_delivered, lsn, asm}, from, state) do
    state = track_window(%{state | asm: asm}, [], lsn)
    send(from, {:sink_committed, lsn})
    {:noreply, state}
  end

  # A2 (Task 9, spec §8.4 loss=0): a NON-txn message arrived while a lib/sink-owned batch is OPEN.
  # Delivering+acking the message's LSN now would advance the slot past the batch's un-checkpointed
  # WAL → loss on a crash-before-flush. So flush+ack the batch FIRST (durability-before-ack), then
  # re-dispatch the SAME message on the flushed assembler (pending_lsn now nil → the second pass
  # reaches deliver_message → {:message_delivered}). If the flush halts (a write fault), the message
  # is NOT delivered — fail-closed, no ack (the message re-streams from the durable checkpoint on
  # restart; at-least-once).
  defp dispatch({:flush_before_message, reason, asm, msg}, from, state) do
    state = %{state | asm: asm}
    {:noreply, flushed_state} = do_flush(state, reason)

    if flushed_state.halted do
      {:noreply, flushed_state}
    else
      dispatch(Assembler.handle_message(flushed_state.asm, msg), from, flushed_state)
    end
  end

  defp dispatch({:schema_change, _sc, asm}, _from, state) do
    # Additive schema change auto-applied mid-stream; no commit boundary, no ack.
    {:noreply, %{state | asm: asm}}
  end

  defp dispatch({:buffered, asm}, _from, state) do
    # Batch still open, no ack yet; arm the flush timer if this txn just opened the batch.
    # Track the just-buffered txn's PKs at RECEIPT (convergence-safe) so a colliding snapshot
    # chunk row is dropped — a no-op when incremental is off.
    state = track_window(%{state | asm: asm}, buffered_changes(asm), buffered_lsn(asm))
    {:noreply, ensure_batch_timer(state)}
  end

  defp dispatch({:flush, reason, asm}, _from, state) do
    # A count/span-cap-tripping batch txn returns {:flush} DIRECTLY, skipping {:buffered} — so it is
    # ALSO a receipt point for drop-set tracking (mode-uniform convergence, spec §2/§4). Track it at
    # RECEIPT here, exactly as dispatch({:buffered}) does, BEFORE do_flush → Assembler.flush_batch
    # RESETS the batch. Sink-owned batch_delivery: buffered_changes(asm) is the tripping txn's changes
    # (batch_txns head). lib+batch: its retained `last_buffered_changes` (a `%Change{}` list → drop-
    # filter, or `:spilled` → taint). Either way buffered_lsn is its commit_lsn. A no-op when
    # incremental is off. The timer-driven flush needs NO tracking here — every txn in that batch
    # already tracked at its own {:buffered} (do_flush left unchanged).
    state = track_window(%{state | asm: asm}, buffered_changes(asm), buffered_lsn(asm))
    do_flush(state, reason)
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

  # --- incremental snapshot window helpers (spec §2/§4) ---

  # Feed a delivered/received txn's PKs into the drop-set for every OPEN table + advance the applied
  # frontier, then drain any newly-closed chunk. A no-op when incremental is off (window: nil).
  defp track_window(%{window: nil} = state, _changes, _lsn), do: state

  # A plain `%Change{}` list — feed its PKs into the drop-set (DROP-FILTER colliding chunk rows) and
  # advance the frontier. EVERY mode with enumerable changes routes here: sink-owned / lib-non-batch
  # per-txn ({:transaction}), sink-owned batch (batch_txns head), AND lib+batch (the retained
  # `last_buffered_changes` — per-txn delivery IS the flush, so tracking at receipt is exact, spec §4).
  defp track_window(%{window: w} = state, changes, lsn) when is_list(changes) do
    {w, _discarded} = Replicant.SnapshotWindow.track_capped(w, changes)
    advance_window(state, w, lsn)
  end

  # A non-list `changes` — a lazy, single-pass spill-backed Enumerable (spec §5, valid ONLY during
  # the sink call: delivered per-txn or in a sink-owned batch) or the `:spilled` marker for a lib+
  # batch txn whose changes were unenumerable. Its PKs can't be extracted, so conservatively TAINT
  # every OPEN table (discard-and-re-read → signal re-read; a spilled row never silently escapes the
  # drop-set). Rare (only >RAM spilled txns) so it converges, never a per-write livelock. Frontier
  # still advances on the commit lsn.
  defp track_window(%{window: w} = state, _unenumerable, lsn) do
    advance_window(state, taint_open_tables(w), lsn)
  end

  defp advance_window(state, w, lsn) do
    w = Replicant.SnapshotWindow.observe_applied(w, lsn)

    {:noreply, state} =
      apply_ready_chunks(%{state | window: w, last_applied: max(state.last_applied, lsn)})

    state
  end

  # Taint (discard pending chunks + reset tracking → signal re-read) EVERY open table. Used when a
  # delivered txn's changes are unenumerable (a lazy spill Reader / the `:spilled` marker) so its PKs
  # can't drop-filter. `taint_table/2` is a no-op for a name with no tracking entry (spec §2/§4).
  defp taint_open_tables(w) do
    Enum.reduce(Map.keys(w.tracking), w, fn q, acc ->
      Replicant.SnapshotWindow.taint_table(acc, q)
    end)
  end

  # The just-committed txn's changes (a list, or a lazy spill Reader); the skipped path passes [].
  defp txn_changes(%Transaction{changes: changes}), do: changes

  # The just-buffered txn's changes (spec §7 batch modes). Sink-owned batch retains the full txn at
  # the head of `batch_txns`; lib+batch delivered it per-txn and retains only its `changes` (a
  # `%Change{}` list → drop-filter, or `:spilled` → taint) on `last_buffered_changes`. Both feed
  # track_window so a colliding snapshot-chunk row is dropped (or tainted for an unenumerable spill).
  defp buffered_changes(%Assembler{batch_txns: [txn | _]}), do: txn.changes
  defp buffered_changes(%Assembler{} = asm), do: Assembler.last_buffered_changes(asm)

  defp buffered_lsn(%Assembler{pending_lsn: lsn}), do: lsn

  # Apply every closed pending chunk in order. Runs on the serial applier; the closure-check → apply
  # pair is atomic here (no interleaving point — spec §4). Always returns {:noreply, state}.
  defp apply_ready_chunks(%{window: %{} = w} = state) do
    case Replicant.SnapshotWindow.pop_ready(w) do
      :none ->
        {:noreply, state |> release_capacity() |> settle_drain()}

      {:apply, kept, chunk, w} ->
        apply_one_chunk(%{state | window: w}, kept, chunk)

      {:discard, _chunk, w} ->
        # A contended chunk was dropped (not applied): the table is now marked needs-re-read (its
        # reader learns on its next window/barrier call). Keep draining the rest.
        apply_ready_chunks(%{state | window: w})
    end
  end

  # Settle a deferred PK-less drain barrier (finish_snapshot_table): reply {:error, :window_reset}
  # if a reconnect reset re-seated the window since the reader opened (stale generation, batches
  # WIPED), {:error, :table_discarded} if the table was contention-discarded, :ok once its chunks
  # have all drained, else keep waiting. The epoch check mirrors the barrier-entry guard (a reset
  # while deferred is normally caught earlier by release_deferred, which nulls deferred_drain — this
  # is a defense-in-depth re-check on the same monotone-epoch invariant).
  defp settle_drain(
         %{deferred_drain: {from, qualified, reader_epoch}, window: %{epoch: e} = w} = state
       ) do
    cond do
      e != reader_epoch ->
        GenServer.reply(from, {:error, :window_reset})
        %{state | deferred_drain: nil}

      Replicant.SnapshotWindow.discarded?(w, qualified) ->
        GenServer.reply(from, {:error, :table_discarded})

        %{
          state
          | deferred_drain: nil,
            window: Replicant.SnapshotWindow.clear_discarded(w, qualified)
        }

      Replicant.SnapshotWindow.table_pending?(w, qualified) ->
        state

      true ->
        GenServer.reply(from, :ok)
        %{state | deferred_drain: nil}
    end
  end

  defp settle_drain(state), do: state

  # EVERY sink return shape is handled at the site (flush-path value-free rule, 2 prior incidents):
  # :ok → progress persisted (lib mode) + telemetry, then drain the next closed chunk; ANY other
  # shape / raise / throw / exit → value-free halt (never inspect the term, never leak it).
  defp apply_one_chunk(state, kept, chunk) do
    case apply_chunk(state, kept, chunk) do
      :ok ->
        Telemetry.event([:replicant, :snapshot, :chunk_completed], %{}, %{
          table: "#{chunk.schema}.#{chunk.table}",
          change_count: length(kept)
        })

        if chunk.complete? do
          # The dedicated completion chunk applied DURABLY (sink-owned atomic, or lib progress written
          # — apply_chunk only returns :ok after persist_progress). The backfill is done and no pending
          # chunks remain, so DROP the window: post-completion streaming must not keep tracking a
          # drop-set for completed tables (bounded, but perpetual per-txn churn otherwise). A resumed
          # completion re-delivery lands here again idempotently; a reconnect re-seeds a fresh window.
          {:noreply, %{state | window: nil}}
        else
          apply_ready_chunks(state)
        end

      :halt ->
        # Value-free surfaced halt: Supervisor.halt/2 DISCARDS its reason (supervisor.ex:48), so this
        # telemetry event is the observable halt channel — reason is a bare allowlisted atom, NEVER
        # the sink's return. Release any deferred reader call so it cannot hang across teardown.
        Telemetry.event([:replicant, :snapshot, :failed], %{}, %{reason: :snapshot_failed})
        Replicant.Supervisor.halt(state.slot_name, {:snapshot, :snapshot_failed})
        release_deferred(state, {:error, :window_reset})

        {:noreply,
         %{state | halted: true, deferred_deliver: nil, deferred_open: nil, deferred_drain: nil}}
    end
  end

  # Deliver a closed chunk's kept rows to the sink behind a value-free boundary. The ctx carries the
  # backfill floor LSN + spec-§6.1 keys; the sink's return is collapsed to :ok | :halt WITHOUT ever
  # inspecting a non-:ok term / a raise / throw / exit reason (Critical Rule 1).
  defp apply_chunk(state, kept, chunk) do
    ctx = %{
      snapshot_lsn: state.floor_lsn,
      table: "#{chunk.schema}.#{chunk.table}",
      first_for_table?: chunk.first?,
      backfill_complete?: chunk.complete?,
      progress: chunk.progress
    }

    result =
      try do
        state.asm.sink.handle_snapshot(kept, ctx)
      rescue
        _ -> :halt_sentinel
      catch
        _kind, _reason -> :halt_sentinel
      end

    case result do
      :ok -> persist_progress(state, chunk.progress)
      _other -> :halt
    end
  end

  # Lib mode: the store owns progress (written AFTER delivery — dup <= 1 chunk, spec §2). A write
  # fault halts fail-closed (never inspect the store reason). Sink-owned: the sink persisted
  # ctx.progress atomically inside handle_snapshot — no-op here.
  defp persist_progress(%{asm: %{mode: :lib}} = state, token) do
    case Replicant.CheckpointStore.write_progress(
           Replicant.CheckpointStore.via(state.slot_name),
           token
         ) do
      :ok -> :ok
      {:error, _} -> :halt
    end
  end

  defp persist_progress(_state, _token), do: :ok

  # After an apply freed a pending slot, release a deferred deliver (re-admit its chunk under the cap)
  # or a paced open (re-checked against the pacing gate). No deferred call → state unchanged.
  defp release_capacity(%{deferred_deliver: {from, chunk}, window: w} = state) do
    if Replicant.SnapshotWindow.discarded?(w, chunk.qualified) do
      # The table was contention-discarded while this deliver waited at capacity: don't re-admit a
      # stale chunk — tell the reader to re-read and clear the flag.
      GenServer.reply(from, {:error, :table_discarded})

      %{
        state
        | deferred_deliver: nil,
          window: Replicant.SnapshotWindow.clear_discarded(w, chunk.qualified)
      }
    else
      case Replicant.SnapshotWindow.add_chunk(w, chunk) do
        {w, :ok} ->
          GenServer.reply(from, :ok)
          {:noreply, state} = apply_ready_chunks(%{state | window: w, deferred_deliver: nil})
          state

        {w, :at_capacity} ->
          %{state | window: w}
      end
    end
  end

  defp release_capacity(%{deferred_open: {from, qualified}} = state) do
    if paced?(state) do
      state
    else
      # Reply {:ok, epoch} — the SAME window-generation contract as the synchronous open reply, so a
      # reader whose open was paced still captures the epoch it operates under (barrier stale guard).
      GenServer.reply(from, {:ok, state.window.epoch})

      %{
        state
        | window: Replicant.SnapshotWindow.open_window(state.window, qualified),
          deferred_open: nil
      }
    end
  end

  defp release_capacity(state), do: state

  # Reply to whichever deferred reader call is outstanding (the serial reader holds at most one) so it
  # is never left hanging across a reconnect-reset or a halt.
  defp release_deferred(%{deferred_deliver: {from, _}}, reply), do: GenServer.reply(from, reply)
  defp release_deferred(%{deferred_open: {from, _}}, reply), do: GenServer.reply(from, reply)
  defp release_deferred(%{deferred_drain: {from, _, _}}, reply), do: GenServer.reply(from, reply)
  defp release_deferred(_state, _reply), do: :ok

  # The pacing gate (spec §4 R2): assembler-local in-flight estimate vs max_inflight_lag ÷ 2 —
  # stream drain gets priority, chunks fill idle capacity. The in-flight base is
  # `max(last_applied, floor_lsn)`, NOT last_applied alone: `frontier` is seeded from ABSOLUTE
  # wal_end LSNs (large — e.g. 1e9), but `last_applied` is 0 until the first commit, so on a fresh
  # slot / idle DB `frontier − 0` is astronomically larger than max_inflight_lag ÷ 2 and the window
  # would be paced open indefinitely (backfill never starts). Before any commit, "applied" is the
  # backfill's consistent-point floor (`floor_lsn`, from {:snapshot_floor}). Same fresh-slot
  # large-absolute-LSN class the batching span-cap base fixed via max(lib_checkpoint, stream_floor).
  defp paced?(%{window: %{frontier: f}, last_applied: a, floor_lsn: floor, asm: asm}) do
    max_lag = asm.max_inflight_lag || Replicant.Connection.default_max_inflight_lag()
    f - max(a, floor) > div(max_lag, 2)
  end
end
