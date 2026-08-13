defmodule Replicant.Assembler.Batch do
  @moduledoc false

  # Batched checkpointing (spec §6/§7), extracted from `Replicant.Assembler` as a module of pure
  # functions on the shared `%Assembler{}` struct. The struct shape is UNCHANGED; `Assembler`
  # delegates. Core↔Batch call cycle (accepted, runtime-resolved in Elixir; both modules
  # `@moduledoc false`): Core's `apply_sink/3` routes sink-owned batch to `buffer_for_delivery/3`;
  # `deliver_now/3` routes lib+batch to `buffer_txn/3`; `flush_batch/2` (public, the AssemblerServer
  # entry) delegates to `flush_sink_batch/2`/`flush_lib_batch/2`. Batch calls Core's `reset/1`
  # + `span_base/1`. The Rule-1 batch-flush scrub (`flush_sink_batch`/`flush_lib_batch`/
  # `sink_batch_failed`) lives here as ONE coherent, tamper-tested unit; every other Rule-1 scrub
  # site stays in Core.

  alias Replicant.{Assembler, Error, Spill, Telemetry, Transaction}

  # --- batched-checkpoint mode predicates (spec §7) ---

  def batching?(%Assembler{mode: :lib, batch: batch}), do: is_list(batch)
  def batching?(_asm), do: false

  def sink_owned_batching?(%Assembler{mode: :sink_owned, batch: batch}), do: is_list(batch)
  def sink_owned_batching?(_asm), do: false

  # The sink persisted the txn's DATA per-txn (announce [:sink, :committed] now); DEFER the
  # store checkpoint write + ack to the batch flush. Accumulate into the open batch and signal
  # a flush when the count OR the LSN-span cap trips. Per spec §4/§7/decision-log #7 the span is
  # `pending_lsn − max(lib_checkpoint, stream_floor)` — the batch's contribution to the §4 in-flight
  # lag, mirroring the Connection's own lag floor (`max(checkpoint_lsn, stream_floor_lsn)`,
  # connection.ex:499). `stream_floor` (the stream's start position, from the Connection's first
  # frame) makes this robust on a fresh slot where lib_checkpoint is 0 but stream LSNs are
  # large-absolute — without it the first txn would spuriously span-flush. The max_delay_ms timer is
  # the AssemblerServer's third trigger. `lib_checkpoint` is NOT advanced here — only at flush (§9).
  def buffer_txn(
        %Assembler{} = asm,
        %Transaction{commit_lsn: lsn, changes: changes} = _txn,
        duration
      ) do
    Telemetry.event([:replicant, :sink, :committed], %{duration: duration}, %{commit_lsn: lsn})

    count = asm.batch_count + 1

    # Retain THIS delivered txn's changes (overwritten each buffer_txn) so the AssemblerServer can
    # feed its PKs into the drop-set — DROP-FILTERING colliding snapshot-chunk rows exactly as
    # lib-non-batch and sink-owned batch already do (spec §4: the drop-set consults the flush
    # boundary, and per-txn lib+batch delivery IS the flush). A lazy/spilled (non-list) `changes` is
    # single-pass and unenumerable → the `:spilled` marker (server taints every open table instead —
    # the only case still needing re-read, and rare enough it converges, never a per-write livelock).
    asm = %{
      Assembler.reset(asm)
      | batch_count: count,
        pending_lsn: lsn,
        last_buffered_changes: changes_for_tracking(changes)
    }

    maybe_trip_batch(count, lsn, asm)
  end

  # Retain a delivered lib+batch txn's `changes` for drop-set tracking: a plain list passes through
  # (the AssemblerServer feeds its PKs to the drop-set → drop-filter); a lazy, single-pass
  # spill-backed `changes` (spec §5) MUST NOT be enumerated, so it yields the `:spilled` marker (the
  # server taints every open table → re-read, the only remaining conservative path).
  def changes_for_tracking(changes) when is_list(changes), do: changes
  def changes_for_tracking(_lazy), do: :spilled

  # Sink-owned batch delivery (spec §6): buffer the committed txn (NO sink call) and signal a
  # flush when the count OR the LSN-span cap trips (span base = max(lib_checkpoint, stream_floor),
  # spec §7 — identical to lib-batch). lib_checkpoint is NOT advanced here — only at flush (§9).
  # No [:sink, :committed] event: the txn is not delivered until flush_batch calls handle_batch.
  # Stored newest-first (prepend); flush_sink_batch reverses to ascending commit-LSN order.
  #
  # `spill_path` (nil for a non-spilled txn) is the on-disk file backing this buffered txn's lazy
  # Reader: the sink-owned batch OWNS the file until flush/reset (the txn is buffered, not delivered,
  # so the deliver_now delete never runs for this path). Record it on `batch_spill_paths` so
  # flush_sink_batch / reset_batch delete it exactly once — no orphan, no use-after-delete (the
  # flush's handle_batch re-reads the Reader from the file). No path is recorded for the per-txn /
  # lib+batch paths (they route to deliver_now, which deletes there) — the two are disjoint.
  def buffer_for_delivery(%Assembler{} = asm, %Transaction{commit_lsn: lsn} = txn, spill_path) do
    count = asm.batch_count + 1

    asm = %{
      Assembler.reset(asm)
      | batch_count: count,
        pending_lsn: lsn,
        batch_txns: [txn | asm.batch_txns],
        batch_spill_paths: prepend_path(asm.batch_spill_paths, spill_path)
    }

    maybe_trip_batch(count, lsn, asm)
  end

  # Shared flush-trigger predicate (spec §6) for BOTH batch modes: trip on the count cap OR the
  # LSN-span cap, else buffer. Centralized so lib-batch (buffer_txn) and sink-owned batch
  # (buffer_for_delivery) cannot drift — a drift would silently change the dup bound for one mode.
  def maybe_trip_batch(count, lsn, asm) do
    cond do
      count >= Keyword.fetch!(asm.batch, :max_transactions) ->
        {:flush, :max_transactions, asm}

      lsn - Assembler.span_base(asm) >= Keyword.fetch!(asm.batch, :max_span) ->
        {:flush, :max_span, asm}

      true ->
        {:buffered, asm}
    end
  end

  # A non-spilled txn (spill_path nil) records nothing; a spilled txn prepends its file path.
  def prepend_path(paths, nil), do: paths
  def prepend_path(paths, path) when is_binary(path), do: [path | paths]

  # --- batch flush (spec §6/§7) — moved from Assembler ---

  # Sink-owned batch delivery (spec §6): deliver the whole buffered batch as ONE atomic
  # handle_batch/1 call. On {:ok, _} advance the span base to the batch's highest LSN and reset
  # the buffer; the AssemblerServer then acks pending_lsn (§9 durability-before-ack). A sink
  # fault (return, raise, throw, exit) is scrubbed value-free (Critical Rule 1) — an N-txn
  # throw/exit reason can embed any buffered row (a GenServer.call timeout carrying the txn), so
  # the reason is NEVER inspected; only an exception module name is kept, exactly as Core's
  # `deliver_now/3` sink-fault scrub does for handle_transaction.
  def flush_sink_batch(%Assembler{pending_lsn: lsn} = asm, reason) do
    start_mono = System.monotonic_time(:millisecond)
    txns = Enum.reverse(asm.batch_txns)

    result =
      try do
        asm.sink.handle_batch(txns)
      rescue
        e -> {:sink_raised, e}
      catch
        kind, reason -> {:sink_caught, kind, reason}
      end

    duration = System.monotonic_time(:millisecond) - start_mono

    case result do
      {:ok, _returned} ->
        # The batch (and every transactional message riding its txns) is durably delivered — emit
        # the per-message [:message, :received] telemetry (spec §10), as deliver_now does for the
        # non-batch modes. Sink-owned batch_delivery defers DELIVERY to handle_batch, so this is the
        # message's delivery point for this mode.
        Enum.each(txns, &Assembler.emit_txn_messages_received/1)

        Telemetry.event([:replicant, :sink, :batch_committed], %{duration: duration}, %{
          change_count: asm.batch_count,
          commit_lsn: lsn,
          reason: reason
        })

        # The batch is durable (handle_batch returned {:ok, _} having re-read each Reader from its
        # spill file); delete the migrated spill files exactly once and clear the list. A non-spilled
        # batch has an empty list → a harmless no-op.
        Enum.each(asm.batch_spill_paths, &Spill.rm/1)

        {:ok, lsn,
         %{
           asm
           | batch_count: 0,
             pending_lsn: nil,
             batch_txns: [],
             batch_spill_paths: [],
             lib_checkpoint: max(asm.lib_checkpoint || 0, lsn)
         }}

      {:sink_raised, %Spill.Error{}} ->
        # The lazy Reader raised a value-free Spill.Error while handle_batch forced its enumeration
        # (a disk/decode fault mid-read, spec §5) — the batch-flush parity of deliver_now's per-txn
        # Reader-fault path. Distinguish it from a sink fault: :spill_io_failed (NOT :sink_failed),
        # value-free (no shape — Spill.Error carries no row byte; Critical Rule 1).
        sink_batch_failed(asm, duration, :spill_io_failed)

      {:error, _reason} ->
        sink_batch_failed(asm, duration)

      {:sink_raised, e} ->
        sink_batch_failed(asm, duration, :sink_failed, Assembler.safe_shape(e))

      {:sink_caught, _kind, _reason} ->
        sink_batch_failed(asm, duration)

      # A non-conforming return (not {:ok,_}/{:error,_}) must NOT raise CaseClauseError here:
      # this runs via do_flush, OUTSIDE handle_message/2's value-free rescue, so the raised
      # term (a buffered row) would leak into the OTP crash log. Halt fail-closed value-free,
      # never inspecting the returned term (Critical Rule 1; spec §8 unexpected-return row).
      _unexpected ->
        sink_batch_failed(asm, duration)
    end
  end

  # `reason` distinguishes the fault class for triage (:sink_failed default; :spill_io_failed for a
  # Reader fault). Centralized so the CV1 spill-file cleanup cannot drift between fault classes —
  # mirrors the maybe_trip_batch centralization for the two batch modes.
  def sink_batch_failed(asm, duration, reason \\ :sink_failed, shape \\ nil) do
    Telemetry.event([:replicant, :sink, :failed], %{duration: duration}, %{reason: reason})

    # Delete the migrated spill files on the fault branch too (CV1): a flush fault halts fail-closed
    # and the batch re-streams from the durable checkpoint (fresh files) on restart, so the buffered
    # spill files must not orphan (cleartext row values at rest). Mirrors the {:ok} branch's cleanup
    # + deliver_now's fault cleanup (dab7f2f); clearing the list keeps a later reset_batch idempotent.
    Enum.each(asm.batch_spill_paths, &Spill.rm/1)
    {:error, %Error{reason: reason, shape: shape}, %{asm | batch_spill_paths: []}}
  end

  def flush_lib_batch(%Assembler{pending_lsn: lsn} = asm, reason) do
    case Assembler.write_checkpoint(asm, lsn) do
      :ok ->
        Telemetry.event([:replicant, :checkpoint_store, :batch_flushed], %{}, %{
          slot_name: asm.slot_name,
          change_count: asm.batch_count,
          byte_size: lsn - Assembler.span_base(asm),
          reason: reason
        })

        # lib+batch NEVER records a spill path (a spilled lib+batch txn is deleted by deliver_now
        # after the sink returns durable — only the CHECKPOINT is batched). The list is therefore
        # always [] here; the delete loop is a no-op kept for symmetry with flush_sink_batch.
        Enum.each(asm.batch_spill_paths, &Spill.rm/1)

        {:ok, lsn,
         %{
           asm
           | batch_count: 0,
             pending_lsn: nil,
             batch_spill_paths: [],
             last_buffered_changes: [],
             lib_checkpoint: max(asm.lib_checkpoint || 0, lsn)
         }}

      {:error, _store_reason} ->
        Telemetry.event([:replicant, :checkpoint_store, :failed], %{}, %{
          slot_name: asm.slot_name,
          reason: :checkpoint_store_failed
        })

        # Scrub to the fixed value-free atom (Critical Rule 1), matching the per-txn `commit_txn`
        # path — the writer type is `{:error, term()}`, so a value-bearing term must never cross
        # into %Error{}. The specific reason is not consumed downstream (Supervisor.halt ignores it).
        {:error, %Error{reason: :checkpoint_store_failed}, asm}
    end
  end
end
