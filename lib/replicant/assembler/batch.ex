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

  alias Replicant.{Assembler, Telemetry, Transaction}

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
end
