defmodule Replicant.Assembler.Streaming do
  @moduledoc false

  # Proto-v2 streaming reassembly + aggregate-resident spill (spec §5/§8), extracted from
  # `Replicant.Assembler` as a module of pure functions on the shared `%Assembler{}` struct.
  # The struct shape is UNCHANGED; `Assembler` delegates. The sink-dispatch + Rule-1 scrub
  # cluster stays in `Assembler` — this module holds NO sink-call rescue, so a streaming-row
  # raise (e.g. a casting fault) is still caught by `handle_message/2`'s outer rescue in Core.
  # Rule-1 telemetry/`%Error{}` sites that live in the moved functions are value-free (counts
  # + allowlisted reasons) and move WITH the functions.

  alias Replicant.{Assembler, Decoder.Messages, Error, Spill, Telemetry, Transaction}

  alias Messages.{Message, StreamAbort, StreamCommit, StreamStart, StreamStop}

  # --- aggregate-resident spill (spec §5) ---

  # Aggregate-resident spill (spec §5): keep total in-memory reassembly ≈ max_inflight_lag. When the
  # sum of per-buffer resident bytes crosses the bound, flush the LARGEST buffer's tail to its spill
  # file. A no-op when spill is not configured. `observe_bytes` runs OUTSIDE handle_message/2's
  # value-free rescue, so a spill I/O fault is scrubbed to a value-free flag here, not raised.
  def maybe_spill(%Assembler{spill: nil} = asm), do: {:ok, asm}

  def maybe_spill(%Assembler{max_inflight_lag: bound, stream_txns: streams} = asm) do
    resident_total = Enum.reduce(streams, 0, fn {_xid, b}, acc -> acc + b.resident_bytes end)

    cond do
      resident_total <= (bound || 0) -> {:ok, asm}
      map_size(streams) == 0 -> {:ok, asm}
      true -> spill_largest(asm)
    end
  end

  def spill_largest(%Assembler{stream_txns: streams} = asm) do
    {top, buf} = Enum.max_by(streams, fn {_xid, b} -> b.resident_bytes end)

    case flush_buffer_tail(asm, top, buf) do
      {:ok, spill_handle, wrote} ->
        # Per-subxid frame counts of the flushed tail — so a spilled txn that later filters to EMPTY
        # (all spilled subxids aborted) is detected at StreamCommit and routed through the CV1
        # empty-suppression (spec §9 v1-indistinguishability), not delivered as an empty %Transaction{}.
        by_subxid =
          Enum.reduce(buf.changes, buf.spilled_by_subxid, fn {sx, _c}, m ->
            Map.update(m, sx, 1, &(&1 + 1))
          end)

        buf = %{
          buf
          | changes: [],
            resident_bytes: 0,
            spilled_bytes: buf.spilled_bytes + wrote,
            spilled_by_subxid: by_subxid,
            spill: spill_handle
        }

        spilled_total = asm.spilled_total + wrote
        asm = %{asm | stream_txns: Map.put(streams, top, buf), spilled_total: spilled_total}

        Telemetry.event([:replicant, :stream, :spilled], %{}, %{byte_size: wrote, change_count: 0})

        # Disk ceiling (spec §8): the frame is already on disk; record the breach on `spill_fault` —
        # the next StreamCommit halts on it (do_handle_message's spill_fault clause matches
        # `%StreamCommit{}` only; the halt is DEFERRED to a commit boundary). Do NOT deliver past it.
        if spilled_total > Keyword.fetch!(asm.spill, :max_spill_bytes) do
          # Surface the disk-ceiling breach as the advertised value-free event (spec §11): byte_size is
          # a count, reason is allowlisted (no telemetry.ex change). Lets operators observe exhaustion
          # at the breach, not only via the halt reason on the next StreamCommit.
          Telemetry.event([:replicant, :stream, :spill_exhausted], %{}, %{
            byte_size: spilled_total,
            reason: :spill_exhausted
          })

          {:ok, %{asm | spill_fault: %Error{reason: :spill_exhausted}}}
        else
          {:ok, asm}
        end

      {:error, %Error{} = err} ->
        {:halt, err, asm}
    end
  end

  # Append the buffer's resident tail (oldest-first) to its spill file, opening lazily on first spill.
  # `top` is the buffer's top-level xid (from spill_largest's Enum.max_by) — the spill file name.
  def flush_buffer_tail(%Assembler{spill: spill} = asm, top, %{spill: existing} = buf) do
    dir = Keyword.fetch!(spill, :dir)

    with {:ok, handle} <- ensure_spill_open(existing, dir, asm.slot_name, top),
         {:ok, wrote} <- append_tail_or_discard(handle, existing, Enum.reverse(buf.changes)) do
      {:ok, handle, wrote}
    end
  end

  def ensure_spill_open(nil, dir, slot_name, top), do: Spill.open(dir, slot_name, top)
  def ensure_spill_open(handle, _dir, _slot, _top), do: {:ok, handle}

  # On an append fault, discard a device we opened THIS call (existing == nil) so it can't leak an FD
  # or an orphan partial file — mirroring Spill.open's own close-on-error boundary. A REUSED handle
  # (existing != nil) stays owned by buf.spill in the un-mutated asm and is discarded at halt/reset,
  # so it is NOT discarded here (double-close/rm would be the bug).
  def append_tail_or_discard(handle, existing, tagged_changes) do
    case append_tail(handle, tagged_changes) do
      {:ok, wrote} ->
        {:ok, wrote}

      {:error, %Error{}} = err ->
        if is_nil(existing), do: Spill.discard(handle)
        err
    end
  end

  def append_tail(handle, tagged_changes) do
    Enum.reduce_while(tagged_changes, {:ok, handle, 0}, fn {subxid, change}, {:ok, h, acc} ->
      case Spill.append(h, subxid, change) do
        {:ok, n} -> {:cont, {:ok, h, acc + n}}
        {:error, %Error{}} = err -> {:halt, err}
      end
    end)
    |> case do
      {:ok, _h, total} -> {:ok, total}
      {:error, %Error{}} = err -> err
    end
  end

  # --- streamed-commit delivery (spec §5/§7) — moved from Assembler ---

  def deliver_or_skip_stream(%Assembler{} = asm, top, buf, lsn, ts) do
    resident = Enum.reject(buf.changes, fn {sx, _c} -> MapSet.member?(buf.aborted, sx) end)

    spilled_live =
      Enum.reduce(buf.spilled_by_subxid, 0, fn {sx, c}, acc ->
        if MapSet.member?(buf.aborted, sx), do: acc, else: acc + c
      end)

    asm = %{
      asm
      | stream_txns: Map.delete(asm.stream_txns, top),
        current_stream_xid: nil,
        spilled_total: asm.spilled_total - buf.spilled_bytes
    }

    # Transactional messages tagged to an aborted subxid are dropped (mirrors the change-reject
    # above); the surviving ones must RIDE the txn even when it carries zero row-changes — the v1
    # path (Begin→Message→Commit) delivers exactly that (effect-once, spec §7.1). A message-bearing
    # txn is therefore v1-DISTINGUISHABLE and must NOT be empty-suppressed, or the message is
    # silently lost (regression: the suppression keyed on row-changes alone).
    live_messages = Enum.reject(buf.messages, &(&1.xid in buf.aborted))

    cond do
      resident == [] and spilled_live == 0 and live_messages == [] ->
        # Empty after aborts (spilled or not) → v1-indistinguishable suppression (parent CV1, spec §7).
        # Use Spill.discard (close + delete), NOT Spill.rm (delete only): this branch runs BEFORE the
        # Spill.close below, so buf.spill is a live open device — Spill.rm would unlink the file but
        # leak the FD (:emfile under repeated spill-then-all-aborted).
        if buf.spill, do: Spill.discard(buf.spill)
        suppress_empty_stream_commit(asm, lsn)

      buf.spill == nil ->
        # Unspilled, non-empty (row-changes and/or a transactional message): the in-memory List
        # replay (stamp commit_lsn + ordinals at replay).
        {changes, change_count} = replay_resident(resident, lsn)
        deliver_stream_commit(asm, lsn, ts, buf.byte_size, changes, change_count, live_messages)

      true ->
        # Spilled, non-empty: close the file for reading, deliver a lazy Reader over frames + tail.
        :ok = Spill.close(buf.spill)
        reader = Spill.Reader.new(buf.spill.path, resident, buf.aborted, lsn)
        deliver_spilled_stream_commit(asm, lsn, ts, buf, reader)
    end
  end

  # Replay the newest-first resident tail into commit order in ONE pass (`Enum.reverse` then
  # `Enum.map_reduce`), stamping the commit-granularity commit_lsn (streamed changes have no LSN
  # before commit, spec §5) and yielding the change_count from the same traversal. The `ordinal` is
  # PRESERVED (stamped at accumulation from the shared per-txn counter, so it interleaves with any
  # transactional message's ordinal) — never re-numbered here; the counter is only a change tally.
  # Extracted VERBATIM from the shipped StreamCommit body to keep `deliver_or_skip_stream` within
  # credo's nesting depth (mirrors the `deliver_stream_commit` extraction).
  def replay_resident(resident, lsn) do
    resident
    |> Enum.reverse()
    |> Enum.map_reduce(0, fn {_subxid, change}, count ->
      {%{change | commit_lsn: lsn}, count + 1}
    end)
  end

  # Deliver a SPILLED streamed txn as a lazy `Spill.Reader` %Transaction{} (spec §5): the sink forces
  # the enumeration inside its own call (single-pass; disk frames then resident tail, aborted subxids
  # rejected, commit_lsn + ordinals stamped by the Reader). `change_count` is unknown without forcing
  # the lazy reader, so the `[:stream, :committed]` telemetry omits it (the allowlist permits a
  # subset). Pre-skip at/below the watermark (effect-once, §2) — delete the file, no delivery — else
  # deliver via the shared apply_sink path, threading the spill PATH so cleanup happens after durable
  # delivery (per-txn / lib+batch delete the file; sink-owned batch keeps it until flush, Task 8).
  def deliver_spilled_stream_commit(%Assembler{} = asm, lsn, ts, buf, reader) do
    buf_messages = Enum.reject(buf.messages, &(&1.xid in buf.aborted))

    txn = %Transaction{
      commit_lsn: lsn,
      commit_timestamp: ts,
      changes: reader,
      messages: buf_messages
    }

    Telemetry.event([:replicant, :stream, :committed], %{}, %{
      commit_lsn: lsn,
      byte_size: buf.byte_size
    })

    if Assembler.skip?(asm, txn) do
      Spill.rm(buf.spill.path)
      {:skipped, lsn, asm}
    else
      Assembler.apply_sink(asm, txn, buf.spill.path)
    end
  end

  # The non-empty StreamCommit delivery path (the pre-computed `change_count` comes from the replay
  # map_reduce): emit the assembled + stream:committed telemetry, then pre-skip at/below the watermark
  # (effect-once, §2) else deliver via the shared apply_sink/2 — a separate clause to keep the
  # StreamCommit body within credo's nesting depth. An empty streamed txn never reaches here (it is
  # suppressed in the StreamCommit clause, spec §7).
  def deliver_stream_commit(asm, lsn, ts, byte_size, changes, change_count, buf_messages) do
    txn = %Transaction{
      commit_lsn: lsn,
      commit_timestamp: ts,
      changes: changes,
      messages: buf_messages
    }

    Telemetry.event([:replicant, :transaction, :assembled], %{}, %{
      change_count: change_count,
      commit_lsn: lsn,
      byte_size: byte_size,
      lag_ms: Assembler.lag_ms(ts)
    })

    Telemetry.event([:replicant, :stream, :committed], %{}, %{
      change_count: change_count,
      commit_lsn: lsn,
      byte_size: byte_size
    })

    if Assembler.skip?(asm, txn) do
      {:skipped, lsn, asm}
    else
      Assembler.apply_sink(asm, txn)
    end
  end

  # An empty streamed txn (spec §7 suppression) carries no changes to deliver, but it MUST NOT
  # `{:skipped}`-ack ahead of an OPEN batch: in batch mode a buffered-but-unflushed txn has a
  # commit_lsn BELOW this lsn (commit order), and `{:skipped}` acks the slot to `lsn` immediately
  # (assembler_server.ex dispatch), which would advance `confirmed_flush` past that un-delivered /
  # un-checkpointed WAL — a crash before flush then drops it (loss). When a batch is open, fold the
  # empty txn's lsn INTO the batch (advance `pending_lsn` and re-check the span cap): the eventual
  # flush acks it only AFTER the buffered data is durable, with no delivery, no `batch_count`
  # increment, and `batch_txns` untouched (so a sink-owned flush never calls `handle_batch([])`).
  # With no batch open there is nothing un-durable below this lsn, so skip-ack immediately —
  # matching the non-batch path and v1-indistinguishability (spec §7). loss=0, effect-once preserved.
  def suppress_empty_stream_commit(%Assembler{} = asm, lsn) do
    if Assembler.batch_pending?(asm) do
      asm = %{asm | pending_lsn: lsn}

      if lsn - Assembler.span_base(asm) >= Keyword.fetch!(asm.batch, :max_span),
        do: {:flush, :max_span, asm},
        else: {:buffered, asm}
    else
      {:skipped, lsn, asm}
    end
  end

  # --- streamed-message handling (spec §5/§8) — moved from Assembler.do_handle_message ---
  # Core's `do_handle_message` keeps the clause HEADS (delegating here) so the load-bearing clause
  # ordering — the xid-guarded streamed-row clauses and the `txn: nil` "row before Begin" guard —
  # is preserved exactly. Only the bodies move.

  # StreamStart opens a per-xid buffer (spec §5). The §8 halt matrix: more than max_concurrent_txns
  # concurrent in-progress streamed transactions halts fail-closed with the spec-named reason so an
  # operator can distinguish a stream-count overflow (tune max_concurrent_txns) from a generic config
  # error.
  def handle_stream_start(%Assembler{} = asm, %StreamStart{xid: xid}) do
    cond do
      Map.has_key?(asm.stream_txns, xid) ->
        {:ok, %{asm | current_stream_xid: xid}}

      map_size(asm.stream_txns) >= (asm.max_concurrent_txns || 64) ->
        {:halt,
         %Error{reason: :too_many_streams, shape: "too many concurrent streamed transactions"},
         asm}

      true ->
        {:ok,
         %{
           asm
           | current_stream_xid: xid,
             stream_txns:
               Map.put(asm.stream_txns, xid, %{
                 changes: [],
                 messages: [],
                 byte_size: 0,
                 resident_bytes: 0,
                 spilled_bytes: 0,
                 spilled_by_subxid: %{},
                 aborted: MapSet.new(),
                 spill: nil,
                 seq: 0
               })
         }}
    end
  end

  def handle_stream_stop(%Assembler{} = asm, %StreamStop{}) do
    {:ok, %{asm | current_stream_xid: nil}}
  end

  # A recorded spill fault (a disk-ceiling breach or an I/O fault flagged on `spill_fault` by the
  # value-free observe_bytes/maybe_spill path, which runs OUTSIDE handle_message/2's rescue) halts
  # the stream fail-closed at its commit boundary (spec §8) — never deliver a transaction assembled
  # past a spill fault. Checked FIRST, before dispatching, so it preempts both the spilled and the
  # resident delivery paths.
  def handle_stream_commit(%Assembler{spill_fault: %Error{} = err} = asm, %StreamCommit{}),
    do: {:halt, err, asm}

  # StreamCommit: route the buffer to empty-suppression, in-memory replay, or lazy-Reader delivery
  # (deliver_or_skip_stream).
  def handle_stream_commit(%Assembler{} = asm, %StreamCommit{
        xid: top,
        commit_lsn: lsn,
        commit_timestamp: ts
      }) do
    case Map.fetch(asm.stream_txns, top) do
      :error ->
        {:halt, %Error{reason: :config_invalid, shape: "stream commit for unknown transaction"},
         asm}

      {:ok, buf} ->
        deliver_or_skip_stream(asm, top, buf, lsn, ts)
    end
  end

  # StreamAbort: whole-transaction (top == sub) discards the buffer + its spill file and decrements
  # spilled bytes; subtransaction (top != sub) drops just the aborted subxid's resident changes and
  # records it in `aborted` for spilled-frame replay filtering (spec §5). An unknown top halts.
  def handle_stream_abort(%Assembler{} = asm, %StreamAbort{xid: top, subxid: sub}) do
    case {Map.fetch(asm.stream_txns, top), top == sub} do
      {{:ok, buf}, true} ->
        Telemetry.event([:replicant, :stream, :aborted], %{}, %{reason: :stream_abort})
        if buf.spill, do: Spill.discard(buf.spill)
        cleared = if asm.current_stream_xid == top, do: nil, else: asm.current_stream_xid

        {:ok,
         %{
           asm
           | stream_txns: Map.delete(asm.stream_txns, top),
             current_stream_xid: cleared,
             spilled_total: asm.spilled_total - buf.spilled_bytes
         }}

      {{:ok, buf}, false} ->
        # In-memory tail: reject-at-abort. Already-spilled frames can't be rejected from an
        # append-only file, so record `sub` in the aborted set — replay skips any spilled frame
        # tagged `sub` (spec §5).
        kept = Enum.reject(buf.changes, fn {subxid, _change} -> subxid == sub end)
        buf = %{buf | changes: kept, aborted: MapSet.put(buf.aborted, sub)}
        {:ok, %{asm | stream_txns: Map.put(asm.stream_txns, top, buf)}}

      {:error, _} ->
        {:halt, %Error{reason: :config_invalid, shape: "stream abort for unknown transaction"},
         asm}
    end
  end

  # A STREAMED transactional message (spec §7.1) carries `xid` and arrives inside an open stream
  # segment. The ordinal is the shared per-txn counter at attach (mirrors the v1 counter) so the
  # message interleaves correctly with the surrounding changes' ordinals (spec §5). `prefix`/
  # `content` are user bytes (Critical Rule 1) — carried on the %Message{} struct, never logged or
  # inspected here; the value-free boundary is Core's `handle_message/2` rescue (which still wraps
  # this call via the delegating clause).
  def handle_streamed_message(
        %Assembler{current_stream_xid: top} = asm,
        %Message{transactional?: true, xid: sx, lsn: lsn, prefix: prefix, content: content}
      )
      when is_integer(top) do
    buf = Map.fetch!(asm.stream_txns, top)

    msg = %Message{
      transactional?: true,
      lsn: lsn,
      prefix: prefix,
      content: content,
      xid: sx,
      ordinal: buf.seq
    }

    buf = %{buf | messages: [msg | buf.messages], seq: buf.seq + 1}
    {:ok, %{asm | stream_txns: Map.put(asm.stream_txns, top, buf)}}
  end
end
