defmodule Replicant.Assembler.Streaming do
  @moduledoc false

  # Proto-v2 streaming reassembly + aggregate-resident spill (spec §5/§8), extracted from
  # `Replicant.Assembler` as a module of pure functions on the shared `%Assembler{}` struct.
  # The struct shape is UNCHANGED; `Assembler` delegates. The sink-dispatch + Rule-1 scrub
  # cluster stays in `Assembler` — this module holds NO sink-call rescue, so a streaming-row
  # raise (e.g. a casting fault) is still caught by `handle_message/2`'s outer rescue in Core.
  # Rule-1 telemetry/`%Error{}` sites that live in the moved functions are value-free (counts
  # + allowlisted reasons) and move WITH the functions.

  alias Replicant.{Assembler, Error, Spill, Telemetry}

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
end
