defmodule Replicant.SnapshotWindow do
  @moduledoc """
  Pure window algebra for the incremental backfill (spec §2/§4): per-table PK
  tracking opened BEFORE the reader captures LW (superset drop-set), pending
  chunks that become ready when the epoch-guarded frontier passes their HW, the
  drop-filter at apply, the derived drop-cap (discard + re-read on breach), and
  the pending-chunk capacity bound.

  Value-free discipline (Critical Rule 1): this module holds PK VALUES (tracking
  sets, chunk rows) as data-plane state; nothing here formats, logs, or raises
  them — every public return is a tagged tuple over the caller's own data.
  """

  alias Replicant.Change

  @type chunk :: %{
          required(:qualified) => String.t(),
          required(:schema) => String.t(),
          required(:table) => String.t(),
          required(:pk_raw) => [String.t()],
          # pg-canonical PK tuple per change, cast via Casting.Types from the __rpk_* text
          # projections — the SAME cast the stream side applied (plan review F1). Parallel
          # to :changes, same order.
          required(:pk_canon) => [[term()]],
          required(:changes) => [Change.t()],
          required(:hw) => Replicant.lsn(),
          required(:first?) => boolean(),
          required(:complete?) => boolean(),
          required(:progress) => binary(),
          required(:bound) => [term()] | nil
        }

  @type t :: %__MODULE__{
          epoch: non_neg_integer(),
          frontier: Replicant.lsn(),
          tracking: %{optional(String.t()) => %{pks: MapSet.t(), pk_raw: [String.t()]}},
          pending: [chunk()],
          drop_cap: pos_integer(),
          max_pending: pos_integer()
        }

  defstruct epoch: 0, frontier: 0, tracking: %{}, pending: [], drop_cap: 1, max_pending: 1

  @spec new(keyword()) :: t()
  def new(opts) do
    %__MODULE__{
      epoch: Keyword.fetch!(opts, :epoch),
      drop_cap: Keyword.fetch!(opts, :drop_cap),
      max_pending: Keyword.fetch!(opts, :max_pending)
    }
  end

  @doc "Start tracking a table's flushed PKs. MUST precede the reader's LW capture (spec §2 R1)."
  @spec open_window(t(), String.t()) :: t()
  def open_window(%__MODULE__{} = w, qualified) do
    tracking = Map.put_new(w.tracking, qualified, %{pks: MapSet.new(), pk_raw: nil})
    %{w | tracking: tracking}
  end

  @doc "Record delivered txn changes' PKs for every OPEN table (receipt-time, convergence-safe)."
  @spec track(t(), [Change.t()]) :: t()
  def track(%__MODULE__{} = w, changes) do
    {w, _discarded} = track_capped(w, changes)
    w
  end

  @doc """
  `track/2` + drop-cap enforcement: a table whose tracking set breaches `drop_cap`
  has its pending chunks DISCARDED and its tracking reset — returned in the
  discard list so the caller can tell the reader to re-read (spec §4).
  """
  @spec track_capped(t(), [Change.t()]) :: {t(), [String.t()]}
  def track_capped(%__MODULE__{} = w, changes) do
    tracking =
      Enum.reduce(changes, w.tracking, fn %Change{} = c, acc ->
        qualified = "#{c.schema}.#{c.table}"

        case Map.fetch(acc, qualified) do
          {:ok, entry} -> Map.put(acc, qualified, put_pk(entry, c))
          :error -> acc
        end
      end)

    breached =
      for {q, %{pks: pks}} <- tracking, MapSet.size(pks) > w.drop_cap, do: q

    if breached == [] do
      {%{w | tracking: tracking}, []}
    else
      pending = Enum.reject(w.pending, &(&1.qualified in breached))

      tracking =
        Enum.reduce(breached, tracking, &Map.put(&2, &1, %{pks: MapSet.new(), pk_raw: nil}))

      {%{w | tracking: tracking, pending: pending}, breached}
    end
  end

  # The pk_raw column list rides in on the first chunk (add_chunk). Before any chunk
  # arrives, PKs are unknown, so the WHOLE record image is the tracking key (superset;
  # normalized to the PK tuple once pk_raw is known, via rebind_pk_raw). `drop_cap`
  # therefore bounds record-image ENTRIES here (memory-correct — it caps tracking-set
  # size), which can OVER-count repeated updates of one hot row (same logical PK,
  # different non-PK columns) as distinct entries. That over-count is FAIL-SAFE: a
  # breach discards + re-reads (never data loss, per the moduledoc convergence-safety
  # note), and it collapses to distinct PK tuples once the first chunk runs
  # rebind_pk_raw.
  defp put_pk(%{pks: pks, pk_raw: nil} = entry, c),
    do: %{entry | pks: MapSet.put(pks, {:record, c.record})}

  defp put_pk(%{pks: pks, pk_raw: pk_raw} = entry, c),
    do: %{entry | pks: MapSet.put(pks, pk_tuple(c.record, pk_raw))}

  defp pk_tuple(record, pk_raw), do: Enum.map(pk_raw, &Map.get(record, &1))

  @doc "Buffer a chunk awaiting closure. `:at_capacity` when max_pending is reached (caller defers the reader)."
  @spec add_chunk(t(), chunk()) :: {t(), :ok | :at_capacity}
  def add_chunk(%__MODULE__{} = w, chunk) do
    if length(w.pending) >= w.max_pending do
      {w, :at_capacity}
    else
      # Late-bind pk_raw into the table's tracking entry and NORMALIZE any
      # whole-record placeholders tracked before the first chunk arrived.
      tracking =
        Map.update(
          w.tracking,
          chunk.qualified,
          %{pks: MapSet.new(), pk_raw: chunk.pk_raw},
          &rebind_pk_raw(&1, chunk.pk_raw)
        )

      {%{w | tracking: tracking, pending: w.pending ++ [chunk]}, :ok}
    end
  end

  # Adopt the now-known pk_raw and NORMALIZE any whole-record placeholders tracked
  # before the first chunk arrived (verbatim from the add_chunk update callback).
  defp rebind_pk_raw(%{pks: pks}, pk_raw) do
    pks =
      MapSet.new(pks, fn
        {:record, record} -> pk_tuple(record, pk_raw)
        tuple -> tuple
      end)

    %{pks: pks, pk_raw: pk_raw}
  end

  @doc "Advance the frontier from an epoch-tagged wal_end (stale epochs ignored — 85672f1 class)."
  @spec set_frontier(t(), non_neg_integer(), Replicant.lsn()) :: t()
  def set_frontier(%__MODULE__{epoch: e} = w, e, lsn), do: %{w | frontier: max(w.frontier, lsn)}
  def set_frontier(%__MODULE__{} = w, _stale_epoch, _lsn), do: w

  @doc "Advance the frontier from an applied/skipped txn commit LSN (same-process, always current epoch)."
  @spec observe_applied(t(), Replicant.lsn()) :: t()
  def observe_applied(%__MODULE__{} = w, lsn), do: %{w | frontier: max(w.frontier, lsn)}

  @doc """
  Pop the first CLOSED pending chunk (frontier >= hw), drop-filtered. Chunks pop in
  delivery order; a not-yet-closed head blocks later chunks of the same table (PK
  order must be preserved per table) — and since the reader is serial, pending
  chunks are already globally ordered.
  """
  @spec pop_ready(t()) :: {:apply, [Change.t()], chunk(), t()} | :none
  def pop_ready(%__MODULE__{pending: [chunk | rest]} = w) when chunk.hw <= w.frontier do
    kept = drop_filter(w, chunk)
    {:apply, kept, chunk, %{w | pending: rest}}
  end

  def pop_ready(%__MODULE__{}), do: :none

  defp drop_filter(w, chunk) do
    case Map.fetch(w.tracking, chunk.qualified) do
      {:ok, %{pks: pks}} -> apply_drop_set(chunk, pks)
      :error -> chunk.changes
    end
  end

  # Reject the colliding chunk rows (PK present in the tracking set); an empty
  # tracking set keeps every row (verbatim from the drop_filter case body).
  defp apply_drop_set(chunk, pks) do
    if MapSet.size(pks) > 0 do
      chunk.changes
      |> Enum.zip(chunk.pk_canon)
      |> Enum.reject(fn {_change, canon} -> MapSet.member?(pks, canon) end)
      |> Enum.map(fn {change, _canon} -> change end)
    else
      chunk.changes
    end
  end

  @doc "True when the PK tuple is in the table's tracking set (test/inspection helper)."
  @spec tracked?(t(), String.t(), [term()]) :: boolean()
  def tracked?(%__MODULE__{} = w, qualified, pk_tuple) do
    case Map.fetch(w.tracking, qualified) do
      {:ok, %{pks: pks}} -> MapSet.member?(pks, pk_tuple)
      :error -> false
    end
  end

  @doc """
  A table's chunks are all applied: stop tracking it (memory hygiene).

  CALLER CONTRACT: call ONLY after ALL of the table's chunks have been popped and
  applied. This is a pure helper with no code guard — calling it while a chunk for
  that table is still pending would make `drop_filter` hit the untracked (`:error`)
  arm and KEEP every row, re-applying rows the drop-set should have removed.
  """
  @spec close_table(t(), String.t()) :: t()
  def close_table(%__MODULE__{} = w, qualified),
    do: %{w | tracking: Map.delete(w.tracking, qualified)}

  @doc """
  Conservatively DISCARD a table's pending chunks and RESET its tracking entry
  (spec §2/§5). Used by the applier when a delivered transaction's `changes` is a
  lazy, single-pass spill-backed `Enumerable` (or is otherwise unavailable) and so
  MUST NOT be enumerated to update the drop-set: the table's in-flight chunks are
  dropped and the reader re-reads from durable progress. Convergence-safe
  (discard-and-re-read — never data loss, never a chunk whose drop-set is now
  unknowable). A no-op for a table that is not being tracked.
  """
  @spec taint_table(t(), String.t()) :: t()
  def taint_table(%__MODULE__{tracking: tracking, pending: pending} = w, qualified) do
    case Map.fetch(tracking, qualified) do
      :error ->
        w

      {:ok, _entry} ->
        %{
          w
          | tracking: Map.put(tracking, qualified, %{pks: MapSet.new(), pk_raw: nil}),
            pending: Enum.reject(pending, &(&1.qualified == qualified))
        }
    end
  end

  @doc "Reconnect re-seed (spec §4): discard ALL pending chunks + tracking, adopt the new epoch."
  @spec reset(t(), non_neg_integer()) :: t()
  def reset(%__MODULE__{} = w, new_epoch),
    do: %{w | epoch: new_epoch, tracking: %{}, pending: [], frontier: 0}
end
