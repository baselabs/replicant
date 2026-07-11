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
          # pg-canonical PK tuple per change, read from the delivered CAST record's PK
          # columns — the SAME Casting.Types term the stream side tracks (plan review F1;
          # every snapshot column is now cast through the stream's path). Parallel to
          # :changes, same order.
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
          max_pending: pos_integer(),
          discarded: %{optional(String.t()) => true}
        }

  defstruct epoch: 0,
            frontier: 0,
            tracking: %{},
            pending: [],
            drop_cap: 1,
            max_pending: 1,
            # Tables whose pending chunks were DISCARDED by a contention event (keyed drop-cap
            # breach OR any tracked write to a PK-less table) and whose reader has NOT yet learned
            # of it. A set-semantics map (a plain map avoids the opaque-MapSet contract_with_opaque
            # dialyzer wart for a directly-constructed struct field). The caller drains it via a
            # reader-facing {:error, :table_discarded} signal (spec §4/§6.4); cleared once the
            # reader is told (clear_discarded/2).
            discarded: %{}

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
  `track/2` + contention enforcement: a table whose pending chunks must be DISCARDED
  and re-read is returned in the discard list AND recorded in `w.discarded` so the
  caller can signal its reader `{:error, :table_discarded}` (spec §4/§6.4). Two triggers:

    * a KEYED table whose tracking set breaches `drop_cap` (superset discard + re-read); and
    * a PK-LESS table (its tracking entry's `pk_raw == []`) that received ANY tracked write —
      `drop_cap` can never fire for a keyless table (its PK tuple collapses to `[]`), so any
      concurrent write potentially staled the whole read ⇒ redo (spec §6.4).
  """
  @spec track_capped(t(), [Change.t()]) :: {t(), [String.t()]}
  def track_capped(%__MODULE__{} = w, changes) do
    {tracking, touched, untrackable} =
      Enum.reduce(changes, {w.tracking, MapSet.new(), []}, &track_change/2)

    keyed_breached =
      for {q, %{pks: pks, pk_raw: pk_raw}} <- tracking,
          pk_raw != [],
          MapSet.size(pks) > w.drop_cap,
          do: q

    keyless_contended =
      for q <- MapSet.to_list(touched),
          match?({:ok, %{pk_raw: []}}, Map.fetch(tracking, q)),
          do: q

    apply_contention(w, tracking, Enum.uniq(keyed_breached ++ keyless_contended ++ untrackable))
  end

  # Fold ONE change into the (tracking, touched, untrackable) accumulator. Only OPEN tables (in
  # `acc`) are tracked; a change with an EMPTY `pk_records` (TRUNCATE / REPLICA-IDENTITY-NOTHING
  # delete) has no per-row PK → its table joins `untrackable` (the caller taints it: discard +
  # re-read, NEVER a `pk_tuple(nil, …)` crash). Every other change adds its PK(s) to the drop-set.
  defp track_change(%Change{schema: s, table: t} = c, {acc, seen, untr}) do
    qualified = "#{s}.#{t}"

    case {Map.fetch(acc, qualified), pk_records(c)} do
      {:error, _} ->
        {acc, seen, untr}

      {{:ok, _entry}, []} ->
        {acc, seen, [qualified | untr]}

      {{:ok, entry}, recs} ->
        {Map.put(acc, qualified, Enum.reduce(recs, entry, &put_pk(&2, &1))),
         MapSet.put(seen, qualified), untr}
    end
  end

  # The record image(s) whose PK(s) identify the changed row(s) for drop-filtering:
  #   * INSERT / SNAPSHOT → the new row in `record`;
  #   * DELETE → its key columns in `old_record` (`record` is nil — pgoutput delete semantics);
  #   * UPDATE → BOTH `old_record` and `record`. A PK-CHANGING update's `old_record` holds the OLD
  #     key, which a snapshot chunk read before the update still carries and MUST drop, else the
  #     old-key row RESURRECTS (§2 ghost); the new key is tracked too (superset, convergence-safe).
  # An EMPTY list — a TRUNCATE, or a delete/update whose key image is absent (REPLICA IDENTITY
  # NOTHING) — has no per-row PK, so the caller taints the table (discard + re-read), never crashes.
  defp pk_records(%Change{op: :update, old_record: old, record: rec}),
    do: Enum.reject([old, rec], &is_nil/1)

  defp pk_records(%Change{op: :delete, old_record: old}), do: Enum.reject([old], &is_nil/1)
  defp pk_records(%Change{op: :truncate}), do: []
  defp pk_records(%Change{record: rec}), do: Enum.reject([rec], &is_nil/1)

  defp apply_contention(w, tracking, []), do: {%{w | tracking: tracking}, []}

  defp apply_contention(w, tracking, discarded) do
    pending = Enum.reject(w.pending, &(&1.qualified in discarded))

    tracking =
      Enum.reduce(discarded, tracking, fn q, acc ->
        # A keyless table keeps `pk_raw == []` across the reset so a subsequent write stays
        # detectable as contention; a keyed table resets to unknown pk_raw (re-bound by the
        # re-read's first chunk), matching the pre-fix reset.
        pk_raw = if match?({:ok, %{pk_raw: []}}, Map.fetch(acc, q)), do: [], else: nil
        Map.put(acc, q, %{pks: MapSet.new(), pk_raw: pk_raw})
      end)

    new_discarded = Enum.reduce(discarded, w.discarded, &Map.put(&2, &1, true))
    {%{w | tracking: tracking, pending: pending, discarded: new_discarded}, discarded}
  end

  @doc "True when `qualified`'s pending chunks were discarded (contention) and its reader not yet told."
  @spec discarded?(t(), String.t()) :: boolean()
  def discarded?(%__MODULE__{discarded: d}, qualified), do: Map.has_key?(d, qualified)

  @doc "Any of `qualified`'s chunks still buffered (pending, not yet applied)?"
  @spec table_pending?(t(), String.t()) :: boolean()
  def table_pending?(%__MODULE__{pending: pending}, qualified),
    do: Enum.any?(pending, &(&1.qualified == qualified))

  @doc "Clear a table's discard flag once its reader has been told to re-read."
  @spec clear_discarded(t(), String.t()) :: t()
  def clear_discarded(%__MODULE__{discarded: d} = w, qualified),
    do: %{w | discarded: Map.delete(d, qualified)}

  # The pk_raw column list rides in on the first chunk (add_chunk). Before any chunk
  # arrives, PKs are unknown, so the WHOLE record image is the tracking key (superset;
  # normalized to the PK tuple once pk_raw is known, via rebind_pk_raw). `drop_cap`
  # therefore bounds record-image ENTRIES here (memory-correct — it caps tracking-set
  # size), which can OVER-count repeated updates of one hot row (same logical PK,
  # different non-PK columns) as distinct entries. That over-count is FAIL-SAFE: a
  # breach discards + re-reads (never data loss, per the moduledoc convergence-safety
  # note), and it collapses to distinct PK tuples once the first chunk runs
  # rebind_pk_raw.
  # `rec` is the resolved PK-bearing record image (see `pk_record/1`) — NEVER nil (a nil source is
  # routed to taint in `track_capped`, so `pk_tuple` can never hit `Map.get(nil, …)`).
  defp put_pk(%{pks: pks, pk_raw: nil} = entry, rec),
    do: %{entry | pks: MapSet.put(pks, {:record, rec})}

  defp put_pk(%{pks: pks, pk_raw: pk_raw} = entry, rec),
    do: %{entry | pks: MapSet.put(pks, pk_tuple(rec, pk_raw))}

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
  @spec pop_ready(t()) :: {:apply, [Change.t()], chunk(), t()} | {:discard, chunk(), t()} | :none
  def pop_ready(%__MODULE__{pending: [chunk | rest]} = w) when chunk.hw <= w.frontier do
    cond do
      Map.has_key?(w.discarded, chunk.qualified) ->
        # The table was discarded (contention) after this chunk was buffered: drop it, never
        # apply a stale chunk (the reader re-reads on its next window call). The discard already
        # removes the table's pending chunks, so this is a fail-closed backstop, never the norm.
        {:discard, chunk, %{w | pending: rest}}

      chunk.pk_raw == [] and keyless_has_writes?(w, chunk.qualified) ->
        # A PK-less chunk whose table saw a tracked write (e.g. a placeholder write BEFORE this
        # chunk bound pk_raw): the read is stale and CANNOT be drop-filtered (no PK) — dropping it
        # to empty would silently lose the batch (the confirmed data-loss bug). Discard it whole
        # and mark the table needs-re-read so the reader redoes it (spec §6.4).
        {:discard, chunk,
         %{w | pending: rest, discarded: Map.put(w.discarded, chunk.qualified, true)}}

      true ->
        kept = drop_filter(w, chunk)
        {:apply, kept, chunk, %{w | pending: rest}}
    end
  end

  def pop_ready(%__MODULE__{}), do: :none

  defp keyless_has_writes?(w, qualified) do
    case Map.fetch(w.tracking, qualified) do
      {:ok, %{pks: pks}} -> MapSet.size(pks) > 0
      :error -> false
    end
  end

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
  Conservatively DISCARD a table's pending chunks, RESET its tracking entry, AND
  SIGNAL RE-READ (spec §2/§4/§5/§6.4). Used by the applier when a delivered
  transaction's `changes` is a lazy, single-pass spill-backed `Enumerable` (or is
  otherwise unavailable) and so MUST NOT be enumerated to update the drop-set: the
  table's in-flight chunks are dropped and the reader re-reads from durable progress.

  The tainted table is folded into `w.discarded` — the SAME reader-facing signal the
  keyed drop-cap breach / keyless concurrent-write taint use (`apply_contention/3`) —
  so the reader's next `open`/`deliver`/barrier for it returns `{:error,
  :table_discarded}` and re-reads. Without this signal the reader would never learn
  its chunks were dropped: its bound would advance, a later chunk would persist a
  bound PAST the discarded chunks, and the discarded chunks' untouched rows would be
  LOST (the confirmed data-loss hole). Convergence-safe (discard-and-re-read — never
  data loss, never a chunk whose drop-set is now unknowable). A no-op for a table that
  is not being tracked (no chunks to drop, nothing to re-read).
  """
  @spec taint_table(t(), String.t()) :: t()
  def taint_table(
        %__MODULE__{tracking: tracking, pending: pending, discarded: discarded} = w,
        qualified
      ) do
    case Map.fetch(tracking, qualified) do
      :error ->
        w

      {:ok, %{pk_raw: pk_raw}} ->
        # Mirror apply_contention/3's reset: a KEYLESS table keeps `pk_raw == []` so a subsequent
        # concurrent write stays detectable as contention; a KEYED table resets to unknown pk_raw
        # (re-bound by the re-read's first chunk).
        reset_pk_raw = if pk_raw == [], do: [], else: nil

        %{
          w
          | tracking: Map.put(tracking, qualified, %{pks: MapSet.new(), pk_raw: reset_pk_raw}),
            pending: Enum.reject(pending, &(&1.qualified == qualified)),
            discarded: Map.put(discarded, qualified, true)
        }
    end
  end

  @doc "Reconnect re-seed (spec §4): discard ALL pending chunks + tracking, adopt the new epoch."
  @spec reset(t(), non_neg_integer()) :: t()
  def reset(%__MODULE__{} = w, new_epoch),
    do: %{w | epoch: new_epoch, tracking: %{}, pending: [], frontier: 0, discarded: %{}}
end
