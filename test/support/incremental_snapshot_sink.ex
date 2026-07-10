defmodule Replicant.Test.IncrementalSnapshotSink do
  @moduledoc """
  A `:state_mirror` sink for the incremental-snapshot crash-injection marquees
  (Tasks 11-12). Persists to THREE named public ETS tables instead of Postgres
  (unlike `snapshot_sink.ex`/`ledger_sink.ex`, which back onto a real `sink_orders`
  table over a named Postgrex connection): a `mirror` table (current row state, keyed
  `{qualified_table, pk}` with an ORIGIN-TAGGED value — `{:chunk, row}` written by
  `handle_snapshot/2`, `{:stream, row}` (or `{:stream, :deleted}`) written by
  `handle_transaction/1` — PLUS the two watermark keys `:checkpoint` and `:progress`),
  an APPEND-ONLY `ledger` table (the effect-once proof substrate), and a `control`
  table (the `fail_next_chunk` fault-injection flag).

  ## Qualified table keying + origin-tagged first_for_table? redo-safety

  Both callbacks key the mirror by the QUALIFIED `"schema.table"`. The assembler builds
  a chunk's `ctx.table` as `"\#{schema}.\#{table}"`, but a stream `Change` carries `schema`
  and a BARE `table` separately — so `handle_transaction/1` reconstructs the SAME
  qualified key (`"\#{change.schema}.\#{change.table}"`). Keying the stream side by the bare
  `table` would land a row that was snapshot-loaded AND stream-updated under TWO mirror
  keys → row-for-row inflation.

  Each mirror value is tagged with the ORIGIN of its latest write: `:chunk` (snapshot) or
  `:stream`. A stream write over a snapshot row flips its origin to `:stream`. This makes
  the `first_for_table?` redo-safety reset surgical: `clear_mirror_table/1` `match_delete`s
  ONLY the `{table, _}` entries whose origin is `:chunk`, PRESERVING `:stream` rows. Under
  frontier ordering a concurrent stream UPDATE can land BEFORE the first chunk closes; a
  blanket clear of all of the table's rows would drop that stream-applied row (the drop-set
  then drops the chunk's collided version → the row is LOST). The library-side drop-set
  keeps a stream-superseded PK out of the chunk's kept rows, so re-applying the kept chunk
  rows as `:chunk` never clobbers a surviving `:stream` row — the stream version wins.
  `mirror/0` strips the tag, so the marquee's row-for-row compare is unaffected.

  ## Ownership (surviving a pipeline crash/restart)

  A module-function sink has no state of its own, so the ETS tables must be owned by
  a process that outlives the PIPELINE under test — Task 11/12 kill and restart the
  supervised pipeline mid-backfill and assert the mirror/ledger/progress persisted
  across that restart. `start_link/1` boots a small `Agent` (mirroring
  `Replicant.Test.RecordingSink`'s exact ownership pattern) that creates the tables
  as `:public, :named_table` in its own `init` — the calling test process links to
  this Agent, never a pipeline-internal process, so a `:one_for_all` supervisor
  restart or a killed `Connection`/`AssemblerServer` cannot take the tables down with
  it. Call `start_link/1` once from test `setup` (idempotent: a second call from a
  later test returns the already-running Agent), then call `reset/0` per test.

  ## Ledger vs mirror (`feedback_pre_dedup_count_defeated_by_query_dedup`)

  The mirror is upsert-by-PK (and delete-by-PK, tombstoned rather than physically
  removed so it can ride the same atomic multi-object insert) — it is DEDUPED by
  construction and therefore cannot prove effect-once. The ledger is APPEND-ONLY:
  every successfully-applied `handle_transaction/1` and `handle_snapshot/2` call adds
  exactly one entry and entries are never rewritten or removed (only `reset/0`, called
  from test setup, clears it). Task 11/12 assertions about dup counts and delivery
  order MUST read `ledger/0`, never `mirror/0`.

  ## Atomicity

  `:ets.insert/2` given a LIST of objects targeting one table is documented as
  "guaranteed to be atomic and isolated" (no concurrent reader observes a partial
  write) — the ETS analogue of `ledger_sink.ex`'s one-Postgrex-transaction discipline.
  `handle_transaction/1` builds ONE list combining the transaction's row upserts/
  tombstones with the `{:checkpoint, commit_lsn}` entry and inserts it in a single
  call. `handle_snapshot/2` builds ONE list combining the chunk's row upserts with the
  `{:progress, ctx.progress}` entry and inserts it in a single call — a crash cannot
  observe the progress token advanced without its rows, or vice versa. The ledger
  append is a separate call: it is an independent audit side-channel, not part of the
  atomic unit under test.

  ## Fault injection

  `set_fail_next_chunk(true)` arms the control flag. The NEXT `handle_snapshot/2`
  call (chunk OR the dedicated `backfill_complete?: true` completion call) checks and
  atomically clears the flag FIRST, before touching the mirror or ledger, and returns
  `{:error, :fail_next_chunk}` — so a faulted call leaves no trace (no row/progress
  write, no ledger entry), exactly modeling a real sink whose atomic write failed
  outright. The pipeline halts fail-closed; resume re-delivers the same chunk.
  """

  @behaviour Replicant.Sink

  use Agent

  alias Replicant.{Change, Transaction}

  @mirror_table :incremental_snapshot_sink_mirror
  @ledger_table :incremental_snapshot_sink_ledger
  @control_table :incremental_snapshot_sink_control

  # -- lifecycle ---------------------------------------------------------------

  @doc """
  Boots the Agent that owns the three ETS tables. Call once from test `setup`
  (mirrors `Replicant.Test.RecordingSink.start_link/0`): idempotent across
  repeated calls (including from later tests in the same run) via the
  `{:already_started, pid}` fallback.
  """
  @spec start_link(keyword()) :: Agent.on_start()
  def start_link(_opts \\ []) do
    case Agent.start_link(&boot_tables/0, name: __MODULE__) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
    end
  end

  defp boot_tables do
    if :ets.whereis(@mirror_table) == :undefined,
      do: :ets.new(@mirror_table, [:set, :public, :named_table])

    if :ets.whereis(@ledger_table) == :undefined,
      do: :ets.new(@ledger_table, [:ordered_set, :public, :named_table])

    if :ets.whereis(@control_table) == :undefined,
      do: :ets.new(@control_table, [:set, :public, :named_table])

    :ok
  end

  # -- Replicant.Sink ------------------------------------------------------------

  @impl true
  def checkpoint do
    case :ets.lookup(@mirror_table, :checkpoint) do
      [{:checkpoint, lsn}] -> {:ok, lsn}
      [] -> {:ok, nil}
    end
  end

  @impl true
  def handle_transaction(%Transaction{commit_lsn: lsn, changes: changes}) do
    {mirror_map, pks} =
      Enum.reduce(changes, {%{}, []}, fn change, {acc, pks} ->
        case mirror_entry(change) do
          nil -> {acc, pks}
          {key, value, pk} -> {Map.put(acc, key, value), [pk | pks]}
        end
      end)

    :ets.insert(@mirror_table, [{:checkpoint, lsn} | Map.to_list(mirror_map)])
    append_ledger({:txn, lsn, Enum.reverse(pks)})

    {:ok, lsn}
  end

  @impl true
  def handle_snapshot(changes, %{table: table, first_for_table?: first?} = ctx) do
    if take_fail_next_chunk?() do
      {:error, :fail_next_chunk}
    else
      apply_chunk(changes, table, first?, ctx)
    end
  end

  defp apply_chunk(changes, table, first?, ctx) do
    progress = Map.get(ctx, :progress)
    complete? = Map.get(ctx, :backfill_complete?, false)

    if first?, do: clear_mirror_table(table)

    {mirror_map, pks} =
      Enum.reduce(changes, {%{}, []}, fn change, {acc, pks} ->
        case chunk_entry(change, table) do
          nil -> {acc, pks}
          {key, value, pk} -> {Map.put(acc, key, value), [pk | pks]}
        end
      end)

    :ets.insert(@mirror_table, [{:progress, progress} | Map.to_list(mirror_map)])
    append_ledger({:chunk, table, Enum.reverse(pks), progress, first?, complete?})

    :ok
  end

  # `table` is the QUALIFIED `ctx.table`. Tag the value `:chunk` (snapshot origin) so a
  # later stream write can flip it to `:stream` and the first_for_table? clear can tell
  # snapshot-loaded rows apart from stream-applied ones.
  defp chunk_entry(%Change{record: r}, table) do
    case r && r["id"] do
      nil -> nil
      pk -> {{table, pk}, {:chunk, r}, pk}
    end
  end

  @impl true
  def snapshot_progress do
    case :ets.lookup(@mirror_table, :progress) do
      [{:progress, token}] -> {:ok, token}
      [] -> {:ok, nil}
    end
  end

  # -- test helpers --------------------------------------------------------------

  @doc "Clears the mirror, the ledger, and the fault-injection flag. Call from test setup."
  @spec reset() :: :ok
  def reset do
    :ets.delete_all_objects(@mirror_table)
    :ets.delete_all_objects(@ledger_table)
    :ets.delete_all_objects(@control_table)
    :ok
  end

  @doc """
  The append-only ledger, in call (insertion) order — the effect-once proof
  substrate. Entries are `{:txn, commit_lsn, pks}` (from `handle_transaction/1`) or
  `{:chunk, table, pks, progress, first_for_table?, backfill_complete?}` (from
  `handle_snapshot/2`). Never deduped: assert dup counts here, never on `mirror/0`.
  """
  @spec ledger() :: [tuple()]
  def ledger do
    @ledger_table
    |> :ets.tab2list()
    |> Enum.map(fn {_seq, entry} -> entry end)
  end

  @doc """
  The current mirror rows, keyed `{qualified_table, pk} => row` — the internal origin
  tag (`:chunk`/`:stream`) is STRIPPED here, and tombstoned rows plus the `:checkpoint`/
  `:progress` watermark entries are excluded, so this reflects live state only (the
  marquee's row-for-row compare sees the same `{table, pk} => row` shape as before).
  """
  @spec mirror() :: %{{String.t(), term()} => map()}
  def mirror do
    @mirror_table
    |> :ets.tab2list()
    |> Enum.flat_map(fn
      {key, _value} when key in [:checkpoint, :progress] -> []
      {_key, {_origin, :deleted}} -> []
      {key, {_origin, value}} -> [{key, value}]
    end)
    |> Map.new()
  end

  @doc """
  Arms (`true`) or disarms (`false`) fault injection. When armed, the NEXT
  `handle_snapshot/2` call (chunk or completion) checks-and-clears the flag first and
  returns `{:error, :fail_next_chunk}` without touching the mirror or ledger.
  """
  @spec set_fail_next_chunk(boolean()) :: :ok
  def set_fail_next_chunk(flag) when is_boolean(flag) do
    if flag do
      :ets.insert(@control_table, {:fail_next_chunk, true})
    else
      :ets.delete(@control_table, :fail_next_chunk)
    end

    :ok
  end

  # -- internal --------------------------------------------------------------

  # Upsert (insert/update) an entry; delete tombstones it (kept, not removed, so the
  # whole batch — including deletes — still lands via ONE :ets.insert call alongside
  # the checkpoint). `mirror/0` filters tombstones back out for callers.
  #
  # Key by the QUALIFIED "schema.table" (a stream Change carries `schema` + a BARE
  # `table` separately) so a stream write keys IDENTICALLY to handle_snapshot's
  # `ctx.table`. Tag the value `:stream`: a stream write over a snapshot row flips its
  # origin so the first_for_table? clear preserves it (see clear_mirror_table/1).
  defp mirror_entry(%Change{op: op, schema: schema, table: table, record: r})
       when op in [:insert, :update] do
    pk = r && r["id"]
    if pk, do: {{"#{schema}.#{table}", pk}, {:stream, r}, pk}
  end

  defp mirror_entry(%Change{op: :delete, schema: schema, table: table, old_record: old}) do
    pk = old && old["id"]
    if pk, do: {{"#{schema}.#{table}", pk}, {:stream, :deleted}, pk}
  end

  defp mirror_entry(_change), do: nil

  # first_for_table? redo-safety reset: clear ONLY this table's snapshot-origin (`:chunk`)
  # rows, preserving `:stream` rows a concurrent UPDATE applied before the first chunk (a
  # blanket `{{table, :_}, :_}` clear would drop that stream row → loss).
  defp clear_mirror_table(table) do
    :ets.match_delete(@mirror_table, {{table, :_}, {:chunk, :_}})
  end

  # :ets.take/2 reads-and-removes the flag in one call — check-and-clear with no
  # separate lookup/delete race window.
  defp take_fail_next_chunk? do
    case :ets.take(@control_table, :fail_next_chunk) do
      [{:fail_next_chunk, true}] -> true
      _absent_or_false -> false
    end
  end

  defp append_ledger(entry) do
    :ets.insert(@ledger_table, {System.unique_integer([:monotonic]), entry})
    :ok
  end
end
