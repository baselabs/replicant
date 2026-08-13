defmodule Replicant.Snapshotter.Incremental do
  @moduledoc """
  The incremental-backfill chunk reader (spec §4): a `spawn_link`'ed process (dies
  with the Connection — parent-spec closeout §17 precedent) on its OWN Postgrex
  connection.

  Loop per chunk: `AssemblerServer.open_snapshot_window/2` (tracking starts BEFORE LW —
  spec §2 R1; the deferred reply is also the §4 pacing gate) → capture LW → keyset
  SELECT → capture HW → `deliver_snapshot_chunk/2` (deferred reply = backpressure).
  Progress tokens ride each chunk (ctx.progress); on `{:error, :window_reset}` (an
  in-process reconnect, from `open`'s released deferred call OR a `deliver`) the loop
  RE-READS durable progress and continues — forward progress is monotone. Completion is
  a dedicated empty final chunk with `complete?: true`, re-delivered until the
  `:complete` token is durable (spec §6.3).

  PK-less tables (spec §6.4): whole-table unit inside one REPEATABLE READ txn,
  provisional streamed batches; contention ⇒ redo with `first?: true` on the attempt's
  FIRST batch; 3 attempts ⇒ halt `:snapshot_table_contended`.

  ## Resume is re-discovered, never token-trusted (Critical Rule 2)

  On BOTH the initial resume and every reconnect-reload, table metadata (`qualified`,
  `pk_quoted`, `pk_raw`, `col_quoted`, `col_type_names`) is re-discovered from the server
  (`quote_ident`/`format('%I.%I')` via `QueryBuilder.pk_columns/0` + `table_columns/0`). The persisted
  progress token is attacker-writable (a row in the progress store) and `decode/1`
  validates only that `pk_quoted`/`pk_raw` are LISTS — NOT that their elements are safe
  identifiers. So the token is used ONLY for the resume POSITION (which tables are done,
  which is current, the PK bound, and the terminal flag), matched to freshly-discovered
  tables by QUALIFIED NAME (a value compared, never interpolated). No token-sourced
  identifier ever reaches `keyset_chunk` SQL, so a tampered token can never inject SQL —
  the M2 injection defense is fully closed. A tampered BOUND is a DIFFERENT axis and is
  NOT convergence-safe: a valid-arity but numerically-HIGHER bound skips the rows below it
  (a data-SKIP, not a re-read). But setting that bound needs progress-store WRITE access —
  the same trust tier as corrupting the sink directly — so it is out of scope for the
  injection defense.

  ## Value-free boundary (Critical Rule 1)

  Every Postgrex/decode fault — raised OR returned — scrubs to
  `%Replicant.Error{reason: :snapshot_failed}` (shape-only), exactly the v1
  Snapshotter's discipline. PK bounds are row values: they live ONLY in the token and
  the bound parameters, never in telemetry, errors, or logs.
  """

  alias Replicant.{AssemblerServer, Change, Error, QueryBuilder, SnapshotProgress, Telemetry}
  alias Replicant.Casting.Types
  alias Replicant.Decoder.OidDatabase

  @max_table_attempts 3

  # A table_ref enriched with the full column list (`col_quoted`) and each column's
  # pgoutput type name (`col_type_names`, keyed by raw attname; an integer OID for a type
  # with no `name_for_type_id` mapping). SnapshotProgress.table_ref/0 does not carry these —
  # they are re-derived on every discovery and never trusted from the token.
  @type table_ref :: %{
          schema: String.t(),
          table: String.t(),
          qualified: String.t(),
          pk_raw: [String.t()],
          pk_quoted: [String.t()],
          col_quoted: [String.t()],
          col_type_names: %{optional(String.t()) => String.t() | non_neg_integer()}
        }

  @type args :: %{
          required(:slot_name) => String.t(),
          required(:connection) => keyword(),
          required(:publication) => [String.t()],
          required(:sink) => module(),
          required(:mode) => :sink_owned | :lib,
          required(:snapshot) => keyword(),
          required(:resume) => SnapshotProgress.t() | nil,
          required(:floor_lsn) => Replicant.lsn()
        }

  @doc "Spawn + LINK the reader to the caller (the Connection). Returns the pid."
  @spec start(args()) :: pid()
  def start(args), do: spawn_link(__MODULE__, :run, [args])

  @doc false
  @spec run(args()) :: :ok
  def run(args) do
    case register_reader(args.slot_name) do
      :ok ->
        case backfill(args) do
          :ok ->
            :ok

          {:error, reason} when is_atom(reason) ->
            Telemetry.event([:replicant, :snapshot, :failed], %{}, %{reason: reason})
            Replicant.Supervisor.halt(args.slot_name, {:snapshot, reason})
            :ok
        end

      {:error, :already_running} ->
        # Structural exactly-one guard fired (spec §8): a prior reader for this slot is still
        # alive — a reconnect path forgot retire_reader/1, the comment-only invariant the 1.0
        # readiness review flagged as the codebase's most comment-load-bearing hazard. Halt
        # fail-closed (value-free :duplicate_reader) rather than double-deliver snapshot chunks.
        # Registry auto-frees the key when the prior reader dies, so a correctly-retired restart
        # (the normal reconnect flow) re-registers cleanly and is unaffected.
        Telemetry.event([:replicant, :snapshot, :failed], %{}, %{reason: :duplicate_reader})
        Replicant.Supervisor.halt(args.slot_name, {:snapshot, :duplicate_reader})
        :ok
    end
  end

  @doc false
  # Structural exactly-one guard: register THIS reader under a per-slot key in
  # `Replicant.Registry` (`:unique`). A live prior registration means a reconnect path skipped
  # `retire_reader/1` — return `{:error, :already_running}` so `run/1` halts fail-closed instead
  # of double-delivering snapshot chunks. Registry removes the key when the owning reader dies,
  # so a correctly-retired restart re-registers `:ok`. Public (`@doc false`) for direct unit test.
  #
  # On collision, `await_prior_death_or_halt/2` covers the transient startup race where
  # `retire_reader/1` has killed the prior but the Registry has not yet freed the key (kill is
  # not awaited): monitor the prior and, if it dies, retry once; a prior that stays alive is a
  # genuine forgotten-reader double-spawn (the hazard) → halt.
  @spec register_reader(String.t()) :: :ok | {:error, :already_running}
  def register_reader(slot_name) do
    case Registry.register(Replicant.Registry, {:incremental_reader, slot_name}, nil) do
      {:ok, _owner} -> :ok
      {:error, {:already_registered, pid}} -> await_prior_death_or_halt(slot_name, pid)
    end
  end

  defp await_prior_death_or_halt(slot_name, pid) do
    ref = Process.monitor(pid)

    receive do
      {:DOWN, ^ref, _, ^pid, _} ->
        # Prior died (a legit retire in flight). Registry frees the key on its own :DOWN
        # (microseconds); retry until it does. The bound (~50 ms) dwarfs Registry processing.
        retry_register(slot_name, 10)
    after
      # A prior that does NOT die in this window is a real forgotten-reader double-spawn → halt.
      100 ->
        Process.demonitor(ref, [:flush])
        {:error, :already_running}
    end
  end

  defp retry_register(_slot_name, 0), do: {:error, :already_running}

  defp retry_register(slot_name, n) do
    case Registry.register(Replicant.Registry, {:incremental_reader, slot_name}, nil) do
      {:ok, _} ->
        :ok

      {:error, _} ->
        Process.sleep(5)
        retry_register(slot_name, n - 1)
    end
  end

  # Whole run inside the value-free boundary.
  defp backfill(args) do
    do_backfill(args)
  rescue
    _ -> {:error, :snapshot_failed}
  catch
    _kind, _reason -> {:error, :snapshot_failed}
  end

  defp do_backfill(args) do
    # Library control opts win (Keyword.merge, second wins): a single pooled conn — the
    # reader issues strictly serial queries (parity with the store + snapshotter merges).
    {:ok, db} = Postgrex.start_link(Keyword.merge(args.connection, pool_size: 1))
    # Stamp this run's start on the threaded `args` map so deliver_completion can report a real
    # `:completed` duration (spec §9) without threading a timestamp through the recursive chunk loop.
    args = Map.put(args, :started_mono, System.monotonic_time(:millisecond))

    try do
      standby? = standby?(db)
      sp = resume_or_discover(db, args, args.resume)
      chunk_rows = Keyword.fetch!(args.snapshot, :chunk_rows)
      loop(db, args, sp, chunk_rows, standby?)
    after
      GenServer.stop(db)
    end
  end

  defp standby?(db),
    do: Postgrex.query!(db, QueryBuilder.is_in_recovery(), []).rows == [[true]]

  # ---- discovery + RESUME RECONCILIATION (Critical Rule 2 injection defense) ----

  # A fresh run discovers from scratch; a resume RE-DISCOVERS and layers the token's
  # POSITION onto trusted, server-quoted metadata (never the token's identifiers).
  defp resume_or_discover(db, args, nil), do: discover(db, args)

  defp resume_or_discover(db, args, %SnapshotProgress{} = token),
    do: reconcile_resume(discover(db, args), token)

  # Fresh run: discover the publication tables + their PKs, build the token. Tables
  # WITHOUT a PK come from publication_tables minus the pk_columns result (spec §6.4),
  # queued LAST (their whole-table windows are the contention-sensitive ones).
  defp discover(db, args) do
    pk_rows = Postgrex.query!(db, QueryBuilder.pk_columns(), [args.publication]).rows
    col_rows = Postgrex.query!(db, QueryBuilder.table_columns(), [args.publication]).rows
    {:ok, all_sql} = QueryBuilder.publication_tables(args.publication)
    all_rows = Postgrex.query!(db, all_sql, [args.publication]).rows
    SnapshotProgress.new(parse_pk_rows(pk_rows, col_rows, all_rows), args.floor_lsn)
  end

  @doc false
  @spec parse_pk_rows([[term()]], [[term()]], [[term()]]) :: [table_ref()]
  def parse_pk_rows(pk_rows, col_rows, all_rows) do
    cols_by_q = Map.new(col_rows, &col_meta/1)

    with_pk =
      Map.new(pk_rows, fn [schema, table, qualified, pk_raw, pk_quoted] ->
        {qualified, keyed_ref(schema, table, qualified, pk_raw, pk_quoted, cols_by_q)}
      end)

    {keyed, keyless} =
      all_rows
      |> Enum.map(fn [schema, table, qualified] ->
        Map.get_lazy(with_pk, qualified, fn ->
          keyless_ref(schema, table, qualified, cols_by_q)
        end)
      end)
      |> Enum.split_with(&(&1.pk_raw != []))

    keyed ++ keyless
  end

  # {qualified => %{col_quoted, col_type_names}}. The OID -> pgoutput type-name mapping is
  # the decoder's OWN (`name_for_type_id`), so a `<col>::text` value casts through EXACTLY
  # the stream side's cast path; an unmapped OID stays its integer (cast_record's fallback
  # returns the raw text — the same value-free graceful path the stream takes).
  defp col_meta([_schema, _table, qualified, col_raw, col_quoted, col_type_oids]) do
    type_names =
      col_raw
      |> Enum.zip(Enum.map(col_type_oids, &OidDatabase.name_for_type_id/1))
      |> Map.new()

    {qualified, %{col_quoted: col_quoted, col_type_names: type_names}}
  end

  defp keyed_ref(schema, table, qualified, pk_raw, pk_quoted, cols_by_q) do
    cmeta = Map.get(cols_by_q, qualified, %{col_quoted: [], col_type_names: %{}})

    %{
      schema: schema,
      table: table,
      qualified: qualified,
      pk_raw: pk_raw,
      pk_quoted: pk_quoted,
      col_quoted: cmeta.col_quoted,
      col_type_names: cmeta.col_type_names
    }
  end

  defp keyless_ref(schema, table, qualified, cols_by_q) do
    cmeta = Map.get(cols_by_q, qualified, %{col_quoted: [], col_type_names: %{}})

    %{
      schema: schema,
      table: table,
      qualified: qualified,
      pk_raw: [],
      pk_quoted: [],
      col_quoted: cmeta.col_quoted,
      col_type_names: cmeta.col_type_names
    }
  end

  @doc false
  # Reconcile a token's RESUME POSITION onto FRESHLY re-discovered table metadata (plan
  # mandate 2). Identifiers (`qualified`/`pk_quoted`) come ONLY from `fresh`; the token
  # supplies which tables are done, which is current (matched by qualified NAME), the
  # bound (bind params, arity-checked), and the terminal flag. Public for a unit proof
  # that a tampered token's identifiers never survive the reconciliation.
  @spec reconcile_resume(SnapshotProgress.t(), SnapshotProgress.t()) :: SnapshotProgress.t()
  # Terminality note (F4, spec §6.3): once a token is `complete?: true`, reconciliation marks
  # EVERY freshly-discovered table done — so a table ADDED to the publication AFTER backfill
  # completion is NOT historically backfilled; the operator remedy is a fresh re-snapshot.
  def reconcile_resume(fresh, %SnapshotProgress{complete?: true}) do
    %{
      fresh
      | complete?: true,
        pending: [],
        current: nil,
        bound: nil,
        done: Enum.map(fresh.pending, & &1.qualified)
    }
  end

  def reconcile_resume(fresh, %SnapshotProgress{} = token) do
    done_set = MapSet.new(token.done)
    original_set = original_table_set(token)

    # The table set is captured ONCE per fresh run (spec §3/§18 non-goal): a table ADDED to the
    # publication mid-run — present in FRESH discovery but ABSENT from the token's original set
    # (done ∪ pending ∪ current) — is NOT historically backfilled; it streams from its add-point
    # only. Fresh discovery supplies injection-safe METADATA for the ORIGINAL tables, never NEW
    # ones. [Closeout 2026-07-10: user-ratified F7 — match the non-goal, not backfill-on-reconnect;
    # supersedes the plan's F3.3 backfill-added-tables-on-resume choice.]
    in_scope = Enum.filter(fresh.pending, &MapSet.member?(original_set, &1.qualified))
    {done_refs, rest} = Enum.split_with(in_scope, &MapSet.member?(done_set, &1.qualified))
    {current_ref, remaining} = select_current(rest, token.current)
    bound = resume_bound(current_ref, token.bound)

    %{
      fresh
      | done: Enum.map(done_refs, & &1.qualified),
        current: current_ref,
        bound: bound,
        pending: remaining,
        complete?: false
    }
  end

  # The ORIGINAL fresh-run table set, recovered from the token: tables only ever move
  # pending → current → done (never added), so their union is EXACTLY the set the fresh run
  # captured — the baseline a resume must not expand (spec §3/§18 non-goal).
  defp original_table_set(%SnapshotProgress{done: done, pending: pending, current: current}) do
    MapSet.new(done ++ Enum.map(pending, & &1.qualified) ++ current_qualified(current))
  end

  defp current_qualified(%{qualified: q}) when is_binary(q), do: [q]
  defp current_qualified(_), do: []

  # Match the token's in-progress table to a FRESH table_ref by qualified name (a string
  # compare — never interpolated). No current / no match ⇒ resume from the queue head.
  defp select_current(rest, %{qualified: q}) when is_binary(q) do
    case Enum.split_with(rest, &(&1.qualified == q)) do
      {[ref], others} -> {ref, others}
      {_none_or_many, _} -> {nil, rest}
    end
  end

  defp select_current(rest, _no_current), do: {nil, rest}

  # The resume bound rides as BIND PARAMETERS (never interpolated) so it is not an
  # injection vector; still, gate it on ARITY matching the fresh table's PK so a
  # malformed token fails closed to a fresh-table re-read (convergence-safe) rather than
  # tripping keyset_chunk's arity guard mid-loop.
  defp resume_bound(nil, _bound), do: nil
  defp resume_bound(_ref, nil), do: nil

  defp resume_bound(%{pk_raw: pk_raw}, bound)
       when is_list(bound) and length(pk_raw) == length(bound),
       do: bound

  defp resume_bound(_ref, _bound), do: nil

  # ---- the chunk loop ----

  defp loop(db, args, sp, chunk_rows, standby?) do
    case SnapshotProgress.next(sp) do
      :complete ->
        deliver_completion(db, args, sp, chunk_rows, standby?)

      {:table, %{pk_raw: []} = table, _bound, sp} ->
        run_keyless_table(db, args, sp, table, standby?, 1)

      {:table, table, bound, sp} ->
        run_chunk(db, args, sp, table, bound, chunk_rows, standby?)
    end
  end

  defp run_chunk(db, args, sp, table, bound, chunk_rows, standby?) do
    fresh_table? = is_nil(bound) and args.resume == nil

    # Capture the window GENERATION so the FINAL-chunk barrier below can reject a stale one (a
    # reconnect reset re-seats the window → its buffered chunks were wiped, spec §4/§6.4).
    epoch = open_window_epoch(server(args), table.qualified)

    _lw = watermark(db, standby?)

    {:ok, sql} =
      QueryBuilder.keyset_chunk(
        table.qualified,
        table.col_quoted,
        table.pk_quoted,
        length(bound || [])
      )

    params = [chunk_rows | bound || []]
    %Postgrex.Result{columns: cols, rows: rows} = Postgrex.query!(db, sql, params)
    hw = watermark(db, standby?)

    # Cast every column to its stream-identical term (the delivered record) + derive the
    # cast pk_canon (drop-set key, matches the window's cast stream PK); the RAW __rpk_*
    # projections give the bind-compatible keyset bound (last_bound).
    {changes, pk_canon, last_bound} = split_changes(table, cols, rows)

    {sp, done?} =
      if length(rows) < chunk_rows do
        {SnapshotProgress.finish_table(sp), true}
      else
        {SnapshotProgress.advance(sp, last_bound), false}
      end

    deliver(args, %{
      qualified: table.qualified,
      schema: table.schema,
      table: table.table,
      pk_raw: table.pk_raw,
      pk_canon: pk_canon,
      changes: changes,
      hw: hw,
      first?: fresh_table?,
      complete?: false,
      progress: SnapshotProgress.encode(sp),
      bound: sp.bound
    })

    if done? do
      # FINAL-CHUNK BARRIER (spec §6.4, CV2). The final (short) keyed chunk carries the `finish_table`
      # (done) token, and the reader is about to move to the NEXT table. `deliver` returned on
      # ACCEPTANCE, not apply, so this chunk is still buffered (frontier < hw). If a contention taint
      # (drop-cap breach / TRUNCATE / spill) discards it before it applies, NO later same-table deliver
      # would catch the discard → the table would persist `done` with un-applied rows = DATA LOSS.
      # Block until ALL of this table's chunks drain (apply): a discard throws :table_discarded → this
      # table re-reads, a reconnect throws :window_reset → reload. (Non-final chunks need no barrier —
      # the NEXT same-table deliver catches a discard; the keyless path has its own barrier at §6.4.)
      reset_guard(AssemblerServer.finish_snapshot_table(server(args), table.qualified, epoch))

      Telemetry.event([:replicant, :snapshot, :table_completed], %{}, %{
        table: table.qualified,
        change_count: 0
      })
    end

    loop(db, args, sp, chunk_rows, standby?)
  catch
    # A reconnect (:window_reset) OR a keyed drop-cap contention discard (:table_discarded) both
    # re-read from durable progress: the discarded chunks were never applied (removed while pending),
    # so re-fetching them is loss-free convergence, never a halt, never an attempt count (spec §4).
    :window_reset ->
      reload_and_continue(db, args, chunk_rows, standby?)

    :table_discarded ->
      # Emit `:chunk_retried` on the keyed drop-cap-breach re-read — spec §9 defines the event for
      # BOTH triggers ("drop-set-cap breach / PK-less redo — no silent re-read loops"); the keyless
      # redo already emits it (run_keyless_table), the keyed breach must too, or a keyed contention
      # loop is invisible. NOT on :window_reset (a reconnect is not a contention retry, §6.4).
      Telemetry.event([:replicant, :snapshot, :chunk_retried], %{}, %{
        table: table.qualified,
        reason: :snapshot_table_contended
      })

      reload_and_continue(db, args, chunk_rows, standby?)
  end

  # An in-process reconnect reset the window: re-read DURABLE progress, RE-DISCOVER table
  # metadata, and continue — applied chunks are never re-read (monotone forward progress,
  # spec §4). Re-discovery here closes the same token-injection vector as the initial
  # resume (the durable token is equally attacker-writable).
  defp reload_and_continue(db, args, chunk_rows, standby?) do
    sp =
      case read_durable_progress(args) do
        {:ok, nil} ->
          discover(db, args)

        {:ok, token} ->
          case SnapshotProgress.decode(token) do
            {:ok, decoded} -> reconcile_resume(discover(db, args), decoded)
            {:error, reason} -> throw({:halt, reason})
          end

        {:error, _} ->
          throw({:halt, :snapshot_failed})
      end

    loop(db, args, sp, chunk_rows, standby?)
  catch
    {:halt, reason} -> {:error, reason}
  end

  defp read_durable_progress(%{mode: :lib} = args),
    do: Replicant.CheckpointStore.read_progress(Replicant.CheckpointStore.via(args.slot_name))

  defp read_durable_progress(%{sink: sink}), do: sink.snapshot_progress()

  # Completion (spec §6.3): a dedicated EMPTY final call, at-least-once until :complete
  # is durable — delivered through the same window path (hw = 0 closes immediately). A
  # window_reset during this delivery re-reads durable progress and re-reaches completion
  # (the moduledoc's "re-delivered until durable" contract), rather than halting.
  defp deliver_completion(db, args, sp, chunk_rows, standby?) do
    done = SnapshotProgress.mark_complete(sp)

    deliver(args, %{
      qualified: "__backfill_complete__",
      schema: "",
      table: "",
      pk_raw: [],
      pk_canon: [],
      changes: [],
      hw: 0,
      first?: false,
      complete?: true,
      progress: SnapshotProgress.encode(done),
      bound: nil
    })

    # This RUN's backfill duration (ms). A resumed backfill spans multiple runs, so this measures the
    # run that reached completion, not the whole historical backfill — the honest per-run analogue of
    # the v1 snapshotter's single-session COPY duration. `started_mono` is stamped in do_backfill.
    duration = System.monotonic_time(:millisecond) - args.started_mono

    Telemetry.event([:replicant, :snapshot, :completed], %{duration: duration}, %{
      commit_lsn: sp.floor_lsn,
      change_count: 0
    })

    :ok
  catch
    :window_reset -> reload_and_continue(db, args, chunk_rows, standby?)
  end

  defp deliver(args, chunk) do
    reset_guard(AssemblerServer.deliver_snapshot_chunk(server(args), chunk))
  end

  # Unify the window calls' reset/discard handling: a reconnect releases `{:error, :window_reset}`;
  # a contention discard (drop-cap breach / a PK-less table's concurrent write) releases
  # `{:error, :table_discarded}`. The two throw DISTINCT atoms so the keyless path counts a
  # contention redo toward the 3-attempt halt but NOT a plain reconnect (spec §6.4). Reachable from
  # the KEYED `open_snapshot_window/2` (whose `{:ok, epoch}` success is discarded — the keyed path
  # never barriers, so it needs no generation), from `deliver_snapshot_chunk/2`, AND from
  # `finish_snapshot_table/3`. The KEYLESS open captures the epoch instead (`open_window_epoch/2`).
  @doc false
  @spec reset_guard(:ok | {:ok, non_neg_integer()} | {:error, :window_reset | :table_discarded}) ::
          :ok | no_return()
  def reset_guard(:ok), do: :ok
  def reset_guard({:ok, _epoch}), do: :ok
  def reset_guard({:error, :window_reset}), do: throw(:window_reset)
  def reset_guard({:error, :table_discarded}), do: throw(:table_discarded)

  # Open a KEYLESS table's tracking window and RETURN the window GENERATION (epoch) the reader now
  # operates under — threaded to `finish_snapshot_table/3`'s stale-generation barrier so a reconnect
  # reset that WIPED this reader's provisional batches yields `{:error, :window_reset}` (re-read)
  # instead of a spurious `:ok` (spec §4/§6.4). Routes the reconnect / contention error replies
  # through the SAME distinct throws as `reset_guard/1`; the explicit `no_return()` on the error
  # clauses keeps the returned epoch a clean `non_neg_integer()` for dialyzer.
  @spec open_window_epoch(GenServer.server(), String.t()) :: non_neg_integer() | no_return()
  defp open_window_epoch(server, qualified) do
    case AssemblerServer.open_snapshot_window(server, qualified) do
      {:ok, epoch} -> epoch
      {:error, :window_reset} -> throw(:window_reset)
      {:error, :table_discarded} -> throw(:table_discarded)
    end
  end

  # ---- PK-less whole-table fallback (spec §6.4) ----

  # One REPEATABLE READ txn, provisional streamed batches (bounded memory), window over
  # the WHOLE read. Contention is detected by the ASSEMBLER discarding the table (drop-cap
  # taint): the reader's NEXT deliver returns {:error, :window_reset} → the reduce throws,
  # the txn rolls back, and this attempt re-reads. After @max_table_attempts the reader
  # halts :snapshot_table_contended (spec §6.4).
  defp run_keyless_table(_db, args, _sp, table, _standby?, attempt)
       when attempt > @max_table_attempts do
    Telemetry.event([:replicant, :snapshot, :failed], %{}, %{
      reason: :snapshot_table_contended,
      table: table.qualified
    })

    Replicant.Supervisor.halt(args.slot_name, {:snapshot, :snapshot_table_contended})
    :ok
  end

  defp run_keyless_table(db, args, sp, table, standby?, attempt) do
    if attempt > 1 do
      Telemetry.event([:replicant, :snapshot, :chunk_retried], %{}, %{
        table: table.qualified,
        reason: :snapshot_table_contended
      })
    end

    # Capture the window GENERATION so the whole-read BARRIER can reject a stale one: a reconnect
    # reset that WIPED these provisional batches bumps the epoch → finish_snapshot_table/3 replies
    # {:error, :window_reset} instead of a spurious :ok that would mark a never-delivered table done.
    epoch = open_window_epoch(server(args), table.qualified)

    # Every provisional batch carries the IN-PROGRESS token — the table still CURRENT, NOT
    # `done` — NOT `finish_table(sp)`. Keyless batches apply INDEPENDENTLY (each has its own
    # HW; SnapshotWindow.pop_ready applies per-HW) and lib mode persists progress PER applied
    # chunk (assembler_server persist_progress). A "done" token on batch-1 would durably mark
    # the table complete while batch-2..N are still pending → a crash there resumes with the
    # table in `done` and NEVER re-reads the rest = DATA GAP. Instead "done" rides ONLY the
    # loop-advance (`sp_after`) and is persisted by the NEXT unit's chunk. A crash mid-table
    # resumes with the table CURRENT → the WHOLE table re-reads at attempt 1 with first?: true
    # on its first batch, whose sink reset clears the provisional rows (effect-once, no gap,
    # no dup — spec §6.4). Contrast the keyed path (run_chunk): intermediate chunks carry
    # `advance(bound)`, only the FINAL short chunk carries `finish_table`.
    sp_after = SnapshotProgress.finish_table(sp)
    encoded_current = keyless_batch_progress(sp)

    result =
      Postgrex.transaction(
        db,
        fn c ->
          Postgrex.query!(c, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", [])

          c
          |> Postgrex.stream(QueryBuilder.keyless_scan(table.qualified, table.col_quoted), [],
            max_rows: 1000
          )
          |> Enum.reduce({:ok, true}, fn %Postgrex.Result{columns: cols, rows: rows},
                                         {:ok, first?} ->
            deliver_keyless_batch(args, table, {cols, rows}, watermark_in_txn(c, standby?),
              first?: first?,
              progress: encoded_current
            )

            # IMPLEMENTATION NOTE 1: only the attempt's FIRST batch carries first?: true (the
            # sink reset clears provisional rows — spec §6.4); every later batch is first?: false.
            {:ok, false}
          end)
        end,
        timeout: :infinity
      )

    case result do
      {:ok, {:ok, _first?}} ->
        # BARRIER (spec §6.4): the provisional batches are all streamed but still PENDING (frontier-
        # gated). Block until they APPLY (converged) or the table was contention-discarded. Without
        # this, a single-batch keyless read completes and advances before a late concurrent write
        # discards its still-pending batch — silently losing the rows. A discard throws
        # :table_discarded → the catch redoes this table (attempt + 1).
        reset_guard(AssemblerServer.finish_snapshot_table(server(args), table.qualified, epoch))
        loop(db, args, sp_after, Keyword.fetch!(args.snapshot, :chunk_rows), standby?)

      {:error, _} ->
        run_keyless_table(db, args, sp, table, standby?, attempt + 1)
    end
  catch
    # A CONTENTION discard redoes the whole table AND counts toward the @max_table_attempts halt;
    # a plain reconnect (:window_reset) redoes at the SAME attempt (a reconnect is not contention —
    # spec §6.4). :chunk_retried fires at the top of each attempt > 1.
    :table_discarded -> run_keyless_table(db, args, sp, table, standby?, attempt + 1)
    :window_reset -> run_keyless_table(db, args, sp, table, standby?, attempt)
  end

  @doc false
  # The progress token a PROVISIONAL keyless batch carries: the IN-PROGRESS token (the
  # table still `current`, `bound: nil`, NOT in `done`). Distinct from the loop-advance
  # token `finish_table(sp)` — see run_keyless_table's DATA-GAP comment. Pure, so the
  # in-progress-vs-done semantics are unit-provable without a live DB/AssemblerServer.
  @spec keyless_batch_progress(SnapshotProgress.t()) :: binary()
  def keyless_batch_progress(sp), do: SnapshotProgress.encode(sp)

  # One provisional keyless batch. `pk_raw`/`pk_canon` are empty (no keyset drop-set for a
  # PK-less table — convergence rests on the attempt-first reset, spec §6.4).
  defp deliver_keyless_batch(args, table, {cols, rows}, hw, opts) do
    deliver(args, %{
      qualified: table.qualified,
      schema: table.schema,
      table: table.table,
      pk_raw: [],
      pk_canon: [],
      changes: build_changes(table, {cols, rows}),
      hw: hw,
      first?: Keyword.fetch!(opts, :first?),
      complete?: false,
      progress: Keyword.fetch!(opts, :progress),
      bound: nil
    })
  end

  # HW inside the read txn: pg_current_wal_lsn()/pg_last_wal_replay_lsn() are LIVE system
  # functions (not snapshot-bound), so querying through the txn's own checked-out conn `c`
  # is both correct AND required — `db` is a pool_size-1 pool whose only connection the
  # enclosing transaction holds (querying `db` here would deadlock — plan review F3).
  defp watermark_in_txn(c, standby?), do: watermark(c, standby?)

  defp watermark(db, standby?) do
    [[lsn_str]] = Postgrex.query!(db, QueryBuilder.watermark_lsn(standby?), []).rows
    Replicant.lsn_from_string(lsn_str)
  end

  defp server(args), do: AssemblerServer.via(args.slot_name)

  @doc false
  # Split the real (cast) columns from the trailing RAW __rpk_* PK projections. The real
  # columns arrive as `<col>::text` and are cast through `Casting.Types.cast_record/2` (the
  # stream's OWN path) into the delivered `%Change{}.record`; `pk_canon` (the drop-set key)
  # is derived from that CAST record's PK columns — identical to the term the stream tracks;
  # `last_bound` is the LAST row's RAW native PK tuple (bind-compatible for the keyset
  # resume — a cast uuid/timestamp value would NOT bind to its typed WHERE column).
  # `table` is a discovery/enriched table_ref (`col_type_names`/`pk_raw`); typed `map()` (not
  # `table_ref()`) because `SnapshotProgress.next/1` returns the declared
  # `SnapshotProgress.table_ref/0`, which erases the enriched keys from dialyzer's view.
  @spec split_changes(map(), [String.t()], [[term()]]) ::
          {[Change.t()], [[term()]], [term()] | nil}
  def split_changes(table, cols, rows) do
    {real_cols, rpk_cols} = Enum.split_with(cols, &(not String.starts_with?(&1, "__rpk_")))
    n_real = length(real_cols)
    n_rpk = length(rpk_cols)

    # Fail closed on an unexpected __rpk_ column count (F1). The keyset SQL emits EXACTLY one
    # __rpk_N per PK column (QueryBuilder.rpk_select), so n_rpk MUST equal the PK arity. A user
    # column literally named `__rpk_<n>` would otherwise be misclassified into rpk_cols,
    # shorten n_real, misalign Enum.split, and silently mis-build the drop-set. A bare-shape
    # raise (no row values) is caught by backfill/1's value-free rescue → :snapshot_failed
    # (Critical Rule 1), halting instead of corrupting convergence.
    unless n_rpk == length(table.pk_raw) do
      raise "unexpected rpk column count"
    end

    triples =
      Enum.map(rows, fn row ->
        {real_vals, rpk_raw} = Enum.split(row, n_real)
        change = hd(build_changes(table, {real_cols, [real_vals]}))
        # pk_canon: the CAST PK tuple (from the delivered record) — matches the window's
        # cast stream PK. The record carries every column (PKs included), so its PK values
        # are already cast through the stream path.
        canon = Enum.map(table.pk_raw, &Map.fetch!(change.record, &1))
        # The RAW native PK tuple for the keyset bound (bind-compatible).
        bound = Enum.take(rpk_raw, n_rpk)
        {change, canon, bound}
      end)

    changes = Enum.map(triples, &elem(&1, 0))
    canons = Enum.map(triples, &elem(&1, 1))

    last_bound =
      case List.last(triples) do
        nil -> nil
        triple -> elem(triple, 2)
      end

    {changes, canons, last_bound}
  end

  @doc false
  @spec build_changes(map(), {[String.t()], [[term()]]}) :: [Change.t()]
  def build_changes(table, {cols, rows}) do
    Enum.map(rows, fn row ->
      record =
        cols
        |> Enum.zip(row)
        |> Map.new(fn {col, val} ->
          {col, cast_value(val, Map.get(table.col_type_names, col))}
        end)

      %Change{
        op: :snapshot,
        schema: table.schema,
        table: table.table,
        record: record,
        commit_lsn: nil
      }
    end)
  end

  # Cast one `<col>::text` value to its Elixir term, EXACTLY mirroring the stream's
  # `Assembler.cast_value/2`: a NULL column stays `nil`, and a text value routes through
  # the shared `Casting.Types.cast_record/2`. `type` is the pgoutput type name (or an
  # integer OID for an unmapped type — `cast_record/2`'s fallback returns the raw text,
  # matching the stream). No other input shape reaches here: the `::text` projection yields
  # only binaries and NULLs.
  defp cast_value(nil, _type), do: nil
  defp cast_value(value, type) when is_binary(value), do: Types.cast_record(value, type)

  @doc false
  @spec reader_error(term()) :: Error.t()
  def reader_error(%{__struct__: mod}), do: %Error{reason: :snapshot_failed, shape: inspect(mod)}
  def reader_error(_other), do: %Error{reason: :snapshot_failed}
end
