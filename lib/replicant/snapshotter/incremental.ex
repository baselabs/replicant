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
  `pk_quoted`, `pk_raw`, `pk_type_names`) is re-discovered from the server
  (`quote_ident`/`format('%I.%I')` via `QueryBuilder.pk_columns/0`). The persisted
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

  # A table_ref enriched with `pk_type_names` (SnapshotProgress.table_ref/0 does not carry
  # them — they are re-derived on every discovery and never trusted from the token).
  @type table_ref :: %{
          schema: String.t(),
          table: String.t(),
          qualified: String.t(),
          pk_raw: [String.t()],
          pk_quoted: [String.t()],
          pk_type_names: [String.t()]
        }

  @type args :: %{
          required(:slot_name) => String.t(),
          required(:connection) => keyword(),
          required(:publication) => String.t(),
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
    case backfill(args) do
      :ok ->
        :ok

      {:error, reason} when is_atom(reason) ->
        Telemetry.event([:replicant, :snapshot, :failed], %{}, %{reason: reason})
        Replicant.Supervisor.halt(args.slot_name, {:snapshot, reason})
        :ok
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
    {:ok, db} = Postgrex.start_link(args.connection ++ [pool_size: 1])

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
    do: Postgrex.query!(db, "SELECT pg_is_in_recovery();", []).rows == [[true]]

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
    {:ok, all_sql} = QueryBuilder.publication_tables(args.publication)
    all_rows = Postgrex.query!(db, all_sql, []).rows
    SnapshotProgress.new(parse_pk_rows(pk_rows, all_rows), args.floor_lsn)
  end

  @doc false
  @spec parse_pk_rows([[term()]], [[term()]]) :: [table_ref()]
  def parse_pk_rows(pk_rows, all_rows) do
    with_pk =
      Map.new(pk_rows, fn [schema, table, qualified, pk_raw, pk_quoted, pk_type_oids] ->
        {qualified,
         %{
           schema: schema,
           table: table,
           qualified: qualified,
           pk_raw: pk_raw,
           pk_quoted: pk_quoted,
           # OID -> pgoutput type-name via the decoder's own mapping, so chunk-side PK
           # text casts through EXACTLY the stream side's cast path (plan review F1).
           pk_type_names: Enum.map(pk_type_oids, &OidDatabase.name_for_type_id/1)
         }}
      end)

    {keyed, keyless} =
      all_rows
      |> Enum.map(fn [schema, table, qualified] ->
        Map.get(with_pk, qualified, %{
          schema: schema,
          table: table,
          qualified: qualified,
          pk_raw: [],
          pk_quoted: [],
          pk_type_names: []
        })
      end)
      |> Enum.split_with(&(&1.pk_raw != []))

    keyed ++ keyless
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
    {done_refs, rest} = Enum.split_with(fresh.pending, &MapSet.member?(done_set, &1.qualified))
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

    reset_guard(AssemblerServer.open_snapshot_window(server(args), table.qualified))

    _lw = watermark(db, standby?)
    {:ok, sql} = QueryBuilder.keyset_chunk(table.qualified, table.pk_quoted, length(bound || []))
    params = [chunk_rows | bound || []]
    %Postgrex.Result{columns: cols, rows: rows} = Postgrex.query!(db, sql, params)
    hw = watermark(db, standby?)

    # Split off the trailing __rpk_* text projections: the record keeps only real columns;
    # the canonical PK tuple casts the pg-rendered text through the stream's OWN cast path
    # (one representation on both drop-set sides — plan review F1).
    {changes, pk_canon} = split_changes(table, cols, rows)

    {sp, done?} =
      if length(rows) < chunk_rows do
        {SnapshotProgress.finish_table(sp), true}
      else
        {SnapshotProgress.advance(sp, bound_of(changes, table.pk_raw)), false}
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
      Telemetry.event([:replicant, :snapshot, :table_completed], %{}, %{
        table: table.qualified,
        change_count: 0
      })
    end

    loop(db, args, sp, chunk_rows, standby?)
  catch
    :window_reset -> reload_and_continue(db, args, chunk_rows, standby?)
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

    Telemetry.event([:replicant, :snapshot, :completed], %{duration: 0}, %{
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

  # Unify the two window calls' reset handling: `open_snapshot_window/2`'s deferred reply
  # (released `{:error, :window_reset}` on a reconnect) and `deliver_snapshot_chunk/2`'s
  # `{:error, :window_reset}` both route to the loop's `catch :window_reset`. A single
  # helper keeps the `{:error, :window_reset}` clause reachable for dialyzer (open's spec
  # is `:: :ok`; deliver's union makes the error arm live).
  defp reset_guard(:ok), do: :ok
  defp reset_guard({:error, :window_reset}), do: throw(:window_reset)

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

    reset_guard(AssemblerServer.open_snapshot_window(server(args), table.qualified))

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
          |> Postgrex.stream("SELECT * FROM #{table.qualified}", [], max_rows: 1000)
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
        loop(db, args, sp_after, Keyword.fetch!(args.snapshot, :chunk_rows), standby?)

      {:error, _} ->
        run_keyless_table(db, args, sp, table, standby?, attempt + 1)
    end
  catch
    :window_reset -> run_keyless_table(db, args, sp, table, standby?, attempt + 1)
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
  # Separate real columns from the trailing __rpk_* projections; cast each __rpk text
  # through Casting.Types.cast_record/2 with the discovered pgoutput type names.
  # `table` is a discovery/enriched table_ref carrying `pk_type_names`; typed `map()`
  # (not `table_ref()`) because `SnapshotProgress.next/1` returns the declared
  # `SnapshotProgress.table_ref/0`, which erases the enriched key from dialyzer's view.
  @spec split_changes(map(), [String.t()], [[term()]]) :: {[Change.t()], [[term()]]}
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
    unless n_rpk == length(table.pk_type_names) do
      raise "unexpected rpk column count"
    end

    rows
    |> Enum.map(fn row ->
      {real_vals, rpk_texts} = Enum.split(row, n_real)
      change = hd(build_changes(table, {real_cols, [real_vals]}))

      canon =
        rpk_texts
        |> Enum.take(n_rpk)
        |> Enum.zip(table.pk_type_names)
        |> Enum.map(fn {text, type_name} -> Types.cast_record(text, type_name) end)

      {change, canon}
    end)
    |> Enum.unzip()
  end

  @doc false
  @spec build_changes(map(), {[String.t()], [[term()]]}) :: [Change.t()]
  def build_changes(table, {cols, rows}) do
    Enum.map(rows, fn row ->
      %Change{
        op: :snapshot,
        schema: table.schema,
        table: table.table,
        record: cols |> Enum.zip(row) |> Map.new(),
        commit_lsn: nil
      }
    end)
  end

  @doc false
  @spec bound_of([Change.t()], [String.t()]) :: [term()]
  def bound_of(changes, pk_raw) do
    last = List.last(changes)
    Enum.map(pk_raw, &Map.get(last.record, &1))
  end

  @doc false
  @spec reader_error(term()) :: Error.t()
  def reader_error(%{__struct__: mod}), do: %Error{reason: :snapshot_failed, shape: inspect(mod)}
  def reader_error(_other), do: %Error{reason: :snapshot_failed}
end
