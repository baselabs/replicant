defmodule Replicant.Connection do
  @moduledoc """
  The `Postgrex.ReplicationConnection` that owns the replication slot and closes
  the exactly-once seam (spec §2/§4/§8). It:

    * replies to a reply-requested keepalive with the **last durably-checkpointed LSN**
      while any published transaction is in flight — never advancing the slot past
      un-persisted publication data (walex's fire-and-forget `wal_end+1` is the
      at-most-once bug this fixes). When IDLE (no open transaction and the checkpoint
      caught up), a `:state_mirror` sink advances the slot to the keepalive `wal_end`
      so a quiet-but-filtered publication stops pinning WAL (spec A1) — the intervening
      WAL carries nothing for the publication. An `:append_log` sink never performs
      that filtered-WAL advance: it acknowledges only its durable checkpoint and uses
      an ordinary published heartbeat transaction to release retained WAL;
    * decodes each XLogData payload behind Plan 1's value-free boundary and forwards
      the decoded message to `Replicant.AssemblerServer` — it never applies the sink,
      so it is always free to answer keepalives;
    * tracks a **bounded in-flight window** (spec §4): the high-water received LSN
      (`received_lsn`, the latest XLogData `wal_end`) minus the confirmed-durable
      floor is the in-flight WAL lag — un-drained WAL, a proxy for the transaction
      BACKLOG accumulating ahead of the sink. A single non-blocking integer
      comparison in `handle_data/2` compares it to `max_inflight_lag` (a
      backlog-sized ceiling, default 64 MiB);
    * when the in-flight lag exceeds the bound the sink is genuinely lagging: it
      **halts fail-closed** with `{:sink_too_slow, lag}` (surfaced telemetry +
      `Replicant.Supervisor.halt/2` + `{:disconnect, :sink_too_slow}`) — never a
      silent reconnect livelock or an unbounded-mailbox OOM. On restart it resumes
      from the checkpoint (loss=0 by the §6 idempotent dedup);
    * advances the ack asynchronously on `{:sink_committed, L}` from the AssemblerServer;
    * on connect/reconnect detects an invalidated slot (`wal_status = 'lost'` or
      `conflicting` on PG16) and halts the pipeline fail-closed — never silently
      dropping and recreating the slot (spec §8 R-ISO).

  The connect chain is a `handle_result/2` state machine:
  `:identity_check` → `:recovery_check` → `:publication_check` →
  `:invalidation_check` →
  (`:create_slot` if absent) → `:streaming`.

  ## Why a fail-closed halt and not soft pacing (Postgrex flow-control)

  `Postgrex.ReplicationConnection` re-arms the replication socket (`active: :once`)
  **automatically** after each `handle_data/2` batch — the handler's return value has
  no way to defer socket re-activation, so there is no lever to pause TCP reading
  from the handler. `:max_messages` bounds only Postgrex's per-batch socket buffer,
  not the downstream `AssemblerServer` mailbox. So true socket-level pacing is not
  available; the bounded in-flight window is enforced by the fail-closed halt at the
  ceiling — which bounds memory (the pipeline tears down before the mailbox grows
  unbounded) and surfaces the overload rather than silently livelocking.
  """
  use Postgrex.ReplicationConnection

  alias Replicant.{AssemblerServer, Decoder, QueryBuilder, Telemetry}
  alias Replicant.Decoder.Messages.{Begin, Commit, StreamAbort, StreamCommit, StreamStart}
  alias Replicant.Decoder.Messages.Relation
  alias Replicant.Decoder.Messages.Relation.Column
  alias Replicant.Decoder.OidDatabase
  alias Replicant.Decoder.Pglogical, as: PglogicalDecoder
  alias Replicant.Decoder.Plugin, as: DecoderPlugin
  alias Replicant.Decoder.Wal2json, as: Wal2jsonDecoder
  alias Replicant.Snapshotter.Incremental

  @pg_epoch DateTime.to_unix(~U[2000-01-01 00:00:00Z], :microsecond)

  # Default in-flight-lag ceiling in WAL bytes (spec §4 bounded in-flight window) —
  # the multi-transaction BACKLOG bound. `received_lsn` advances per XLogData frame
  # while `checkpoint_lsn` only advances at a Commit boundary, so the in-flight lag
  # transiently includes the WAL of the single transaction currently mid-stream. This
  # ceiling (64 MiB) is therefore sized to sit comfortably above any single
  # normal-but-large transaction (bulk insert/update, large/TOASTed rows) so it fires
  # ONLY when the sink is genuinely lagging and a real backlog of un-drained
  # transactions accumulates — never on one in-flight txn. Override per pipeline via
  # `Replicant.Config`'s `:max_inflight_lag`.
  #
  # PROTO-V1 LIMITATION: a single transaction whose own buffered WAL exceeds the bound
  # would still trip the halt mid-transaction (there is no Commit boundary to advance
  # the checkpoint until the whole txn arrives). Unbounded single-transaction size is
  # a future slice (proto-v2 streaming of in-progress transactions); for v1, size the
  # bound above the largest expected single transaction.
  @default_max_inflight_lag 67_108_864

  # A6 command-error watchdog default budget — sourced DIRECTLY from the store retry family's
  # `Replicant.CheckpointStore.default_max_retries/0` (=5), not a parallel literal, so the two
  # cannot drift (the codebase centralized that accessor to prevent exactly this; cross-vendor
  # GLM + per-task review both flagged the earlier hardcoded `5`). `Replicant.Config` is the
  # runtime source of truth (it validates `max_command_retries`, defaulting to that same store
  # accessor, and `init/1` reads it); this struct fallback only backs a directly-constructed
  # state (tests).
  #
  # The watchdog's mutable count and its budget live together in ONE struct field
  # (`command_error: %{count, max_retries}`) rather than two flat fields — the mod_state is a
  # hot-path struct already at the maximum 31 flat fields (`Credo.Check.Warning.StructFieldAmount`;
  # a 32nd field crosses the BEAM boundary where a struct's internal map representation changes).
  @default_max_command_retries Replicant.CheckpointStore.default_max_retries()

  @type step ::
          :disconnected
          | :identity_check
          | :recovery_check
          | :publication_check
          | :decoder_table_discovery
          | :decoder_table_info
          | :invalidation_check
          | :create_slot
          | :create_export_slot
          | :create_incremental_slot
          | :read_backfill_origin
          | :read_slot_origin
          | :snapshotting
          | :streaming

  @type t :: %__MODULE__{
          slot_name: String.t(),
          publication: [String.t()] | nil,
          sink: module(),
          go_forward_only: boolean(),
          snapshot: boolean() | keyword(),
          connection: keyword(),
          checkpoint_lsn: Replicant.lsn(),
          checkpoint_state: :present | :empty | :fault | :fault_permanent,
          received_lsn: Replicant.lsn(),
          stream_floor_lsn: Replicant.lsn() | nil,
          max_inflight_lag: pos_integer(),
          spilled_bytes: non_neg_integer(),
          max_spill_bytes: non_neg_integer() | nil,
          checkpoint_store: keyword() | nil,
          batch_delivery: keyword() | nil,
          failover: boolean(),
          server_version_num: non_neg_integer(),
          in_recovery: boolean(),
          streaming: keyword() | nil,
          in_stream: boolean(),
          in_txn: boolean(),
          open_streams: MapSet.t(),
          last_commit_lsn: Replicant.lsn(),
          store_retry_count: non_neg_integer(),
          command_error: %{
            count: non_neg_integer(),
            max_retries: non_neg_integer(),
            store_paced: boolean()
          },
          frontier: %{epoch: non_neg_integer(), last_cast: non_neg_integer()},
          backfill_floor: Replicant.lsn() | nil,
          reader_pid: pid() | nil,
          step: step(),
          messages: boolean(),
          # The decoder trio rides ONE map field (the 31-flat-field BEAM boundary the
          # struct comment above documents): which plugin decoder, its per-connection
          # cache, the configured table list (wal2json), and the discovered table set
          # (pglogical's sets → tables, read at connect).
          decoder: %{
            kind: Replicant.Decoder.Plugin.decoder_atom(),
            cache: Replicant.Decoder.Plugin.cache(),
            tables: [{String.t(), String.t()}] | nil,
            discovered: [{String.t(), String.t()}] | nil,
            replication_sets: [String.t()] | nil
          }
        }

  # The decoder group's defaults (one map field — see the 31-field comment above).
  @decoder_defaults %{
    kind: :pgoutput,
    cache: nil,
    tables: nil,
    discovered: nil,
    replication_sets: nil
  }

  defstruct [
    :slot_name,
    :publication,
    :sink,
    :go_forward_only,
    :snapshot,
    :connection,
    :stream_floor_lsn,
    :backfill_floor,
    checkpoint_lsn: 0,
    checkpoint_state: :empty,
    received_lsn: 0,
    max_inflight_lag: @default_max_inflight_lag,
    spilled_bytes: 0,
    max_spill_bytes: nil,
    checkpoint_store: nil,
    batch_delivery: nil,
    failover: false,
    server_version_num: 0,
    in_recovery: false,
    streaming: nil,
    in_stream: false,
    in_txn: false,
    open_streams: MapSet.new(),
    last_commit_lsn: 0,
    store_retry_count: 0,
    command_error: %{count: 0, max_retries: @default_max_command_retries, store_paced: false},
    frontier: %{epoch: 0, last_cast: 0},
    reader_pid: nil,
    step: :disconnected,
    messages: false,
    decoder: @decoder_defaults
  ]

  @doc "The default in-flight-lag ceiling (WAL bytes) when the config omits it."
  @spec default_max_inflight_lag() :: pos_integer()
  def default_max_inflight_lag, do: @default_max_inflight_lag

  @doc "Start the replication connection for a pipeline (called by `Replicant.Pipeline`)."
  @spec start_link(Replicant.Config.t()) :: {:ok, pid()} | {:error, term()}
  def start_link(config) do
    Postgrex.ReplicationConnection.start_link(__MODULE__, config, connection_opts(config))
  end

  @doc false
  @spec connection_opts(Replicant.Config.t()) :: keyword()
  def connection_opts(config) do
    # The library's control opts MUST win over any same-key opts in the caller's
    # :connection list: a caller `sync_connect: true` would break the non-blocking
    # facade, a `name` would break Registry wiring, `auto_reconnect: false` would
    # disable resilience. Keyword.merge/2 gives the SECOND list precedence and
    # dedupes — unlike `++`, where the caller's first-occurrence key would win.
    Keyword.merge(config.connection,
      name: via(config.slot_name),
      sync_connect: false,
      auto_reconnect: true
    )
  end

  @doc "The Registry via-name a pipeline's Connection registers under."
  @spec via(String.t()) :: {:via, module(), term()}
  def via(slot_name), do: {:via, Registry, {Replicant.Registry, {slot_name, :connection}}}

  # ---- Postgrex.ReplicationConnection callbacks ----

  @impl true
  def init(config) do
    {:ok,
     %__MODULE__{
       slot_name: config.slot_name,
       publication: config.publication,
       sink: config.sink,
       go_forward_only: config.go_forward_only,
       snapshot: Map.get(config, :snapshot, false),
       connection: config.connection,
       checkpoint_lsn: 0,
       checkpoint_state: :empty,
       received_lsn: 0,
       stream_floor_lsn: nil,
       max_inflight_lag: Map.get(config, :max_inflight_lag, @default_max_inflight_lag),
       spilled_bytes: 0,
       max_spill_bytes: spill_ceiling(Map.get(config, :streaming)),
       checkpoint_store: Map.get(config, :checkpoint_store),
       batch_delivery: Map.get(config, :batch_delivery),
       failover: Map.get(config, :failover, false),
       streaming: Map.get(config, :streaming),
       in_stream: false,
       store_retry_count: 0,
       command_error: %{
         count: 0,
         max_retries: Map.get(config, :max_command_retries, @default_max_command_retries),
         store_paced: false
       },
       step: :disconnected,
       messages: Map.get(config, :messages, false),
       decoder: %{
         kind: Map.get(config, :decoder, :pgoutput),
         cache: DecoderPlugin.module_for(Map.get(config, :decoder, :pgoutput)).init_cache([]),
         tables: Map.get(config, :tables),
         discovered: if(Map.get(config, :decoder) == :wal2json, do: Map.get(config, :tables)),
         replication_sets: Map.get(config, :replication_sets),
         allow_keyless: Map.get(config, :allow_keyless_tables, false),
         schema_check_interval: Map.get(config, :schema_check_interval)
       }
     }}
  end

  @impl true
  def handle_connect(state) do
    # A6 chain-progress watchdog — the FIRST action, BEFORE any store read. When the accumulated
    # command-error count is over budget, halt fail-closed and STAY IDLE (do NOT issue the recovery
    # query). `faults > 0` means the INITIAL connect never halts (even at `max_command_retries: 0`);
    # the budget is enforced by the shared `CheckpointStore.retry_decision/2` (faults - 1 = retries
    # already made), so command and store faults count identically. The halt returns before
    # `read_checkpoint/1` — no blocking store read on the halt path.
    %{count: faults, max_retries: max_retries} = state.command_error

    if faults > 0 and
         Replicant.CheckpointStore.retry_decision(faults - 1, max_retries) == :halt do
      halt_command_error(state)
    else
      handle_connect_proceed(state)
    end
  end

  # ADR-0009 §4 — the plugin decoders' connect-time probe over ONE short-lived REGULAR
  # connection: (a) it warms postgrex's shared type table, without which a replication
  # connection to a PG < 10 walsender fails inside Postgrex's own bootstrap (the
  # walsender there rejects all SQL — OBSERVED live); and (b) it pre-flights the
  # decoder's output-plugin option set via pg_logical_slot_peek_changes — a genuine
  # option rejection (22023, OBSERVED on the wal2json 2.4 build) halts
  # :decoder_option_unsupported instead of hanging the stream command (a rejected or
  # erroring peek on the WALSENDER never returns ReadyForQuery, so it cannot be
  # probed there). pgoutput keeps the untouched default path.
  defp plugin_connect_probe(%{decoder: %{kind: :pgoutput}} = state), do: {:ok, state}

  defp plugin_connect_probe(state) do
    case probe_connect(state) do
      {:ok, db} ->
        try do
          Postgrex.query!(db, "SELECT 1", [])
          probe_slot_options(db, state)
        rescue
          _ -> {:ok, state}
        catch
          _kind, _reason -> {:ok, state}
        after
          GenServer.stop(db)
        end

      # The probe connection itself failed (server momentarily down, connections
      # exhausted): a probe fault — fail open, the stream path owns real faults.
      {:error, _reason} ->
        {:ok, state}
    end
  end

  # The probe peeks the pipeline's OWN slot: an old build rejects an unknown option at
  # the plugin's startup callback (22023 — OBSERVED on the wal2json 2.4 build) no
  # matter the slot's data, and a slot created with a DIFFERENT build (the wal2json2_4
  # halt lever) carries that build's behavior — so the real slot is the honest target.
  # ANY result other than the explicit rejection proves nothing against the options;
  # the stream proceeds.
  defp probe_slot_options(db, state) do
    plugin = DecoderPlugin.module_for(decoder_kind(state))

    options =
      plugin.start_options(
        publications: state.publication || [],
        streaming: streaming?(state),
        messages: state.messages,
        replication_sets: decoder_sets(state) || [],
        tables: decoder_tables(state) || []
      )

    case QueryBuilder.decoder_option_probe(state.slot_name, probe_options(state, options)) do
      {:ok, probe_sql} ->
        if peek_throwaway(db, probe_sql) == :rejected,
          do: {:halt, :decoder_option_unsupported},
          else: {:ok, state}

      {:error, _} ->
        {:ok, state}
    end
  end

  # The probe's short-lived REGULAR connection. sync_connect: the probe queries
  # immediately — an async pool would race its own handshake and read as a fault.
  defp probe_connect(state) do
    Postgrex.start_link(
      Keyword.merge(state.connection,
        pool_size: 1,
        backoff_type: :stop,
        sync_connect: true
      )
    )
  end

  defp handle_connect_proceed(state) do
    case plugin_connect_probe(state) do
      {:ok, state} ->
        {:query, QueryBuilder.identify_system(), %{state | step: :identity_check}}

      {:halt, :decoder_option_unsupported} ->
        Telemetry.event([:replicant, :connection, :slot_invalidated], %{}, %{
          reason: :decoder_option_unsupported
        })

        Replicant.Supervisor.halt(state.slot_name, {:decoder, :option_unsupported})
        {:noreply, state}
    end
  end

  defp begin_recovery(state) do
    # Read the durable checkpoint (off the keepalive path — a blocking read is fine
    # here). In lib mode the store is the connect authority (a read FAULT halts
    # fail-closed in :invalidation_check — never fail-open, since a non-idempotent
    # lib-mode sink cannot dedup a resume-from-0). In sink-owned mode a read fault is
    # fail-open (§14.15): resume from 0, the idempotent sink dedups the re-stream.
    # This value seeds every keepalive ack until the first async advance.
    {checkpoint_state, checkpoint_lsn} = read_checkpoint(state)
    # Reset the store-retry counter on any SUCCESSFUL read (a separate later outage — the
    # transient-self-heal case — starts fresh at 0); keep it across a fault so the paced
    # retry accumulates toward the bound. Delegated to a @doc false helper (unit-tested).
    store_retry_count = reset_retry_count(state.store_retry_count, checkpoint_state)

    # Reset the in-flight window on (re)connect. `stream_floor_lsn` is re-derived from
    # the FIRST XLogData frame of the new stream (see `inflight_lag/1`) — PG clamps
    # `START_REPLICATION` to the slot's server-side `confirmed_flush_lsn`, which for a
    # fresh/empty checkpoint is the slot-creation LSN (a large absolute value), NOT 0.
    # Measuring lag against that per-stream floor (never against absolute 0) is what
    # keeps the very first frame from reading as a ~50 MB false "lag".
    {:query, QueryBuilder.recovery_and_version(),
     %{
       state
       | checkpoint_lsn: checkpoint_lsn,
         checkpoint_state: checkpoint_state,
         store_retry_count: store_retry_count,
         received_lsn: checkpoint_lsn,
         stream_floor_lsn: nil,
         in_txn: false,
         open_streams: MapSet.new(),
         last_commit_lsn: 0,
         spilled_bytes: 0,
         # The frontier epoch is KEPT across (re)connect (monotonic; only `start_streaming`
         # bumps it) so a fresh window always adopts a strictly-higher epoch than any
         # in-flight pre-reconnect frontier cast (85672f1 stale-epoch class). The per-stream
         # rate-limit anchor resets to 0 so the first frame of the new stream can cast.
         frontier: %{epoch: frontier_epoch(state), last_cast: 0},
         # `reader_pid` is KEPT across (re)connect (LOAD-BEARING): the prior incremental reader
         # is `spawn_link`ed to THIS Connection, which persists (handle_disconnect stays alive),
         # so its link never fires and it survives the reconnect. Carrying the pid here lets the
         # next `start_streaming_with_backfill` RETIRE it before respawning — exactly-one-reader
         # ([[replicant-otp-async-lifetime-hygiene]]). Explicit (not implicit) so a future merge
         # cannot silently clobber it back to nil and resurrect the double-reader bug.
         reader_pid: state.reader_pid,
         step: :recovery_check
     }}
  end

  @impl true
  def handle_disconnect(state) do
    Telemetry.event([:replicant, :connection, :disconnected], %{}, %{})

    # A6 watchdog increment: an ESTABLISHED-then-dropped connection is a command-fault candidate.
    # EXEMPT only the disconnect the store-retry timer itself paced, identified by the one-shot
    # `store_paced` marker set in `pace_store_retry`'s committed `{:noreply}` (the state postgrex
    # hands this callback). Consume it here so the NEXT disconnect counts. Keying on
    # `store_retry_count` instead is WRONG: that counter is stale-nonzero after a store episode
    # (its reset in `handle_connect` is discarded on a command-error `{:disconnect}` — the same
    # state-revert that broke the original seam), so a command error during/after a store episode
    # was wrongly exempted → livelock (both closeout lenses, cross-vendor-corroborated). A
    # server-down connection never establishes → this callback never fires → self-heal preserved.
    #
    # Accepted bounded imprecision (GLM lens 2026-07-14, non-defect): the marker is consumed by
    # ANY disconnect, so a genuine socket drop landing inside a store episode's backoff window is
    # exempted (under-counted by one) instead of counted. This CANNOT livelock or false-halt — an
    # active store episode independently halts via `store_retry_step`, and once the store recovers
    # no pace re-arms the marker so later drops count normally. Distinguishing the two precisely
    # would need the disconnect reason threaded through state, which postgrex discards on
    # `{:disconnect}` — the marker is the best available discriminator; the residual is off-by-one
    # in the conservative direction, in a rare concurrent-outage corner.
    ce = state.command_error

    command_error =
      if ce.store_paced,
        do: %{ce | store_paced: false},
        else: %{ce | count: ce.count + 1}

    {:noreply, %{state | step: :disconnected, command_error: command_error}}
  end

  @impl true
  def handle_result([%Postgrex.Result{} = result], %{step: :identity_check} = state) do
    with {:ok, identity} <- Replicant.SessionIdentity.from_result(result),
         :ok <-
           Replicant.Sink.accept_session_identity(state.sink, identity, %{
             slot_name: state.slot_name,
             publication: state.publication
           }) do
      begin_recovery(state)
    else
      {:error, _reason} -> halt_session_identity(state)
    end
  end

  # ADR-0009 §9 — a walsender on PG < 10 rejects ALL SQL (`SELECT 1` included; arbitrary
  # queries on replication connections arrived in PG 10 — OBSERVED on the live 9.6
  # substrate), so the whole catalog phase runs on a short-lived REGULAR connection and
  # the chain rejoins at the slot decision. The same QueryBuilder SQL executes either way.
  def handle_result(
        %Postgrex.Error{postgres: %{code: _code}},
        %{step: :recovery_check} = state
      ) do
    case probe_pre10_catalog(state) do
      {:ok, %{version: version, in_recovery: in_recovery} = probe} ->
        state = %{state | server_version_num: version, in_recovery: in_recovery}

        Telemetry.event([:replicant, :connection, :connected], %{}, %{
          kind: recovery_kind(in_recovery)
        })

        pre10_gate(state, probe)

      {:error, _reason} ->
        {:disconnect, :query_error}
    end
  end

  def handle_result(
        [%Postgrex.Result{rows: [[in_recovery, version]]}],
        %{step: :recovery_check} = state
      ) do
    # Replication simple-query results arrive as TEXT (bool → "t"/"f", int → "160014"), so
    # coerce at this boundary: a binary version compares > any integer in Elixir term ordering,
    # which would force the PG17 4-col invalidation query on PG16 (undefined_column → reconnect
    # storm). repl_int/repl_bool also accept already-typed values (unit-test inputs).
    version = repl_int(version)
    in_recovery = repl_bool(in_recovery)

    Telemetry.event([:replicant, :connection, :connected], %{}, %{
      kind: recovery_kind(in_recovery)
    })

    state = %{state | server_version_num: version, in_recovery: in_recovery}

    cond do
      # ADR-0009 §9 — pgoutput and publications exist from PG 10; on an older server the
      # pgoutput decoder is refused BEFORE slot creation (a permanent fault: the version
      # never changes across a reconnect — same terminal discipline as failover-unsupported).
      decoder_kind(state) == :pgoutput and version < 100_000 ->
        halt_decoder_unsupported_on_server(state)

      state.failover and version < 170_000 ->
        halt_failover_unsupported(state)

      true ->
        # A3 existence gate (decision #18): START_REPLICATION with a missing publication silently
        # streams the EXISTING subset (whole-publication data loss), so the existence check MUST
        # run BEFORE the slot-keyed query. The names are already in the SQL string (simple-query
        # protocol cannot bind `$1`); a mismatch halts fail-closed in :publication_check.
        # The table-set existence query is per decoder (ADR-0009 §2): pg_publication for
        # pgoutput, pglogical.replication_set for pglogical; the wal2json decoder's
        # configured tables are existence-checked by the relation-info read instead
        # (it has no server-side named set).
        case decoder_table_set_check(state) do
          {:query, sql} ->
            {:query, sql, %{state | step: :publication_check}}

          :skip ->
            request_decoder_table_info(state)
        end
    end
  end

  # The A3 fail-closed gate (decoder-aware, ADR-0009 §2): `found` is the set of names the
  # server reports for the validated `IN (...)` list — pubnames for pgoutput, set names
  # for pglogical; `requested` is the configured set. They MUST be equal — a strict subset
  # means a publication/replication set is absent (silent data loss if we proceeded to
  # START_REPLICATION), and a strict superset (extra rows) is a defensive never-proceed.
  # Either inequality is a PERMANENT config fault → halt and STAY IDLE (mirrors
  # halt_failover_unsupported: a disconnect would let auto_reconnect spin the connect
  # chain on the unchangeable fault until the async teardown lands). Telemetry is
  # VALUE-FREE (Rule 1): the reason carries no name.

  def handle_result([%Postgrex.Result{rows: rows}], %{step: :publication_check} = state) do
    found = MapSet.new(rows, fn [name] -> name end)
    requested = MapSet.new(decoder_table_set_names(state))

    if MapSet.equal?(found, requested) do
      case decoder_kind(state) do
        :pgoutput ->
          {:ok, sql} =
            QueryBuilder.slot_invalidation_status(state.slot_name, state.server_version_num)

          {:query, sql, %{state | step: :invalidation_check}}

        :pglogical ->
          # The sets exist; derive their member tables next (pglogical's own catalog),
          # then read the relation info for those tables (ADR-0009 §5).
          {:ok, sql} = QueryBuilder.replication_set_tables(decoder_sets(state))
          {:query, sql, %{state | step: :decoder_table_discovery}}
      end
    else
      halt_publication_missing(state)
    end
  end

  # pglogical set-membership rows (`[schemaname, tablename, qualified]`) → the relation-info
  # read for exactly those tables. A set with no tables mirrors an empty publication: the
  # stream simply idles (no info read, an empty decoder cache).
  def handle_result([%Postgrex.Result{rows: rows}], %{step: :decoder_table_discovery} = state) do
    tables = Enum.map(rows, fn [schemaname, tablename, _qualified] -> {schemaname, tablename} end)
    request_decoder_table_info(set_decoder_tables(state, tables))
  end

  # The relation-info rows (`[schemaname, tablename, oid, relreplident, attnum, attname,
  # atttypid, atttypmod]`): everything the plugin's wire format cannot express. A
  # configured/derived table with NO rows is ABSENT on the server — a fail-closed halt
  # (streaming would silently miss it), value-free.
  def handle_result([%Postgrex.Result{rows: rows}], %{step: :decoder_table_info} = state) do
    tables = decoder_discovered(state) || decoder_tables(state)

    present =
      rows |> Enum.map(fn [s, t | _rest] -> {s, t} end) |> MapSet.new()

    if Enum.all?(tables, &MapSet.member?(present, &1)) do
      state = set_decoder_cache(state, init_decoder_cache(state, group_relation_info(rows)))

      case enforce_keyless_tables(state) do
        {:ok, state} ->
          {:ok, sql} =
            QueryBuilder.slot_invalidation_status(state.slot_name, state.server_version_num)

          {:query, sql, %{state | step: :invalidation_check}}

        {:halted, state} ->
          {:noreply, state}
      end
    else
      Telemetry.event([:replicant, :connection, :slot_invalidated], %{}, %{
        reason: :decoder_table_missing
      })

      Replicant.Supervisor.halt(state.slot_name, {:decoder, :table_missing})
      {:noreply, state}
    end
  end

  def handle_result([%Postgrex.Result{rows: rows}], %{step: :invalidation_check} = state) do
    invalidation_check_flow(rows, state)
  end

  # A fresh go-forward slot was created (R04): its CREATE result row is
  # `[slot, consistent_point, snapshot_name(nil for NOEXPORT), plugin]`. Expose the typed
  # consistent_point to a go-forward append consumer (`handle_slot_origin/2`, reused?: false)
  # before streaming; a veto/raise halts fail-closed. A sink without the callback is byte-identical
  # (notify is a no-op :ok → start_streaming), matching the prior struct-only match.
  def handle_result([%Postgrex.Result{} = result], %{step: :create_slot} = state) do
    Telemetry.event([:replicant, :connection, :slot_active], %{}, %{})

    case notify_new_slot_origin(state, result) do
      :ok -> start_streaming(state)
      {:error, reason} -> halt_slot_origin(state, reason)
    end
  end

  # The EXPORT_SNAPSHOT slot was created (spec §4): capture its consistent point and
  # exported snapshot name, spawn+LINK the Snapshotter to read the base snapshot on a
  # separate connection, and idle in :snapshotting to hold the exported snapshot valid.
  # The link binds the snapshotter to this Connection: a pipeline teardown mid-snapshot
  # tears the snapshotter down with it (no orphan). The handoff LSN arrives on
  # `{:snapshot_done, lsn}` and is where streaming resumes once the snapshot lands.
  def handle_result(
        [%Postgrex.Result{rows: [[_slot, consistent_point, snapshot_name, _plugin]]}],
        %{step: :create_export_slot} = state
      ) do
    {:ok, cp} = Replicant.lsn_from_string(consistent_point)

    Replicant.Snapshotter.start(%{
      snapshot_name: snapshot_name,
      consistent_point: cp,
      connection: state.connection,
      publication: state.publication,
      tables: decoder_discovered(state),
      sink: state.sink,
      mode: if(lib_mode?(state), do: :lib, else: :sink_owned),
      reply_to: self()
    })

    {:noreply, %{state | step: :snapshotting}}
  end

  # A FRESH incremental slot was created (NOEXPORT_SNAPSHOT): its consistent_point is the
  # backfill floor (the LSN at/after which the stream carries every change; the reader
  # snapshots the base rows as-of this point). `snapshot_name` is nil for NOEXPORT. Seed the
  # floor, spawn the reader, and stream from the checkpoint (0 on a fresh slot). Same 4-col
  # result-row shape as the EXPORT_SNAPSHOT create (probe §11.1).
  def handle_result(
        [%Postgrex.Result{rows: [[_slot, consistent_point, _snap_name, _plugin]]}],
        %{step: :create_incremental_slot} = state
      ) do
    {:ok, floor} = Replicant.lsn_from_string(consistent_point)

    case persist_backfill_pending(state) do
      :ok ->
        Telemetry.event([:replicant, :connection, :slot_active], %{}, %{})
        Telemetry.event([:replicant, :snapshot, :started], %{}, %{commit_lsn: floor})
        state = %{state | backfill_floor: floor}
        start_streaming_with_backfill(state, nil)

      {:error, _reason} ->
        halt_snapshot(state, :snapshot_progress_invalid)
    end
  end

  def handle_result(
        [%Postgrex.Result{rows: rows}],
        %{step: :read_backfill_origin} = state
      ) do
    case parse_confirmed_flush(rows) do
      {:ok, confirmed_flush} ->
        floor = max(state.checkpoint_lsn, confirmed_flush)
        Telemetry.event([:replicant, :snapshot, :resumed], %{}, %{slot_name: state.slot_name})
        start_streaming_with_backfill(%{state | backfill_floor: floor}, nil)

      {:error, _reason} ->
        halt_snapshot(state, :snapshot_origin_unavailable)
    end
  end

  # A reused (present) logical slot's origin read (R04): PostgreSQL starts at the greater of the
  # requested checkpoint and the slot's confirmed_flush_lsn. Expose that effective origin
  # (`handle_slot_origin/2`, reused?: true) before streaming. Missing/NULL/malformed slot state and
  # callback vetoes halt fail-closed. Only reached when the sink implements the callback.
  def handle_result([%Postgrex.Result{rows: rows}], %{step: :read_slot_origin} = state) do
    with {:ok, confirmed_flush} <- parse_confirmed_flush(rows),
         origin = max(state.checkpoint_lsn, confirmed_flush),
         :ok <-
           Replicant.Sink.notify_slot_origin(state.sink, origin, %{
             slot_name: state.slot_name,
             reused?: true
           }) do
      start_streaming(state)
    else
      {:error, reason} -> halt_slot_origin(state, reason)
    end
  end

  # A missing extension/catalog at the table-set gate is a PERMANENT config fault
  # (e.g. the pglogical extension is not installed — the set-exists query 42P01s on
  # pglogical.replication_set): halt immediately with a truthful value-free reason
  # instead of reconnect-looping to the command-error watchdog's misleading terminal.
  def handle_result(
        %Postgrex.Error{postgres: %{code: :undefined_table}},
        %{step: :publication_check} = state
      ) do
    Telemetry.event([:replicant, :connection, :slot_invalidated], %{}, %{
      reason: :decoder_extension_missing
    })

    Replicant.Supervisor.halt(state.slot_name, {:decoder, :extension_missing})
    {:noreply, state}
  end

  def handle_result(%Postgrex.Error{}, _state) do
    # A replication-command error (value-bearing message never inspected). Disconnect;
    # auto_reconnect re-runs the connect chain.
    {:disconnect, :query_error}
  end

  def handle_result(_result, _state), do: {:disconnect, :unexpected_result}

  # The per-decoder table-set existence SQL (the :publication_check step that follows).
  # Every name was Identifier-validated at Config, so the builders never error here
  # (their `{:error, _}` arms are unreachable defense — same discipline as the prior
  # inline `{:ok, sql} =` match).
  defp decoder_table_set_check(%{decoder: %{kind: :pgoutput}, publication: pubs}) do
    {:ok, sql} = QueryBuilder.publication_exists(pubs)
    {:query, sql}
  end

  defp decoder_table_set_check(%{decoder: %{kind: :pglogical}} = state) do
    sets = decoder_sets(state)
    {:ok, sql} = QueryBuilder.replication_set_exists(sets)
    {:query, sql}
  end

  defp decoder_table_set_check(%{decoder: %{kind: :wal2json}}), do: :skip

  defp decoder_table_set_names(%{decoder: %{kind: :pgoutput}, publication: pubs}), do: pubs
  defp decoder_table_set_names(%{decoder: %{kind: :pglogical}} = state), do: decoder_sets(state)

  # Issue the relation-info catalog read (ADR-0009 §5) for the decoder's tables — wal2json's
  # configured list, or the tables pglogical's sets carry (in `discovered`). An empty
  # effective list skips the read (nothing to type; the stream idles) — a fresh pglogical
  # node's sets are empty until replication_set_add_table, the default first-run state.
  defp request_decoder_table_info(state) do
    case decoder_discovered(state) || decoder_tables(state) do
      [] ->
        state = set_decoder_cache(state, init_decoder_cache(state, %{}))

        {:ok, sql} =
          QueryBuilder.slot_invalidation_status(state.slot_name, state.server_version_num)

        {:query, sql, %{state | step: :invalidation_check}}

      tables ->
        {:ok, sql} = QueryBuilder.configured_table_info(tables)
        {:query, sql, %{state | step: :decoder_table_info}}
    end
  end

  # The option-probe peek: only an explicit option-rejection code reads :rejected —
  # success, pglogical's "produces binary output" (XX000, AFTER options parsed), or
  # any fault proves nothing against the options and lets the stream proceed.
  defp peek_throwaway(db, probe_sql) do
    case Postgrex.query(db, probe_sql, []) do
      {:ok, _} ->
        :ok

      # 22023 alone: the plugin's startup callback refused an option (OBSERVED on the
      # wal2json 2.4 build). 0A000 must NOT read as rejection — the SQL peek cannot
      # carry a BINARY-output plugin's changes, and when the slot has pending WAL the
      # server refuses with exactly feature_not_supported ("produces binary output…
      # expects textual data", OBSERVED on the live 12: it falsely halted every
      # pglogical resume with pending WAL). The pglogical probe runs proto_format
      # 'json' (see probe_options/2) so its peek is textual and can never trip it.
      {:error, %Postgrex.Error{postgres: %{code: :invalid_parameter_value}}} ->
        :rejected

      {:error, _other} ->
        :ok
    end
  end

  @doc false
  # The probe's option list per decoder. pglogical probes with `proto_format 'json'`
  # instead of the stream's `native`: the probe's job is to prove the installed build
  # PARSES our option names/values (startup_params_format, min/max_proto_version,
  # pglogical.replication_set_names — all processed identically in either format),
  # while json output is TEXTUAL, so the SQL peek never hits the binary-output
  # feature_not_supported refusal that a native peek on a slot with pending WAL
  # always raises (OBSERVED live — it falsely halted every pglogical resume).
  # wal2json probes with its exact stream options: its output is textual and a
  # genuinely old build rejects the unknown option at startup (22023, OBSERVED).
  @spec probe_options(map(), [{String.t(), String.t()}]) :: [{String.t(), String.t()}]
  def probe_options(%{decoder: %{kind: :pglogical}}, options) do
    Enum.map(options, fn
      {"proto_format", "native"} -> {"proto_format", "json"}
      other -> other
    end)
  end

  def probe_options(_state, options), do: options

  defp frontier_epoch(%{frontier: f}), do: f.epoch
  defp last_frontier_cast(%{frontier: f}), do: f.last_cast

  defp set_last_frontier_cast(%{frontier: f} = state, wal_end),
    do: %{state | frontier: %{f | last_cast: wal_end}}

  # Struct-field accessors (one stable map shape for the type checker).
  defp decoder_kind(%{decoder: d}), do: d.kind
  defp decoder_cache(%{decoder: d}), do: d.cache
  defp decoder_tables(%{decoder: d}), do: d.tables
  defp decoder_discovered(%{decoder: d}), do: d.discovered

  defp decoder_sets(%{decoder: d}), do: d.replication_sets

  # Struct-field helpers for the decoder map (keeps the flat-field budget and the
  # type checker happy).
  defp set_decoder_cache(%{decoder: d} = state, cache),
    do: %{state | decoder: %{d | cache: cache}}

  defp set_decoder_tables(%{decoder: d} = state, tables),
    do: %{state | decoder: %{d | discovered: tables}}

  # The :invalidation_check decision, shared by the walsender chain and the PG < 10
  # probe path (ADR-0009 §9).
  defp invalidation_check_flow(rows, state) do
    cond do
      lib_mode?(state) and state.checkpoint_state == :fault_permanent ->
        halt_store_permanent(state)

      lib_mode?(state) and state.checkpoint_state == :fault ->
        pace_store_retry(state)

      lib_mode?(state) and lib_go_forward_violation?(state) ->
        Replicant.Supervisor.halt(state.slot_name, {:config, :go_forward_required})
        {:disconnect, :go_forward_required}

      true ->
        classify_and_begin(Enum.map(rows, &coerce_status_row/1), state)
    end
  end

  # The PG < 10 catalog phase, run on a short-lived REGULAR connection (a walsender
  # there accepts no SQL at all): the SAME QueryBuilder strings the walsender chain
  # uses, executed synchronously in the connection process — the blocking-read
  # precedent of read_checkpoint/1; a fault disconnects and auto_reconnect re-runs.
  defp probe_pre10_catalog(state) do
    case probe_connect(state) do
      {:ok, db} ->
        pre10_probe_body(db, state)

      # The probe connection itself failed: a probe fault → the caller disconnects and
      # auto_reconnect re-runs the chain (the documented fault path).
      {:error, _reason} ->
        {:error, :probe_failed}
    end
  end

  defp pre10_probe_body(db, state) do
    [[in_recovery, version]] = Postgrex.query!(db, QueryBuilder.recovery_and_version(), []).rows

    probe = %{
      version: repl_int(version),
      in_recovery: repl_bool(in_recovery),
      exists_ok: true,
      table_rows: [],
      info_rows: [],
      invalidation_rows: []
    }

    # pgoutput on a pre-10 server halts in the caller's gate (no pgoutput, no
    # publications) — do not run the PG10+ catalog query that would throw here.
    probe =
      if decoder_kind(state) == :pgoutput and probe.version < 100_000,
        do: probe,
        else: pre10_table_set_probe(db, state, probe)

    probe =
      case probe.table_rows do
        [] ->
          probe

        tables ->
          {:ok, info_sql} = QueryBuilder.configured_table_info(tables)
          %{probe | info_rows: Postgrex.query!(db, info_sql, []).rows}
      end

    {:ok, invalidation_sql} =
      QueryBuilder.slot_invalidation_status(state.slot_name, probe.version)

    invalidation_rows = Postgrex.query!(db, invalidation_sql, []).rows

    {:ok, %{probe | invalidation_rows: invalidation_rows}}
  catch
    # A probe fault is value-free (the query strings carry only validated identifiers)
    _kind, _reason -> {:error, :probe_failed}
  after
    GenServer.stop(db)
  end

  # The per-decoder table-set leg of the pre-10 probe.
  defp pre10_table_set_probe(db, state, probe) do
    case decoder_kind(state) do
      :pgoutput ->
        found =
          Postgrex.query!(db, elem(QueryBuilder.publication_exists(state.publication), 1), []).rows
          |> MapSet.new(fn [name] -> name end)

        %{probe | exists_ok: MapSet.equal?(found, MapSet.new(state.publication))}

      :pglogical ->
        found =
          Postgrex.query!(
            db,
            elem(QueryBuilder.replication_set_exists(decoder_sets(state)), 1),
            []
          ).rows
          |> MapSet.new(fn [name] -> name end)

        tables =
          Postgrex.query!(
            db,
            elem(QueryBuilder.replication_set_tables(decoder_sets(state)), 1),
            []
          ).rows
          |> Enum.map(fn [s, t, _q] -> {s, t} end)

        %{
          probe
          | exists_ok: MapSet.equal?(found, MapSet.new(decoder_sets(state))),
            table_rows: tables
        }

      :wal2json ->
        %{probe | table_rows: decoder_tables(state) || []}
    end
  end

  # Fold the probe's discovered tables + relation info into the state the walsender
  # chain would have built (mirrors the :decoder_table_discovery/:decoder_table_info
  # handlers, absent-table halt included — and STAYING IDLE after the halt, the
  # walsender twin's terminal discipline).
  defp seed_probe_state(state, probe) do
    tables = probe.table_rows
    present = probe.info_rows |> Enum.map(fn [s, t | _rest] -> {s, t} end) |> MapSet.new()

    if Enum.all?(tables, &MapSet.member?(present, &1)) do
      state = set_decoder_tables(state, tables)

      enforce_keyless_tables(
        set_decoder_cache(state, init_decoder_cache(state, group_relation_info(probe.info_rows)))
      )
    else
      Telemetry.event([:replicant, :connection, :slot_invalidated], %{}, %{
        reason: :decoder_table_missing
      })

      Replicant.Supervisor.halt(state.slot_name, {:decoder, :table_missing})
      {:halted, state}
    end
  end

  # The wal2json keyless pre-flight predicate (ADR-0009 divergence resolution):
  # exactly the tables wal2json.c's UPDATE/DELETE guard would skip — no
  # replica-identity-index column AND identity ≠ FULL (REPLICA IDENTITY NOTHING, or
  # default/index identity whose index does not exist). Such a table's U/D never
  # reaches the wire (a server WARNING only), so a configured one halts at start
  # unless the operator opted into insert-only semantics (`allow_keyless_tables: true`).
  @doc false
  @spec keyless_tables(map(), [{String.t(), String.t()}]) :: [{String.t(), String.t()}]
  def keyless_tables(cache, tables) do
    relations = Map.get(cache, :relations, %{})

    Enum.filter(tables, fn key ->
      case Map.get(relations, key) do
        %Relation{replica_identity: :all_columns} -> false
        %Relation{} = rel -> not Enum.any?(rel.columns, &(:key in (&1.flags || [])))
        nil -> false
      end
    end)
  end

  # The shared enforcement: after a wal2json cache is seeded (either catalog path),
  # halt fail-closed on a keyless configured table and STAY IDLE (the same discipline
  # as :decoder_table_missing). pgoutput/pglogical never route here — their
  # keylessness enforcement is the server's / the runtime decode halt's.
  defp enforce_keyless_tables(%{decoder: %{kind: :wal2json, allow_keyless: false}} = state) do
    cache = decoder_cache(state)
    tables = decoder_discovered(state) || decoder_tables(state) || []

    if keyless_tables(cache, tables) == [] do
      {:ok, state}
    else
      Telemetry.event([:replicant, :connection, :slot_invalidated], %{}, %{
        reason: :decoder_table_keyless
      })

      Replicant.Supervisor.halt(state.slot_name, {:decoder, :table_keyless})
      {:halted, state}
    end
  end

  defp enforce_keyless_tables(state), do: {:ok, state}

  # Group the flat relation-info rows into the per-table maps the decoders' caches carry.
  # `relreplident` is the pg_class char ("d"/"n"/"f"/"i") — mapped to the SAME atom the
  # pgoutput Relation decoder produces; type names resolve through the SAME OidDatabase
  # call the pgoutput decoder uses, so casting is identical across decoders.
  defp group_relation_info(rows) do
    Enum.reduce(
      rows,
      %{},
      fn [s, t, oid, replident, _attnum, attname, atttypid, atttypmod, identity_key], acc ->
        key = {s, t}

        entry =
          Map.get(acc, key, %{
            oid: repl_int(oid),
            replident: replident_atom(replident),
            columns: []
          })

        entry = %{
          entry
          | columns: [
              %{
                name: attname,
                type_oid: repl_int(atttypid),
                typmod: repl_int(atttypmod),
                identity_key: identity_key in [true, "t"]
              }
              | entry.columns
            ]
        }

        Map.put(acc, key, entry)
      end
    )
    |> Enum.map(fn {key, entry} -> {key, %{entry | columns: Enum.reverse(entry.columns)}} end)
    |> Map.new()
  end

  defp replident_atom("d"), do: :default
  defp replident_atom("n"), do: :nothing
  defp replident_atom("f"), do: :all_columns
  defp replident_atom("i"), do: :index
  defp replident_atom(_other), do: nil

  # Seed the configured decoder's cache from the connect-time relation info (ADR-0009 §5):
  # pglogical gets column types + replica identities (its wire metadata has neither);
  # wal2json additionally gets PRE-SEEDED relations (full column sets incl. typmods, the
  # pg_class oid as the stable relation id, and the replica identity for old-tuple
  # classification) — so even the first change for a table resolves against exact
  # metadata, and an unchanged-TOAST column missing from the FIRST change is still
  # recognized. pgoutput ignores the read entirely.
  defp init_decoder_cache(%{decoder: %{kind: :pglogical}} = _state, info) do
    column_types =
      Map.new(info, fn {key, entry} ->
        {key,
         Map.new(entry.columns, fn col ->
           {col.name, {OidDatabase.name_for_type_id(col.type_oid), col.typmod}}
         end)}
      end)

    replica_identity = Map.new(info, fn {key, entry} -> {key, entry.replident} end)

    PglogicalDecoder.init_cache(
      column_types: column_types,
      replica_identity: replica_identity
    )
  end

  defp init_decoder_cache(%{decoder: %{kind: :wal2json}} = _state, info) do
    replica_identity = Map.new(info, fn {key, entry} -> {key, entry.replident} end)
    relids = Map.new(info, fn {key, entry} -> {key, entry.oid} end)

    relations =
      Map.new(info, fn {key, entry} ->
        columns =
          Enum.map(entry.columns, fn col ->
            %Column{
              name: col.name,
              flags: if(col.identity_key, do: [:key], else: []),
              type: OidDatabase.name_for_type_id(col.type_oid),
              type_modifier: col.typmod
            }
          end)

        relation = %Relation{
          id: entry.oid,
          namespace: elem(key, 0),
          name: elem(key, 1),
          replica_identity: entry.replident,
          columns: columns
        }

        {key, relation}
      end)

    Wal2jsonDecoder.init_cache(
      relations: relations,
      replica_identity: replica_identity,
      relids: relids
    )
  end

  defp init_decoder_cache(%{decoder: %{kind: :pgoutput}}, _info), do: nil

  # The PG < 10 probe's fail-closed gates, in the same order the walsender chain
  # checks them: the pgoutput-before-10 refusal, failover-unsupported, the missing
  # set/publication (the A3 silent-intersection loss), then the table seeding.
  defp pre10_gate(state, %{version: version} = probe) do
    cond do
      decoder_kind(state) == :pgoutput and version < 100_000 ->
        halt_decoder_unsupported_on_server(state)

      state.failover and version < 170_000 ->
        halt_failover_unsupported(state)

      not probe.exists_ok ->
        halt_publication_missing(state)

      true ->
        case seed_probe_state(state, probe) do
          {:ok, state} -> invalidation_check_flow(probe.invalidation_rows, state)
          {:halted, state} -> {:noreply, state}
        end
    end
  end

  @impl true
  # XLogData: advance the in-flight high-water to this frame's `wal_end`, then decode
  # behind the value-free boundary and forward the decoded message + raw byte-size to
  # the AssemblerServer. A decode error halts fail-closed. When the in-flight WAL lag
  # (`inflight_lag/1`, received frontier minus the confirmed-durable floor) exceeds
  # the bound, the sink cannot keep up: halt fail-closed (§4) before the mailbox grows
  # unbounded — a single non-blocking integer comparison, so keepalives are never
  # starved.
  def handle_data(<<?w, _wal_start::64, wal_end::64, _clock::64, payload::binary>>, state) do
    # Capture the per-stream floor from the FIRST frame — the position PG actually
    # began streaming at (its clamped `confirmed_flush_lsn`), so lag is measured
    # relative to the stream, never absolute 0.
    stream_floor_lsn = state.stream_floor_lsn || wal_end

    # On the FIRST frame of a (re)connected stream, report the floor to the AssemblerServer — it is
    # the cold-start component of the batch span-cap base `max(lib_checkpoint, stream_floor)` (§7),
    # so a large-absolute first-txn LSN on a fresh slot (lib_checkpoint 0) does not spuriously flush.
    if is_nil(state.stream_floor_lsn) and batches?(state) do
      GenServer.cast(AssemblerServer.via(state.slot_name), {:stream_floor, stream_floor_lsn})
    end

    received_lsn = max(state.received_lsn, wal_end)

    # A6 watchdog reset: this XLogData frame proves START_REPLICATION was accepted, so the
    # pre-frame command-error budget is spent. Only rebuild the inner map when the counter is
    # non-zero (keep the steady-state hot path allocation-free); the reset itself rides the
    # frame's existing state rebuild.
    command_error =
      if state.command_error.count > 0,
        do: %{state.command_error | count: 0},
        else: state.command_error

    state = %{
      state
      | received_lsn: received_lsn,
        stream_floor_lsn: stream_floor_lsn,
        command_error: command_error
    }

    # Whenever the pipeline is in incremental MODE (incremental?/1 tests is_list(snapshot) and
    # stays true for the whole pipeline lifetime, not only during an active backfill), forward
    # the XLogData frontier so a pending chunk window can close on a busy-but-publication-filtered
    # source (spec §2; plan F4: such a source never applies commits, so keepalive-only forwarding
    # would stall closure to the ~30 s cadence). Post-completion the cast is a cheap no-op (the
    # assembler's pop_ready returns :none). Rate-limited to ≥ 1 MiB of advance so the hot path
    # stays cheap. Epoch-tagged with the CURRENT window epoch so a stale pre-reconnect cast can
    # never close a fresh window (85672f1 class).
    state =
      if incremental?(state) and wal_end - last_frontier_cast(state) >= 1_048_576 do
        GenServer.cast(
          AssemblerServer.via(state.slot_name),
          {:snapshot_frontier, frontier_epoch(state), wal_end}
        )

        set_last_frontier_cast(state, wal_end)
      else
        state
      end

    lag = inflight_lag(state)

    if lag > effective_lag_bound(state) do
      halt_sink_too_slow(state, lag)
    else
      forward_message(payload, state)
    end
  end

  # Primary keepalive (spec A1 §3.1): forward the frontier in incremental mode, then dispatch to
  # keepalive_ack/3 — idle-advance a state mirror to wal_end, while an append log and every
  # non-idle sink reply only with the durable checkpoint (never the received wal_end).
  def handle_data(<<?k, wal_end::64, _clock::64, reply::8>>, state) do
    # Whenever the pipeline is in incremental MODE (incremental?/1 stays true for the whole
    # pipeline lifetime, not only during an active backfill), forward the keepalive frontier so
    # a pending chunk window can close on an idle/trickle source (spec §2 D3). Cheap: one cast
    # per keepalive (~seconds apart), so unconditional; post-completion it is a no-op in the
    # assembler (pop_ready returns :none). Epoch-tagged with the CURRENT window epoch so a stale
    # pre-reconnect cast can never close a fresh window (85672f1 class).
    if incremental?(state) do
      GenServer.cast(
        AssemblerServer.via(state.slot_name),
        {:snapshot_frontier, frontier_epoch(state), wal_end}
      )
    end

    # A6 watchdog reset: a primary keepalive proves the walsender accepted START_REPLICATION and
    # is live, so an idle-but-healthy publication that reconnects on a blip still clears its
    # pre-frame command-error budget. Cheap: only rebuild when the counter is non-zero (the
    # steady-state keepalive leaves it untouched).
    state =
      if state.command_error.count > 0,
        do: %{state | command_error: %{state.command_error | count: 0}},
        else: state

    keepalive_ack(wal_end, reply, state)
  end

  def handle_data(_other, state), do: {:noreply, state}

  @impl true
  # Async ack: the AssemblerServer durably committed a txn ending at `lsn`.
  # Advance monotonically and report the new flush position. The in-memory advance
  # and the telemetry are UNCONDITIONAL; the standby-status-update buffer is emitted
  # only while streaming — a sink_committed racing a reconnect lands in a simple-query
  # phase, where handing Postgrex a raw buffer is unspecified behavior (1.3.0). The
  # next keepalive in :streaming reports the advanced position.
  def handle_info({:sink_committed, lsn}, state) when is_integer(lsn) do
    checkpoint = max(state.checkpoint_lsn, lsn)
    Telemetry.event([:replicant, :checkpoint, :advanced], %{}, %{commit_lsn: checkpoint})
    state = %{state | checkpoint_lsn: checkpoint}

    if state.step == :streaming,
      do: {:noreply, [encode_status_update(checkpoint)], state},
      else: {:noreply, [], state}
  end

  # Snapshot finished durably (checkpoint := consistent_point): seed the checkpoint and
  # stream from the handoff LSN. The snapshotter's own :normal exit does not propagate
  # over the link, so this graceful message still drives the handoff. In lib mode the
  # sink does NOT own the checkpoint, so the handoff LSN is written to the store HERE,
  # ordered before streaming — a write fault halts fail-closed (the whole snapshot
  # re-runs, since the store checkpoint stays nil until the handoff lands).
  def handle_info({:snapshot_done, lsn}, %{step: :snapshotting} = state) when is_integer(lsn) do
    case write_snapshot_handoff(state, lsn) do
      :ok ->
        Telemetry.event([:replicant, :connection, :slot_active], %{}, %{})
        prime_assembler(state, lsn)

        {:ok, sql} = start_replication_sql(state, lsn)

        {:stream, sql, [],
         %{
           arm_schema_guard(state)
           | step: :streaming,
             checkpoint_lsn: lsn,
             received_lsn: lsn,
             stream_floor_lsn: nil,
             in_stream: false,
             in_txn: false,
             open_streams: MapSet.new(),
             last_commit_lsn: 0
         }}

      {:error, _} ->
        Telemetry.event([:replicant, :checkpoint_store, :failed], %{}, %{
          slot_name: state.slot_name,
          reason: :checkpoint_store_failed
        })

        Replicant.Supervisor.halt(state.slot_name, {:checkpoint_store, :snapshot_handoff_failed})
        {:disconnect, :checkpoint_store_failed}
    end
  end

  def handle_info({:snapshot_failed, _error}, %{step: :snapshotting} = state) do
    Replicant.Supervisor.halt(state.slot_name, {:snapshot, :snapshot_failed})
    {:disconnect, :snapshot_failed}
  end

  # The paced-retry timer fired while a fault episode is still active (store_retry_count > 0):
  # disconnect so `auto_reconnect` re-runs the FULL connect chain (fresh recovery +
  # slot-invalidation + store read). The retry counter persists on the Connection struct
  # across the disconnect (Postgrex.ReplicationConnection keeps mod_state), so attempts
  # accumulate toward the bound.
  def handle_info(:store_retry_reconnect, %{store_retry_count: count} = _state)
      when count > 0 do
    {:disconnect, :checkpoint_store_retry}
  end

  # A STALE paced-retry timer: an independent replication-connection reconnect during the
  # backoff window already re-ran `handle_connect`, and a recovered store read reset
  # `store_retry_count` to 0 (`reset_retry_count/2`) and resumed streaming. The timer was
  # never canceled (it is fire-and-forget), so it must be a no-op here — disconnecting a
  # recovered stream would be a spurious blip (a recovered read always zeroes the counter,
  # so count == 0 uniquely marks "no active fault episode").
  def handle_info(:store_retry_reconnect, state) do
    {:noreply, state}
  end

  # The AssemblerServer reports its running spilled-byte total so the §4 numerator can subtract it
  # (spilled bytes are on disk, not RAM). Coarse: the last value wins; a frame or two of staleness
  # is acceptable for this guard (spec §9/§10). MUST precede the catch-all below, or the catch-all
  # swallows it and the spill window never extends.

  def handle_info({:spilled_bytes, total}, state) when is_integer(total) do
    {:noreply, %{state | spilled_bytes: total}}
  end

  # ---- wal2json periodic schema guard (ADR-0009 divergence resolution) ----
  # A dropped VARLENA column on an update-only table is wire-indistinguishable from
  # the unchanged-TOAST sentinel (the decoder's insert/fixed-width rules cover every
  # other drop immediately), so a wal2json pipeline periodically re-reads the
  # configured tables' columns on a SHORT-LIVED REGULAR connection (a pre-10
  # walsender rejects SQL — the same connect-probe pattern) and diffs them against
  # the decoder cache. A column in the cache but gone from the catalog is re-emitted
  # as a subset %Relation{} to the AssemblerServer — the shipped column_dropped
  # classification halts :destructive fail-closed, the same semantics a pgoutput
  # Relation re-emit gives on the same DDL. A guard-read fault fails OPEN (skip this
  # tick): the guard is a backstop, the wire rules are the primary detection, and a
  # transient catalog fault must not halt a healthy stream.
  def handle_info(:schema_guard_tick, %{step: :streaming, decoder: %{kind: :wal2json}} = state) do
    tables = decoder_discovered(state) || decoder_tables(state) || []

    if tables != [] do
      connection = state.connection
      pid = self()
      schema_guard_spawn(connection, tables, pid)
    end

    {:noreply, arm_schema_guard(state)}
  end

  # Not streaming (halted/idle/snapshotting) or not wal2json: the tick does NOT
  # re-arm — a halted pipeline stops paying for the guard, and the next connect
  # chain re-arms it on the streaming entries.
  def handle_info(:schema_guard_tick, state), do: {:noreply, state}

  def handle_info({:schema_guard_diff, rows}, %{decoder: %{kind: :wal2json}} = state) do
    case Map.fetch(decoder_cache(state), :relations) do
      {:ok, relations} ->
        fresh = group_relation_info(rows)
        tables = decoder_discovered(state) || decoder_tables(state) || []
        Enum.each(tables, &cast_if_dropped(state, relations, fresh, &1))

      :error ->
        :ok
    end

    {:noreply, state}
  end

  def handle_info({:schema_guard_diff, _rows}, state), do: {:noreply, state}
  def handle_info(_other, state), do: {:noreply, state}

  # The guard's off-process catalog read: ANY fault (connect or query) means the
  # connection is unreachable or the catalog unreadable this tick — skip it (fail-open;
  # the wire rules remain the primary detection and a transient fault must not halt a
  # healthy stream).
  defp schema_guard_spawn(connection, tables, pid) do
    Task.start(fn ->
      case schema_guard_read(connection, tables) do
        nil -> :skip
        rows -> send(pid, {:schema_guard_diff, rows})
      end
    end)
  end

  # One configured table's diff: a cache column gone from the fresh catalog read is a
  # drop — cast the subset relation to the AssemblerServer, whose shipped
  # column_dropped classification halts :destructive (same as a pgoutput Relation
  # re-emit after the same DDL).
  defp cast_if_dropped(state, relations, fresh, key) do
    with {:ok, rel} <- Map.fetch(relations, key),
         {:ok, fresh_entry} <- Map.fetch(fresh, key) do
      fresh_names = fresh_entry.columns |> Enum.map(& &1.name) |> MapSet.new()
      dropped = for col <- rel.columns || [], not MapSet.member?(fresh_names, col.name), do: col

      if dropped != [] do
        dropped_names = Enum.map(dropped, & &1.name)
        subset = %{rel | columns: Enum.reject(rel.columns, &(&1.name in dropped_names))}
        GenServer.cast(AssemblerServer.via(state.slot_name), {:message, subset, 0, self()})
      end
    end
  end

  defp arm_schema_guard(%{decoder: %{kind: :wal2json}} = state) do
    interval = state.decoder[:schema_check_interval] || 30_000
    _ = :erlang.send_after(interval, self(), :schema_guard_tick)
    state
  end

  defp arm_schema_guard(state), do: state

  # The catalog read for one guard tick. ANY fault (connect or query) skips the tick
  # (fail-open — the backstop must not halt a healthy stream on a transient fault).
  defp schema_guard_read(connection, tables) do
    with {:ok, sql} <- QueryBuilder.configured_table_info(tables),
         {:ok, conn} <-
           Postgrex.start_link(
             Keyword.merge(connection, pool_size: 1, backoff_type: :stop, sync_connect: true)
           ) do
      try do
        case Postgrex.query(conn, sql, []) do
          {:ok, %Postgrex.Result{rows: rows}} -> rows
          _else -> nil
        end
      after
        GenServer.stop(conn)
      end
    else
      _else -> nil
    end
  rescue
    _ -> nil
  end

  # ---- public helpers (unit-tested directly) ----

  @doc """
  Encode a Standby Status Update reporting `lsn` as the write/flush/apply position
  (spec §2 — the slot never advances past the durable checkpoint). `reply_requested`
  is 0 (we volunteer status). `clock` is microseconds since the PG 2000 epoch.
  """
  @spec encode_status_update(Replicant.lsn()) :: binary()
  def encode_status_update(lsn) when is_integer(lsn) and lsn >= 0 do
    clock = System.os_time(:microsecond) - @pg_epoch
    <<?r, lsn::64, lsn::64, lsn::64, clock::64, 0>>
  end

  # On a keepalive: when a STATE MIRROR is IDLE (no open transaction AND every received commit
  # durable) and the server WAL end is ahead of our durable checkpoint, advance the slot to
  # `wal_end` (spec A1 §3.1). The intervening WAL is provably empty for this publication
  # (idle ⟹ no in-flight published change), so this releases WAL pinned on a quiet publication
  # WITHOUT ever acking past un-persisted publication data. Append logs deliberately skip this
  # optimization: only a delivered append/heartbeat may advance their durable checkpoint. When
  # not eligible, reply==1 acks the durable checkpoint and reply==0 sends nothing.
  defp keepalive_ack(wal_end, reply, state) do
    if idle?(state) and wal_end > state.checkpoint_lsn and
         Replicant.Sink.sink_kind(state.sink) != :append_log do
      if reply == 1 do
        # `kind: :idle` distinguishes an idle WAL-skip advance (over filtered WAL, `commit_lsn` is
        # the acked wal_end, not a committed txn) from a durable-commit advance (which omits `kind`).
        Telemetry.event([:replicant, :checkpoint, :advanced], %{}, %{
          commit_lsn: wal_end,
          kind: :idle
        })
      end

      {:noreply, [encode_status_update(wal_end)], %{state | checkpoint_lsn: wal_end}}
    else
      non_advancing_ack(reply, state)
    end
  end

  # The unchanged (non-advancing) keepalive reply: used when NOT idle, or idle but wal_end is
  # not ahead of the checkpoint. reply==1 volunteers the durable checkpoint; reply==0 sends nothing.
  defp non_advancing_ack(1, state),
    do: {:noreply, [encode_status_update(state.checkpoint_lsn)], state}

  defp non_advancing_ack(_reply, state), do: {:noreply, state}

  # Idle (spec A1 §3.1): no published transaction is open AND checkpoint has caught up to the
  # last commit (in lib/batch mode this waits for the flush; NOT `received_lsn <= checkpoint_lsn`,
  # which is never true after a commit — the end_lsn tail gap, spec §3.1.1).
  @spec idle?(t()) :: boolean()
  defp idle?(%{in_txn: in_txn, open_streams: streams, checkpoint_lsn: cp, last_commit_lsn: lc}),
    do: not in_txn and MapSet.size(streams) == 0 and cp >= lc

  @doc """
  Classify a `pg_replication_slots` invalidation-status result (spec §5/§8). `[]` →
  `:absent`. On the **PG15 1-col** row `[wal_status]` (PG15 has no `conflicting` column):
  `wal_status = "lost"` → `{:invalidated, :wal_lost}`, otherwise `:ok`. On the **PG16 2-col**
  row `[wal_status, conflicting]`: `wal_status = "lost"` →
  `{:invalidated, :wal_lost}`; `conflicting = true` → `{:invalidated, :conflict}`; otherwise
  `:ok`. On the **PG17 4-col** row `[wal_status, conflicting, invalidation_reason, synced]`:
  the legacy signals classify first (same as above), then any non-empty `invalidation_reason`
  → `{:invalidated, <fixed atom>}` via `invalidation_reason_atom/1` (never `String.to_atom`);
  a `nil`/`""` reason is `:ok`.
  """
  @spec classify_slot_status([[term()]]) ::
          :absent
          | :ok
          | {:invalidated,
             :wal_lost | :conflict | :rows_removed | :wal_level_insufficient | :invalidated}
  def classify_slot_status([]), do: :absent

  def classify_slot_status([[wal_status, conflicting, invalidation_reason, _synced] | _rest]) do
    cond do
      wal_status == "lost" -> {:invalidated, :wal_lost}
      conflicting == true -> {:invalidated, :conflict}
      invalidation_reason in [nil, ""] -> :ok
      true -> {:invalidated, invalidation_reason_atom(invalidation_reason)}
    end
  end

  def classify_slot_status([[wal_status, conflicting] | _rest]) do
    cond do
      wal_status == "lost" -> {:invalidated, :wal_lost}
      conflicting == true -> {:invalidated, :conflict}
      true -> :ok
    end
  end

  # PG15 1-col row `[wal_status]` — `conflicting` does not exist on PG15, so recovery-conflict
  # is not observable by construction; `wal_status = 'lost'` (WAL removed) is PG15's sole
  # invalidation signal. Anything else is :ok.
  def classify_slot_status([[wal_status] | _rest]) do
    if wal_status == "lost", do: {:invalidated, :wal_lost}, else: :ok
  end

  # Map PG's invalidation_reason enum string to a FIXED atom class (spec §5.2). NEVER
  # String.to_atom (atom-table exhaustion / Critical Rule 1) — an unknown/future reason maps
  # to the generic :invalidated so a new PG cause still halts fail-closed.
  defp invalidation_reason_atom("wal_removed"), do: :wal_lost
  defp invalidation_reason_atom("rows_removed"), do: :rows_removed
  defp invalidation_reason_atom("wal_level_insufficient"), do: :wal_level_insufficient
  defp invalidation_reason_atom(_other), do: :invalidated

  @doc false
  @spec lib_mode?(map()) :: boolean()
  def lib_mode?(%{checkpoint_store: store}), do: is_list(store)
  def lib_mode?(_), do: false

  @doc false
  @spec sink_owned_batch?(map()) :: boolean()
  def sink_owned_batch?(%{checkpoint_store: store, batch_delivery: bd}),
    do: not is_list(store) and is_list(bd)

  def sink_owned_batch?(_), do: false

  # Modes that use the assembler's LSN-span batch machinery (and therefore the stream floor):
  # lib mode (batched checkpointing) OR sink-owned batch delivery.
  defp batches?(state), do: lib_mode?(state) or sink_owned_batch?(state)

  # proto-v2 streaming of in-progress transactions is enabled (spec §5). Orthogonal to the
  # checkpoint mode (lib / sink-owned / per-txn) — a `:streaming` keyword turns it on.
  defp streaming?(%{streaming: s}), do: is_list(s)
  defp streaming?(_), do: false

  # The disk spill ceiling (bytes) from `streaming[:spill][:max_spill_bytes]`, or nil when
  # spill is not configured. Resolved once at `init/1` and extends the §4 halt ceiling.
  defp spill_ceiling(streaming) when is_list(streaming) do
    case Keyword.get(streaming, :spill) do
      spill when is_list(spill) -> Keyword.get(spill, :max_spill_bytes)
      _ -> nil
    end
  end

  defp spill_ceiling(_), do: nil

  @doc false
  @spec lib_go_forward_violation?(map()) :: boolean()
  def lib_go_forward_violation?(%{
        checkpoint_state: :empty,
        sink: sink,
        go_forward_only: false,
        snapshot: snapshot
      }),
      # ANY snapshot intent (`true` or a `[mode: :incremental]` keyword) is a safe seed and
      # bypasses the empty-checkpoint state-mirror refusal; only a bare `false` (no seed, no
      # go-forward) trips the violation.
      do: snapshot == false and Replicant.Sink.sink_kind(sink) == :state_mirror

  def lib_go_forward_violation?(_), do: false

  @doc false
  @spec incremental?(map()) :: boolean()
  def incremental?(%{snapshot: s}), do: is_list(s)
  def incremental?(_), do: false

  @doc false
  # Retire any prior incremental chunk reader before a reconnect respawns a fresh one, so the
  # slot is served by EXACTLY ONE reader (spec §8). The reader is `spawn_link`ed to this
  # Connection, but `handle_disconnect` keeps the Connection ALIVE across an `auto_reconnect`
  # cycle — so the link never fires and the old reader survives the reconnect. A second reader
  # spawned by `start_streaming_with_backfill` would then concurrently backfill the same slot
  # and double-deliver chunks ([[replicant-otp-async-lifetime-hygiene]]: an async primitive whose
  # lifetime is not bound to the state it serves survives reconnect and acts stale).
  #
  # UNLINK BEFORE KILL: the `spawn_link` link is bidirectional, so killing a still-linked reader
  # would deliver its `:kill` EXIT back to this Connection and fault it. Unlink first, then kill.
  # A dead or nil `reader_pid` is a no-op. Clears `reader_pid` (the caller threads in the fresh one).
  @spec retire_reader(t()) :: t()
  def retire_reader(%__MODULE__{reader_pid: pid} = state) when is_pid(pid) do
    if Process.alive?(pid) do
      Process.unlink(pid)
      Process.exit(pid, :kill)
    end

    %{state | reader_pid: nil}
  end

  def retire_reader(state), do: state

  @doc false
  # Classify a progress-token read for the §8 matrix. A decode failure / store fault
  # classifies as :fault (fail-closed halt :snapshot_progress_invalid) — NEVER :none (which
  # would silently restart the backfill over a possibly-populated mirror = data corruption).
  @spec classify_progress({:ok, binary() | nil | :backfill_pending} | {:error, term()}) ::
          :none
          | :backfill_pending
          | :complete
          | {:in_flight, Replicant.SnapshotProgress.t()}
          | :fault
  def classify_progress({:ok, nil}), do: :none
  def classify_progress({:ok, :backfill_pending}), do: :backfill_pending

  def classify_progress({:ok, token}) when is_binary(token) do
    case Replicant.SnapshotProgress.decode(token) do
      {:ok, sp} ->
        if Replicant.SnapshotProgress.complete?(sp), do: :complete, else: {:in_flight, sp}

      # Fail-closed on ANY decode error. Today decode/1 returns only :snapshot_progress_invalid;
      # this catch-all hardens against a future decode-contract drift to a different error atom
      # (which would otherwise FunctionClauseError-crash here instead of halting fail-closed).
      {:error, _} ->
        :fault
    end
  end

  def classify_progress({:error, _}), do: :fault

  # ---- private ----

  # A6 command-error watchdog exhaustion: the pre-frame replication-command livelock has burned its
  # budget (N failed connect cycles with no replication frame in between). Halt fail-closed and STAY
  # IDLE (mirror halt_store_permanent) — do NOT issue the recovery query, so auto_reconnect cannot
  # re-spin the connect chain into the same fault until the async teardown lands. The event is
  # VALUE-FREE (Rule 1): only the count / budget / slot — never any error content (the
  # `%Postgrex.Error{}` is discarded at handle_result, never carried here).
  defp halt_command_error(state) do
    Telemetry.event([:replicant, :connection, :command_error_halt], %{}, %{
      attempt: state.command_error.count,
      max_retries: state.command_error.max_retries,
      slot_name: state.slot_name
    })

    Replicant.Supervisor.halt(state.slot_name, {:command_error, :exhausted})
    {:noreply, state}
  end

  defp halt_session_identity(state) do
    Telemetry.event([:replicant, :connection, :session_identity_rejected], %{}, %{
      reason: :session_identity_rejected
    })

    Replicant.Supervisor.halt(state.slot_name, {:session_identity, :rejected})
    {:noreply, state}
  end

  # A PERMANENT store fault at connect (wrong column type / invalid table id) cannot be
  # fixed by retrying — halt fail-closed immediately (spec §7/§9). This ALSO fixes the
  # interim bug where a schema-mismatch retried forever.
  defp halt_store_permanent(state) do
    Telemetry.event([:replicant, :checkpoint_store, :failed], %{}, %{
      slot_name: state.slot_name,
      reason: :checkpoint_store_failed
    })

    Replicant.Supervisor.halt(state.slot_name, {:checkpoint_store, :schema})
    # Stay idle (do NOT disconnect) so `auto_reconnect` cannot re-run the connect chain and
    # re-detect the permanent fault in a spin until the async teardown lands — same terminal
    # discipline as the exhaustion path (spec §9). The async `Supervisor.halt` kills us.
    {:noreply, state}
  end

  # A PERMANENT config fault — `pgoutput` on a server older than PG 10 (no pgoutput, no
  # publications; ADR-0009 §9). The version never changes across a reconnect, so halt and
  # STAY IDLE (do NOT disconnect), mirroring halt_failover_unsupported: a disconnect would
  # let auto_reconnect re-run the connect chain and re-halt in a spin until the async
  # teardown lands.
  defp halt_decoder_unsupported_on_server(state) do
    Telemetry.event([:replicant, :connection, :slot_invalidated], %{}, %{
      reason: :decoder_unsupported_on_server
    })

    Replicant.Supervisor.halt(state.slot_name, {:config, :decoder_unsupported_on_server})
    {:noreply, state}
  end

  # `failover: true` against a PG < 17 server (which rejects the FAILOVER slot option). A
  # PERMANENT config fault — the version never changes across a reconnect — so halt and STAY
  # IDLE (do NOT disconnect), mirroring halt_store_permanent: a disconnect would let
  # auto_reconnect re-run the connect chain and re-halt in a spin until the async teardown lands.
  defp halt_failover_unsupported(state) do
    Telemetry.event([:replicant, :connection, :slot_invalidated], %{}, %{
      reason: :failover_unsupported
    })

    Replicant.Supervisor.halt(state.slot_name, {:config, :failover_unsupported})
    {:noreply, state}
  end

  # A PERMANENT config fault — a configured publication is absent on the server (or the set is
  # otherwise non-equal). The publications won't appear across a reconnect, so halt and STAY IDLE
  # (do NOT disconnect), mirroring halt_failover_unsupported: a disconnect would let auto_reconnect
  # re-run the connect chain and re-halt in a spin until the async teardown lands.
  defp halt_publication_missing(state) do
    Telemetry.event([:replicant, :connection, :slot_invalidated], %{}, %{
      reason: :publication_missing
    })

    Replicant.Supervisor.halt(state.slot_name, {:publication, :missing})
    {:noreply, state}
  end

  # A TRANSIENT store read fault: paced bounded retry (spec §4). On a retry, arm a timer and
  # stay idle for the backoff, then `handle_info(:store_retry_reconnect)` disconnects so the
  # framework's OWN connect chain re-runs FRESH (re-checking slot invalidation every attempt —
  # never replaying a stale invalidation snapshot). On exhaustion, HALT by staying idle: emit
  # `:failed`, `Supervisor.halt`, and return `{:noreply}` — we do NOT disconnect, so no
  # post-halt reconnect can race the async teardown into an N+1 read (spec §9 terminal guard).
  defp pace_store_retry(state) do
    case store_retry_step(state) do
      {:retry, state} ->
        Process.send_after(self(), :store_retry_reconnect, retry_backoff_ms(state))
        # Mark this idle cycle as store-paced so the A6 watchdog EXEMPTS the disconnect the
        # timer is about to cause (reaching here proves the command chain succeeded to
        # :invalidation_check — no command fault this cycle). This committed `{:noreply}` is the
        # state `handle_disconnect` sees when `:store_retry_reconnect` fires.
        {:noreply, %{state | command_error: %{state.command_error | store_paced: true}}}

      :halt ->
        Telemetry.event([:replicant, :checkpoint_store, :failed], %{}, %{
          slot_name: state.slot_name,
          reason: :checkpoint_store_failed
        })

        Replicant.Supervisor.halt(state.slot_name, {:checkpoint_store, :unavailable})
        {:noreply, state}
    end
  end

  @doc false
  @spec store_retry_step(map()) :: {:retry, map()} | :halt
  def store_retry_step(state) do
    max = max_retries(state)

    case Replicant.CheckpointStore.retry_decision(state.store_retry_count, max) do
      :retry ->
        attempt = state.store_retry_count + 1
        Replicant.CheckpointStore.emit_retrying(state.slot_name, attempt, max)
        {:retry, %{state | store_retry_count: attempt}}

      :halt ->
        :halt
    end
  end

  # Read the retry policy. Config normalizes these onto :checkpoint_store; the fallback is
  # the single-source default on CheckpointStore (spec §6), defending a directly-constructed
  # config map in tests.
  defp max_retries(%{checkpoint_store: store}),
    do: Keyword.get(store, :max_retries, Replicant.CheckpointStore.default_max_retries())

  defp retry_backoff_ms(%{checkpoint_store: store}),
    do:
      Keyword.get(store, :retry_backoff_ms, Replicant.CheckpointStore.default_retry_backoff_ms())

  @doc false
  # Keep the retry counter across a fault (accumulate toward the bound); reset to 0 on a
  # SUCCESSFUL read so a later, separate outage (the transient self-heal case) starts fresh.
  @spec reset_retry_count(non_neg_integer(), atom()) :: non_neg_integer()
  def reset_retry_count(count, checkpoint_state)
      when checkpoint_state in [:fault, :fault_permanent],
      do: count

  def reset_retry_count(_count, _success), do: 0

  # In-flight WAL lag (bytes): received frontier minus the confirmed-durable floor,
  # LESS the bytes already spilled to disk (§5 — spilled bytes are no longer resident,
  # so they must not count toward the RAM-bounded numerator). The floor is the higher
  # of the durable `checkpoint_lsn` (once a commit advances it to a real absolute LSN)
  # and the per-stream `stream_floor_lsn` (the position PG began streaming at, used
  # before the first commit while checkpoint is still 0). A cheap integer subtraction —
  # safe to call on the keepalive-free hot path.
  defp inflight_lag(%{
         received_lsn: received,
         checkpoint_lsn: cp,
         stream_floor_lsn: floor,
         spilled_bytes: spilled
       }) do
    received - max(cp, floor || received) - spilled
  end

  # The §4 halt ceiling. No spill configured (`max_spill_bytes: nil`): the base
  # `max_inflight_lag` (RAM-only bound, unchanged). Spill configured: extend the ceiling
  # by the disk budget — resident lag may run up to RAM + disk before the sink is
  # genuinely too slow (the numerator already subtracts the spilled bytes).
  defp effective_lag_bound(%{max_inflight_lag: base, max_spill_bytes: nil}), do: base
  defp effective_lag_bound(%{max_inflight_lag: base, max_spill_bytes: ceil}), do: base + ceil

  defp forward_message(payload, state) do
    case Decoder.decode(payload,
           decoder: decoder_kind(state),
           cache: decoder_cache(state),
           streaming: state.in_stream
         ) do
      {:ok, messages, cache} ->
        # A plugin may emit several messages for one payload (wal2json: a re-emitted
        # Relation ahead of a schema-drifted change) — each is forwarded in order and
        # each updates the stream/txn tracking, exactly as a single message would.
        #
        # ORDERING INVARIANT (spec A1 §3.2): track_txn runs HERE, when the Connection
        # forwards each message in WAL order, so a Begin sets in_txn BEFORE any following
        # keepalive is handled — an idle-ack can never fire while an open transaction has
        # been received. Moving this off the per-message forward path, or dropping a
        # boundary clause, opens a silent-loss window. update_in_stream tracks the decode
        # frame; track_txn the txn.
        state = set_decoder_cache(state, cache)

        # The payload's WAL bytes are accounted ONCE (on the FIRST message), not per
        # message: a multi-message payload (wal2json's relation + change) must not
        # double-count in txn.byte_size and the spill accounting.
        state = forward_each_message(messages, payload, state)

        {:noreply, state}

      {:error, error} ->
        Replicant.Supervisor.halt(state.slot_name, error)
        {:disconnect, :decode_failure}
    end
  end

  defp forward_each_message(messages, payload, state) do
    messages
    |> Enum.with_index()
    |> Enum.reduce(state, fn {message, i}, acc ->
      bytes = if i == 0, do: byte_size(payload), else: 0

      GenServer.cast(
        AssemblerServer.via(state.slot_name),
        {:message, message, bytes, self()}
      )

      acc |> update_in_stream(message) |> track_txn(message)
    end)
  end

  # Track the streaming decode context: StreamStart opens it (the following change messages carry
  # the (sub)xid prefix that only decodes correctly with streaming: true), StreamStop closes it.
  # Reset to false on every (re)connect (see start_streaming / snapshot handoff) so a mid-stream
  # reconnect never leaves the flag stale (spec §9; a stale flag would mis-frame the next streamed
  # change-message decode — I/U/D, whose (sub)xid prefix strip mis-parses → value-free halt).
  defp update_in_stream(state, %Replicant.Decoder.Messages.StreamStart{}),
    do: %{state | in_stream: true}

  defp update_in_stream(state, %Replicant.Decoder.Messages.StreamStop{}),
    do: %{state | in_stream: false}

  defp update_in_stream(state, _message), do: state

  @doc false
  # Maintain the transaction-in-flight signal for the idle-ack (spec A1 §3.1). A transaction is
  # "open" from its opening boundary until its closing boundary; `idle?/1` refuses to advance the
  # slot while any is open. `last_commit_lsn` records the most recent commit's commit_lsn so the
  # keepalive path can confirm it durable (`checkpoint_lsn >= last_commit_lsn`) before advancing.
  # `last_commit_lsn` is monotonic (max) to ignore any out-of-order/nil commit lsn.
  #
  # NON-STREAMED (proto-v1, and small txns in v2): the `in_txn` boolean — Begin opens, Commit closes;
  # only one is in flight at a time (they arrive in commit order, non-interleaved).
  # STREAMED (proto-v2): the assembler holds up to `max_concurrent_txns` CONCURRENT xid-keyed
  # in-progress buffers (assembler.ex), so a single boolean under-counts them. Track the SET of open
  # streamed xids: StreamStart opens one, StreamCommit closes it. StreamAbort is a SUBTRANSACTION
  # (savepoint) abort when `xid != subxid` — the parent stays open (the assembler keeps its buffer),
  # so it must NOT close the stream; only a whole-txn abort (`xid == subxid`) closes it (the aborted
  # changes are discarded, never delivered, so advancing past them is safe). StreamStop is a
  # mid-transaction PAUSE that touches neither. (Correctness/cross-vendor review CV1/CV2.)
  @spec track_txn(t(), struct()) :: t()
  def track_txn(state, %Begin{}), do: %{state | in_txn: true}

  def track_txn(state, %Commit{lsn: lsn}),
    do: %{state | in_txn: false, last_commit_lsn: max(state.last_commit_lsn, lsn || 0)}

  def track_txn(state, %StreamStart{xid: xid}),
    do: %{state | open_streams: MapSet.put(state.open_streams, xid)}

  def track_txn(state, %StreamCommit{xid: xid, commit_lsn: lsn}),
    do: %{
      state
      | open_streams: MapSet.delete(state.open_streams, xid),
        last_commit_lsn: max(state.last_commit_lsn, lsn || 0)
    }

  def track_txn(state, %StreamAbort{xid: top, subxid: sub}) when top == sub,
    do: %{state | open_streams: MapSet.delete(state.open_streams, top)}

  def track_txn(state, %StreamAbort{}), do: state

  # A2 (Task 9, spec §8.1 idle-ack seam): a NON-transactional pg_logical_emit_message arrives
  # standalone (no Begin/Commit bracket) and delivers via handle_message/2 — at-least-once, NO
  # transaction-watermark dedup. Its LSN is a PENDING DELIVERABLE the keepalive path must NOT ack
  # past. The generic catch-all below would leave last_commit_lsn unchanged → idle?/1 stays true →
  # the next reply-requested keepalive idle-advances the slot to wal_end, acking PAST the
  # undelivered message → SILENT LOSS (the probed failure mode of §8.1). Treat the message LSN as a
  # pending commit: bump last_commit_lsn so idle?/1 returns false until {:sink_committed, msg_lsn}
  # advances checkpoint_lsn >= last_commit_lsn. A transactional message is intentionally NOT bumped
  # here — Begin/Commit owns its idle? signal (the next clause is the catch-all no-op).
  def track_txn(state, %Replicant.Decoder.Messages.Message{transactional?: false, lsn: msg_lsn}),
    do: %{state | last_commit_lsn: max(state.last_commit_lsn, msg_lsn || 0)}

  def track_txn(state, _msg), do: state

  # Fail-closed lag-halt (spec §4): the sink is not draining fast enough — the
  # in-flight window is exceeded. Surface it with value-free telemetry (the `reason`
  # meta key is allowlisted; the `lag` measurement is a WAL-byte count, never a row
  # value), tear the pipeline down permanently, and disconnect. This BOUNDS memory
  # (no unbounded mailbox) and SURFACES the overload (no silent livelock). Restart
  # resumes from the durable checkpoint (loss=0 by §6 dedup).
  defp halt_sink_too_slow(state, lag) do
    Telemetry.event([:replicant, :connection, :disconnected], %{lag: lag}, %{
      reason: :sink_too_slow
    })

    Replicant.Supervisor.halt(state.slot_name, {:sink_too_slow, lag})
    {:disconnect, :sink_too_slow}
  end

  defp start_streaming(state) do
    # SINGLE WRITER of frontier_epoch: an incremental (re)connect opens a fresh window epoch
    # (fe + 1) HERE and nowhere else, so the {:reset_snapshot_window, epoch} prime_assembler
    # casts and every subsequent {:snapshot_frontier, epoch, _} keepalive/XLogData cast all read
    # this one stored value and therefore always agree (85672f1 stale-epoch class). A
    # resumed-PLAIN stream (progress :complete / :none+present) flows through here too, so it
    # also adopts the fresh epoch and its stale-window reset matches. Non-incremental pipelines
    # are unaffected (fe stays 0, prime_assembler casts nothing, no frontier casts fire).
    state = %{state | frontier: %{state.frontier | epoch: next_frontier_epoch(state)}}
    prime_assembler(state, state.checkpoint_lsn)

    stream_now(state)
  end

  defp stream_now(state) do
    {:ok, sql} = start_replication_sql(state, state.checkpoint_lsn)

    {:stream, sql, [], arm_schema_guard(%{state | step: :streaming, in_stream: false})}
  end

  # The START_REPLICATION command for the configured decoder (ADR-0009 §2): the plugin
  # module builds its option list from the SAME config the rest of the chain uses, and
  # QueryBuilder renders it (byte-identical to 1.3.0 for the pgoutput default — pinned
  # by test).
  defp start_replication_sql(state, start_lsn) do
    plugin = DecoderPlugin.module_for(decoder_kind(state))

    options =
      plugin.start_options(
        publications: state.publication || [],
        streaming: streaming?(state),
        messages: state.messages,
        replication_sets: decoder_sets(state) || [],
        tables: decoder_tables(state) || []
      )

    QueryBuilder.start_replication_for(state.slot_name, plugin.slot_plugin(), options, start_lsn)
  end

  # The window epoch this (re)connect adopts: bumped once per incremental (re)connect so a fresh
  # window strictly outranks any in-flight pre-reconnect frontier cast; unchanged (0) otherwise.
  defp next_frontier_epoch(state) do
    if incremental?(state), do: frontier_epoch(state) + 1, else: frontier_epoch(state)
  end

  # Incremental backfill start (spec §8): re-seat the window at a fresh epoch + seed the floor on
  # the assembler, spawn_link the chunk reader (Snapshotter §17 precedent — dies with this
  # Connection), then stream. The window reset + floor cast BEFORE the reader spawns so the reader
  # never opens a chunk window against a stale epoch. `sp` nil = fresh run (the reader discovers
  # tables + builds the token); non-nil = resume from the durable token.
  #
  # frontier_epoch stays SINGLE-WRITER: this reset uses `frontier_epoch(state) + 1`, and
  # start_streaming (called with the same pre-bump state) independently computes the identical
  # fe + 1 and is the sole writer of the returned struct's frontier_epoch — so the two reset casts
  # carry the same epoch, matching every later frontier cast.
  defp start_streaming_with_backfill(state, sp) do
    # Both callers set backfill_floor to a concrete LSN immediately before dispatching here
    # (fresh: the NOEXPORT consistent_point; resume: sp.floor_lsn), so it is never nil at this
    # point — the assembler floor and the reader floor read it directly.
    floor = state.backfill_floor

    # EXACTLY-ONE-READER: retire any reader from a prior life BEFORE re-seating the window and
    # spawning the fresh one. On a reconnect the persistent Connection still holds the old
    # `reader_pid` (it survived because the link never fired), so retire_reader unlink+kills it
    # here; the window-reset that follows discards any old-epoch chunk it delivered mid-flight,
    # and the fresh reader resumes from durable progress. Ordering: retire → reset → spawn.
    state = retire_reader(state)
    server = AssemblerServer.via(state.slot_name)
    GenServer.cast(server, {:reset_snapshot_window, frontier_epoch(state) + 1})
    GenServer.cast(server, {:snapshot_floor, floor})

    reader_pid =
      Incremental.start(%{
        slot_name: state.slot_name,
        connection: state.connection,
        publication: state.publication,
        tables: decoder_discovered(state),
        sink: state.sink,
        mode: if(lib_mode?(state), do: :lib, else: :sink_owned),
        snapshot: state.snapshot,
        resume: sp,
        floor_lsn: floor
      })

    # Thread the fresh reader pid into the state start_streaming returns (it preserves reader_pid),
    # so the NEXT reconnect can retire THIS reader in turn.
    start_streaming(%{state | reader_pid: reader_pid})
  end

  # On every (re)connect prepare the assembler for the resumed stream: lib mode SEEDS the
  # in-memory watermark from the connect-time store read; sink-owned batch mode DISCARDS any open
  # batch (reset) so a transient reconnect re-buffers a fresh batch (spec §9; the span base
  # lib_checkpoint is left to the assembler). Per-transaction sink-owned mode: no-op.
  defp prime_assembler(state, seed_lsn) do
    # Streaming is orthogonal to the checkpoint mode: on every (re)connect discard any
    # partially-buffered streamed transactions (spec §9) so a mid-stream reconnect never
    # replays a half-received in-progress txn. Runs before the mode-specific seed/reset.
    if streaming?(state), do: reset_assembler_streams(state)

    # Sweep THIS slot's spill subdir before the resumed stream (spec §5): removes any spill
    # file a prior abnormal exit left. Per-slot and idempotent — safe here because no stream
    # is active yet. A no-op when spill is not configured.
    if spill_dir(state), do: Replicant.Spill.sweep_slot(spill_dir(state), state.slot_name)

    # Incremental: re-seat the snapshot window at THIS connect's epoch (start_streaming already
    # bumped frontier_epoch to the single-writer value). For a resumed-PLAIN stream (progress
    # :complete / :none+present, no reader) this clears any stale window a previous life left; for
    # a backfill it is an idempotent second reset at the same epoch as start_streaming_with_backfill.
    if incremental?(state),
      do:
        GenServer.cast(
          AssemblerServer.via(state.slot_name),
          {:reset_snapshot_window, frontier_epoch(state)}
        )

    cond do
      lib_mode?(state) -> seed_assembler(state, seed_lsn)
      sink_owned_batch?(state) -> reset_assembler_batch(state)
      true -> :ok
    end
  end

  # The configured spill directory from `streaming[:spill][:dir]`, or nil when spill is not
  # configured. Used to sweep this slot's stale spill files at (re)connect setup.
  defp spill_dir(%{streaming: streaming}) when is_list(streaming) do
    case Keyword.get(streaming, :spill) do
      spill when is_list(spill) -> Keyword.get(spill, :dir)
      _ -> nil
    end
  end

  defp spill_dir(_), do: nil

  defp reset_assembler_streams(state) do
    GenServer.cast(AssemblerServer.via(state.slot_name), {:reset_streams})
  end

  defp reset_assembler_batch(state) do
    GenServer.cast(AssemblerServer.via(state.slot_name), {:reset_batch})
  end

  # Seed the lib-mode watermark from the SAME store read the connect used, before any
  # Commit cast — this bounds the resume dup to a single transaction (the store
  # checkpoint, ahead of the slot's confirmed_flush, otherwise re-streams N txns
  # un-deduped). A no-op in sink-owned mode (never called; the assembler ignores
  # lib_checkpoint there).
  defp seed_assembler(state, lsn) when is_integer(lsn) do
    GenServer.cast(AssemblerServer.via(state.slot_name), {:seed_lib_checkpoint, lsn})
  end

  # The snapshot handoff write. Sink-owned: the snapshotter already durably set the sink
  # checkpoint via handle_snapshot_complete/1, so this is a no-op (:ok) and the stream
  # branch runs unchanged. Lib mode: write the handoff LSN to the store here (after all
  # batches, before streaming) — the store is the durable checkpoint the resume reads.
  defp write_snapshot_handoff(%{checkpoint_store: store, slot_name: slot}, lsn)
       when is_list(store),
       do: Replicant.CheckpointStore.write(Replicant.CheckpointStore.via(slot), lsn)

  defp write_snapshot_handoff(_state, _lsn), do: :ok

  @doc false
  # Read the progress token from its per-mode home (spec §6.2). Lib mode: the checkpoint store
  # (its returns are already value-free-scrubbed). Sink-owned: the sink's snapshot_progress/0
  # behind a value-free rescue — a raise/throw becomes {:error, :snapshot_progress_read_fault},
  # which classify_progress maps to :fault (fail-closed halt), never :none. `@doc false def` (not
  # `defp`) so the sink-mode value-free boundary is unit-tested directly, like its siblings.
  @spec read_progress(map()) ::
          {:ok, binary() | nil | :backfill_pending} | {:error, term()}
  def read_progress(%{checkpoint_store: store, slot_name: slot}) when is_list(store) do
    pending = Replicant.SnapshotProgress.pending_store_token()

    case Replicant.CheckpointStore.read_progress(Replicant.CheckpointStore.via(slot)) do
      {:ok, ^pending} -> {:ok, :backfill_pending}
      other -> other
    end
  end

  def read_progress(%{sink: sink}) do
    sink.snapshot_progress()
  rescue
    _ -> {:error, :snapshot_progress_read_fault}
  catch
    _kind, _reason -> {:error, :snapshot_progress_read_fault}
  end

  # ---- connect matrix (spec §8): slot presence × checkpoint state × snapshot mode ----

  # The unchanged sink-owned slot-classification decision (also the lib-mode path once
  # the two fail-closed gates in :invalidation_check pass): map the invalidation-status
  # rows to a connect action. Extracted from the `cond` only to keep that branch
  # cyclomatically small (credo) — every branch here is the pre-lib-mode behavior,
  # verbatim.
  defp classify_and_begin(rows, state) do
    if synced_unpromoted?(rows, state) do
      # A slot SYNCED from a primary, observed on an UNPROMOTED standby (in_recovery). It cannot
      # be consumed until promotion — halt fail-closed with a distinct reason instead of the
      # :query_error auto_reconnect livelock a consume attempt would otherwise cause (spec §7).
      Telemetry.event([:replicant, :connection, :slot_invalidated], %{}, %{
        reason: :slot_synced_unpromoted
      })

      Replicant.Supervisor.halt(state.slot_name, {:slot_synced_unpromoted})
      {:disconnect, :slot_synced_unpromoted}
    else
      case classify_slot_status(rows) do
        :absent when state.checkpoint_lsn > 0 ->
          # DATA GAP (spec §8): the sink has durable state (checkpoint > 0) but the slot
          # is gone (dropped/rebuilt/restored-from-backup). A fresh slot would begin
          # streaming at its creation LSN, silently skipping every transaction between
          # the sink's checkpoint and now — unrecoverable loss. Halt fail-closed with a
          # distinct data-gap signal; NEVER silently recreate. (An EMPTY checkpoint —
          # nil or a §14.15 read-fault, both read as 0 — is a genuine first run / go
          # forward and DOES create the slot below.)
          Telemetry.event([:replicant, :connection, :slot_invalidated], %{}, %{reason: :data_gap})
          Replicant.Supervisor.halt(state.slot_name, {:data_gap, :slot_missing_with_checkpoint})
          {:disconnect, :data_gap}

        :absent ->
          begin_absent_slot(state)

        :ok ->
          begin_present_slot(state)

        {:invalidated, reason} ->
          Telemetry.event([:replicant, :connection, :slot_invalidated], %{}, %{reason: reason})
          Replicant.Supervisor.halt(state.slot_name, {:slot_invalidated, reason})
          {:disconnect, :slot_invalidated}
      end
    end
  end

  # PG17-only guard (spec §7). A 4-col row carries `synced`; a standby's OWN logical slot has
  # synced=false, so `synced=true AND in_recovery=true` unambiguously marks a synced failover
  # slot on an unpromoted standby. PG16 rows are 2-col and never match. After promotion
  # in_recovery is false, so the guard correctly does not fire and consumption resumes.
  defp synced_unpromoted?([[_wal_status, _conflicting, _reason, synced] | _], %{
         server_version_num: v,
         in_recovery: in_recovery
       })
       when v >= 170_000,
       do: synced == true and in_recovery == true

  defp synced_unpromoted?(_rows, _state), do: false

  # Replication simple-query results arrive as TEXT; coerce the invalidation-status boolean
  # columns (conflicting, synced) so classify_slot_status / synced_unpromoted? see real booleans.
  # 1-col PG15 row: [wal_status] (no boolean to coerce); 2-col PG16 row: [wal_status, conflicting];
  # 4-col PG17: [wal_status, conflicting, reason, synced].
  defp coerce_status_row([wal_status]), do: [wal_status]

  defp coerce_status_row([wal_status, conflicting]), do: [wal_status, repl_bool(conflicting)]

  defp coerce_status_row([wal_status, conflicting, reason, synced]),
    do: [wal_status, repl_bool(conflicting), reason, repl_bool(synced)]

  defp coerce_status_row(other), do: other

  # Postgres text-protocol scalar coercion (a replication simple-query returns all columns as
  # text). Accept already-typed values too so unit tests can feed native ints/bools.
  defp repl_int(v) when is_integer(v), do: v
  defp repl_int(v) when is_binary(v), do: String.to_integer(v)

  defp repl_bool(v) when is_boolean(v), do: v
  defp repl_bool("t"), do: true
  defp repl_bool("f"), do: false
  defp repl_bool(nil), do: nil

  # ---- incremental snapshot (spec §8 of the incremental design) ----
  # These clauses come FIRST; the `is_list/1` guard keeps them disjoint from the v1
  # `snapshot: true` clauses and the non-snapshot catch-all below.

  # Slot ABSENT. Progress in flight/complete + an absent slot = the stream gap between the
  # backfill floor and now is unfillable (deletes lost) → :data_gap (the §14.19 family). No
  # progress → a genuine fresh run: create the NOEXPORT slot. A decode/read fault → fail-closed.
  # Checkpoint UNKNOWN takes precedence over empty progress: without a known checkpoint, a fresh
  # durable slot would silently skip the unknown WAL interval before its creation LSN.
  defp begin_absent_slot(%{snapshot: s, checkpoint_state: cp_state} = state)
       when is_list(s) and cp_state in [:fault, :fault_permanent],
       do: halt_checkpoint_unknown(state)

  defp begin_absent_slot(%{snapshot: s} = state) when is_list(s) do
    case classify_progress(read_progress(state)) do
      classification when classification in [:none, :backfill_pending] ->
        {:ok, sql} =
          QueryBuilder.create_durable_slot(
            state.slot_name,
            state.failover,
            DecoderPlugin.module_for(decoder_kind(state)).slot_plugin(),
            state.server_version_num
          )

        {:query, sql, %{state | step: :create_incremental_slot}}

      :fault ->
        halt_snapshot(state, :snapshot_progress_invalid)

      _in_flight_or_complete ->
        Telemetry.event([:replicant, :connection, :slot_invalidated], %{}, %{reason: :data_gap})
        Replicant.Supervisor.halt(state.slot_name, {:data_gap, :slot_missing_with_progress})
        {:disconnect, :data_gap}
    end
  end

  # Slot ABSENT, checkpoint not durable (empty or fault-as-0).
  defp begin_absent_slot(%{snapshot: true, checkpoint_state: :fault} = state),
    do: halt_snapshot(state, :checkpoint_unreadable)

  defp begin_absent_slot(%{snapshot: true} = state), do: create_export_slot(state)

  # Slot ABSENT, checkpoint state UNKNOWN (a sink-owned checkpoint READ FAULT, §14.15 — a
  # raise/error from `sink.checkpoint()` reads as checkpoint_lsn 0). The §14.15 streaming
  # fail-open (resume-from-0, the idempotent sink dedups the re-stream) is SAFE only when
  # the slot is PRESENT: a resume clamps to the slot's server-side confirmed_flush_lsn, so
  # nothing is skipped. With the slot ABSENT there is nothing to resume — creating a fresh
  # slot would begin streaming at its own creation LSN and silently skip every transaction
  # between the (unknown) real checkpoint and now, an unrecoverable data gap. A read fault
  # is NOT a genuine first run, so we cannot treat it as :empty. Fail closed with the
  # :data_gap family (spec §8: never silently drop and recreate); a DISTINCT telemetry
  # reason (`:checkpoint_unknown`, value-free) separates this from the checkpoint-present
  # gap above. (`:fault_permanent` is defensive: sink-owned mode only ever yields :fault,
  # and lib mode's fault is already halted in :invalidation_check before it reaches here —
  # but a fail-closed guard admits every not-known-good state, never just the one observed.)
  defp begin_absent_slot(%{checkpoint_state: cp_state} = state)
       when cp_state in [:fault, :fault_permanent],
       do: halt_checkpoint_unknown(state)

  defp begin_absent_slot(state) do
    {:ok, sql} =
      QueryBuilder.create_durable_slot(
        state.slot_name,
        state.failover,
        DecoderPlugin.module_for(decoder_kind(state)).slot_plugin(),
        state.server_version_num
      )

    {:query, sql, %{state | step: :create_slot}}
  end

  defp halt_checkpoint_unknown(state) do
    Telemetry.event([:replicant, :connection, :slot_invalidated], %{}, %{
      reason: :checkpoint_unknown
    })

    Replicant.Supervisor.halt(state.slot_name, {:data_gap, :slot_missing_checkpoint_unknown})
    {:disconnect, :data_gap}
  end

  # Slot PRESENT (incremental — FIRST; `is_list/1` guard keeps it disjoint from the v1 clauses).
  # :complete → plain resume; in-flight → resume streaming + respawn the reader from the durable
  # token; :none + checkpoint present → bootstrapped-elsewhere no-op resume; :none + empty
  # checkpoint → a foreign/crashed-v1 slot → the v1 :snapshot_incomplete discipline; :fault →
  # fail-closed.
  defp begin_present_slot(%{snapshot: s} = state) when is_list(s) do
    case classify_progress(read_progress(state)) do
      :complete ->
        Telemetry.event([:replicant, :connection, :slot_active], %{}, %{})
        start_streaming(state)

      {:in_flight, sp} ->
        Telemetry.event([:replicant, :snapshot, :resumed], %{}, %{slot_name: state.slot_name})
        state = %{state | backfill_floor: sp.floor_lsn}
        start_streaming_with_backfill(state, sp)

      :backfill_pending ->
        read_backfill_origin(state)

      :none when state.checkpoint_state == :present ->
        Telemetry.event([:replicant, :connection, :slot_active], %{}, %{})
        start_streaming(state)

      :none ->
        halt_snapshot(state, :snapshot_incomplete)

      :fault ->
        halt_snapshot(state, :snapshot_progress_invalid)
    end
  end

  # Slot PRESENT.
  defp begin_present_slot(%{snapshot: true, checkpoint_state: :fault} = state),
    do: halt_snapshot(state, :checkpoint_unreadable)

  defp begin_present_slot(%{snapshot: true, checkpoint_state: :empty} = state),
    do: halt_snapshot(state, :snapshot_incomplete)

  defp begin_present_slot(state) do
    # :present (resume) or non-snapshot go-forward with an existing slot. R04: when the sink wants
    # the slot origin, read the reused slot's confirmed_flush_lsn (a `:read_slot_origin` step) before
    # streaming; otherwise stream directly (byte-identical to the prior behavior — no extra query).
    Telemetry.event([:replicant, :connection, :slot_active], %{}, %{})

    if wants_slot_origin?(state), do: read_slot_origin(state), else: start_streaming(state)
  end

  defp create_export_slot(state) do
    {:ok, sql} =
      QueryBuilder.create_export_slot(
        state.slot_name,
        state.failover,
        DecoderPlugin.module_for(decoder_kind(state)).slot_plugin(),
        state.server_version_num
      )

    {:query, sql, %{state | step: :create_export_slot}}
  end

  # Fail-closed snapshot halt (spec §8) — never auto-drops a slot.
  defp halt_snapshot(state, reason) do
    Telemetry.event([:replicant, :snapshot, :failed], %{}, %{reason: reason})
    Replicant.Supervisor.halt(state.slot_name, {:snapshot, reason})
    {:disconnect, reason}
  end

  # A sink-owned adapter persists its own `:backfill_pending` state before
  # Replicant creates the slot. Lib mode needs the same durable third state in
  # its progress table, written after slot creation and before either the reader
  # or stream can commit. A first real chunk replaces this marker atomically.
  defp persist_backfill_pending(%{checkpoint_store: store, slot_name: slot})
       when is_list(store) do
    Replicant.CheckpointStore.write_progress(
      Replicant.CheckpointStore.via(slot),
      Replicant.SnapshotProgress.pending_store_token()
    )
  end

  defp persist_backfill_pending(_state), do: :ok

  # Reused incremental slot with an armed backfill but no first chunk token:
  # read the same effective origin PostgreSQL will stream from, then rediscover
  # the publication and restart a fresh reader at that floor.
  defp read_backfill_origin(state) do
    {:ok, sql} = QueryBuilder.slot_confirmed_flush(state.slot_name)
    {:query, sql, %{state | step: :read_backfill_origin}}
  end

  # ---- R04 slot-origin exposure (go-forward append consumers) ----

  # True when the sink implements handle_slot_origin/2. Gates BOTH the reused-slot confirmed_flush
  # read and (via notify_new_slot_origin/2) the new-slot exposure, so a sink without the callback
  # keeps the prior connect behavior exactly.
  defp wants_slot_origin?(%{sink: sink}), do: Replicant.Sink.supports_slot_origin?(sink)

  # Reused slot: issue the confirmed_flush_lsn read (only ever called when wants_slot_origin?).
  defp read_slot_origin(state) do
    {:ok, sql} = QueryBuilder.slot_confirmed_flush(state.slot_name)
    {:query, sql, %{state | step: :read_slot_origin}}
  end

  # New slot: notify with the CREATE consistent_point (reused?: false). A no-op :ok when the sink
  # does not implement the callback (parse is skipped — the prior struct-only path).
  defp notify_new_slot_origin(state, result) do
    if wants_slot_origin?(state) do
      with {:ok, origin} <- parse_consistent_point(result.rows) do
        Replicant.Sink.notify_slot_origin(state.sink, origin, %{
          slot_name: state.slot_name,
          reused?: false
        })
      end
    else
      :ok
    end
  end

  # Fail-closed halt when the database cannot supply a valid origin or the consumer vetoes it.
  # Both reasons are structural and value-free; the database value and sink reason are discarded.
  # Stay idle while the asynchronous supervisor teardown lands: returning :disconnect here would
  # let auto-reconnect re-enter the same terminal fault and eventually obscure this halt reason.
  defp halt_slot_origin(state, reason)
       when reason in [:slot_origin_unavailable, :slot_origin_rejected] do
    Telemetry.event([:replicant, :connection, :slot_invalidated], %{}, %{
      reason: reason
    })

    Replicant.Supervisor.halt(state.slot_name, {:slot_origin, reason})
    {:noreply, state}
  end

  # The CREATE_REPLICATION_SLOT result's consistent_point (row 2 of `[slot, cp, snap, plugin]`;
  # NOEXPORT sets snap = nil). Any other shape fails closed; 0 is not a fabricated origin.
  defp parse_consistent_point([[_slot, cp, _snap, _plugin]]) when is_binary(cp),
    do: parse_slot_origin(cp)

  defp parse_consistent_point(_rows), do: {:error, :slot_origin_unavailable}

  # A logical slot's confirmed_flush_lsn is pg_lsn text. NULL identifies no usable logical origin
  # (PostgreSQL documents it for physical slots); a missing row can also mean the slot vanished
  # between checks. Both, plus malformed text, fail closed rather than fabricating origin 0.
  defp parse_confirmed_flush([[lsn]]) when is_binary(lsn) and lsn != "",
    do: parse_slot_origin(lsn)

  defp parse_confirmed_flush(_rows), do: {:error, :slot_origin_unavailable}

  defp parse_slot_origin(lsn) do
    with [upper, lower] <- String.split(lsn, "/", parts: 2),
         true <- valid_lsn_component?(upper),
         true <- valid_lsn_component?(lower),
         {:ok, lsn_int} <- Replicant.lsn_from_string(lsn) do
      {:ok, lsn_int}
    else
      _ -> {:error, :slot_origin_unavailable}
    end
  end

  defp valid_lsn_component?(component) when byte_size(component) in 1..8,
    do: String.match?(component, ~r/\A[0-9A-Fa-f]+\z/)

  defp valid_lsn_component?(_component), do: false

  # Definitive checkpoint read for the connect decision. Distinguishes a durable value
  # (:present) from a genuine first-run/empty (:empty) from a read fault (:fault).
  #
  # Lib mode (a `:checkpoint_store` keyword is present): read the lib-owned store — the
  # connect authority. A store read FAULT halts fail-closed (handled in
  # :invalidation_check) — NOT fail-open: a non-idempotent lib-mode sink cannot dedup a
  # resume-from-0. Sink-owned mode: unchanged (reads `sink.checkpoint()`); only snapshot
  # mode acts on :fault (halt), the streaming/go-forward path treats :fault as 0
  # (fail-open, §14.15) via checkpoint_lsn.
  defp read_checkpoint(%{checkpoint_store: store, slot_name: slot}) when is_list(store) do
    read_checkpoint_result(Replicant.CheckpointStore.read(Replicant.CheckpointStore.via(slot)))
  end

  defp read_checkpoint(%{sink: sink}) do
    case safe_checkpoint(sink) do
      {:ok, lsn} when is_integer(lsn) and lsn > 0 -> {:present, lsn}
      {:ok, _nil_or_zero} -> {:empty, 0}
      _fault -> {:fault, 0}
    end
  end

  @doc false
  @spec read_checkpoint_result(term()) ::
          {:present | :empty | :fault | :fault_permanent, Replicant.lsn()}
  def read_checkpoint_result({:ok, lsn}) when is_integer(lsn) and lsn > 0, do: {:present, lsn}
  def read_checkpoint_result({:ok, _nil_or_zero}), do: {:empty, 0}

  def read_checkpoint_result({:error, %Replicant.Error{reason: reason}}) do
    if Replicant.CheckpointStore.permanent_reason?(reason),
      do: {:fault_permanent, 0},
      else: {:fault, 0}
  end

  def read_checkpoint_result({:error, _other}), do: {:fault, 0}

  defp safe_checkpoint(sink) do
    sink.checkpoint()
  rescue
    _ -> :read_fault
  catch
    _kind, _reason -> :read_fault
  end

  defp recovery_kind(true), do: :standby
  defp recovery_kind(_other), do: :primary
end
