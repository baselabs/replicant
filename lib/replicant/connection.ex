defmodule Replicant.Connection do
  @moduledoc """
  The `Postgrex.ReplicationConnection` that owns the replication slot and closes
  the exactly-once seam (spec §2/§4/§8). It:

    * replies to a reply-requested keepalive with the **last durably-checkpointed LSN**
      while any published transaction is in flight — never advancing the slot past
      un-persisted publication data (walex's fire-and-forget `wal_end+1` is the
      at-most-once bug this fixes). When IDLE (no open transaction and the checkpoint
      caught up), it advances the slot to the keepalive `wal_end` so a quiet-but-filtered
      publication stops pinning WAL (spec A1) — the intervening WAL carries nothing for
      the publication;
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
  `:recovery_check` → `:invalidation_check` → (`:create_slot` if absent) → `:streaming`.

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

  @type step ::
          :disconnected
          | :recovery_check
          | :invalidation_check
          | :create_slot
          | :create_export_slot
          | :create_incremental_slot
          | :snapshotting
          | :streaming

  @type t :: %__MODULE__{
          slot_name: String.t(),
          publication: String.t(),
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
          frontier_epoch: non_neg_integer(),
          backfill_floor: Replicant.lsn() | nil,
          last_frontier_cast: non_neg_integer(),
          reader_pid: pid() | nil,
          step: step()
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
    frontier_epoch: 0,
    last_frontier_cast: 0,
    reader_pid: nil,
    step: :disconnected
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
       step: :disconnected
     }}
  end

  @impl true
  def handle_connect(state) do
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
         # `frontier_epoch` is KEPT across (re)connect (monotonic; only `start_streaming`
         # bumps it) so a fresh window always adopts a strictly-higher epoch than any
         # in-flight pre-reconnect frontier cast (85672f1 stale-epoch class). The per-stream
         # rate-limit anchor resets to 0 so the first frame of the new stream can cast.
         frontier_epoch: state.frontier_epoch,
         last_frontier_cast: 0,
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
    {:noreply, %{state | step: :disconnected}}
  end

  @impl true
  def handle_result(
        [%Postgrex.Result{rows: [[in_recovery, version]]}],
        %{step: :recovery_check} = state
      ) do
    Telemetry.event([:replicant, :connection, :connected], %{}, %{
      kind: recovery_kind(in_recovery)
    })

    state = %{state | server_version_num: version, in_recovery: in_recovery}
    {:ok, sql} = QueryBuilder.slot_invalidation_status(state.slot_name)
    {:query, sql, %{state | step: :invalidation_check}}
  end

  def handle_result([%Postgrex.Result{rows: rows}], %{step: :invalidation_check} = state) do
    cond do
      lib_mode?(state) and state.checkpoint_state == :fault_permanent ->
        halt_store_permanent(state)

      lib_mode?(state) and state.checkpoint_state == :fault ->
        pace_store_retry(state)

      lib_mode?(state) and lib_go_forward_violation?(state) ->
        Replicant.Supervisor.halt(state.slot_name, {:config, :go_forward_required})
        {:disconnect, :go_forward_required}

      true ->
        classify_and_begin(rows, state)
    end
  end

  def handle_result([%Postgrex.Result{}], %{step: :create_slot} = state) do
    Telemetry.event([:replicant, :connection, :slot_active], %{}, %{})
    start_streaming(state)
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
    cp = Replicant.lsn_from_string(consistent_point)

    Replicant.Snapshotter.start(%{
      snapshot_name: snapshot_name,
      consistent_point: cp,
      connection: state.connection,
      publication: state.publication,
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
    floor = Replicant.lsn_from_string(consistent_point)
    Telemetry.event([:replicant, :connection, :slot_active], %{}, %{})
    Telemetry.event([:replicant, :snapshot, :started], %{}, %{commit_lsn: floor})
    state = %{state | backfill_floor: floor}
    start_streaming_with_backfill(state, nil)
  end

  def handle_result(%Postgrex.Error{}, _state) do
    # A replication-command error (value-bearing message never inspected). Disconnect;
    # auto_reconnect re-runs the connect chain.
    {:disconnect, :query_error}
  end

  def handle_result(_result, _state), do: {:disconnect, :unexpected_result}

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
    state = %{state | received_lsn: received_lsn, stream_floor_lsn: stream_floor_lsn}

    # Whenever the pipeline is in incremental MODE (incremental?/1 tests is_list(snapshot) and
    # stays true for the whole pipeline lifetime, not only during an active backfill), forward
    # the XLogData frontier so a pending chunk window can close on a busy-but-publication-filtered
    # source (spec §2; plan F4: such a source never applies commits, so keepalive-only forwarding
    # would stall closure to the ~30 s cadence). Post-completion the cast is a cheap no-op (the
    # assembler's pop_ready returns :none). Rate-limited to ≥ 1 MiB of advance so the hot path
    # stays cheap. Epoch-tagged with the CURRENT window epoch so a stale pre-reconnect cast can
    # never close a fresh window (85672f1 class).
    state =
      if incremental?(state) and wal_end - state.last_frontier_cast >= 1_048_576 do
        GenServer.cast(
          AssemblerServer.via(state.slot_name),
          {:snapshot_frontier, state.frontier_epoch, wal_end}
        )

        %{state | last_frontier_cast: wal_end}
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
  # keepalive_ack/3 — idle-advance the slot to wal_end, else reply with the durable checkpoint
  # (never the received wal_end).
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
        {:snapshot_frontier, state.frontier_epoch, wal_end}
      )
    end

    keepalive_ack(wal_end, reply, state)
  end

  def handle_data(_other, state), do: {:noreply, state}

  @impl true
  # Async ack: the AssemblerServer durably committed a txn ending at `lsn`.
  # Advance monotonically and report the new flush position.
  def handle_info({:sink_committed, lsn}, state) when is_integer(lsn) do
    checkpoint = max(state.checkpoint_lsn, lsn)
    Telemetry.event([:replicant, :checkpoint, :advanced], %{}, %{commit_lsn: checkpoint})
    {:noreply, [encode_status_update(checkpoint)], %{state | checkpoint_lsn: checkpoint}}
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

        {:ok, sql} =
          QueryBuilder.start_replication(state.slot_name, state.publication,
            start_lsn: lsn,
            streaming: streaming?(state)
          )

        {:stream, sql, [],
         %{
           state
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

  def handle_info(_other, state), do: {:noreply, state}

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

  # On a keepalive: when IDLE (no open transaction AND every received commit durable) and the
  # server WAL end is ahead of our durable checkpoint, advance the slot to `wal_end` (spec A1
  # §3.1). The intervening WAL is provably empty for this publication (idle ⟹ no in-flight
  # published change), so this releases WAL pinned on a quiet publication WITHOUT ever acking
  # past un-persisted publication data. Fires regardless of the reply-request flag so a
  # busy-but-filtered source releases WAL promptly. When NOT idle, behavior is unchanged:
  # reply==1 acks the durable checkpoint, reply==0 sends nothing.
  defp keepalive_ack(wal_end, reply, state) do
    if idle?(state) and wal_end > state.checkpoint_lsn do
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
  Classify a `pg_replication_slots` invalidation-status result (spec §8, PG16
  columns): `[]` → `:absent`; `wal_status = "lost"` → `{:invalidated, :wal_lost}`;
  `conflicting = true` → `{:invalidated, :conflict}`; otherwise `:ok`.
  """
  @spec classify_slot_status([[term()]]) :: :absent | :ok | {:invalidated, :wal_lost | :conflict}
  def classify_slot_status([]), do: :absent

  def classify_slot_status([[wal_status, conflicting] | _rest]) do
    cond do
      wal_status == "lost" -> {:invalidated, :wal_lost}
      conflicting == true -> {:invalidated, :conflict}
      true -> :ok
    end
  end

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
  @spec classify_progress({:ok, binary() | nil} | {:error, term()}) ::
          :none | :complete | {:in_flight, Replicant.SnapshotProgress.t()} | :fault
  def classify_progress({:ok, nil}), do: :none

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
        {:noreply, state}

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
    case Decoder.decode(payload, streaming: state.in_stream) do
      {:ok, message} ->
        GenServer.cast(
          AssemblerServer.via(state.slot_name),
          {:message, message, byte_size(payload), self()}
        )

        # ORDERING INVARIANT (spec A1 §3.2): track_txn runs HERE, when the Connection forwards
        # each message in WAL order, so a Begin sets in_txn BEFORE any following keepalive is
        # handled — an idle-ack can never fire while an open transaction has been received.
        # Moving this off the per-message forward path, or dropping a boundary clause, opens a
        # silent-loss window. update_in_stream tracks the decode frame; track_txn the txn.
        {:noreply, state |> update_in_stream(message) |> track_txn(message)}

      {:error, error} ->
        Replicant.Supervisor.halt(state.slot_name, error)
        {:disconnect, :decode_failure}
    end
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
    state = %{state | frontier_epoch: next_frontier_epoch(state)}
    prime_assembler(state, state.checkpoint_lsn)

    {:ok, sql} =
      QueryBuilder.start_replication(state.slot_name, state.publication,
        start_lsn: state.checkpoint_lsn,
        streaming: streaming?(state)
      )

    {:stream, sql, [], %{state | step: :streaming, in_stream: false}}
  end

  # The window epoch this (re)connect adopts: bumped once per incremental (re)connect so a fresh
  # window strictly outranks any in-flight pre-reconnect frontier cast; unchanged (0) otherwise.
  defp next_frontier_epoch(state) do
    if incremental?(state), do: state.frontier_epoch + 1, else: state.frontier_epoch
  end

  # Incremental backfill start (spec §8): re-seat the window at a fresh epoch + seed the floor on
  # the assembler, spawn_link the chunk reader (Snapshotter §17 precedent — dies with this
  # Connection), then stream. The window reset + floor cast BEFORE the reader spawns so the reader
  # never opens a chunk window against a stale epoch. `sp` nil = fresh run (the reader discovers
  # tables + builds the token); non-nil = resume from the durable token.
  #
  # frontier_epoch stays SINGLE-WRITER: this reset uses `state.frontier_epoch + 1`, and
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
    GenServer.cast(server, {:reset_snapshot_window, state.frontier_epoch + 1})
    GenServer.cast(server, {:snapshot_floor, floor})

    reader_pid =
      Incremental.start(%{
        slot_name: state.slot_name,
        connection: state.connection,
        publication: state.publication,
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
          {:reset_snapshot_window, state.frontier_epoch}
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
  @spec read_progress(map()) :: {:ok, binary() | nil} | {:error, term()}
  def read_progress(%{checkpoint_store: store, slot_name: slot}) when is_list(store),
    do: Replicant.CheckpointStore.read_progress(Replicant.CheckpointStore.via(slot))

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

  # ---- incremental snapshot (spec §8 of the incremental design) ----
  # These clauses come FIRST; the `is_list/1` guard keeps them disjoint from the v1
  # `snapshot: true` clauses and the non-snapshot catch-all below.

  # Slot ABSENT. Progress in flight/complete + an absent slot = the stream gap between the
  # backfill floor and now is unfillable (deletes lost) → :data_gap (the §14.19 family). No
  # progress → a genuine fresh run: create the NOEXPORT slot. A decode/read fault → fail-closed.
  defp begin_absent_slot(%{snapshot: s} = state) when is_list(s) do
    case classify_progress(read_progress(state)) do
      :none ->
        {:ok, sql} = QueryBuilder.create_durable_slot(state.slot_name)
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

  defp begin_absent_slot(state) do
    {:ok, sql} = QueryBuilder.create_durable_slot(state.slot_name)
    {:query, sql, %{state | step: :create_slot}}
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
    # :present (resume) or non-snapshot go-forward with an existing slot — unchanged.
    Telemetry.event([:replicant, :connection, :slot_active], %{}, %{})
    start_streaming(state)
  end

  defp create_export_slot(state) do
    {:ok, sql} = QueryBuilder.create_export_slot(state.slot_name)
    {:query, sql, %{state | step: :create_export_slot}}
  end

  # Fail-closed snapshot halt (spec §8) — never auto-drops a slot.
  defp halt_snapshot(state, reason) do
    Telemetry.event([:replicant, :snapshot, :failed], %{}, %{reason: reason})
    Replicant.Supervisor.halt(state.slot_name, {:snapshot, reason})
    {:disconnect, reason}
  end

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
