defmodule Replicant.Connection do
  @moduledoc """
  The `Postgrex.ReplicationConnection` that owns the replication slot and closes
  the exactly-once seam (spec §2/§4/§8). It:

    * replies to every reply-requested keepalive with the **last durably-checkpointed
      LSN** as the flush position — never the received `wal_end` (walex's
      fire-and-forget `wal_end+1` is the at-most-once bug this fixes), so the slot
      never advances past un-persisted data;
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
          | :snapshotting
          | :streaming

  @type t :: %__MODULE__{
          slot_name: String.t(),
          publication: String.t(),
          sink: module(),
          go_forward_only: boolean(),
          snapshot: boolean(),
          connection: keyword(),
          checkpoint_lsn: Replicant.lsn(),
          checkpoint_state: :present | :empty | :fault,
          received_lsn: Replicant.lsn(),
          stream_floor_lsn: Replicant.lsn() | nil,
          max_inflight_lag: pos_integer(),
          checkpoint_store: keyword() | nil,
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
    checkpoint_lsn: 0,
    checkpoint_state: :empty,
    received_lsn: 0,
    max_inflight_lag: @default_max_inflight_lag,
    checkpoint_store: nil,
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
       checkpoint_store: Map.get(config, :checkpoint_store),
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

    # Reset the in-flight window on (re)connect. `stream_floor_lsn` is re-derived from
    # the FIRST XLogData frame of the new stream (see `inflight_lag/1`) — PG clamps
    # `START_REPLICATION` to the slot's server-side `confirmed_flush_lsn`, which for a
    # fresh/empty checkpoint is the slot-creation LSN (a large absolute value), NOT 0.
    # Measuring lag against that per-stream floor (never against absolute 0) is what
    # keeps the very first frame from reading as a ~50 MB false "lag".
    {:query, QueryBuilder.is_in_recovery(),
     %{
       state
       | checkpoint_lsn: checkpoint_lsn,
         checkpoint_state: checkpoint_state,
         received_lsn: checkpoint_lsn,
         stream_floor_lsn: nil,
         step: :recovery_check
     }}
  end

  @impl true
  def handle_disconnect(state) do
    Telemetry.event([:replicant, :connection, :disconnected], %{}, %{})
    {:noreply, %{state | step: :disconnected}}
  end

  @impl true
  def handle_result([%Postgrex.Result{rows: [[in_recovery]]}], %{step: :recovery_check} = state) do
    Telemetry.event([:replicant, :connection, :connected], %{}, %{
      kind: recovery_kind(in_recovery)
    })

    {:ok, sql} = QueryBuilder.slot_invalidation_status(state.slot_name)
    {:query, sql, %{state | step: :invalidation_check}}
  end

  def handle_result([%Postgrex.Result{rows: rows}], %{step: :invalidation_check} = state) do
    cond do
      lib_mode?(state) and state.checkpoint_state == :fault ->
        lib_store_fault(state)

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
      reply_to: self()
    })

    {:noreply, %{state | step: :snapshotting}}
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
    received_lsn = max(state.received_lsn, wal_end)
    state = %{state | received_lsn: received_lsn, stream_floor_lsn: stream_floor_lsn}
    lag = inflight_lag(state)

    if lag > state.max_inflight_lag do
      halt_sink_too_slow(state, lag)
    else
      forward_message(payload, state)
    end
  end

  # Primary keepalive: reply with the durable checkpoint ONLY when a reply is
  # requested (reply == 1). Never the received wal_end.
  def handle_data(<<?k, _wal_end::64, _clock::64, reply::8>>, state) do
    if reply == 1 do
      {:noreply, [encode_status_update(state.checkpoint_lsn)], state}
    else
      {:noreply, state}
    end
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
  # over the link, so this graceful message still drives the handoff.
  def handle_info({:snapshot_done, lsn}, %{step: :snapshotting} = state) when is_integer(lsn) do
    Telemetry.event([:replicant, :connection, :slot_active], %{}, %{})

    {:ok, sql} =
      QueryBuilder.start_replication(state.slot_name, state.publication, start_lsn: lsn)

    {:stream, sql, [],
     %{state | step: :streaming, checkpoint_lsn: lsn, received_lsn: lsn, stream_floor_lsn: nil}}
  end

  def handle_info({:snapshot_failed, _error}, %{step: :snapshotting} = state) do
    Replicant.Supervisor.halt(state.slot_name, {:snapshot, :snapshot_failed})
    {:disconnect, :snapshot_failed}
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
  @spec lib_go_forward_violation?(map()) :: boolean()
  def lib_go_forward_violation?(%{
        checkpoint_state: :empty,
        sink: sink,
        go_forward_only: false,
        snapshot: false
      }),
      do: Replicant.Sink.sink_kind(sink) == :state_mirror

  def lib_go_forward_violation?(_), do: false

  # ---- private ----

  # A lib-mode store read FAULT at connect is potentially TRANSIENT — the store's
  # non-sync Postgrex may still be connecting, or a brief blip. Do NOT permanently
  # halt: disconnect so `auto_reconnect` re-runs the connect (re-reading the store). A
  # transient blip self-heals on the next connect; we never stream past an unknown
  # checkpoint (fail-closed BY DESIGN). Distinct from a WRITE fault mid-stream, which
  # halts permanently (Task 5). Matches spec §8: a transient blip is absorbed, not halted.
  #
  # UNPACED under a PERSISTENT store outage: the store is a SEPARATE Postgrex connection.
  # This replication connection's own `Protocol.connect` still SUCCEEDS every attempt (PG
  # is up; only the store is down), so Postgrex's `reconnect_backoff` timer never arms —
  # there is NO backoff between attempts. The connect → read-store → :fault → disconnect
  # loop therefore spins with no pacing (bounded only by the per-attempt TCP connect +
  # recovery/invalidation queries), surfacing this telemetry each turn. This is fail-closed
  # by design (loss=0), NOT a paced retry. Bounded-retry-then-halt is the named §14.18
  # future (deliberately out of this plan's scope).
  defp lib_store_fault(state) do
    Telemetry.event([:replicant, :checkpoint_store, :failed], %{}, %{
      slot_name: state.slot_name,
      reason: :checkpoint_store_failed
    })

    {:disconnect, :checkpoint_store_unavailable}
  end

  # In-flight WAL lag (bytes): received frontier minus the confirmed-durable floor.
  # The floor is the higher of the durable `checkpoint_lsn` (once a commit advances
  # it to a real absolute LSN) and the per-stream `stream_floor_lsn` (the position PG
  # began streaming at, used before the first commit while checkpoint is still 0).
  # A cheap two-integer subtraction — safe to call on the keepalive-free hot path.
  defp inflight_lag(%{received_lsn: received, checkpoint_lsn: cp, stream_floor_lsn: floor}) do
    received - max(cp, floor || received)
  end

  defp forward_message(payload, state) do
    case Decoder.decode(payload) do
      {:ok, message} ->
        GenServer.cast(
          AssemblerServer.via(state.slot_name),
          {:message, message, byte_size(payload), self()}
        )

        {:noreply, state}

      {:error, error} ->
        Replicant.Supervisor.halt(state.slot_name, error)
        {:disconnect, :decode_failure}
    end
  end

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
    if lib_mode?(state), do: seed_assembler(state, state.checkpoint_lsn)

    {:ok, sql} =
      QueryBuilder.start_replication(state.slot_name, state.publication,
        start_lsn: state.checkpoint_lsn
      )

    {:stream, sql, [], %{state | step: :streaming}}
  end

  # Seed the lib-mode watermark from the SAME store read the connect used, before any
  # Commit cast — this bounds the resume dup to a single transaction (the store
  # checkpoint, ahead of the slot's confirmed_flush, otherwise re-streams N txns
  # un-deduped). A no-op in sink-owned mode (never called; the assembler ignores
  # lib_checkpoint there).
  defp seed_assembler(state, lsn) when is_integer(lsn) do
    GenServer.cast(AssemblerServer.via(state.slot_name), {:seed_lib_checkpoint, lsn})
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

  # Slot ABSENT, checkpoint not durable (empty or fault-as-0).
  defp begin_absent_slot(%{snapshot: true, checkpoint_state: :fault} = state),
    do: halt_snapshot(state, :checkpoint_unreadable)

  defp begin_absent_slot(%{snapshot: true} = state), do: create_export_slot(state)

  defp begin_absent_slot(state) do
    {:ok, sql} = QueryBuilder.create_durable_slot(state.slot_name)
    {:query, sql, %{state | step: :create_slot}}
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
    case Replicant.CheckpointStore.read(Replicant.CheckpointStore.via(slot)) do
      {:ok, lsn} when is_integer(lsn) and lsn > 0 -> {:present, lsn}
      {:ok, _nil_or_zero} -> {:empty, 0}
      {:error, _} -> {:fault, 0}
    end
  end

  defp read_checkpoint(%{sink: sink}) do
    case safe_checkpoint(sink) do
      {:ok, lsn} when is_integer(lsn) and lsn > 0 -> {:present, lsn}
      {:ok, _nil_or_zero} -> {:empty, 0}
      _fault -> {:fault, 0}
    end
  end

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
