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

  @type step :: :disconnected | :recovery_check | :invalidation_check | :create_slot | :streaming

  @type t :: %__MODULE__{
          slot_name: String.t(),
          publication: String.t(),
          sink: module(),
          go_forward_only: boolean(),
          checkpoint_lsn: Replicant.lsn(),
          received_lsn: Replicant.lsn(),
          stream_floor_lsn: Replicant.lsn() | nil,
          max_inflight_lag: pos_integer(),
          step: step()
        }

  defstruct [
    :slot_name,
    :publication,
    :sink,
    :go_forward_only,
    :stream_floor_lsn,
    checkpoint_lsn: 0,
    received_lsn: 0,
    max_inflight_lag: @default_max_inflight_lag,
    step: :disconnected
  ]

  @doc "The default in-flight-lag ceiling (WAL bytes) when the config omits it."
  @spec default_max_inflight_lag() :: pos_integer()
  def default_max_inflight_lag, do: @default_max_inflight_lag

  @doc "Start the replication connection for a pipeline (called by `Replicant.Pipeline`)."
  @spec start_link(Replicant.Config.t()) :: {:ok, pid()} | {:error, term()}
  def start_link(config) do
    opts =
      config.connection ++
        [name: via(config.slot_name), sync_connect: false, auto_reconnect: true]

    Postgrex.ReplicationConnection.start_link(__MODULE__, config, opts)
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
       checkpoint_lsn: 0,
       received_lsn: 0,
       stream_floor_lsn: nil,
       max_inflight_lag: Map.get(config, :max_inflight_lag, @default_max_inflight_lag),
       step: :disconnected
     }}
  end

  @impl true
  def handle_connect(state) do
    # Read the durable checkpoint (off the keepalive path — a blocking sink call is
    # fine here). A read fault is fail-open (§14.15): resume from 0, the idempotent
    # sink dedups the re-stream. This value seeds every keepalive ack until the
    # first async advance.
    checkpoint_lsn = read_checkpoint(state.sink)

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
    case classify_slot_status(rows) do
      :absent ->
        {:ok, sql} = QueryBuilder.create_durable_slot(state.slot_name)
        {:query, sql, %{state | step: :create_slot}}

      :ok ->
        Telemetry.event([:replicant, :connection, :slot_active], %{}, %{})
        start_streaming(state)

      {:invalidated, reason} ->
        Telemetry.event([:replicant, :connection, :slot_invalidated], %{}, %{reason: reason})
        Replicant.Supervisor.halt(state.slot_name, {:slot_invalidated, reason})
        {:disconnect, :slot_invalidated}
    end
  end

  def handle_result([%Postgrex.Result{}], %{step: :create_slot} = state) do
    Telemetry.event([:replicant, :connection, :slot_active], %{}, %{})
    start_streaming(state)
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

  # ---- private ----

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
    {:ok, sql} =
      QueryBuilder.start_replication(state.slot_name, state.publication,
        start_lsn: state.checkpoint_lsn
      )

    {:stream, sql, [], %{state | step: :streaming}}
  end

  defp read_checkpoint(sink) do
    case safe_checkpoint(sink) do
      {:ok, lsn} when is_integer(lsn) -> lsn
      _other -> 0
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
