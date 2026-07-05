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
    * advances the ack asynchronously on `{:sink_committed, L}` from the AssemblerServer;
    * on connect/reconnect detects an invalidated slot (`wal_status = 'lost'` or
      `conflicting` on PG16) and halts the pipeline fail-closed — never silently
      dropping and recreating the slot (spec §8 R-ISO).

  The connect chain is a `handle_result/2` state machine:
  `:recovery_check` → `:invalidation_check` → (`:create_slot` if absent) → `:streaming`.
  """
  use Postgrex.ReplicationConnection

  alias Replicant.{AssemblerServer, Decoder, QueryBuilder, Telemetry}

  @pg_epoch DateTime.to_unix(~U[2000-01-01 00:00:00Z], :microsecond)

  @type step :: :disconnected | :recovery_check | :invalidation_check | :create_slot | :streaming

  @type t :: %__MODULE__{
          slot_name: String.t(),
          publication: String.t(),
          sink: module(),
          go_forward_only: boolean(),
          checkpoint_lsn: Replicant.lsn(),
          step: step()
        }

  defstruct [
    :slot_name,
    :publication,
    :sink,
    :go_forward_only,
    checkpoint_lsn: 0,
    step: :disconnected
  ]

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

    {:query, QueryBuilder.is_in_recovery(),
     %{state | checkpoint_lsn: checkpoint_lsn, step: :recovery_check}}
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
  # XLogData: decode behind the value-free boundary, forward the decoded message
  # + raw byte-size to the AssemblerServer. A decode error halts fail-closed.
  def handle_data(<<?w, _wal_start::64, _wal_end::64, _clock::64, payload::binary>>, state) do
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
