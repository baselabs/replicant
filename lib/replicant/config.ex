defmodule Replicant.Config do
  @moduledoc """
  Validates `Replicant.start_link/1` options and enforces the **go-forward-only
  start guard**: a `:state_mirror` sink resuming from an empty
  checkpoint without `go_forward_only: true` would silently deliver partial data
  from the slot's creation point, so it is refused at start.

  Pure — no processes, no connection. `Replicant.start_link/1` (the facade) calls
  `validate/1` then `guard/1` before spawning a pipeline.
  """

  alias Replicant.{CheckpointStore, Connection, Identifier, Sink}

  @type t :: %{
          optional(:batch) => keyword() | nil,
          optional(:batch_delivery) => keyword() | nil,
          optional(:streaming) => keyword() | nil,
          connection: keyword(),
          slot_name: String.t(),
          publication: String.t(),
          sink: module(),
          go_forward_only: boolean(),
          snapshot: boolean() | keyword(),
          max_inflight_lag: pos_integer(),
          checkpoint_store: keyword() | nil
        }

  @doc """
  Validate raw `start_link` options into a normalised config map, or return a
  plain-atom error: `:config_invalid` (missing/mis-shaped connection or opts),
  `:invalid_identifier` (slot/publication fails the Postgres-identifier
  allowlist), `:invalid_sink` (sink is not a module exporting the two mandatory
  callbacks), `:conflicting_start_mode` (`go_forward_only: true` AND any snapshot
  intent — `snapshot: true` OR `snapshot: [mode: :incremental]` — mutually
  exclusive start intents), `:snapshot_unsupported` (a snapshot intent whose sink
  is missing the required callbacks for that mode: `snapshot: true` needs both v1
  callbacks; sink-owned `snapshot: [mode: :incremental]` needs `handle_snapshot/2`
  + `snapshot_progress/0`), `:batch_unsupported` (`batch_delivery` is set but the
  sink does not implement `handle_batch/1`). A `snapshot` value that is neither a
  boolean nor a `[mode: :incremental, ...]` keyword (or has non-positive knobs) is
  `:config_invalid`.
  """
  @spec validate(keyword()) ::
          {:ok, t()}
          | {:error,
             :config_invalid
             | :invalid_identifier
             | :invalid_sink
             | :conflicting_start_mode
             | :snapshot_unsupported
             | :batch_unsupported}
  def validate(opts) when is_list(opts) do
    with {:ok, connection} <- fetch_connection(opts),
         {:ok, slot_name} <- fetch_identifier(opts, :slot_name),
         {:ok, publication} <- fetch_identifier(opts, :publication),
         {:ok, checkpoint_store} <- fetch_checkpoint_store(opts),
         {:ok, max_inflight_lag} <- fetch_max_inflight_lag(opts),
         {:ok, batch_delivery} <- fetch_batch_delivery(opts, checkpoint_store, max_inflight_lag),
         {:ok, sink} <- fetch_sink(opts, checkpoint_store != nil, batch_delivery != nil),
         {:ok, batch} <- fetch_batch(opts, checkpoint_store, max_inflight_lag),
         {:ok, streaming} <- fetch_streaming(opts, max_inflight_lag),
         go_forward_only = Keyword.get(opts, :go_forward_only, false) == true,
         {:ok, snapshot} <- fetch_snapshot(opts),
         :ok <- validate_start_mode(go_forward_only, snapshot),
         :ok <- validate_snapshot_support(snapshot, sink, checkpoint_store != nil),
         :ok <- validate_lib_batch_snapshot(batch, snapshot) do
      {:ok,
       %{
         connection: connection,
         slot_name: slot_name,
         publication: publication,
         sink: sink,
         go_forward_only: go_forward_only,
         snapshot: snapshot,
         max_inflight_lag: max_inflight_lag,
         checkpoint_store: checkpoint_store,
         batch: batch,
         batch_delivery: batch_delivery,
         streaming: streaming
       }}
    end
  end

  def validate(_opts), do: {:error, :config_invalid}

  @doc """
  The go-forward-only start guard. Refuses ONLY the exact unsafe triple — a
  `:state_mirror` sink (default kind) with a **definitively empty** checkpoint
  (`{:ok, nil}`) and `go_forward_only: false`. An `:append_log` sink, a non-nil
  checkpoint, `go_forward_only: true`, OR a checkpoint READ fault (raise/exit/
  `{:error, _}`) all pass — the read-fault path is deliberately fail-open (a
  re-dispatched already-persisted txn is deduped by the idempotent sink; only a
  definitive empty checkpoint proves partial-delivery risk).
  `snapshot: true` ALSO bypasses the empty-checkpoint refusal (alongside
  `go_forward_only: true`): the backfill IS the safe seed, so an empty checkpoint
  is the expected first-run state, not a partial-delivery risk.
  """
  @spec guard(t()) :: :ok | {:error, :go_forward_required}
  # Lib mode (a present :checkpoint_store): the library owns the checkpoint, so the
  # empty-checkpoint go-forward enforcement moves to connect time — the guard defers here.
  def guard(%{checkpoint_store: store}) when is_list(store), do: :ok

  def guard(%{sink: sink, go_forward_only: go_forward_only, snapshot: snapshot}) do
    cond do
      go_forward_only == true -> :ok
      snapshot != false -> :ok
      Sink.sink_kind(sink) != :state_mirror -> :ok
      checkpoint_definitively_empty?(sink) -> {:error, :go_forward_required}
      true -> :ok
    end
  end

  defp checkpoint_definitively_empty?(sink) do
    case safe_checkpoint(sink) do
      {:ok, nil} -> true
      _other -> false
    end
  end

  # A checkpoint read fault (raise/exit) is fail-open (§14.15) and value-free: we
  # never inspect the reason. Return a sentinel that is NOT {:ok, nil}, so the
  # guard treats "unknown" as "not definitively empty" → allow start.
  defp safe_checkpoint(sink) do
    sink.checkpoint()
  rescue
    _ -> :read_fault
  catch
    _kind, _reason -> :read_fault
  end

  # snapshot: false | true (v1) | [mode: :incremental, chunk_rows: n, max_pending_chunks: m]
  # (spec §6.5). Knobs default 1000 / 4; both positive integers. Unknown mode or any other
  # shape → :config_invalid, never a silent fallback. The drop-set cap and pacing gate are
  # DERIVED downstream (10 × chunk_rows; max_inflight_lag ÷ 2) — never user knobs.
  defp fetch_snapshot(opts) do
    case Keyword.get(opts, :snapshot, false) do
      bool when is_boolean(bool) ->
        {:ok, bool}

      kw when is_list(kw) ->
        chunk_rows = Keyword.get(kw, :chunk_rows, 1000)
        max_pending = Keyword.get(kw, :max_pending_chunks, 4)

        if Keyword.get(kw, :mode) == :incremental and positive_integer?(chunk_rows) and
             positive_integer?(max_pending) do
          {:ok, [mode: :incremental, chunk_rows: chunk_rows, max_pending_chunks: max_pending]}
        else
          {:error, :config_invalid}
        end

      _other ->
        {:error, :config_invalid}
    end
  end

  # go_forward_only and ANY snapshot intent (v1 or incremental) are mutually exclusive.
  defp validate_start_mode(true, snapshot) when snapshot != false,
    do: {:error, :conflicting_start_mode}

  defp validate_start_mode(_gfo, _snapshot), do: :ok

  # Lib-mode batched checkpointing (`checkpoint_store: [batch: …]`) combined with an incremental
  # snapshot is REFUSED fail-closed (`:config_invalid`, consistent with the batch_delivery +
  # checkpoint_store mutually-exclusive-mode rejection above). In lib+batch every buffered txn is
  # delivered-then-DISCARDED (`AssemblerServer.buffered_changes/1 == :unavailable`), so the drop-set
  # can never learn its PKs — the incremental window can only TAINT (discard + re-read) the affected
  # tables. Now that a taint SIGNALS re-read (the data-loss fix), lib+batch would re-read a
  # backfilling table on EVERY concurrent write to it → livelock (no convergence). The proper fix
  # (retain the discarded txn's PKs so lib+batch can DROP-FILTER instead of re-read) is a larger
  # follow-up; until it lands the combination cannot hold BOTH loss=0 AND liveness, so it is rejected
  # here. NOT rejected: lib-NON-batch + incremental (the drop-set tracks per-txn), sink-owned
  # batch_delivery + incremental (a separate `:batch_delivery` field, not `:batch`), and incremental
  # + spill (a rare taint → converges, never a per-write livelock).
  defp validate_lib_batch_snapshot(batch, [mode: :incremental] ++ _) when is_list(batch),
    do: {:error, :config_invalid}

  defp validate_lib_batch_snapshot(_batch, _snapshot), do: :ok

  # v1 requires BOTH v1 snapshot callbacks (unchanged). Sink-owned incremental requires
  # handle_snapshot/2 + snapshot_progress/0 (spec §6.1); lib mode carries progress in the
  # store, so only handle_snapshot/2 is required there. false → no snapshot → :ok.
  defp validate_snapshot_support(false, _sink, _lib?), do: :ok

  defp validate_snapshot_support(true, sink, _lib?) do
    if Sink.supports_snapshot?(sink), do: :ok, else: {:error, :snapshot_unsupported}
  end

  defp validate_snapshot_support([mode: :incremental] ++ _ = _snapshot, sink, lib?) do
    supported? =
      if lib?,
        do: function_exported?(sink, :handle_snapshot, 2),
        else: Sink.supports_incremental_snapshot?(sink)

    if supported?, do: :ok, else: {:error, :snapshot_unsupported}
  end

  # The bounded in-flight window ceiling (WAL bytes, spec §4). Omitted → the
  # Connection default. A present value must be a positive integer; anything else is
  # a config error (never a silent fallback that would mask a mis-set bound).
  defp fetch_max_inflight_lag(opts) do
    case Keyword.fetch(opts, :max_inflight_lag) do
      :error -> {:ok, Connection.default_max_inflight_lag()}
      {:ok, n} when is_integer(n) and n > 0 -> {:ok, n}
      {:ok, _bad} -> {:error, :config_invalid}
    end
  end

  # Batching (spec §7) is opt-in via a NESTED :batch under :checkpoint_store — whose presence
  # IS lib mode — so batching in sink-owned mode is structurally unreachable. A misplaced
  # TOP-LEVEL :batch (a sibling of :checkpoint_store, the likely mistake) is a config error,
  # never silently ignored. The two user knobs are positive integers; the lag-safety cap
  # max_span is DERIVED = div(max_inflight_lag, 4) (never a user knob).
  defp fetch_batch(opts, checkpoint_store, max_inflight_lag) do
    cond do
      Keyword.has_key?(opts, :batch) -> {:error, :config_invalid}
      not is_list(checkpoint_store) -> {:ok, nil}
      true -> normalize_batch(Keyword.get(checkpoint_store, :batch), max_inflight_lag)
    end
  end

  # Batch delivery (spec §6) is opt-in via a TOP-LEVEL :batch_delivery keyword (a sibling of
  # :sink), sink-owned mode only. It is mutually exclusive with :checkpoint_store — a lib-mode
  # (non-transactional) sink cannot honor handle_batch/1's atomic data+checkpoint contract, so
  # the combination is rejected fail-closed rather than silently ignored. The knobs are the same
  # two positive integers as lib-mode :batch, with the same DERIVED max_span (never a user knob).
  defp fetch_batch_delivery(opts, checkpoint_store, max_inflight_lag) do
    cond do
      not Keyword.has_key?(opts, :batch_delivery) -> {:ok, nil}
      is_list(checkpoint_store) -> {:error, :config_invalid}
      true -> normalize_batch(Keyword.get(opts, :batch_delivery), max_inflight_lag)
    end
  end

  # Streaming (spec §7) is opt-in via a TOP-LEVEL :streaming keyword. Presence enables
  # proto_version 2 + streaming 'on' and in-memory whole reassembly. max_concurrent_txns bounds
  # the in-progress stream_txns map (default 64). Orthogonal to every checkpoint/delivery mode.
  # Absent OR an explicit `nil` both disable it (parity with :batch_delivery); a non-list value
  # (e.g. an atom) is a config error, never silently ignored.
  defp fetch_streaming(opts, max_inflight_lag) do
    case Keyword.get(opts, :streaming) do
      nil ->
        {:ok, nil}

      streaming when is_list(streaming) ->
        max_concurrent = Keyword.get(streaming, :max_concurrent_txns, 64)

        with true <- positive_integer?(max_concurrent),
             {:ok, spill} <- fetch_spill(Keyword.get(streaming, :spill), max_inflight_lag) do
          {:ok, [max_concurrent_txns: max_concurrent] ++ spill}
        else
          _ -> {:error, :config_invalid}
        end

      _ ->
        {:error, :config_invalid}
    end
  end

  # Spill (spec §7) is opt-in via a NESTED :spill under :streaming. Presence enables consumer disk
  # spill. `dir` (default a replicant-owned subdir of System.tmp_dir!(), created 0700 in a later
  # task) is where spill files live; `max_spill_bytes` (default 16 * max_inflight_lag) is the disk
  # ceiling. Absent → no spill.
  defp fetch_spill(nil, _max_inflight_lag), do: {:ok, []}

  defp fetch_spill(spill, max_inflight_lag) when is_list(spill) do
    dir = Keyword.get(spill, :dir, Path.join(System.tmp_dir!(), "replicant_spill"))
    max_spill_bytes = Keyword.get(spill, :max_spill_bytes, 16 * max_inflight_lag)

    if is_binary(dir) and dir != "" and positive_integer?(max_spill_bytes),
      do: {:ok, [spill: [dir: dir, max_spill_bytes: max_spill_bytes]]},
      else: {:error, :config_invalid}
  end

  defp fetch_spill(_bad, _max_inflight_lag), do: {:error, :config_invalid}

  defp normalize_batch(nil, _max_inflight_lag), do: {:ok, nil}

  defp normalize_batch(batch, max_inflight_lag) when is_list(batch) do
    max_transactions = Keyword.get(batch, :max_transactions, 100)
    max_delay_ms = Keyword.get(batch, :max_delay_ms, 1000)

    if positive_integer?(max_transactions) and positive_integer?(max_delay_ms) do
      {:ok,
       [
         max_transactions: max_transactions,
         max_delay_ms: max_delay_ms,
         max_span: div(max_inflight_lag, 4)
       ]}
    else
      {:error, :config_invalid}
    end
  end

  defp normalize_batch(_bad, _max_inflight_lag), do: {:error, :config_invalid}

  defp positive_integer?(n), do: is_integer(n) and n > 0

  # A present :checkpoint_store must be a keyword with a non-empty :connection list and,
  # if given, a :table that passes the identifier allowlist (Critical Rule 2). Absent →
  # nil (sink-owned mode). A mis-shaped value is a config error, never a silent fallback.
  defp fetch_checkpoint_store(opts) do
    case Keyword.fetch(opts, :checkpoint_store) do
      :error ->
        {:ok, nil}

      {:ok, store} when is_list(store) ->
        with conn when is_list(conn) and conn != [] <- Keyword.get(store, :connection),
             :ok <- validate_store_table(Keyword.get(store, :table)),
             :ok <- validate_store_table(Keyword.get(store, :progress_table)),
             {:ok, max_retries, backoff} <- validate_retry_opts(store) do
          {:ok, Keyword.merge(store, max_retries: max_retries, retry_backoff_ms: backoff)}
        else
          {:error, :invalid_identifier} = err -> err
          _ -> {:error, :config_invalid}
        end

      {:ok, _bad} ->
        {:error, :config_invalid}
    end
  end

  defp validate_store_table(nil), do: :ok
  defp validate_store_table(table), do: Identifier.validate(table)

  # Retry policy (spec §6/§7): `max_retries` is a non-negative integer (0 = halt-now,
  # opting out of retry); `retry_backoff_ms` is a positive integer. Defaults 5 / 1000 →
  # the default pipeline tolerates ~5s of store outage before halting.
  defp validate_retry_opts(store) do
    max_retries = Keyword.get(store, :max_retries, CheckpointStore.default_max_retries())
    backoff = Keyword.get(store, :retry_backoff_ms, CheckpointStore.default_retry_backoff_ms())

    if is_integer(max_retries) and max_retries >= 0 and is_integer(backoff) and backoff > 0 do
      {:ok, max_retries, backoff}
    else
      {:error, :config_invalid}
    end
  end

  defp fetch_connection(opts) do
    case Keyword.get(opts, :connection) do
      conn when is_list(conn) and conn != [] -> {:ok, conn}
      _ -> {:error, :config_invalid}
    end
  end

  defp fetch_identifier(opts, key) do
    value = Keyword.get(opts, key)

    case Identifier.validate(value) do
      :ok -> {:ok, value}
      {:error, :invalid_identifier} = err -> err
    end
  end

  # Callback requirements by mode. Batch-delivery mode requires handle_batch/1 (the delivery
  # callback) AND checkpoint/0 (the resume/go-forward watermark) — but NOT handle_transaction/1,
  # which is never called under batch delivery. Lib mode and per-transaction sink-owned mode are
  # unchanged. All checks are runtime function_exported?/3 (the callbacks are @optional_callbacks).
  defp fetch_sink(opts, lib_mode?, batch_mode?) do
    sink = Keyword.get(opts, :sink)

    cond do
      not (is_atom(sink) and sink != nil and Code.ensure_loaded?(sink)) ->
        {:error, :invalid_sink}

      batch_mode? ->
        fetch_batch_sink(sink)

      not function_exported?(sink, :handle_transaction, 1) ->
        {:error, :invalid_sink}

      lib_mode? ->
        {:ok, sink}

      function_exported?(sink, :checkpoint, 0) ->
        {:ok, sink}

      true ->
        {:error, :invalid_sink}
    end
  end

  # A batch-delivery sink must export handle_batch/1 (the delivery callback) and checkpoint/0
  # (the resume/go-forward watermark). handle_transaction/1 is never called in this mode.
  defp fetch_batch_sink(sink) do
    cond do
      not Sink.supports_batch?(sink) -> {:error, :batch_unsupported}
      not function_exported?(sink, :checkpoint, 0) -> {:error, :invalid_sink}
      true -> {:ok, sink}
    end
  end
end
