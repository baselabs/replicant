defmodule Replicant.Test.LedgerSink do
  @moduledoc """
  A transactional LSN-watermark sink for the crash-injection suite (spec §6). Each
  `handle_transaction/1` persists the transaction's rows into `sink_orders` AND the
  checkpoint into `_replicant_checkpoint` in ONE database transaction (the
  checkpoint-after-persist-atomic contract), records the outcome in
  `_replicant_calls` (for loss/dup auditing), and dedups by
  `commit_lsn <= checkpoint` — a re-delivered transaction is recorded `skipped` and
  applied zero more times (effect-once). Rows upsert by PK.

  Uses a normal (non-replication) named Postgrex connection `Replicant.Test.LedgerConn`,
  started by the integration test setup.
  """
  @behaviour Replicant.Sink

  alias Replicant.{Change, Transaction}

  @conn Replicant.Test.LedgerConn

  @impl true
  def checkpoint do
    case Postgrex.query(@conn, "SELECT lsn FROM _replicant_checkpoint WHERE id = 1", []) do
      {:ok, %Postgrex.Result{rows: [[lsn]]}} -> {:ok, lsn}
      {:ok, %Postgrex.Result{rows: []}} -> {:ok, nil}
      {:error, _reason} = err -> err
    end
  end

  @impl true
  def handle_transaction(%Transaction{commit_lsn: lsn, changes: changes}) do
    result =
      Postgrex.transaction(
        @conn,
        fn c ->
          case current_checkpoint(c) do
            cp when is_integer(cp) and lsn <= cp ->
              record_call(c, lsn, "skipped")

            _not_yet_applied ->
              Enum.each(changes, &apply_change(c, &1))
              set_checkpoint(c, lsn)
              record_call(c, lsn, "applied")
          end
        end,
        timeout: 60_000
      )

    case result do
      {:ok, _} -> {:ok, lsn}
      {:error, reason} -> {:error, reason}
    end
  end

  defp current_checkpoint(c) do
    case Postgrex.query!(c, "SELECT lsn FROM _replicant_checkpoint WHERE id = 1", []).rows do
      [[lsn]] -> lsn
      [] -> nil
    end
  end

  defp set_checkpoint(c, lsn) do
    Postgrex.query!(
      c,
      "INSERT INTO _replicant_checkpoint (id, lsn) VALUES (1, $1) " <>
        "ON CONFLICT (id) DO UPDATE SET lsn = EXCLUDED.lsn",
      [lsn]
    )
  end

  defp record_call(c, lsn, outcome) do
    Postgrex.query!(c, "INSERT INTO _replicant_calls (lsn, outcome) VALUES ($1, $2)", [
      lsn,
      outcome
    ])
  end

  defp apply_change(c, %Change{op: op, record: r}) when op in [:insert, :update] do
    Postgrex.query!(
      c,
      "INSERT INTO sink_orders (id, note) VALUES ($1, $2) " <>
        "ON CONFLICT (id) DO UPDATE SET note = EXCLUDED.note",
      [r["id"], r["note"]]
    )
  end

  defp apply_change(c, %Change{op: :delete, old_record: old}) do
    Postgrex.query!(c, "DELETE FROM sink_orders WHERE id = $1", [old["id"]])
  end

  defp apply_change(c, %Change{op: :truncate}) do
    Postgrex.query!(c, "TRUNCATE sink_orders", [])
  end
end

defmodule Replicant.Test.SlowLedgerSink do
  @moduledoc "LedgerSink with a per-transaction delay, to exercise §4 backpressure."
  @behaviour Replicant.Sink

  alias Replicant.Test.LedgerSink

  @impl true
  def checkpoint, do: LedgerSink.checkpoint()

  @impl true
  def handle_transaction(txn) do
    Process.sleep(25)
    LedgerSink.handle_transaction(txn)
  end
end

defmodule Replicant.Test.StuckLedgerSink do
  @moduledoc """
  A sink whose `handle_transaction/1` blocks forever, so the AssemblerServer never
  drains and the durable checkpoint never advances. Used to prove the fail-closed
  sink-lag halt: with the checkpoint pinned, the Connection's in-flight lag
  (`received_lsn - max(checkpoint_lsn, stream_floor_lsn)`) grows monotonically past
  the ceiling and trips `:sink_too_slow` instead of OOMing. `checkpoint/0` delegates
  to `LedgerSink` (a real durable read).
  """
  @behaviour Replicant.Sink

  alias Replicant.Test.LedgerSink

  @impl true
  def checkpoint, do: LedgerSink.checkpoint()

  @impl true
  # Block forever via a receive with no matching clause — dialyzer infers `no_return`
  # (unlike `Process.sleep(:infinity)`, whose success type `:ok` mismatches the
  # `{:ok, lsn} | {:error, _}` callback contract).
  def handle_transaction(_txn) do
    receive do
      :never -> {:ok, 0}
    end
  end
end

defmodule Replicant.Test.FailOpenLedgerSink do
  @moduledoc """
  A `LedgerSink` whose `checkpoint/0` reports `{:ok, nil}` — the spec §14.15
  checkpoint-read-fail-open condition. Used ONLY by the re-delivery-dedup test to
  exercise the sink idempotency contract (spec §6) end-to-end through the live
  pipeline.

  `checkpoint/0` is read in two places (Task 2/1): the `Connection` seeds its
  resume LSN from it, and the `Assembler` pre-skips a re-delivered transaction with
  it. Reporting `nil` makes the Connection resume from `0/0` AND disables the
  Assembler pre-skip (so a re-delivered transaction reaches the sink) — the ONLY path
  on which the sink's own watermark dedup is exercised. `handle_transaction/1`
  delegates to `LedgerSink`, which reads its DURABLE `_replicant_checkpoint`
  watermark inside the apply transaction and records the re-delivery as `skipped`
  (applied zero more times, effect-once). Rows still upsert by PK, so a bare
  re-delivery without the watermark would also be effect-safe; the watermark makes
  the dedup observable.

  Why the fail-open path is needed to exercise the SINK's dedup: whether PG
  re-delivers a transaction is governed by the slot's **server-side
  `confirmed_flush_lsn`**, not the client's `START_REPLICATION ... <start_lsn>`
  argument (the client value is a clamped hint — PG never streams below
  `confirmed_flush_lsn`). More to the point, with a plain `LedgerSink` any
  re-delivery is caught by the **Assembler's Commit-path pre-skip**
  (`commit_lsn <= sink.checkpoint()` → `{:skipped}`) BEFORE it ever reaches the
  sink, so the sink's own watermark dedup is unreachable that way. Reporting `nil`
  from `checkpoint/0` disables that pre-skip, letting the re-delivery fall through to
  the sink so its watermark dedup is the thing under test.
  """
  @behaviour Replicant.Sink

  alias Replicant.Test.LedgerSink

  @impl true
  def checkpoint, do: {:ok, nil}

  @impl true
  def handle_transaction(txn), do: LedgerSink.handle_transaction(txn)
end

defmodule Replicant.Test.RaisingCheckpointLedgerSink do
  @moduledoc """
  A `LedgerSink` whose `checkpoint/0` RAISES — the spec §14.15 checkpoint-read-FAULT
  condition (as distinct from `FailOpenLedgerSink`, which reports `{:ok, nil}` = a
  genuine empty checkpoint). The Connection's `read_checkpoint/1` wraps the sink read in
  a value-free rescue, so a raise resolves to checkpoint_state `:fault` (checkpoint_lsn 0).

  Used by the R01 live probe to prove that an UNKNOWN checkpoint (read fault) combined
  with an ABSENT replication slot halts fail-closed and NEVER creates a fresh slot — a
  fresh slot would begin at its own creation LSN and silently skip the WAL between the
  (unknown) real checkpoint and now. `handle_transaction/1` delegates to `LedgerSink` but
  is never reached: the pipeline halts at the connect decision, before streaming.
  """
  @behaviour Replicant.Sink

  alias Replicant.Test.LedgerSink

  @impl true
  def checkpoint, do: raise("checkpoint read fault (R01 live probe)")

  @impl true
  def handle_transaction(txn), do: LedgerSink.handle_transaction(txn)
end

defmodule Replicant.Test.PauseGate do
  @moduledoc """
  Test-only coordination gate for `Replicant.Test.PausingLedgerSink`. Holds the pid
  to notify and whether the sink is currently "armed" to pause the next multi-row
  transaction. An `Agent` registered under this module name, started (and torn down)
  by the mid-transaction crash-injection test. Not part of `lib/` — a pure test
  harness so the mid-transaction kill lands deterministically at the sink boundary
  (the assembler having fully assembled the txn but NOT yet durably committed it),
  which `:sys.get_state`-polling the assembler's in-buffer state could not catch
  reliably (the assembler churns a large frame burst faster than a poll can observe).
  """

  @doc "Start the gate armed to pause the next txn of `>= min_changes` rows, notifying `notify`."
  @spec start_link(pid(), pos_integer()) :: Agent.on_start()
  def start_link(notify, min_changes) do
    Agent.start_link(fn -> %{notify: notify, armed: true, min_changes: min_changes} end,
      name: __MODULE__
    )
  end

  @doc "Disarm the gate so the re-streamed txn (after the kill) applies without pausing."
  @spec disarm() :: :ok
  def disarm, do: Agent.update(__MODULE__, &%{&1 | armed: false})

  @doc """
  Decide whether to pause for a txn of `change_count` rows. When armed AND
  `change_count >= min_changes`, atomically disarm (so only ONE txn is paused),
  return `{:pause, notify_pid}`; else `:apply`.
  """
  @spec decide(non_neg_integer()) :: {:pause, pid()} | :apply
  def decide(change_count) do
    Agent.get_and_update(__MODULE__, fn state ->
      if state.armed and change_count >= state.min_changes do
        {{:pause, state.notify}, %{state | armed: false}}
      else
        {:apply, state}
      end
    end)
  end
end

defmodule Replicant.Test.PausingLedgerSink do
  @moduledoc """
  A `LedgerSink` that pauses (blocks the AssemblerServer) inside `handle_transaction/1`
  for the FIRST large multi-row transaction, to make a MID-TRANSACTION kill (spec §12.2)
  deterministic. When the `Replicant.Test.PauseGate` is armed and the transaction has
  `>= min_changes` changes, it notifies the test with `{:sink_paused, commit_lsn,
  change_count}` — the transaction is now fully ASSEMBLED but NOT durably committed (no
  rows written, the checkpoint still at the prior txn) — then blocks on `receive` until
  the test either releases it or (the real path) kills the Connection, whose `:one_for_all`
  restart terminates this blocked AssemblerServer, discarding the in-flight transaction.

  On the re-streamed retry after the restart the gate is disarmed, so the transaction
  applies exactly once through the durable `LedgerSink`. `checkpoint/0` delegates to the
  real durable read so resume works normally.

  This is the sanctioned test-only instrumentation hook (the task's fallback): it fires
  DETERMINISTICALLY at the sink boundary with the transaction in-flight-not-committed —
  the exact precondition a mid-transaction crash must survive — rather than relying on a
  racy `:sys.get_state` catch of the transient in-buffer state. No `lib/` change, no
  `lib/` telemetry.
  """
  @behaviour Replicant.Sink

  alias Replicant.Test.{LedgerSink, PauseGate}
  alias Replicant.Transaction

  @impl true
  def checkpoint, do: LedgerSink.checkpoint()

  @impl true
  def handle_transaction(%Transaction{commit_lsn: lsn, changes: changes} = txn) do
    case PauseGate.decide(length(changes)) do
      {:pause, notify} ->
        send(notify, {:sink_paused, lsn, length(changes)})
        # Block here with the txn assembled but NOT durably committed. The test kills
        # the Connection during this window; :one_for_all terminates this process, so
        # this receive typically never returns. `:release` is a graceful escape hatch.
        receive do
          :release -> LedgerSink.handle_transaction(txn)
        end

      :apply ->
        LedgerSink.handle_transaction(txn)
    end
  end
end

defmodule Replicant.Test.MessagePauseGate do
  @moduledoc """
  Test-only coordination gate for `Replicant.Test.PausingMessageSink` (the §8.1 idle-ack
  marquee harness in test/integration/messages_test.exs). Mirrors `Replicant.Test.PauseGate`:
  when armed, the FIRST non-txn message's `handle_message/2` blocks (notifying `notify`) until
  `release/0` is called, constructing the undelivered-message window the §8.1 idle-ack seam
  must respect (the Connection has bumped `last_commit_lsn` via `track_txn` but the sink has
  not returned `:ok`, so `{:sink_committed, msg_lsn}` has not fired).

  `handle_message/2` runs in the AssemblerServer's process, so when it blocks it blocks the
  AssemblerServer. The gate captures `self()` (the blocked caller) at decide-time and
  `release/0` sends `:release` to THAT pid. An `Agent` registered under this module name,
  started by the marquee's setup.
  """

  @doc "Start the gate armed, notifying `notify` when the first non-txn message pauses."
  @spec start_link(pid()) :: Agent.on_start()
  def start_link(notify) do
    Agent.start_link(fn -> %{notify: notify, armed: true, paused_pid: nil} end, name: __MODULE__)
  end

  @doc "Disarm the gate so subsequent messages apply without pausing."
  @spec disarm() :: :ok
  def disarm, do: Agent.update(__MODULE__, &%{&1 | armed: false})

  @doc """
  Decide whether to pause for the current message. When armed, atomically disarm (so only ONE
  message is paused), capture the blocked caller (`self()`), and return `{:pause, notify_pid}`;
  else `:apply`.
  """
  @spec decide() :: {:pause, pid()} | :apply
  def decide do
    caller = self()

    Agent.get_and_update(__MODULE__, fn state ->
      if state.armed do
        {{:pause, state.notify}, %{state | armed: false, paused_pid: caller}}
      else
        {:apply, state}
      end
    end)
  end

  @doc "Release the paused message: send :release to the blocked AssemblerServer caller."
  @spec release() :: :ok
  def release do
    case Agent.get(__MODULE__, & &1.paused_pid) do
      nil -> :ok
      pid -> send(pid, :release)
    end
  end
end
