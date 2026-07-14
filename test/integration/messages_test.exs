defmodule Replicant.MessagesTest do
  @moduledoc """
  Integration marquees for the A2 logical-decoding-messages feature (spec §6.3, §7.1, §7.2,
  §8.1, §8.3, §8.4). Each marquee exercises a durability seam landed in Tasks 1–9 against a
  LIVE PG16. Gated on `REPLICANT_TEST_URL` (see test/test_helper.exs): each test body is wrapped
  in `if PG16.enabled?() do ... end`, so the suite skips cleanly without a live PG (the
  `@moduletag :integration` excludes the module entirely when the URL is unset). When the URL
  IS set, every assertion below runs for real — these are the §12 DoD evidence, NOT vacuous.

  The marquees prove:

    1. Transactional message effect-once (§7.1) — `pg_logical_emit_message(true, ...)` rides
       `%Transaction.messages` and delivers ONCE across a crash (dup=0).
    2. Non-txn message at-least-once (§7.2) — `pg_logical_emit_message(false, ...)` reaches
       `handle_message/2`.
    3. THE idle-ack loss=0 marquee (§8.1) — while a non-txn message is undelivered, the slot
       does NOT ack past its LSN; only after `handle_message/2` returns `:ok` does it advance.
       This is the hardest seam: it proves the `track_txn` bump (connection.ex Task 9) prevents
       the idle-ack from silently advancing past a pending non-txn message.
    4/5. Batch composition (§8.4) — a non-txn message is a batch boundary in BOTH lib-batch and
       sink-owned `batch_delivery` (the buffered batch flushes BEFORE the message's LSN acks).
    6. Streamed message in a spilled txn (§8.3) — a transactional message in a spilled streamed
       txn stays resident and rides `%Transaction.messages`.
    7. Default path byte-unchanged — `messages: false` + single-string publication reproduces
       the 0.1.0 START_REPLICATION (no `messages 'true'` option).
  """

  use ExUnit.Case, async: false
  @moduletag :integration
  # Each marquee drives real PG16 + (several) crash-injection / spill sequences that are
  # genuinely ~10s isolated under shared-PG16 load; match the streaming/spill suite ceiling.
  @moduletag timeout: 120_000

  # The lib-mode checkpoint-store table (mirrors batching_test.exs's @cp_table). Defined at the
  # top so the §8.4 lib-batch marquee's `start_pipeline` reference below resolves in source order.
  @cp_table "replicant_msg_checkpoints"

  alias Replicant.{Change, Transaction}
  alias Replicant.Decoder.Messages.Message
  alias Replicant.Test.{MessagePauseGate, PG16}

  # A transactional sink for the txn-message + non-txn-message + spilled-message + default
  # marquees. Records every delivered transaction's changes into `msg_sink_rows` (append-only,
  # no PK → duplicates detectable), every delivered non-txn message into `msg_sink_messages`
  # (append-only → at-least-once duplicates detectable), and the checkpoint into `msg_sink_cp`
  # — all in ONE atomic database transaction (effect-once). `msg_sink_calls` is an append-only
  # per-delivered-txn ledger (the dup=0 signal, mirroring streaming_test.exs).
  defmodule MessageSink do
    @moduledoc false
    @behaviour Replicant.Sink

    @conn Replicant.Test.MessagesConn

    @impl true
    def checkpoint do
      case Postgrex.query(@conn, "SELECT lsn FROM msg_sink_cp WHERE id = 1", []) do
        {:ok, %Postgrex.Result{rows: [[lsn]]}} -> {:ok, lsn}
        {:ok, %Postgrex.Result{rows: []}} -> {:ok, nil}
        {:error, _} = err -> err
      end
    end

    @impl true
    def handle_transaction(%Transaction{commit_lsn: lsn, changes: changes, messages: msgs}) do
      result =
        Postgrex.transaction(@conn, fn c ->
          Postgrex.query!(
            c,
            "INSERT INTO msg_sink_calls (lsn, n_changes, n_messages) VALUES ($1, $2, $3)",
            [
              lsn,
              length(changes),
              length(msgs)
            ]
          )

          Enum.each(changes, &apply_change(c, &1))

          Enum.each(msgs, fn %Message{prefix: prefix, content: content, ordinal: ordinal} ->
            Postgrex.query!(
              c,
              "INSERT INTO msg_sink_txn_messages (commit_lsn, prefix, content, ordinal) VALUES ($1, $2, $3, $4)",
              [lsn, prefix, content, ordinal]
            )
          end)

          Postgrex.query!(
            c,
            "INSERT INTO msg_sink_cp (id, lsn) VALUES (1, $1) ON CONFLICT (id) DO UPDATE SET lsn = EXCLUDED.lsn",
            [lsn]
          )
        end)

      case result do
        {:ok, _} -> {:ok, lsn}
        {:error, reason} -> {:error, reason}
      end
    end

    @impl true
    def handle_message(%Message{lsn: lsn, prefix: prefix, content: content}, %{lsn: ctx_lsn}) do
      result =
        Postgrex.transaction(@conn, fn c ->
          Postgrex.query!(
            c,
            "INSERT INTO msg_sink_messages (lsn, prefix, content) VALUES ($1, $2, $3)",
            [ctx_lsn || lsn, prefix, content]
          )

          Postgrex.query!(
            c,
            "INSERT INTO msg_sink_cp (id, lsn) VALUES (1, $1) ON CONFLICT (id) DO UPDATE SET lsn = EXCLUDED.lsn",
            [ctx_lsn || lsn]
          )
        end)

      case result do
        {:ok, _} -> {:ok, ctx_lsn || lsn}
        {:error, reason} -> {:error, reason}
      end
    end

    defp apply_change(c, %Change{op: op, record: r}) when op in [:insert, :update] do
      Postgrex.query!(c, "INSERT INTO msg_sink_rows (commit_lsn, id) VALUES ($1, $2)", [
        # commit_lsn is not in apply_change's signature; msg_sink_rows is append-only (no PK)
        # so 0 is fine as provenance — the dup=0 signal lives in msg_sink_calls, not here.
        0,
        r["id"]
      ])
    end

    defp apply_change(_c, _change), do: :ok
  end

  # A MessageSink variant whose `handle_message/2` BLOCKS on the first non-txn message until
  # the test releases it — the §8.1 idle-ack window harness (mirrors PausingLedgerSink's gate).
  # `handle_transaction/1` delegates to MessageSink so the pre-message state is real. The pause
  # constructs the undelivered-message window: the Connection has received the `M` frame (bumping
  # last_commit_lsn via track_txn, Task 9) but the sink has not returned :ok, so
  # {:sink_committed, msg_lsn} has not fired and checkpoint_lsn < last_commit_lsn. During this
  # window a reply-requested keepalive must NOT idle-advance the slot past msg_lsn.
  defmodule PausingMessageSink do
    @moduledoc false
    @behaviour Replicant.Sink

    alias Replicant.Test.MessagePauseGate

    @impl true
    def checkpoint, do: MessageSink.checkpoint()

    @impl true
    def handle_transaction(txn), do: MessageSink.handle_transaction(txn)

    @impl true
    def handle_message(%Message{} = msg, ctx) do
      case MessagePauseGate.decide() do
        {:pause, notify} ->
          send(notify, {:message_paused, msg.lsn, msg.prefix})
          # Block with the message received-but-not-acked. The test asserts the slot did NOT
          # advance past msg.lsn during this window, then sends :release.
          receive do
            :release -> MessageSink.handle_message(msg, ctx)
          end

        :apply ->
          MessageSink.handle_message(msg, ctx)
      end
    end
  end

  # A lib-mode (non-transactional) message-recording sink for the §8.4 lib-batch marquee. The
  # library owns the checkpoint (`checkpoint_store`); this sink records delivered changes and
  # messages WITHOUT writing a checkpoint. `handle_message/2` appends to `msg_sink_messages`
  # (at-least-once observable). sink_kind :append_log so a fresh checkpoint starts clean.
  defmodule LibMessageSink do
    @moduledoc false
    @behaviour Replicant.Sink

    @conn Replicant.Test.MessagesConn

    @impl true
    def sink_kind, do: :append_log

    @impl true
    def handle_transaction(%Transaction{commit_lsn: lsn, changes: changes, messages: msgs}) do
      Enum.each(changes, fn
        %Change{op: op, record: r} when op in [:insert, :update] ->
          Postgrex.query!(@conn, "INSERT INTO msg_sink_rows (commit_lsn, id) VALUES ($1, $2)", [
            lsn,
            r["id"]
          ])

        _other ->
          :ok
      end)

      Enum.each(msgs, fn %Message{prefix: prefix, content: content} ->
        Postgrex.query!(
          @conn,
          "INSERT INTO msg_sink_txn_messages (commit_lsn, prefix, content) VALUES ($1, $2, $3)",
          [
            lsn,
            prefix,
            content
          ]
        )
      end)

      {:ok, lsn}
    end

    @impl true
    def handle_message(%Message{lsn: lsn, prefix: prefix, content: content}, _ctx) do
      Postgrex.query!(
        @conn,
        "INSERT INTO msg_sink_messages (lsn, prefix, content) VALUES ($1, $2, $3)",
        [
          lsn,
          prefix,
          content
        ]
      )

      {:ok, lsn}
    end
  end

  # A sink-owned batch sink for the §8.4 sink-owned-batch marquee: handle_batch/1 persists a
  # batch's transactions + checkpoint atomically AND records each delivered txn to an append-only
  # `msg_sink_calls` ledger (dup=0 signal). handle_message/2 records non-txn messages. Mirrors
  # LedgerBatchSink (test/support/ledger_batch_sink.ex) plus message support.
  defmodule BatchMessageSink do
    @moduledoc false
    @behaviour Replicant.Sink

    alias Replicant.{Change, Transaction}

    @conn Replicant.Test.MessagesConn

    @impl true
    def checkpoint, do: MessageSink.checkpoint()

    @impl true
    def handle_batch(transactions) do
      highest = transactions |> Enum.map(& &1.commit_lsn) |> Enum.max()

      result =
        Postgrex.transaction(@conn, fn c ->
          Enum.each(transactions, fn %Transaction{commit_lsn: lsn, changes: changes} ->
            Postgrex.query!(
              c,
              "INSERT INTO msg_sink_calls (lsn, n_changes, n_messages) VALUES ($1, $2, $3)",
              [
                lsn,
                length(changes),
                0
              ]
            )

            Enum.each(changes, &apply_change(c, &1))
          end)

          Postgrex.query!(
            c,
            "INSERT INTO msg_sink_cp (id, lsn) VALUES (1, $1) ON CONFLICT (id) DO UPDATE SET lsn = EXCLUDED.lsn",
            [highest]
          )
        end)

      case result do
        {:ok, _} -> {:ok, highest}
        {:error, reason} -> {:error, reason}
      end
    end

    @impl true
    def handle_message(%Message{lsn: lsn, prefix: prefix, content: content}, _ctx) do
      Postgrex.query!(
        @conn,
        "INSERT INTO msg_sink_messages (lsn, prefix, content) VALUES ($1, $2, $3)",
        [
          lsn,
          prefix,
          content
        ]
      )

      {:ok, lsn}
    end

    defp apply_change(c, %Change{op: op, record: r}) when op in [:insert, :update] do
      Postgrex.query!(c, "INSERT INTO msg_sink_rows (commit_lsn, id) VALUES ($1, $2)", [
        0,
        r["id"]
      ])
    end

    defp apply_change(_c, _change), do: :ok
  end

  setup do
    {:ok, ctrl} =
      Postgrex.start_link(PG16.pg_opts() ++ [name: Replicant.Test.MsgCtrlConn, pool_size: 3])

    {:ok, _} =
      Postgrex.start_link(PG16.pg_opts() ++ [name: Replicant.Test.MessagesConn, pool_size: 2])

    slot = "rep_msg_#{System.unique_integer([:positive])}"
    reset_schema(ctrl)
    drop_slot(ctrl, slot)

    on_exit(fn ->
      Replicant.stop(slot)
      PG16.wait_until(fn -> Registry.lookup(Replicant.Registry, {slot, :pipeline}) == [] end, 400)
      {:ok, c} = Postgrex.start_link(PG16.pg_opts())
      drop_slot(c, slot)
    end)

    %{ctrl: ctrl, slot: slot}
  end

  describe "transactional messages (spec §7.1, effect-once)" do
    test "a transactional message rides %Transaction.messages and is effect-once across a crash (dup=0)",
         %{ctrl: ctrl, slot: slot} do
      if PG16.enabled?() do
        start_pipeline(slot, MessageSink, messages: true)

        # A transactional message INSIDE a txn + an insert: the message must ride
        # %Transaction.messages (prefix 'outbox', content 'payload'), atomically with the row.
        Postgrex.transaction(ctrl, fn c ->
          Postgrex.query!(c, "INSERT INTO msg_orders (id, note) VALUES ($1, $2)", [1, "row1"])

          Postgrex.query!(c, "SELECT pg_logical_emit_message(true, 'outbox', 'payload')", [])
        end)

        # Wait for the txn to land (row + message both delivered atomically).
        PG16.wait_until(fn -> MapSet.member?(row_ids(ctrl), 1) end, 800)

        # The transactional message rode %Transaction.messages (NOT handle_message/2).
        assert txn_messages(ctrl) == [{"outbox", "payload"}]
        assert non_txn_messages(ctrl) == []
        # dup=0 (load-bearing): exactly ONE delivered txn, carrying 1 change + 1 message.
        # msg_sink_calls is append-only (no PK) — a re-delivery would push the list length > 1.
        # The first tuple element is the txn's real commit_lsn (a large WAL position), so it is
        # matched as _lsn, not asserted to a literal; the count + n_changes + n_messages carry
        # the dup=0 invariant.
        assert [{_lsn, 1, 1}] = calls(ctrl)
        assert MapSet.member?(row_ids(ctrl), 1)

        cp_before = cp_lsn(ctrl)

        # Crash-injection: fault the NEXT delivery's checkpoint write, emit a 2nd txn+message,
        # then clear the fault and restart. The 2nd delivery re-runs once (dup=0 on the
        # transactional path). This mirrors streaming_test.exs's crash pattern.
        Postgrex.query!(
          ctrl,
          "ALTER TABLE msg_sink_cp ADD CONSTRAINT tmp_block CHECK (lsn < 0) NOT VALID",
          []
        )

        Postgrex.transaction(ctrl, fn c ->
          Postgrex.query!(c, "INSERT INTO msg_orders (id, note) VALUES ($1, $2)", [2, "row2"])
          Postgrex.query!(c, "SELECT pg_logical_emit_message(true, 'outbox', 'second')", [])
        end)

        # The delivery halts fail-closed; the checkpoint did NOT advance past the 1st txn.
        PG16.wait_until(
          fn -> Registry.lookup(Replicant.Registry, {slot, :pipeline}) == [] end,
          800
        )

        assert cp_lsn(ctrl) == cp_before
        refute MapSet.member?(row_ids(ctrl), 2)

        # Clear the fault and restart: the 2nd txn+message re-delivers ONCE (dup=0).
        Postgrex.query!(ctrl, "ALTER TABLE msg_sink_cp DROP CONSTRAINT tmp_block", [])
        start_pipeline(slot, MessageSink, messages: true)

        PG16.wait_until(fn -> MapSet.member?(row_ids(ctrl), 2) end, 800)

        # dup=0, load-bearing: TWO delivered txns total (1st once + 2nd once after the fault),
        # each carrying exactly 1 change + 1 transactional message. A re-delivery of EITHER txn
        # would push the count > 2 or duplicate the message. The message order is preserved.
        assert length(calls(ctrl)) == 2
        assert txn_messages(ctrl) == [{"outbox", "payload"}, {"outbox", "second"}]
        # The non-txn path was never invoked for a transactional message.
        assert non_txn_messages(ctrl) == []
        assert cp_lsn(ctrl) > cp_before
      end
    end
  end

  describe "non-transactional messages (spec §7.2, at-least-once)" do
    test "a non-txn message delivers via handle_message/2 (at-least-once)", %{
      ctrl: ctrl,
      slot: slot
    } do
      if PG16.enabled?() do
        start_pipeline(slot, MessageSink, messages: true)

        # A non-transactional message arrives STANDALONE (no Begin/Commit) and routes to
        # handle_message/2 (spec §7.2). force_inventory emits it outside any txn.
        Postgrex.query!(
          ctrl,
          "SELECT pg_logical_emit_message(false, 'heartbeat', 'tick')",
          []
        )

        PG16.wait_until(fn -> non_txn_messages(ctrl) != [] end, 800)

        # The non-txn message reached handle_message/2 (NOT %Transaction.messages).
        assert non_txn_messages(ctrl) == [{"heartbeat", "tick"}]
        assert txn_messages(ctrl) == []
      end
    end

    test "MARQUEE: the idle-ack does NOT ack past an undelivered non-txn message (§8.1 loss=0)",
         %{ctrl: ctrl, slot: slot} do
      if PG16.enabled?() do
        # Arm the pause gate: the FIRST non-txn message's handle_message/2 will BLOCK until
        # released. This constructs the undelivered-message window the idle-ack must respect.
        MessagePauseGate.start_link(self())

        start_pipeline(slot, PausingMessageSink, messages: true)

        # Emit a non-txn message. The Connection receives the `M` frame and track_txn bumps
        # last_commit_lsn := msg.lsn (Task 9, connection.ex:943). The AssemblerServer delivers
        # it to PausingMessageSink.handle_message/2, which BLOCKS → {:sink_committed, msg_lsn}
        # is NOT sent → checkpoint_lsn < last_commit_lsn → idle?/1 is false.
        %Postgrex.Result{rows: [[msg_lsn_str]]} =
          Postgrex.query!(
            ctrl,
            "SELECT pg_logical_emit_message(false, 'idle_probe', 'pending')::text",
            []
          )

        msg_lsn = Replicant.lsn_from_string(msg_lsn_str)

        # Wait for the sink to enter the paused window (the message was RECEIVED but not acked).
        assert_receive {:message_paused, ^msg_lsn, "idle_probe"}, 10_000

        # Drive reply-requested keepalive traffic: insert into an UNPUBLISHED table + a WAL
        # switch generates WAL the walsender streams as keepalives (some reply=1). WITHOUT the
        # §8.1 fix (the track_txn bump), idle?/1 would be true and the next reply-requested
        # keepalive would idle-advance confirmed_flush_lsn to wal_end (>> msg.lsn) → SILENT LOSS.
        Enum.each(1..50, fn _ ->
          Postgrex.query!(ctrl, "INSERT INTO msg_noise (v) VALUES ($1)", ["n"])
        end)

        Postgrex.query!(ctrl, "SELECT pg_switch_wal()", [])

        # Poll over a real-time window: confirmed_flush_lsn must STAY below msg.lsn while the
        # message is undelivered. Give the keepalive traffic time to arrive and be (correctly)
        # refused. This is the load-bearing §8.1 assertion — RED-capable: reverting the
        # track_txn bump makes idle?/1 true and the slot advance past msg.lsn here.
        kept_below =
          Enum.reduce_while(1..40, true, fn _, _acc ->
            cf = confirmed_flush(ctrl, slot)

            if cf >= msg_lsn do
              {:halt, false}
            else
              _ = Process.sleep(25)
              {:cont, true}
            end
          end)

        assert kept_below,
               "confirmed_flush_lsn advanced past the undelivered non-txn message " <>
                 "(msg_lsn=#{Replicant.lsn_to_string(msg_lsn)}, " <>
                 "cf=#{Replicant.lsn_to_string(confirmed_flush(ctrl, slot))}) — §8.1 idle-ack loss=0 violated"

        # The message is still undelivered (the sink is blocked).
        assert non_txn_messages(ctrl) == []

        # Release the gate: handle_message/2 returns :ok → {:sink_committed, msg_lsn} →
        # checkpoint_lsn advances to msg.lsn. Now (and only now) the slot may advance past it.
        MessagePauseGate.release()
        MessagePauseGate.disarm()

        PG16.wait_until(fn -> non_txn_messages(ctrl) != [] end, 800)

        # The message delivered exactly once via handle_message/2.
        assert non_txn_messages(ctrl) == [{"idle_probe", "pending"}]
        # The checkpoint advanced to (at least) the message's LSN.
        PG16.wait_until(fn -> confirmed_flush(ctrl, slot) >= msg_lsn end, 800)
        assert confirmed_flush(ctrl, slot) >= msg_lsn
      end
    end
  end

  describe "batch composition (§8.4)" do
    test "a non-txn message is a batch boundary in lib-batch (loss=0)", %{ctrl: ctrl, slot: slot} do
      if PG16.enabled?() do
        # lib-mode + batched checkpointing: the library owns the checkpoint and writes it once
        # per batch. max_transactions high + max_delay_ms high so the batch stays OPEN until the
        # message-boundary flush trips it (not the count/timer).
        start_pipeline(
          slot,
          LibMessageSink,
          checkpoint_store: [
            connection: PG16.pg_opts(),
            table: @cp_table,
            batch: [max_transactions: 100, max_delay_ms: 60_000]
          ],
          messages: true
        )

        # Buffer a txn (opens a batch, data applied but NOT yet checkpointed).
        Postgrex.query!(ctrl, "INSERT INTO msg_orders (id, note) VALUES ($1, $2)", [1, "row1"])

        PG16.wait_until(fn -> MapSet.member?(row_ids(ctrl), 1) end, 800)

        # The batch is open: the store checkpoint has NOT advanced (the txn is buffered).
        cp_before = store_lsn(ctrl, slot)

        # Emit a non-txn message. §8.4 seam: the open batch must FLUSH (checkpoint+ack its
        # WAL) BEFORE the message's LSN is acked, else a crash-before-flush would lose the
        # buffered txn. The assembler signals {:flush_before_message} (assembler.ex:582).
        Postgrex.query!(
          ctrl,
          "SELECT pg_logical_emit_message(false, 'boundary', 'flush-me')",
          []
        )

        # The message delivers via handle_message/2 (the batch flushed first, then the message).
        PG16.wait_until(fn -> non_txn_messages(ctrl) != [] end, 800)

        assert non_txn_messages(ctrl) == [{"boundary", "flush-me"}]

        # loss=0, load-bearing: the store checkpoint ADVANCED (the buffered batch flushed
        # BEFORE the message boundary). A regression that acked the message past an
        # un-flushed batch would still deliver the message but the checkpoint would not have
        # advanced here — and a crash in that window would lose the buffered txn.
        assert store_lsn(ctrl, slot) > (cp_before || 0),
               "lib-batch did not flush before the non-txn message boundary — §8.4 loss=0 violated"

        # The buffered txn's row is durable (delivered before the flush).
        assert MapSet.member?(row_ids(ctrl), 1)
      end
    end

    test "a non-txn message is a batch boundary in sink-owned batch_delivery (loss=0)",
         %{ctrl: ctrl, slot: slot} do
      if PG16.enabled?() do
        # sink-owned batch_delivery: handle_batch/1 persists the batch atomically. Same seam
        # as lib-batch but different mode — the buffered txns are held in the assembler, not
        # applied per-txn. max_transactions high + max_delay_ms high so the message-boundary
        # flush is the trigger.
        start_pipeline(
          slot,
          BatchMessageSink,
          batch_delivery: [max_transactions: 100, max_delay_ms: 60_000],
          messages: true,
          go_forward_only: true
        )

        # Buffer a txn into the open batch (handle_transaction is NOT called yet — the txn is
        # held until the batch flushes via handle_batch/1).
        Postgrex.query!(ctrl, "INSERT INTO msg_orders (id, note) VALUES ($1, $2)", [1, "row1"])

        # The batch is open: no delivery yet (handle_batch has not fired), checkpoint nil.
        cp_before = cp_lsn(ctrl)

        # Emit a non-txn message. §8.4 seam: the buffered batch flushes (handle_batch commits +
        # acks its LSN) BEFORE the message's LSN, else a crash-before-flush would lose the
        # buffered txn's data (it was never durably persisted).
        Postgrex.query!(
          ctrl,
          "SELECT pg_logical_emit_message(false, 'boundary', 'flush-me')",
          []
        )

        # Both land: the batch flushed (row delivered via handle_batch) AND the message via
        # handle_message/2.
        PG16.wait_until(fn -> MapSet.member?(row_ids(ctrl), 1) end, 800)

        PG16.wait_until(fn -> non_txn_messages(ctrl) != [] end, 800)

        assert MapSet.member?(row_ids(ctrl), 1)
        assert non_txn_messages(ctrl) == [{"boundary", "flush-me"}]

        # loss=0, load-bearing: the checkpoint advanced (the batch flushed before the message).
        assert cp_lsn(ctrl) > (cp_before || 0),
               "sink-owned batch did not flush before the non-txn message boundary — §8.4 loss=0 violated"
      end
    end
  end

  describe "streamed message in a spilled txn (§8.3)" do
    test "a transactional message in a spilled streamed txn stays resident + rides %Transaction.messages",
         %{ctrl: ctrl, slot: slot} do
      if PG16.enabled?() do
        # Lower logical_decoding_work_mem so a large streamed txn SPILLS to disk; the message
        # payload is small (stays resident) but rides %Transaction.messages on delivery.
        Postgrex.query!(ctrl, "ALTER ROLE postgres SET logical_decoding_work_mem = '64kB'", [])

        spill_dir =
          Path.join(
            System.tmp_dir!(),
            "replicant_msg_spill_#{System.unique_integer([:positive])}"
          )

        start_pipeline(
          slot,
          MessageSink,
          messages: true,
          streaming: [max_concurrent_txns: 64],
          spill: [dir: spill_dir, max_spill_bytes: 64 * 1024 * 1024],
          go_forward_only: true
        )

        on_exit(fn ->
          # RESET the role-level work_mem so it doesn't bleed into subsequent integration tests
          # under a shared live PG run (precedent: streaming_spill_test.exs:33).
          {:ok, c} = Postgrex.start_link(PG16.pg_opts())
          Postgrex.query!(c, "ALTER ROLE postgres RESET logical_decoding_work_mem", [])
          File.rm_rf(spill_dir)
        end)

        # Attach the stream probe (anti-vacuous: prove the txn actually STREAMED, mirroring
        # streaming_test.exs). If this never fires, the marquee is not exercising §8.3.
        stream_ref = attach_stream_probe()

        # A large streamed txn (well over 64kB) with a SMALL transactional message inside. The
        # message stays RESIDENT (small payload) while the rows spill; on StreamCommit the
        # message rides %Transaction.messages.
        Postgrex.transaction(
          ctrl,
          fn c ->
            Enum.each(1..4000, &insert(c, &1))

            Postgrex.query!(
              c,
              "SELECT pg_logical_emit_message(true, 'spilled', 'resident-msg')",
              []
            )
          end,
          timeout: 120_000
        )

        PG16.wait_until(fn -> cp_lsn(ctrl) not in [nil, 0] end, 2000)

        PG16.wait_until(fn -> MapSet.subset?(MapSet.new([1, 4000]), row_ids(ctrl)) end, 2000)

        # The txn actually streamed (re-assembled from StreamStart/StreamCommit, not the v1
        # Commit path) — if this is 0, the marquee is not exercising the spilled-streamed seam.
        assert stream_committed_count(stream_ref) >= 1
        detach_stream_probe(stream_ref)

        assert MapSet.subset?(MapSet.new(1..4000), row_ids(ctrl))

        # The transactional message rode %Transaction.messages on the spilled streamed txn
        # (§8.3: a small message stays resident, delivered with the txn).
        assert txn_messages(ctrl) == [{"spilled", "resident-msg"}]
        assert non_txn_messages(ctrl) == []

        # dup=0: exactly one delivered txn, carrying the changes + 1 message. A spilled-txn
        # re-delivery would push calls > 1.
        assert length(calls(ctrl)) == 1
        [{_lsn, n_changes, 1}] = calls(ctrl)
        # §4-bounded numerator: the change count is the committed rows (sub-aborts excluded;
        # this txn has none). Asserting n_changes > 0 proves the txn carried data (the message
        # did not displace it) and the §4 byte_size numerator counted the resident message.
        assert n_changes == 4000

        # Spill files cleaned up post-delivery (mirrors streaming_spill_test.exs).
        assert spill_files(spill_dir, slot) == []
      end
    end
  end

  describe "default path byte-unchanged (messages: false)" do
    test "messages: false + single-string publication reproduces the 0.1.0 START_REPLICATION",
         %{ctrl: ctrl, slot: slot} do
      if PG16.enabled?() do
        # Regression: the published default (messages: false absent) emits NO `messages 'true'`
        # option in START_REPLICATION, and a non-txn message emitted on the source is NOT
        # delivered (the slot does not subscribe to messages). Capture the START_REPLICATION
        # SQL via telemetry-free introspection: query pg_stat_replication's query is unreliable
        # across PG versions, so the load-bearing assertion is OBSERVABLE behavior — a non-txn
        # message emitted while messages: false is NEVER delivered to handle_message/2.
        start_pipeline(slot, MessageSink, [])

        # A non-txn message on the source: with messages: false the slot does NOT stream it,
        # so handle_message/2 is never called. Insert a row so the pipeline is provably live.
        Postgrex.query!(ctrl, "INSERT INTO msg_orders (id, note) VALUES ($1, $2)", [1, "row1"])

        Postgrex.query!(
          ctrl,
          "SELECT pg_logical_emit_message(false, 'heartbeat', 'should-not-deliver')",
          []
        )

        PG16.wait_until(fn -> MapSet.member?(row_ids(ctrl), 1) end, 800)

        # The row delivered (pipeline is live and processing the stream).
        assert MapSet.member?(row_ids(ctrl), 1)

        # The non-txn message was NOT delivered (messages: false → no `messages 'true'` option
        # → the message is not part of the stream). Give it a real-time window to disprove a
        # vacuous pass, then assert it never arrived.
        kept_absent =
          Enum.reduce_while(1..20, true, fn _, _acc ->
            if non_txn_messages(ctrl) != [] do
              {:halt, false}
            else
              _ = Process.sleep(25)
              {:cont, true}
            end
          end)

        assert kept_absent,
               "non-txn message delivered under messages: false — byte-unchanged regression"

        assert non_txn_messages(ctrl) == []
        assert txn_messages(ctrl) == []
      end
    end
  end

  # ---- helpers ----

  defp start_pipeline(slot, sink, extra) do
    ref = make_ref()
    test_pid = self()

    :telemetry.attach(
      {__MODULE__, {:active, slot}, ref},
      [:replicant, :connection, :slot_active],
      fn _e, _m, _meta, _ -> send(test_pid, {:slot_active, ref}) end,
      nil
    )

    {:ok, _} =
      Replicant.start_link(
        [
          connection: PG16.pg_opts(),
          slot_name: slot,
          publication: "msg_pub",
          sink: sink,
          go_forward_only: true
        ] ++ extra
      )

    receive do
      {:slot_active, ^ref} -> :ok
    after
      15_000 -> ExUnit.Assertions.flunk("pipeline never reached slot_active for #{slot}")
    end

    :telemetry.detach({__MODULE__, {:active, slot}, ref})
    PG16.wait_until(fn -> connection_pid(slot) != nil end, 800)
  end

  defp attach_stream_probe do
    ref = make_ref()
    test_pid = self()

    :telemetry.attach(
      {__MODULE__, :stream, ref},
      [:replicant, :stream, :committed],
      fn _e, _m, _meta, _ -> send(test_pid, {:stream_committed, ref}) end,
      nil
    )

    ref
  end

  defp stream_committed_count(ref), do: drain_stream(ref, 0)

  defp drain_stream(ref, acc) do
    receive do
      {:stream_committed, ^ref} -> drain_stream(ref, acc + 1)
    after
      0 -> acc
    end
  end

  defp detach_stream_probe(ref), do: :telemetry.detach({__MODULE__, :stream, ref})

  defp reset_schema(c) do
    Postgrex.query!(c, "DROP PUBLICATION IF EXISTS msg_pub", [])
    Postgrex.query!(c, "DROP TABLE IF EXISTS msg_orders", [])
    Postgrex.query!(c, "CREATE TABLE msg_orders (id int PRIMARY KEY, note text)", [])
    Postgrex.query!(c, "DROP TABLE IF EXISTS msg_noise", [])
    Postgrex.query!(c, "CREATE TABLE msg_noise (id bigserial PRIMARY KEY, v text)", [])

    Postgrex.query!(c, "DROP TABLE IF EXISTS msg_sink_rows", [])
    # Append-only (no PK) so duplicate deliveries are detectable; commit_lsn for provenance.
    Postgrex.query!(c, "CREATE TABLE msg_sink_rows (commit_lsn bigint, id int)", [])

    Postgrex.query!(c, "DROP TABLE IF EXISTS msg_sink_txn_messages", [])

    Postgrex.query!(
      c,
      "CREATE TABLE msg_sink_txn_messages (commit_lsn bigint, prefix text, content text, ordinal int)",
      []
    )

    Postgrex.query!(c, "DROP TABLE IF EXISTS msg_sink_messages", [])

    Postgrex.query!(
      c,
      "CREATE TABLE msg_sink_messages (lsn bigint, prefix text, content text)",
      []
    )

    Postgrex.query!(c, "DROP TABLE IF EXISTS msg_sink_cp", [])
    Postgrex.query!(c, "CREATE TABLE msg_sink_cp (id int PRIMARY KEY, lsn bigint)", [])

    Postgrex.query!(c, "DROP TABLE IF EXISTS msg_sink_calls", [])

    Postgrex.query!(
      c,
      "CREATE TABLE msg_sink_calls (lsn bigint, n_changes int, n_messages int)",
      []
    )

    # Drop any stale lib-mode checkpoint table but do NOT re-create it: the CheckpointStore's
    # own `ensure/1` creates it with the canonical 3-column schema (slot_name, commit_lsn,
    # updated_at). A hand-rolled 2-column table (missing updated_at NOT NULL) passes the
    # commit_lsn shape-probe but FAILS the upsert's `INSERT (..., updated_at) VALUES (..., now())`,
    # halting the pipeline on the first batch flush (mirrors the working batching_test.exs, which
    # likewise lets the store own the table).
    Postgrex.query!(c, "DROP TABLE IF EXISTS #{@cp_table}", [])

    Postgrex.query!(c, "CREATE PUBLICATION msg_pub FOR TABLE msg_orders", [])
  end

  defp insert(c, id),
    do: Postgrex.query!(c, "INSERT INTO msg_orders (id, note) VALUES ($1, $2)", [id, "n#{id}"])

  defp row_ids(c) do
    %Postgrex.Result{rows: rows} = Postgrex.query!(c, "SELECT id FROM msg_sink_rows", [])
    rows |> Enum.map(fn [id] -> id end) |> MapSet.new()
  end

  defp txn_messages(c) do
    %Postgrex.Result{rows: rows} =
      Postgrex.query!(
        c,
        "SELECT prefix, content FROM msg_sink_txn_messages ORDER BY commit_lsn, ordinal",
        []
      )

    Enum.map(rows, fn [prefix, content] -> {prefix, content} end)
  end

  defp non_txn_messages(c) do
    %Postgrex.Result{rows: rows} =
      Postgrex.query!(c, "SELECT prefix, content FROM msg_sink_messages ORDER BY lsn", [])

    Enum.map(rows, fn [prefix, content] -> {prefix, content} end)
  end

  defp calls(c) do
    %Postgrex.Result{rows: rows} =
      Postgrex.query!(c, "SELECT lsn, n_changes, n_messages FROM msg_sink_calls ORDER BY lsn", [])

    Enum.map(rows, fn [lsn, n_changes, n_messages] -> {lsn, n_changes, n_messages} end)
  end

  defp cp_lsn(c) do
    case Postgrex.query(c, "SELECT lsn FROM msg_sink_cp WHERE id = 1", []) do
      {:ok, %Postgrex.Result{rows: [[lsn]]}} -> lsn
      {:ok, %Postgrex.Result{rows: []}} -> nil
      {:error, %Postgrex.Error{postgres: %{code: :undefined_table}}} -> nil
    end
  end

  defp store_lsn(c, slot) do
    case Postgrex.query(c, "SELECT commit_lsn FROM #{@cp_table} WHERE slot_name = $1", [slot]) do
      {:ok, %Postgrex.Result{rows: [[lsn]]}} -> lsn
      {:ok, %Postgrex.Result{rows: []}} -> nil
      {:error, %Postgrex.Error{postgres: %{code: :undefined_table}}} -> nil
    end
  end

  defp confirmed_flush(c, slot) do
    case Postgrex.query!(
           c,
           "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
           [slot]
         ).rows do
      [[lsn]] when is_binary(lsn) -> Replicant.lsn_from_string(lsn)
      _ -> 0
    end
  end

  defp spill_files(dir, slot) do
    case File.ls(dir) do
      {:ok, files} -> Enum.filter(files, &String.starts_with?(&1, slot))
      {:error, _} -> []
    end
  end

  defp connection_pid(slot) do
    case Registry.lookup(Replicant.Registry, {slot, :connection}) do
      [{pid, _}] -> pid
      [] -> nil
    end
  end

  defp drop_slot(c, slot), do: drop_slot(c, slot, 20)

  defp drop_slot(c, slot, 0) do
    Postgrex.query!(
      c,
      "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
      [slot]
    )
  end

  defp drop_slot(c, slot, tries) do
    Postgrex.query!(
      c,
      "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
      [slot]
    )
  rescue
    _ ->
      Process.sleep(50)
      drop_slot(c, slot, tries - 1)
  end
end
