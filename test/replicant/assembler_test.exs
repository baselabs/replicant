defmodule Replicant.AssemblerTest.AcceptingSink do
  @moduledoc false
  @behaviour Replicant.Sink
  use Agent

  def start_link(_opts \\ []) do
    case Agent.start_link(fn -> [] end, name: __MODULE__) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
    end
  end

  @impl Replicant.Sink
  def checkpoint, do: {:ok, nil}

  @impl Replicant.Sink
  def handle_transaction(%Replicant.Transaction{} = txn), do: {:ok, txn.commit_lsn}

  @impl Replicant.Sink
  def handle_schema_change(_sc, _ctx), do: :ok
end

defmodule Replicant.AssemblerTest.ThrowingSink do
  @moduledoc false
  @behaviour Replicant.Sink
  @impl true
  def checkpoint, do: {:ok, nil}
  @impl true
  def handle_transaction(_txn), do: throw(:sink_threw)
end

defmodule Replicant.AssemblerTest.ExitingSink do
  @moduledoc false
  @behaviour Replicant.Sink
  @impl true
  def checkpoint, do: {:ok, nil}
  # exit reason embeds the whole transaction — mimics a GenServer.call/Agent timeout
  # whose call args carry the row values (the Critical-Rule-1 leak vector).
  @impl true
  def handle_transaction(txn), do: exit({:sink_down, txn})
end

defmodule Replicant.AssemblerTest.RaisingSink do
  @moduledoc false
  @behaviour Replicant.Sink
  @impl true
  def checkpoint, do: {:ok, nil}
  @impl true
  def handle_transaction(_txn), do: raise("sink boom")
end

defmodule Replicant.AssemblerTest.ExitCheckpointSink do
  @moduledoc false
  @behaviour Replicant.Sink
  # checkpoint/0 exits (e.g. a GenServer.call to a dead checkpoint-store process)
  @impl true
  def checkpoint, do: exit(:checkpoint_store_down)
  @impl true
  def handle_transaction(txn), do: {:ok, txn.commit_lsn}
end

defmodule Replicant.AssemblerTest do
  use ExUnit.Case, async: false

  alias Replicant.{
    Assembler,
    Change,
    Decoder.Messages,
    SchemaChange,
    Test.RecordingSink,
    Transaction
  }

  alias Replicant.AssemblerTest.AcceptingSink

  @lsn 0x10

  defp begin_msg, do: %Messages.Begin{final_lsn: @lsn, commit_timestamp: nil, xid: 1}

  defp commit_msg,
    do: %Messages.Commit{
      lsn: @lsn,
      end_lsn: @lsn,
      commit_timestamp: ~U[2026-07-04 00:00:00Z],
      flags: []
    }

  defp relation_msg(columns, id \\ 16_384, identity \\ :default) do
    %Messages.Relation{
      id: id,
      namespace: "public",
      name: "orders",
      replica_identity: identity,
      columns: columns
    }
  end

  defp col(name, type, flags \\ []),
    do: %Messages.Relation.Column{
      name: name,
      type: type,
      flags: flags,
      type_modifier: 4_294_967_295
    }

  setup do
    {:ok, pid} = RecordingSink.start_link()

    on_exit(fn ->
      # Deterministic teardown: stopping the linked named Agent can race its own
      # link-driven death (TOCTOU on Process.alive?/1), so tolerate a dead pid.
      try do
        Agent.stop(pid)
      catch
        :exit, _ -> :ok
      end
    end)

    RecordingSink.reset()
    :ok
  end

  describe "full transaction cycle (synchronous sink apply)" do
    test "Begin → Relation → Insert → Commit yields a Transaction with commit_lsn and one Change" do
      asm = Assembler.new(RecordingSink)

      {:ok, asm} = Assembler.handle_message(asm, begin_msg())

      {:ok, asm} =
        Assembler.handle_message(
          asm,
          relation_msg([col("id", "int4", [:key]), col("status", "text")])
        )

      insert = %Messages.Insert{relation_id: 16_384, tuple_data: {<<0, 0, 0, 1>>, "paid"}}
      {:ok, asm} = Assembler.handle_message(asm, insert)

      assert {:transaction, %Transaction{commit_lsn: @lsn} = txn, returned_lsn, _asm} =
               Assembler.handle_message(asm, commit_msg())

      assert returned_lsn == @lsn

      assert [%Change{op: :insert, table: "orders", record: record, columns: columns}] =
               txn.changes

      assert record["status"] == "paid"
      # spec §10: keys stay binaries through the real decode→assemble producer path
      # (no String.to_atom) — the non-vacuous string-key guard the seam table assigns here.
      assert Enum.all?(Map.keys(record), &is_binary/1)
      # change.columns carries the relation's per-column metadata (memoized at
      # relation-cache time), projected to Change.Column structs.
      assert Enum.map(columns, & &1.name) == ["id", "status"]
      assert Enum.map(columns, & &1.type) == ["int4", "text"]
      assert hd(columns).flags == [:key]
      # watermark-int: the sink was dispatched exactly once with the txn
      assert [{@lsn, [_change]}] = RecordingSink.seen()
    end
  end

  describe "unchanged TOAST (spec §7) — the sentinel never masquerades as a value" do
    test "an unchanged-TOAST column is in `unchanged`, ABSENT from `record`" do
      asm = Assembler.new(RecordingSink)
      {:ok, asm} = Assembler.handle_message(asm, begin_msg())

      {:ok, asm} =
        Assembler.handle_message(
          asm,
          relation_msg([col("id", "int4", [:key]), col("payload", "text")])
        )

      # tuple_data: id=1 (text "1"), payload = :unchanged_toast sentinel
      update = %Messages.Update{relation_id: 16_384, tuple_data: {"1", :unchanged_toast}}
      {:ok, asm} = Assembler.handle_message(asm, update)

      {:transaction, %Transaction{changes: [change]}, @lsn, _} =
        Assembler.handle_message(asm, commit_msg())

      assert change.op == :update
      assert "payload" in change.unchanged
      refute Map.has_key?(change.record, "payload"), "TOAST sentinel must not appear in record"
      # id is int4 → cast_value("1", "int4") returns the integer 1 (verified against
      # walex casting: Integer.parse("1") → {1, ""}). Asserting the cast happened also
      # proves materialize/2 ran the value through the caster.
      assert change.record["id"] == 1
    end
  end

  describe "watermark skip (spec §2) — re-delivered transaction is not re-dispatched" do
    test "a transaction whose commit_lsn <= checkpoint is skipped, not applied" do
      # Sink already checkpointed AT the txn's LSN → re-delivery must skip.
      RecordingSink
      |> stub_checkpoint(@lsn)

      asm = Assembler.new(RecordingSink)
      {:ok, asm} = Assembler.handle_message(asm, begin_msg())
      {:ok, asm} = Assembler.handle_message(asm, relation_msg([col("id", "int4", [:key])]))

      {:ok, asm} =
        Assembler.handle_message(asm, %Messages.Insert{relation_id: 16_384, tuple_data: {"1"}})

      assert {:skipped, @lsn, _asm} = Assembler.handle_message(asm, commit_msg())
      # the sink was NOT dispatched
      assert RecordingSink.seen() == []
    end
  end

  describe "lib mode (checkpoint-store)" do
    defmodule OkSink do
      # No @behaviour — checkpoint/0 is still mandatory at this commit point (Task 10).
      def handle_transaction(%Replicant.Transaction{} = txn), do: {:ok, txn.commit_lsn}
    end

    alias Replicant.Decoder.Messages.{Begin, Commit}

    test "a committed txn writes the checkpoint via the writer BEFORE returning, and advances the in-memory watermark" do
      parent = self()

      writer = fn lsn ->
        send(parent, {:wrote, lsn})
        :ok
      end

      asm = Assembler.new(OkSink, mode: :lib, checkpoint_writer: writer, lib_checkpoint: nil)

      {:ok, asm} = Assembler.handle_message(asm, %Begin{final_lsn: 100, xid: 1})

      assert {:transaction, _txn, 100, asm} =
               Assembler.handle_message(asm, %Commit{lsn: 100, commit_timestamp: nil})

      assert_received {:wrote, 100}
      assert asm.lib_checkpoint == 100
    end

    test "a re-delivered txn <= the in-memory watermark is pre-skipped (no writer call)" do
      parent = self()

      writer = fn lsn ->
        send(parent, {:wrote, lsn})
        :ok
      end

      asm = Assembler.new(OkSink, mode: :lib, checkpoint_writer: writer, lib_checkpoint: 100)

      {:ok, asm} = Assembler.handle_message(asm, %Begin{final_lsn: 50, xid: 2})

      assert {:skipped, 50, _asm} =
               Assembler.handle_message(asm, %Commit{lsn: 50, commit_timestamp: nil})

      refute_received {:wrote, _}
    end

    test "a checkpoint-store WRITE fault halts fail-closed (:checkpoint_store_failed), never announcing commit" do
      # Gate the checkpoint-after-persist ORDERING, not just the halt: a regression
      # that emitted [:sink, :committed] and THEN halted would still satisfy the
      # {:halt, ...} assertion — so we must also prove committed was NEVER emitted.
      attach_telemetry(self())

      writer = fn _lsn -> {:error, :store_down} end
      asm = Assembler.new(OkSink, mode: :lib, checkpoint_writer: writer, lib_checkpoint: nil)

      {:ok, asm} = Assembler.handle_message(asm, %Begin{final_lsn: 100, xid: 1})

      assert {:halt, %Replicant.Error{reason: :checkpoint_store_failed}, _asm} =
               Assembler.handle_message(asm, %Commit{lsn: 100, commit_timestamp: nil})

      # The write faulted → the sink's commit must never have been announced.
      refute_received {:tel, [:replicant, :sink, :committed], _}
    after
      :telemetry.detach({__MODULE__, :telemetry})
    end

    test "a WRITE fault emits [:checkpoint_store, :failed] carrying slot_name (spec §10 shape)" do
      # §10 specifies the :failed metadata as (slot_name, reason). The Assembler is the
      # third emission site (with CheckpointStore + Connection); it must carry slot_name too.
      attach_telemetry(self())

      writer = fn _lsn -> {:error, :store_down} end

      asm =
        Assembler.new(OkSink,
          mode: :lib,
          checkpoint_writer: writer,
          lib_checkpoint: nil,
          slot_name: "rep_slot_x"
        )

      {:ok, asm} = Assembler.handle_message(asm, %Begin{final_lsn: 100, xid: 1})

      assert {:halt, %Replicant.Error{reason: :checkpoint_store_failed}, _asm} =
               Assembler.handle_message(asm, %Commit{lsn: 100, commit_timestamp: nil})

      assert_received {:tel, [:replicant, :checkpoint_store, :failed],
                       %{slot_name: "rep_slot_x", reason: :checkpoint_store_failed}}
    after
      :telemetry.detach({__MODULE__, :telemetry})
    end

    test "a dead-store writer EXIT is caught value-free and halts :checkpoint_store_failed (not :decode_failure)" do
      writer = fn _lsn -> exit(:noproc) end
      asm = Assembler.new(OkSink, mode: :lib, checkpoint_writer: writer, lib_checkpoint: nil)
      {:ok, asm} = Assembler.handle_message(asm, %Begin{final_lsn: 7, xid: 1})

      assert {:halt, %Replicant.Error{reason: :checkpoint_store_failed}, _asm} =
               Assembler.handle_message(asm, %Commit{lsn: 7, commit_timestamp: nil})
    end
  end

  describe "schema-change diff (spec §9) — destructive halts fail-closed" do
    test "an added column is additive and does not halt" do
      asm = Assembler.new(RecordingSink)
      {:ok, asm} = Assembler.handle_message(asm, relation_msg([col("id", "int4", [:key])]))
      # second Relation with an added column → additive, no halt
      assert {:schema_change, %SchemaChange{kind: :additive}, _asm} =
               Assembler.handle_message(
                 asm,
                 relation_msg([col("id", "int4", [:key]), col("note", "text")])
               )
    end

    test "a dropped column is destructive and halts" do
      asm = Assembler.new(RecordingSink)

      {:ok, asm} =
        Assembler.handle_message(
          asm,
          relation_msg([col("id", "int4", [:key]), col("payload", "text")])
        )

      assert {:halt, %SchemaChange{kind: :destructive, change: :column_dropped}, _asm} =
               Assembler.handle_message(asm, relation_msg([col("id", "int4", [:key])]))
    end
  end

  describe "Delete + multi-row + schema-change delegation" do
    test "a Delete yields op: :delete with old_record from the key tuple and nil record" do
      asm = Assembler.new(RecordingSink)
      {:ok, asm} = Assembler.handle_message(asm, begin_msg())
      {:ok, asm} = Assembler.handle_message(asm, relation_msg([col("id", "int4", [:key])]))

      delete = %Messages.Delete{
        relation_id: 16_384,
        old_tuple_data: nil,
        changed_key_tuple_data: {"1"}
      }

      {:ok, asm} = Assembler.handle_message(asm, delete)

      {:transaction, %Transaction{changes: [change]}, @lsn, _} =
        Assembler.handle_message(asm, commit_msg())

      assert change.op == :delete
      assert change.record == nil
      assert change.old_record["id"] == 1
    end

    test "a multi-row transaction preserves arrival order and ordinals (guards the O(n) accumulation)" do
      asm = Assembler.new(RecordingSink)
      {:ok, asm} = Assembler.handle_message(asm, begin_msg())

      {:ok, asm} =
        Assembler.handle_message(asm, relation_msg([col("id", "int4", [:key]), col("n", "text")]))

      {:ok, asm} =
        Assembler.handle_message(asm, %Messages.Insert{
          relation_id: 16_384,
          tuple_data: {"1", "a"}
        })

      {:ok, asm} =
        Assembler.handle_message(asm, %Messages.Insert{
          relation_id: 16_384,
          tuple_data: {"2", "b"}
        })

      {:ok, asm} =
        Assembler.handle_message(asm, %Messages.Insert{
          relation_id: 16_384,
          tuple_data: {"3", "c"}
        })

      {:transaction, %Transaction{changes: changes}, @lsn, _} =
        Assembler.handle_message(asm, commit_msg())

      assert Enum.map(changes, & &1.record["n"]) == ["a", "b", "c"]
      assert Enum.map(changes, & &1.ordinal) == [0, 1, 2]
    end

    test "a destructive change is APPLIED (not halted) when the sink accepts it via handle_schema_change/2" do
      {:ok, pid} = AcceptingSink.start_link()

      on_exit(fn ->
        # Deterministic teardown: stopping the linked named Agent can race its own
        # link-driven death (TOCTOU on Process.alive?/1), so tolerate a dead pid.
        try do
          Agent.stop(pid)
        catch
          :exit, _ -> :ok
        end
      end)

      asm = Assembler.new(AcceptingSink)

      {:ok, asm} =
        Assembler.handle_message(
          asm,
          relation_msg([col("id", "int4", [:key]), col("payload", "text")])
        )

      # dropped column = destructive; the accepting sink returns :ok → {:schema_change, ...}, NOT {:halt, ...}
      assert {:schema_change, %SchemaChange{kind: :destructive, change: :column_dropped}, _asm} =
               Assembler.handle_message(asm, relation_msg([col("id", "int4", [:key])]))
    end
  end

  describe "value-free boundary on casting raises (Critical Rule 1)" do
    test "a malformed numeric is scrubbed to {:halt, _} and never raises or leaks the value" do
      asm = Assembler.new(RecordingSink)
      {:ok, asm} = Assembler.handle_message(asm, begin_msg())

      {:ok, asm} =
        Assembler.handle_message(
          asm,
          relation_msg([col("id", "int4", [:key]), col("amount", "numeric")])
        )

      # "amount" carries a malformed numeric → Decimal.new/1 raises ArgumentError
      # (which embeds the offending string). The Assembler's public boundary must
      # scrub it to a value-free {:halt, _} and NEVER raise out of handle_message/2.
      insert = %Messages.Insert{relation_id: 16_384, tuple_data: {"1", "not-a-number-value"}}
      assert {:halt, err, _asm} = Assembler.handle_message(asm, insert)
      refute inspect(err) <> Exception.message(err) =~ "not-a-number-value"
      assert err.reason == :decode_failure
    end
  end

  describe "telemetry (spec §10 — Assembler-owned events; exercises Task 11's allowlist)" do
    test "a commit emits [:transaction, :assembled] then [:sink, :committed], value-free meta" do
      attach_telemetry(self())

      asm = Assembler.new(RecordingSink)
      {:ok, asm} = Assembler.handle_message(asm, begin_msg())
      {:ok, asm} = Assembler.handle_message(asm, relation_msg([col("id", "int4", [:key])]))

      {:ok, asm} =
        Assembler.handle_message(asm, %Messages.Insert{relation_id: 16_384, tuple_data: {"1"}})

      {:transaction, %Transaction{}, @lsn, _} = Assembler.handle_message(asm, commit_msg())

      assert_received {:tel, [:replicant, :transaction, :assembled],
                       %{commit_lsn: @lsn, change_count: 1}}

      assert_received {:tel, [:replicant, :sink, :committed], %{commit_lsn: @lsn}}
    after
      :telemetry.detach({__MODULE__, :telemetry})
    end
  end

  describe "fail-closed boundary hardening (closeout review, 2026-07-04)" do
    alias Replicant.AssemblerTest.{ExitCheckpointSink, ExitingSink, RaisingSink, ThrowingSink}
    alias Replicant.Error

    test "a sink that THROWS is scrubbed to {:halt, :sink_failed} — never escapes the boundary" do
      assert {:halt, %Error{reason: :sink_failed}, _asm} = drive_single_row(ThrowingSink)
    end

    test "a sink that EXITS with a row-value-bearing reason is scrubbed value-free — no escape, no leak" do
      asm = Assembler.new(ExitingSink)
      {:ok, asm} = Assembler.handle_message(asm, begin_msg())

      {:ok, asm} =
        Assembler.handle_message(
          asm,
          relation_msg([col("id", "int4", [:key]), col("v", "text")])
        )

      {:ok, asm} =
        Assembler.handle_message(asm, %Messages.Insert{
          relation_id: 16_384,
          tuple_data: {"1", "secret-row-97"}
        })

      assert {:halt, %Error{reason: :sink_failed} = err, _asm} =
               Assembler.handle_message(asm, commit_msg())

      refute inspect(err) <> Exception.message(err) =~ "secret-row-97"
    end

    test "a sink that RAISES is labeled :sink_failed (distinguishable from a casting :decode_failure)" do
      assert {:halt, %Error{reason: :sink_failed}, _asm} = drive_single_row(RaisingSink)
    end

    test "a checkpoint/0 that EXITS is caught, does not escape the boundary (fail-open, dup-safe)" do
      assert {:transaction, %Transaction{}, @lsn, _asm} = drive_single_row(ExitCheckpointSink)
    end

    test "a row for an uncached relation halts fail-closed and does NOT dispatch the sink" do
      asm = Assembler.new(RecordingSink)
      {:ok, asm} = Assembler.handle_message(asm, begin_msg())

      assert {:halt, %Error{reason: :config_invalid}, _asm} =
               Assembler.handle_message(asm, %Messages.Insert{
                 relation_id: 999,
                 tuple_data: {"x"}
               })

      assert RecordingSink.seen() == []
    end

    test "a truncate for an uncached relation halts fail-closed" do
      asm = Assembler.new(RecordingSink)
      {:ok, asm} = Assembler.handle_message(asm, begin_msg())

      assert {:halt, %Error{reason: :config_invalid}, _asm} =
               Assembler.handle_message(asm, %Messages.Truncate{
                 number_of_relations: 1,
                 options: [],
                 truncated_relations: [999]
               })
    end
  end

  describe "old_record is key-only under non-FULL identity (spec §7)" do
    test "a key-identity delete drops non-key nil placeholders from old_record" do
      asm = Assembler.new(RecordingSink)
      {:ok, asm} = Assembler.handle_message(asm, begin_msg())

      {:ok, asm} =
        Assembler.handle_message(
          asm,
          relation_msg([col("id", "int4", [:key]), col("name", "text")], 16_384, :index)
        )

      # Delete USING INDEX: full-width key tuple {"5", nil} — id is key, name non-key sent NULL.
      delete = %Messages.Delete{
        relation_id: 16_384,
        old_tuple_data: nil,
        changed_key_tuple_data: {"5", nil}
      }

      {:ok, asm} = Assembler.handle_message(asm, delete)

      {:transaction, %Transaction{changes: [change]}, @lsn, _} =
        Assembler.handle_message(asm, commit_msg())

      assert change.old_record == %{"id" => 5}
      refute Map.has_key?(change.old_record, "name")
    end

    test "a FULL-identity old tuple keeps all columns (real values, none dropped)" do
      asm = Assembler.new(RecordingSink)
      {:ok, asm} = Assembler.handle_message(asm, begin_msg())

      {:ok, asm} =
        Assembler.handle_message(
          asm,
          relation_msg([col("id", "int4", [:key]), col("name", "text")], 16_384, :all_columns)
        )

      # FULL identity → old_tuple_data carries the whole old row; nothing is dropped.
      update = %Messages.Update{
        relation_id: 16_384,
        tuple_data: {"5", "new"},
        old_tuple_data: {"5", "old"},
        changed_key_tuple_data: nil
      }

      {:ok, asm} = Assembler.handle_message(asm, update)

      {:transaction, %Transaction{changes: [change]}, @lsn, _} =
        Assembler.handle_message(asm, commit_msg())

      assert change.old_record == %{"id" => 5, "name" => "old"}
    end
  end

  describe "truncate ordinals (closeout review)" do
    test "truncate assigns unique monotonic ordinals that do not collide with following changes" do
      asm = Assembler.new(RecordingSink)
      {:ok, asm} = Assembler.handle_message(asm, begin_msg())

      {:ok, asm} =
        Assembler.handle_message(asm, relation_msg([col("id", "int4", [:key])], 16_384))

      {:ok, asm} =
        Assembler.handle_message(asm, relation_msg([col("id", "int4", [:key])], 16_385))

      {:ok, asm} =
        Assembler.handle_message(asm, %Messages.Insert{relation_id: 16_384, tuple_data: {"1"}})

      {:ok, asm} =
        Assembler.handle_message(asm, %Messages.Truncate{
          number_of_relations: 2,
          options: [],
          truncated_relations: [16_384, 16_385]
        })

      {:ok, asm} =
        Assembler.handle_message(asm, %Messages.Insert{relation_id: 16_384, tuple_data: {"2"}})

      {:transaction, %Transaction{changes: changes}, @lsn, _} =
        Assembler.handle_message(asm, commit_msg())

      ordinals = Enum.map(changes, & &1.ordinal)
      assert ordinals == [0, 1, 2, 3]
      assert length(Enum.uniq(ordinals)) == length(ordinals)
    end
  end

  # Drive Begin → Relation → Insert → Commit through `sink`, returning the Commit result.
  defp drive_single_row(sink) do
    asm = Assembler.new(sink)
    {:ok, asm} = Assembler.handle_message(asm, begin_msg())
    {:ok, asm} = Assembler.handle_message(asm, relation_msg([col("id", "int4", [:key])]))

    {:ok, asm} =
      Assembler.handle_message(asm, %Messages.Insert{relation_id: 16_384, tuple_data: {"1"}})

    Assembler.handle_message(asm, commit_msg())
  end

  # Helper: configure the recording sink's checkpoint/0 return for the skip test.
  defp stub_checkpoint(_module, lsn) do
    Process.put({RecordingSink, :checkpoint}, lsn)
  end

  defp attach_telemetry(pid) do
    :telemetry.attach_many(
      {__MODULE__, :telemetry},
      [
        [:replicant, :transaction, :assembled],
        [:replicant, :sink, :committed],
        [:replicant, :sink, :failed],
        [:replicant, :checkpoint_store, :failed],
        [:replicant, :schema_change, :additive],
        [:replicant, :schema_change, :halted]
      ],
      fn name, _meas, meta, _cfg -> send(pid, {:tel, name, meta}) end,
      nil
    )
  end

  describe "byte_size + lag_ms enrichment on [:transaction, :assembled]" do
    defmodule EnrichSink do
      @behaviour Replicant.Sink
      @impl true
      def checkpoint, do: {:ok, nil}
      @impl true
      def handle_transaction(txn), do: {:ok, txn.commit_lsn}
    end

    test "a commit carries the accumulated byte_size and a non-negative lag_ms (value-free)" do
      alias Replicant.Decoder.Messages.{Begin, Commit, Insert, Relation}
      alias Replicant.Decoder.Messages.Relation.Column

      :telemetry.attach(
        {__MODULE__, :enrich},
        [:replicant, :transaction, :assembled],
        fn _event, _measurements, meta, pid -> send(pid, {:assembled_meta, meta}) end,
        self()
      )

      relation = %Relation{
        id: 1,
        namespace: "public",
        name: "t",
        replica_identity: :default,
        columns: [%Column{name: "id", type: "int4", flags: [:key], type_modifier: -1}]
      }

      # The AssemblerServer ordering: observe_bytes(payload_size) THEN handle_message.
      # byte_size accrues for the messages processed while a txn buffer is open
      # (Begin's own bytes fall before the buffer exists — a wire-size gauge, not exact).
      asm =
        Replicant.Assembler.new(EnrichSink)
        |> step(20, %Begin{final_lsn: 0x2A, commit_timestamp: ~U[2026-07-04 00:00:00Z], xid: 7})
        |> step(30, relation)
        |> step(15, %Insert{relation_id: 1, tuple_data: {"1"}})

      # Commit is observed (buffer still open) then handled → emit reads byte_size.
      asm = Replicant.Assembler.observe_bytes(asm, 8)

      {:transaction, _txn, _lsn, _asm} =
        Replicant.Assembler.handle_message(asm, %Commit{
          lsn: 0x2A,
          end_lsn: 0x2A,
          commit_timestamp: ~U[2026-07-04 00:00:00Z],
          flags: []
        })

      assert_received {:assembled_meta, meta}
      assert meta.change_count == 1
      assert meta.commit_lsn == 0x2A
      # 30 (relation) + 15 (insert) + 8 (commit) = 53; Begin's 20 fell before the buffer opened.
      assert meta.byte_size == 53
      assert is_integer(meta.lag_ms) and meta.lag_ms >= 0

      :telemetry.detach({__MODULE__, :enrich})
    end

    # observe_bytes THEN handle_message, returning the updated assembler.
    defp step(asm, bytes, message) do
      asm = Replicant.Assembler.observe_bytes(asm, bytes)
      {:ok, asm} = Replicant.Assembler.handle_message(asm, message)
      asm
    end
  end

  describe "batched checkpointing (lib mode, spec §7)" do
    defmodule BatchSink do
      def handle_transaction(%Replicant.Transaction{} = txn), do: {:ok, txn.commit_lsn}
    end

    alias Replicant.Decoder.Messages.{Begin, Commit}

    defp batched(writer, policy) do
      Replicant.Assembler.new(BatchSink,
        mode: :lib,
        checkpoint_writer: writer,
        slot_name: "rep_batch",
        lib_checkpoint: 0,
        batch: policy
      )
    end

    # Drive one committed txn (Begin(lsn) → Commit(lsn)) through `asm`, returning the result.
    defp commit_txn(asm, lsn) do
      {:ok, asm} = Assembler.handle_message(asm, %Begin{final_lsn: lsn, xid: lsn})
      Assembler.handle_message(asm, %Commit{lsn: lsn, commit_timestamp: nil})
    end

    test "a txn under the count/span cap is BUFFERED (no writer call, watermark not advanced)" do
      parent = self()
      writer = fn lsn -> send(parent, {:wrote, lsn}) && :ok end
      asm = batched(writer, max_transactions: 3, max_delay_ms: 1000, max_span: 1_000_000)

      assert {:buffered, asm} = commit_txn(asm, 100)
      refute_received {:wrote, _}
      assert Assembler.batch_pending?(asm)
      # lib_checkpoint stays at the pre-batch value until a flush (per-batch advance).
      assert asm.lib_checkpoint == 0
    end

    test "reaching max_transactions returns {:flush, :max_transactions, _}; flush_batch writes once and advances the watermark per-batch" do
      parent = self()
      writer = fn lsn -> send(parent, {:wrote, lsn}) && :ok end
      asm = batched(writer, max_transactions: 2, max_delay_ms: 1000, max_span: 1_000_000)

      assert {:buffered, asm} = commit_txn(asm, 100)
      assert {:flush, :max_transactions, asm} = commit_txn(asm, 200)
      refute_received {:wrote, _}

      assert {:ok, 200, asm} = Assembler.flush_batch(asm, :max_transactions)
      assert_received {:wrote, 200}
      # ONE write for the whole batch, at the highest LSN; watermark now == durable checkpoint.
      assert asm.lib_checkpoint == 200
      refute Assembler.batch_pending?(asm)
    end

    test "the LSN-span cap trips {:flush, :max_span, _} — span measured from max(lib_checkpoint, stream_floor), NOT the first buffered txn (spec §4/§7/decision-log #7)" do
      writer = fn _lsn -> :ok end
      # Fresh-ish: lib_checkpoint 0, stream_floor 100 → span base = max(0, 100) = 100.
      asm = %{
        batched(writer, max_transactions: 100, max_delay_ms: 1000, max_span: 10)
        | stream_floor: 100
      }

      assert {:buffered, asm} = commit_txn(asm, 105)
      # span = 105 - 100 = 5 < 10 → buffered.
      assert {:buffered, asm} = commit_txn(asm, 108)
      # span = 108 - 100 = 8 < 10 → buffered.
      # Third txn lsn 112: span = 112 - 100 = 12 >= 10 → flush by span. (Measured from the FLOOR 100,
      # not the first buffered txn 105 — under the old batch_base_lsn base this would be 112-105=7 and
      # would NOT flush; this is the ratified `pending_lsn − max(lib_checkpoint, stream_floor)`.)
      assert {:flush, :max_span, _asm} = commit_txn(asm, 112)
    end

    test "span base is max(lib_checkpoint, stream_floor): a large absolute first-txn LSN on a fresh slot (lib_checkpoint 0) does NOT spuriously span-flush (cold-start fix)" do
      writer = fn _lsn -> :ok end

      # Fresh slot: lib_checkpoint 0 but the stream started at a large absolute LSN (stream_floor).
      # Without the floor, span would be `1_000_050 − 0` → a spurious flush of the first txn.
      asm = %{
        batched(writer, max_transactions: 100, max_delay_ms: 1000, max_span: 100)
        | stream_floor: 1_000_000
      }

      # span = 1_000_050 - max(0, 1_000_000) = 50 < 100 → buffered, batching NOT defeated.
      assert {:buffered, _asm} = commit_txn(asm, 1_000_050)
    end

    test "flush_batch on a WRITE fault returns {:error, %Error{}, _} and does NOT advance the watermark" do
      writer = fn _lsn -> {:error, :checkpoint_store_failed} end
      asm = batched(writer, max_transactions: 1, max_delay_ms: 1000, max_span: 1_000_000)

      assert {:flush, :max_transactions, asm} = commit_txn(asm, 100)

      assert {:error, %Replicant.Error{reason: :checkpoint_store_failed}, asm} =
               Assembler.flush_batch(asm, :max_transactions)

      # fail-closed: the batch is NOT checkpointed, so the watermark stays behind → restart re-delivers.
      assert asm.lib_checkpoint == 0
    end

    test "flush_batch scrubs a value-bearing write-fault reason to :checkpoint_store_failed (value-free, Critical Rule 1)" do
      # The checkpoint_writer type is (lsn -> :ok | {:error, term()}); a writer returning a
      # value-bearing term must NOT leak it into %Error{} (the per-txn commit_txn path already
      # collapses any {:error, _} to :checkpoint_store_failed — flush_batch must match).
      writer = fn _lsn -> {:error, %{row_payload: "secret"}} end
      asm = batched(writer, max_transactions: 1, max_delay_ms: 1000, max_span: 1_000_000)

      assert {:flush, :max_transactions, asm} = commit_txn(asm, 100)

      assert {:error, %Replicant.Error{reason: :checkpoint_store_failed}, asm} =
               Assembler.flush_batch(asm, :max_transactions)

      assert asm.lib_checkpoint == 0
    end

    test "flush_batch with no open batch is :empty (a stale flush-timer fire is a no-op)" do
      writer = fn _lsn -> :ok end
      asm = batched(writer, max_transactions: 5, max_delay_ms: 1000, max_span: 1_000_000)
      assert :empty = Assembler.flush_batch(asm, :max_delay_ms)
    end

    test "a re-delivered txn <= the watermark is SKIPPED in batch mode (never buffered; watermark unchanged, spec §12.4)" do
      writer = fn _lsn -> :ok end
      # Seed a durable watermark; on resume the slot re-delivers txns <= it, which MUST pre-skip
      # (not buffer) so a skip-ack can never exceed the durable checkpoint (spec §9 invariant).
      asm =
        %{
          batched(writer, max_transactions: 5, max_delay_ms: 1000, max_span: 1_000_000)
          | lib_checkpoint: 500
        }

      {:ok, asm} = Assembler.handle_message(asm, %Begin{final_lsn: 300, xid: 1})

      assert {:skipped, 300, asm} =
               Assembler.handle_message(asm, %Commit{lsn: 300, commit_timestamp: nil})

      # The skip opened NO batch and did NOT advance the watermark past the durable checkpoint.
      refute Assembler.batch_pending?(asm)
      assert asm.batch_count == 0
      assert asm.lib_checkpoint == 500
    end
  end

  describe "batch delivery (sink-owned, spec §6) — flush_batch delivers via handle_batch" do
    # These unit tests call Assembler.flush_batch/2 DIRECTLY (in the test process), so the sink's
    # handle_batch runs in the test process → `self()` IS the test pid. No Application env needed.
    defmodule DeliverSink do
      def checkpoint, do: {:ok, nil}

      def handle_batch(txns) do
        send(self(), {:delivered, txns})
        {:ok, List.last(txns).commit_lsn}
      end
    end

    defmodule ErrorBatchSink do
      def checkpoint, do: {:ok, nil}
      def handle_batch(_txns), do: {:error, %{secret: "row"}}
    end

    defmodule RaisingBatchSink do
      def checkpoint, do: {:ok, nil}
      def handle_batch(_txns), do: raise(ArgumentError, "boom")
    end

    defmodule ExitingBatchSink do
      def checkpoint, do: {:ok, nil}
      # struct-shaped exit reason (a buffered row) → a safe_shape(reason) regression in the
      # catch arm would surface inspect(Replicant.Transaction) here, turning shape: nil red.
      def handle_batch(_txns), do: exit(%Replicant.Transaction{commit_lsn: 999, changes: []})
    end

    defmodule UnexpectedReturnBatchSink do
      def checkpoint, do: {:ok, nil}
      # A NON-conforming NORMAL return (not {:ok,_}/{:error,_}) embedding a row value. The `try`
      # wraps only the handle_batch CALL, so this return reaches the result `case`; without a
      # value-free catch-all it raises CaseClauseError inlining the term → an uncontrolled
      # Rule-1 leak into the OTP crash log (do_flush has no outer rescue). Effect-once/spec §8.
      def handle_batch(_txns),
        do: {:committed, %Replicant.Transaction{commit_lsn: 700, changes: []}}
    end

    # A sink-owned batch assembler with a manually-seeded buffer (Task 4 wires apply_sink to
    # populate it; here we test flush_batch in isolation). batch_txns is stored newest-first
    # (prepend), so flush_batch must reverse it to ascending commit-LSN order.
    defp buffered(sink, txns) do
      lsns = Enum.map(txns, & &1.commit_lsn)

      %{
        Replicant.Assembler.new(sink,
          batch: [max_transactions: 10, max_delay_ms: 1000, max_span: 1_000_000]
        )
        | batch_txns: Enum.reverse(txns),
          batch_count: length(txns),
          pending_lsn: Enum.max(lsns)
      }
    end

    alias Replicant.Decoder.Messages.{Begin, Commit}

    # Drive one committed txn (Begin(lsn) → Commit(lsn), no changes) through `asm`.
    defp bd_commit(asm, lsn) do
      {:ok, asm} = Assembler.handle_message(asm, %Begin{final_lsn: lsn, xid: lsn})
      Assembler.handle_message(asm, %Commit{lsn: lsn, commit_timestamp: nil})
    end

    defp sink_batched(sink, policy) do
      %{Replicant.Assembler.new(sink, batch: policy) | stream_floor: 0}
    end

    test "a committed txn is BUFFERED (handle_batch NOT called; not delivered until flush)" do
      asm =
        sink_batched(DeliverSink, max_transactions: 3, max_delay_ms: 1000, max_span: 1_000_000)

      assert {:buffered, asm} = bd_commit(asm, 100)
      refute_received {:delivered, _}
      assert Assembler.batch_pending?(asm)
      assert [%Replicant.Transaction{commit_lsn: 100}] = asm.batch_txns
    end

    test "reaching max_transactions returns {:flush, :max_transactions, _} carrying the ordered buffer" do
      asm =
        sink_batched(DeliverSink, max_transactions: 2, max_delay_ms: 1000, max_span: 1_000_000)

      assert {:buffered, asm} = bd_commit(asm, 100)
      assert {:flush, :max_transactions, asm} = bd_commit(asm, 200)
      # buffered newest-first; flush_batch reverses to ascending order.
      assert [%{commit_lsn: 200}, %{commit_lsn: 100}] = asm.batch_txns

      assert {:ok, 200, _asm} = Assembler.flush_batch(asm, :max_transactions)
      assert_received {:delivered, [%{commit_lsn: 100}, %{commit_lsn: 200}]}
    end

    test "the LSN-span cap trips {:flush, :max_span, _} measured from max(lib_checkpoint, stream_floor)" do
      asm = %{
        sink_batched(DeliverSink, max_transactions: 100, max_delay_ms: 1000, max_span: 10)
        | stream_floor: 100
      }

      assert {:buffered, asm} = bd_commit(asm, 105)
      assert {:flush, :max_span, _asm} = bd_commit(asm, 112)
    end

    test "a re-delivered txn <= the live sink checkpoint is SKIPPED, never buffered" do
      # DeliverSink.checkpoint/0 returns {:ok, nil}; use a sink whose checkpoint is 500.
      defmodule At500Sink do
        def checkpoint, do: {:ok, 500}
        def handle_batch(_txns), do: {:ok, 0}
      end

      asm = sink_batched(At500Sink, max_transactions: 5, max_delay_ms: 1000, max_span: 1_000_000)
      assert {:skipped, 300, asm} = bd_commit(asm, 300)
      refute Assembler.batch_pending?(asm)
      assert asm.batch_txns == []
    end

    test "flush_batch delivers the buffered txns as ONE handle_batch call in ascending LSN order, advances the span base, emits [:sink, :batch_committed]" do
      t1 = %Replicant.Transaction{commit_lsn: 100, changes: []}
      t2 = %Replicant.Transaction{commit_lsn: 200, changes: []}
      asm = buffered(DeliverSink, [t1, t2])

      test = self()

      :telemetry.attach(
        {__MODULE__, :bd_committed},
        [:replicant, :sink, :batch_committed],
        fn _e, _m, meta, _ -> send(test, {:tel, meta}) end,
        nil
      )

      assert {:ok, 200, asm} = Assembler.flush_batch(asm, :max_transactions)
      assert_received {:delivered, [^t1, ^t2]}
      assert asm.lib_checkpoint == 200
      refute Assembler.batch_pending?(asm)
      assert asm.batch_txns == []
      assert_received {:tel, %{change_count: 2, commit_lsn: 200, reason: :max_transactions}}
      :telemetry.detach({__MODULE__, :bd_committed})
    end

    test "a handle_batch {:error, _} halts fail-closed (value-free :sink_failed), watermark unchanged" do
      asm = buffered(ErrorBatchSink, [%Replicant.Transaction{commit_lsn: 100, changes: []}])

      assert {:error, %Replicant.Error{reason: :sink_failed, shape: nil}, asm} =
               Assembler.flush_batch(asm, :max_transactions)

      assert asm.lib_checkpoint == nil
    end

    test "a handle_batch raise is scrubbed value-free to :sink_failed (only the module name kept)" do
      asm = buffered(RaisingBatchSink, [%Replicant.Transaction{commit_lsn: 100, changes: []}])

      assert {:error, %Replicant.Error{reason: :sink_failed, shape: shape}, _asm} =
               Assembler.flush_batch(asm, :max_transactions)

      assert shape == inspect(ArgumentError)
    end

    test "a handle_batch exit is scrubbed WITHOUT inspecting the reason (Critical Rule 1)" do
      asm = buffered(ExitingBatchSink, [%Replicant.Transaction{commit_lsn: 100, changes: []}])

      assert {:error, %Replicant.Error{reason: :sink_failed, shape: nil}, _asm} =
               Assembler.flush_batch(asm, :max_transactions)
    end

    test "an UNEXPECTED handle_batch return shape halts fail-closed value-free (no CaseClauseError leak, Critical Rule 1)" do
      asm =
        buffered(UnexpectedReturnBatchSink, [%Replicant.Transaction{commit_lsn: 100, changes: []}])

      # Must NOT raise CaseClauseError (which would inline the returned term — a buffered row —
      # into an uncontrolled crash outside the value-free boundary). A non-conforming return
      # halts fail-closed value-free, exactly like {:error, _}: no term, no module name.
      assert {:error, %Replicant.Error{reason: :sink_failed, shape: nil}, asm} =
               Assembler.flush_batch(asm, :max_transactions)

      assert asm.lib_checkpoint == nil
    end

    test "flush_batch with no open batch is :empty" do
      asm =
        Replicant.Assembler.new(DeliverSink,
          batch: [max_transactions: 5, max_delay_ms: 1000, max_span: 1_000_000]
        )

      assert :empty = Assembler.flush_batch(asm, :max_delay_ms)
    end
  end

  describe "streaming reassembly (spec §5) — StreamStart/Stop + accumulate" do
    alias Replicant.Decoder.Messages.{
      Delete,
      Insert,
      Relation,
      StreamAbort,
      StreamCommit,
      StreamStart,
      StreamStop,
      Truncate,
      Update
    }

    alias Replicant.Decoder.Messages.Relation.Column

    defp streamed(max_concurrent \\ 64) do
      Replicant.Assembler.new(Replicant.Test.RecordingSink, max_concurrent_txns: max_concurrent)
    end

    defp with_relation(asm, rid) do
      rel = %Relation{
        id: rid,
        namespace: "public",
        name: "t",
        replica_identity: :default,
        columns: [%Column{name: "v", type: "int4", flags: [:key], type_modifier: nil}]
      }

      {:ok, asm} = Assembler.handle_message(asm, rel)
      asm
    end

    defp with_named_relation(asm, rid, name) do
      rel = %Relation{
        id: rid,
        namespace: "public",
        name: name,
        replica_identity: :default,
        columns: [%Column{name: "v", type: "int4", flags: [:key], type_modifier: nil}]
      }

      {:ok, asm} = Assembler.handle_message(asm, rel)
      asm
    end

    test "StreamStart opens a top-xid buffer; a streamed Insert accumulates tagged with its subxid" do
      asm = streamed() |> with_relation(1)
      {:ok, asm} = Assembler.handle_message(asm, %StreamStart{xid: 100, first_segment: true})
      assert asm.current_stream_xid == 100
      assert Map.has_key?(asm.stream_txns, 100)

      {:ok, asm} =
        Assembler.handle_message(asm, %Insert{xid: 100, relation_id: 1, tuple_data: {"5"}})

      assert [{100, %Replicant.Change{op: :insert, table: "t"}}] = asm.stream_txns[100].changes

      {:ok, asm} = Assembler.handle_message(asm, %StreamStop{})
      assert asm.current_stream_xid == nil
    end

    test "a streamed change under a SUBTRANSACTION is tagged with the subxid, not the top xid" do
      asm = streamed() |> with_relation(1)
      {:ok, asm} = Assembler.handle_message(asm, %StreamStart{xid: 100, first_segment: true})

      {:ok, asm} =
        Assembler.handle_message(asm, %Insert{xid: 100, relation_id: 1, tuple_data: {"1"}})

      {:ok, asm} =
        Assembler.handle_message(asm, %Insert{xid: 101, relation_id: 1, tuple_data: {"2"}})

      # newest-first; both live under top-xid 100's buffer, tagged 100 and 101.
      assert [{101, _}, {100, _}] = asm.stream_txns[100].changes
    end

    test "observe_bytes attributes a streamed change's bytes to the current stream buffer" do
      asm = streamed() |> with_relation(1)
      {:ok, asm} = Assembler.handle_message(asm, %StreamStart{xid: 100, first_segment: true})
      asm = Assembler.observe_bytes(asm, 42)
      assert asm.stream_txns[100].byte_size == 42
    end

    test "exceeding max_concurrent_txns halts fail-closed" do
      asm = streamed(1)
      {:ok, asm} = Assembler.handle_message(asm, %StreamStart{xid: 100, first_segment: true})
      {:ok, asm} = Assembler.handle_message(asm, %StreamStop{})

      assert {:halt, %Replicant.Error{reason: :config_invalid}, _} =
               Assembler.handle_message(asm, %StreamStart{xid: 200, first_segment: true})
    end

    test "reset_streams/1 discards all in-progress stream buffers" do
      asm = streamed()
      {:ok, asm} = Assembler.handle_message(asm, %StreamStart{xid: 100, first_segment: true})
      asm = Assembler.reset_streams(asm)
      assert asm.stream_txns == %{}
      assert asm.current_stream_xid == nil
    end

    test "a streamed Truncate of multiple relations accumulates each tagged, reversed for rids order" do
      asm =
        streamed()
        |> with_named_relation(1, "t1")
        |> with_named_relation(2, "t2")

      {:ok, asm} = Assembler.handle_message(asm, %StreamStart{xid: 100, first_segment: true})
      {:ok, asm} = Assembler.handle_message(asm, %Truncate{xid: 100, truncated_relations: [1, 2]})

      # `stream_accumulate_truncate` does `Enum.reverse(tagged, buf.changes)`: rids [1, 2] → tagged
      # [t1, t2] reversed onto the (empty) buffer yields head-first [t2, t1]. Head = newest, so a
      # commit-time `Enum.reverse` (Task 6) restores the delivered order to rids order [t1, t2].
      # This exact shape regresses if `Enum.reverse/2` were dropped or replaced with `++`.
      assert [
               {100, %Replicant.Change{op: :truncate, table: "t2"}},
               {100, %Replicant.Change{op: :truncate, table: "t1"}}
             ] = asm.stream_txns[100].changes
    end

    test "a streamed change with no open stream segment halts fail-closed" do
      # No StreamStart → current_stream_xid is nil; a streamed row must halt, never accumulate.
      asm = streamed() |> with_relation(1)

      assert {:halt, %Replicant.Error{reason: :config_invalid}, _} =
               Assembler.handle_message(asm, %Insert{xid: 100, relation_id: 1, tuple_data: {"1"}})
    end

    test "a streamed change for an uncached relation halts fail-closed" do
      asm = streamed() |> with_relation(1)
      {:ok, asm} = Assembler.handle_message(asm, %StreamStart{xid: 100, first_segment: true})

      # relation 999 was never cached → the table cannot be identified → halt (never emit a
      # table-less change that would be checkpointed as success).
      assert {:halt, %Replicant.Error{reason: :config_invalid}, _} =
               Assembler.handle_message(asm, %Insert{
                 xid: 100,
                 relation_id: 999,
                 tuple_data: {"1"}
               })
    end

    test "a streamed Truncate touching an uncached relation halts fail-closed" do
      asm = streamed() |> with_relation(1)
      {:ok, asm} = Assembler.handle_message(asm, %StreamStart{xid: 100, first_segment: true})

      # relation 1 is cached but 999 is not → `Enum.all?(rids, &Map.has_key?(relations, &1))` fails
      # → the whole truncate halts (fault-path symmetry with the streamed-row uncached-relation halt).
      assert {:halt, %Replicant.Error{reason: :config_invalid}, _} =
               Assembler.handle_message(asm, %Truncate{xid: 100, truncated_relations: [1, 999]})
    end

    test "a streamed Truncate with no open stream segment halts fail-closed" do
      # No StreamStart → current_stream_xid is nil; a streamed truncate must halt, never accumulate
      # (fault-path symmetry with the streamed-row no-open-segment halt).
      asm = streamed() |> with_relation(1)

      assert {:halt, %Replicant.Error{reason: :config_invalid}, _} =
               Assembler.handle_message(asm, %Truncate{xid: 100, truncated_relations: [1]})
    end

    test "streamed Update and Delete accumulate tagged with their subxid" do
      asm = streamed() |> with_relation(1)
      {:ok, asm} = Assembler.handle_message(asm, %StreamStart{xid: 100, first_segment: true})

      update = %Update{
        xid: 100,
        relation_id: 1,
        tuple_data: {"9"},
        old_tuple_data: nil,
        changed_key_tuple_data: {"5"}
      }

      {:ok, asm} = Assembler.handle_message(asm, update)
      assert [{100, %Replicant.Change{op: :update}}] = asm.stream_txns[100].changes

      delete = %Delete{
        xid: 101,
        relation_id: 1,
        old_tuple_data: nil,
        changed_key_tuple_data: {"9"}
      }

      {:ok, asm} = Assembler.handle_message(asm, delete)
      # newest-first: the Delete (subxid 101) heads the buffer, the Update (subxid 100) follows.
      assert [{101, %Replicant.Change{op: :delete}}, {100, %Replicant.Change{op: :update}}] =
               asm.stream_txns[100].changes
    end

    # --- Task 6: StreamCommit reassembly + StreamAbort ---
    #
    # Named `Stream*Sink` to avoid redefining the same-name `DeliverSink`/`At500Sink` modules in
    # the "batch delivery" describe block (which carry handle_batch/1, not handle_transaction/1) —
    # a redefinition warning fails `--warnings-as-errors`.
    defmodule StreamDeliverSink do
      def checkpoint, do: {:ok, nil}

      def handle_transaction(txn) do
        send(self(), {:delivered, txn})
        {:ok, txn.commit_lsn}
      end
    end

    defmodule StreamAt500Sink do
      def checkpoint, do: {:ok, 500}
      def handle_transaction(txn), do: {:ok, txn.commit_lsn}
    end

    defp deliver_streamed(sink, xid) do
      asm = %{Replicant.Assembler.new(sink, max_concurrent_txns: 64) | stream_floor: 0}
      asm = with_relation(asm, 1)
      {:ok, asm} = Assembler.handle_message(asm, %StreamStart{xid: xid, first_segment: true})
      asm
    end

    test "StreamCommit replays the buffer into a complete %Transaction{} (ascending, commit_lsn + ordinals stamped) via handle_transaction" do
      asm = deliver_streamed(StreamDeliverSink, 100)

      {:ok, asm} =
        Assembler.handle_message(asm, %Insert{xid: 100, relation_id: 1, tuple_data: {"1"}})

      {:ok, asm} =
        Assembler.handle_message(asm, %Insert{xid: 100, relation_id: 1, tuple_data: {"2"}})

      {:ok, asm} = Assembler.handle_message(asm, %StreamStop{})

      assert {:transaction, txn, 900, asm} =
               Assembler.handle_message(asm, %StreamCommit{
                 xid: 100,
                 commit_lsn: 900,
                 end_lsn: 901,
                 commit_timestamp: nil
               })

      assert [
               %Replicant.Change{record: %{"v" => 1}, commit_lsn: 900, ordinal: 0},
               %Replicant.Change{record: %{"v" => 2}, commit_lsn: 900, ordinal: 1}
             ] = txn.changes

      assert txn.commit_lsn == 900
      assert_received {:delivered, ^txn}
      # buffer cleared
      refute Map.has_key?(asm.stream_txns, 100)
    end

    test "StreamAbort(top, sub) drops ONLY the sub-aborted changes; the rest deliver on commit" do
      asm = deliver_streamed(StreamDeliverSink, 100)

      {:ok, asm} =
        Assembler.handle_message(asm, %Insert{xid: 100, relation_id: 1, tuple_data: {"1"}})

      # savepoint
      {:ok, asm} =
        Assembler.handle_message(asm, %Insert{xid: 101, relation_id: 1, tuple_data: {"2"}})

      # rollback
      {:ok, asm} = Assembler.handle_message(asm, %StreamAbort{xid: 100, subxid: 101})
      # post-rollback
      {:ok, asm} =
        Assembler.handle_message(asm, %Insert{xid: 102, relation_id: 1, tuple_data: {"3"}})

      {:ok, asm} = Assembler.handle_message(asm, %StreamStop{})

      assert {:transaction, txn, 900, _asm} =
               Assembler.handle_message(asm, %StreamCommit{
                 xid: 100,
                 commit_lsn: 900,
                 end_lsn: 901,
                 commit_timestamp: nil
               })

      # 2 (subxid 101) filtered out
      assert Enum.map(txn.changes, & &1.record["v"]) == [1, 3]
    end

    test "StreamAbort(top, top) discards the whole transaction (no delivery)" do
      asm = deliver_streamed(StreamDeliverSink, 100)

      {:ok, asm} =
        Assembler.handle_message(asm, %Insert{xid: 100, relation_id: 1, tuple_data: {"1"}})

      {:ok, asm} = Assembler.handle_message(asm, %StreamAbort{xid: 100, subxid: 100})
      refute Map.has_key?(asm.stream_txns, 100)
      refute_received {:delivered, _}
    end

    test "a StreamCommit at or below the sink checkpoint is pre-skipped (effect-once)" do
      asm = deliver_streamed(StreamAt500Sink, 100)

      {:ok, asm} =
        Assembler.handle_message(asm, %Insert{xid: 100, relation_id: 1, tuple_data: {"1"}})

      {:ok, asm} = Assembler.handle_message(asm, %StreamStop{})

      assert {:skipped, 300, _asm} =
               Assembler.handle_message(asm, %StreamCommit{
                 xid: 100,
                 commit_lsn: 300,
                 end_lsn: 301,
                 commit_timestamp: nil
               })
    end

    test "StreamCommit for an unknown xid halts fail-closed" do
      asm = %{
        Replicant.Assembler.new(StreamDeliverSink, max_concurrent_txns: 64)
        | stream_floor: 0
      }

      assert {:halt, %Replicant.Error{reason: :config_invalid}, _} =
               Assembler.handle_message(asm, %StreamCommit{
                 xid: 999,
                 commit_lsn: 1,
                 end_lsn: 2,
                 commit_timestamp: nil
               })
    end

    test "a multi-relation streamed Truncate is DELIVERED in rids order (t1 ordinal 0, t2 ordinal 1)" do
      asm =
        %{Replicant.Assembler.new(StreamDeliverSink, max_concurrent_txns: 64) | stream_floor: 0}
        |> with_named_relation(1, "t1")
        |> with_named_relation(2, "t2")

      {:ok, asm} = Assembler.handle_message(asm, %StreamStart{xid: 100, first_segment: true})
      {:ok, asm} = Assembler.handle_message(asm, %Truncate{xid: 100, truncated_relations: [1, 2]})
      {:ok, asm} = Assembler.handle_message(asm, %StreamStop{})

      assert {:transaction, _txn, 900, _asm} =
               Assembler.handle_message(asm, %StreamCommit{
                 xid: 100,
                 commit_lsn: 900,
                 end_lsn: 901,
                 commit_timestamp: nil
               })

      assert_received {:delivered, txn}

      # End-to-end order lock the Task-5 buffer-level test could not assert: the buffer stores
      # truncate changes reversed (head-first [t2, t1]); commit-time Enum.reverse must restore rids
      # order [t1, t2] with ascending ordinals. FAILS if the delivered order were reversed.
      assert [
               %Replicant.Change{op: :truncate, table: "t1", commit_lsn: 900, ordinal: 0},
               %Replicant.Change{op: :truncate, table: "t2", commit_lsn: 900, ordinal: 1}
             ] = txn.changes
    end
  end
end
