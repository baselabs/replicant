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
      assert [%Change{op: :insert, table: "orders", record: record}] = txn.changes
      assert record["status"] == "paid"
      # spec §10: keys stay binaries through the real decode→assemble producer path
      # (no String.to_atom) — the non-vacuous string-key guard the seam table assigns here.
      assert Enum.all?(Map.keys(record), &is_binary/1)
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
        [:replicant, :schema_change, :additive],
        [:replicant, :schema_change, :halted]
      ],
      fn name, _meas, meta, _cfg -> send(pid, {:tel, name, meta}) end,
      nil
    )
  end
end
