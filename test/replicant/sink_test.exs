defmodule Replicant.SinkTest.AppendLogSink do
  @moduledoc false
  @behaviour Replicant.Sink

  @impl Replicant.Sink
  def checkpoint, do: {:ok, nil}

  @impl Replicant.Sink
  def handle_transaction(_txn), do: {:ok, 0}

  @impl Replicant.Sink
  def sink_kind, do: :append_log
end

defmodule Replicant.SinkTest.SnapshotCapable do
  @moduledoc false
  @behaviour Replicant.Sink

  @impl Replicant.Sink
  def checkpoint, do: {:ok, nil}

  @impl Replicant.Sink
  def handle_transaction(_txn), do: {:ok, 0}

  @impl Replicant.Sink
  def handle_snapshot(_changes, _ctx), do: :ok

  @impl Replicant.Sink
  def handle_snapshot_complete(lsn), do: {:ok, lsn}
end

defmodule Replicant.SinkTest.HalfSnapshot do
  @moduledoc false
  @behaviour Replicant.Sink

  @impl Replicant.Sink
  def checkpoint, do: {:ok, nil}

  @impl Replicant.Sink
  def handle_transaction(_txn), do: {:ok, 0}

  @impl Replicant.Sink
  def handle_snapshot(_changes, _ctx), do: :ok

  # deliberately missing handle_snapshot_complete/1
end

defmodule Replicant.SinkTest do
  use ExUnit.Case, async: false

  alias Replicant.{Change, Sink, Test.RecordingSink, Transaction}

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

  describe "mandatory callbacks" do
    test "RecordingSink implements checkpoint/0 and handle_transaction/1" do
      assert {:ok, nil} = RecordingSink.checkpoint()

      txn = %Transaction{commit_lsn: 0x10, changes: [%Change{op: :insert, record: %{"id" => 1}}]}
      assert {:ok, 0x10} = RecordingSink.handle_transaction(txn)
      assert [{0x10, [_change]}] = RecordingSink.seen()
    end
  end

  describe "optional callbacks are truly optional" do
    test "sink_kind/0 defaults to :state_mirror when the callback is absent" do
      # function_exported?/3 is how the Assembler must dispatch.
      refute function_exported?(RecordingSink, :sink_kind, 0)
      assert Sink.sink_kind(RecordingSink) == :state_mirror
    end

    test "handle_schema_change/2 absence is detectable (Assembler provides the default)" do
      refute function_exported?(RecordingSink, :handle_schema_change, 2)
    end

    test "sink_kind/1 returns the sink's own kind when sink_kind/0 IS implemented" do
      assert function_exported?(Replicant.SinkTest.AppendLogSink, :sink_kind, 0)
      assert Sink.sink_kind(Replicant.SinkTest.AppendLogSink) == :append_log
    end

    test "supports_snapshot?/1 is false for a sink with neither snapshot callback" do
      refute Sink.supports_snapshot?(RecordingSink)
    end

    test "supports_snapshot?/1 requires BOTH snapshot callbacks" do
      assert Sink.supports_snapshot?(Replicant.SinkTest.SnapshotCapable)
      refute Sink.supports_snapshot?(Replicant.SinkTest.HalfSnapshot)
    end

    test "a snapshot-capable sink still compiles as a valid minimal sink" do
      assert function_exported?(Replicant.SinkTest.SnapshotCapable, :handle_snapshot, 2)
      assert function_exported?(Replicant.SinkTest.SnapshotCapable, :handle_snapshot_complete, 1)
    end

    test "checkpoint/0 is an optional callback (a lib-mode sink omits it and still satisfies @behaviour)" do
      assert {:checkpoint, 0} in Replicant.Sink.behaviour_info(:optional_callbacks)
      # WriteOnlySink declares @behaviour Replicant.Sink WITHOUT checkpoint/0 and must compile.
      # ensure_loaded first: a test/support module referenced only by fully-qualified
      # name (no alias/call) is not yet loaded into the VM, and function_exported?/3
      # does not force-load it (same reason lib/replicant/config.ex checks
      # Code.ensure_loaded?/1 before function_exported?/3 on a configured sink).
      assert Code.ensure_loaded?(Replicant.Test.WriteOnlySink)
      assert function_exported?(Replicant.Test.WriteOnlySink, :handle_transaction, 1)
      refute function_exported?(Replicant.Test.WriteOnlySink, :checkpoint, 0)
    end
  end
end
