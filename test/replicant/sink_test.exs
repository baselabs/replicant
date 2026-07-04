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

defmodule Replicant.SinkTest do
  use ExUnit.Case, async: false

  alias Replicant.{Change, Sink, Test.RecordingSink, Transaction}

  setup do
    {:ok, _pid} = RecordingSink.start_link()
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
  end
end
