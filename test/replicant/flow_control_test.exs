defmodule Replicant.FlowControlTest do
  use ExUnit.Case, async: true
  alias Replicant.FlowControl

  test "only cumulative credits from the current generation release queued bytes" do
    flow = FlowControl.new() |> FlowControl.push(100) |> FlowControl.push(200)
    assert FlowControl.full?(flow, 300)
    assert flow == FlowControl.processed(flow, make_ref(), 300, 2)
    assert flow == FlowControl.processed(flow, flow.epoch, 301, 2)
    assert flow == FlowControl.processed(flow, flow.epoch, 300, 3)
    flow = FlowControl.processed(flow, flow.epoch, 100, 1)
    assert FlowControl.queued_bytes(flow) == 200
    refute FlowControl.drained?(flow, 300)
    assert flow == FlowControl.processed(flow, flow.epoch, 0, 0)
    flow = FlowControl.processed(flow, flow.epoch, 300, 2)
    assert FlowControl.drained?(flow, 300)
    assert FlowControl.queued_messages(flow) == 0
  end
end
