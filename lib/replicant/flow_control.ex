defmodule Replicant.FlowControl do
  @moduledoc "Connection-local flow control state."

  # Cumulative, connection-generation-scoped credits cover the assembler mailbox.
  # They are independent of WAL positions and of durable checkpoint acknowledgements.
  @max_messages 512

  @type t :: %__MODULE__{
          epoch: reference() | nil,
          reconnect_barrier: reference() | nil,
          queue: :atomics.atomics_ref() | nil,
          paused_at: integer() | nil,
          stall_since: integer() | nil,
          stall_timer: {reference(), reference()} | nil,
          sent_bytes: non_neg_integer(),
          sent_messages: non_neg_integer(),
          processed_bytes: non_neg_integer(),
          processed_messages: non_neg_integer(),
          paused: boolean(),
          timer: {reference(), reference()} | nil
        }

  defstruct epoch: nil,
            reconnect_barrier: nil,
            queue: nil,
            paused_at: nil,
            stall_since: nil,
            stall_timer: nil,
            sent_bytes: 0,
            sent_messages: 0,
            processed_bytes: 0,
            processed_messages: 0,
            paused: false,
            timer: nil

  def new, do: %__MODULE__{epoch: make_ref(), queue: :atomics.new(1, [])}

  def push(%{epoch: nil}, bytes), do: push(new(), bytes)

  def push(flow, bytes) do
    :atomics.add(flow.queue, 1, 1)

    since =
      if queued_messages(flow) == 0,
        do: System.monotonic_time(:millisecond),
        else: flow.stall_since

    %{
      flow
      | sent_bytes: flow.sent_bytes + bytes,
        sent_messages: flow.sent_messages + 1,
        stall_since: since
    }
  end

  def processed(flow, epoch, bytes, messages)
      when epoch == flow.epoch and bytes >= flow.processed_bytes and
             bytes <= flow.sent_bytes and messages >= flow.processed_messages and
             messages <= flow.sent_messages do
    since =
      cond do
        messages == flow.sent_messages -> nil
        messages > flow.processed_messages -> System.monotonic_time(:millisecond)
        true -> flow.stall_since
      end

    %{flow | processed_bytes: bytes, processed_messages: messages, stall_since: since}
  end

  def processed(flow, _epoch, _bytes, _messages), do: flow

  def queued_bytes(flow), do: flow.sent_bytes - flow.processed_bytes
  def queued_messages(flow), do: flow.sent_messages - flow.processed_messages

  def full?(flow, limit),
    do: queued_bytes(flow) >= limit or queued_messages(flow) >= @max_messages

  def drained?(flow, limit),
    do: queued_bytes(flow) <= div(limit, 2) and queued_messages(flow) <= div(@max_messages, 2)
end
