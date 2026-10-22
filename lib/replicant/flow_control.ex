defmodule Replicant.FlowControl do
  @moduledoc false

  # Cumulative, connection-generation-scoped credits cover the assembler mailbox.
  # They are independent of WAL positions and of durable checkpoint acknowledgements.
  @max_messages 512

  @type t :: %__MODULE__{
          epoch: reference() | nil,
          sent_bytes: non_neg_integer(),
          sent_messages: non_neg_integer(),
          processed_bytes: non_neg_integer(),
          processed_messages: non_neg_integer(),
          paused: boolean(),
          timer: {reference(), reference()} | nil
        }

  defstruct epoch: nil,
            sent_bytes: 0,
            sent_messages: 0,
            processed_bytes: 0,
            processed_messages: 0,
            paused: false,
            timer: nil

  def new, do: %__MODULE__{epoch: make_ref()}

  def push(%{epoch: nil}, bytes), do: push(new(), bytes)

  def push(flow, bytes) do
    %{flow | sent_bytes: flow.sent_bytes + bytes, sent_messages: flow.sent_messages + 1}
  end

  def processed(flow, epoch, bytes, messages)
      when epoch == flow.epoch and bytes >= flow.processed_bytes and
             bytes <= flow.sent_bytes and messages >= flow.processed_messages and
             messages <= flow.sent_messages do
    %{flow | processed_bytes: bytes, processed_messages: messages}
  end

  def processed(flow, _epoch, _bytes, _messages), do: flow

  def queued_bytes(flow), do: flow.sent_bytes - flow.processed_bytes
  def queued_messages(flow), do: flow.sent_messages - flow.processed_messages

  def full?(flow, limit),
    do: queued_bytes(flow) >= limit or queued_messages(flow) >= @max_messages

  def drained?(flow, limit),
    do: queued_bytes(flow) <= div(limit, 2) and queued_messages(flow) <= div(@max_messages, 2)
end
