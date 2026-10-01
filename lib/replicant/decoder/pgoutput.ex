defmodule Replicant.Decoder.PgOutput do
  @moduledoc """
  The `pgoutput` decoder — the default, byte-identical to 1.3.0.

  Thin behavior adapter over the vendored pgoutput byte parser (`Replicant.Decoder`);
  the wire grammar itself lives there. `start_options/1` reproduces the exact option
  ordering the shipped `START_REPLICATION` builder emits.
  """

  @behaviour Replicant.Decoder.Plugin

  @impl true
  @spec slot_plugin() :: String.t()
  def slot_plugin, do: "pgoutput"

  @impl true
  @spec capabilities() :: [:streaming | :messages]
  def capabilities, do: [:streaming, :messages]

  @impl true
  @spec start_options(keyword()) :: [{String.t(), String.t()}]
  def start_options(opts) do
    publications = Keyword.fetch!(opts, :publications)

    proto =
      if Keyword.get(opts, :streaming),
        do: [{"proto_version", "2"}, {"streaming", "on"}],
        else: [{"proto_version", "1"}]

    base = proto ++ [{"publication_names", Enum.join(publications, ",")}]

    if Keyword.get(opts, :messages),
      do: base ++ [{"messages", "true"}],
      else: base
  end

  @impl true
  @spec init_cache(keyword()) :: nil
  def init_cache(_opts), do: nil

  @impl true
  @spec decode(binary(), nil, keyword()) ::
          {:ok, [struct()], nil} | {:error, Replicant.Error.t()}
  def decode(payload, nil, opts) do
    case Replicant.Decoder.pgoutput_decode(payload, opts) do
      {:ok, message} -> {:ok, [message], nil}
      {:error, %Replicant.Error{}} = err -> err
    end
  end
end
