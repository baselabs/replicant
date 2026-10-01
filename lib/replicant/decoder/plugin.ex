defmodule Replicant.Decoder.Plugin do
  @moduledoc """
  The decoder behavior behind the replication stream (ADR-0009).

  One logical-decoding output plugin per implementation. The plugin's format leaks into
  the pipeline in exactly the places this behavior covers:

    * `slot_plugin/0` — the plugin name for `CREATE_REPLICATION_SLOT … LOGICAL <plugin>`;
    * `start_options/1` — the ordered `key 'value'` options for `START_REPLICATION`,
      built ONLY from `Replicant.Identifier`-validated names (Critical Rule 2);
    * `capabilities/0` — what the decoder can express; `Replicant.Config` refuses at
      start (`:decoder_capability_unsupported`) any configured capability it lacks, never
      degrading at run time;
    * `init_cache/1` — the per-connection decoder state seeded at every connect
      (pgoutput: none; pglogical: the connect-time catalog read of column types and
      `relreplident`; wal2json: that read plus the synthesized-relation map);
    * `decode/3` — one XLogData payload plus the cache to the existing
      `Replicant.Decoder.Messages` structs, returning zero or more messages and the
      updated cache (wal2json emits a synthesized `%Relation{}` ahead of a change whose
      column list differs from the cached relation — ADR-0009 §5).

  `Replicant.Decoder.decode/2` remains the ONLY rescue/catch boundary (ADR-0003): every
  plugin decodes behind it, so a raise/throw/exit from any parser still scrubs to a
  value-free `%Replicant.Error{}`.
  """

  alias Replicant.Error

  @typedoc "The decoder-selection atoms accepted by `Replicant.start_link/1`'s `decoder:` option."
  @type decoder_atom :: :pgoutput | :pglogical | :wal2json

  @typedoc "The per-connection decoder cache threaded through `decode/3` by the Connection."
  @type cache :: term()

  @callback slot_plugin() :: String.t()

  @callback capabilities() :: [:streaming | :messages]

  @doc """
  The ordered `START_REPLICATION` plugin options. Every VALUE that carries a name was
  `Replicant.Identifier.validate/1`-validated by `Replicant.Config` before reaching here.
  """
  @callback start_options(keyword()) :: [{String.t(), String.t()}]

  @callback init_cache(keyword()) :: cache()

  @callback decode(binary(), cache(), keyword()) ::
              {:ok, [struct()], cache()} | {:error, Error.t()}

  @doc "The behavior module implementing a decoder atom (fixed map — never user input)."
  @spec module_for(decoder_atom()) :: module()
  def module_for(:pgoutput), do: Replicant.Decoder.PgOutput
  def module_for(:pglogical), do: Replicant.Decoder.Pglogical
  def module_for(:wal2json), do: Replicant.Decoder.Wal2json

  @doc "The decoder atoms `decoder:` accepts."
  @spec decoder_atoms() :: [decoder_atom()]
  def decoder_atoms, do: [:pgoutput, :pglogical, :wal2json]
end
