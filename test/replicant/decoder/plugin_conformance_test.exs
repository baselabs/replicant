defmodule Replicant.Decoder.PluginConformanceTest do
  @moduledoc """
  Real-byte conformance for the plugin decoders (ADR-0009): frames captured from the
  LIVE 9.6 and 12 substrate servers (see `test/fixtures/`) decode to the expected
  message kinds, and each fixture is tamper-checked — a byte flip must make the known
  good decode diverge, so a mistyped fixture can never pass vacuously (the discipline
  `test/replicant/decoder/conformance_test.exs` established for pgoutput).
  """

  use ExUnit.Case, async: true

  alias Replicant.Decoder
  alias Replicant.Decoder.Pglogical
  alias Replicant.Decoder.Wal2json

  alias Replicant.Decoder.Messages.Begin
  alias Replicant.Decoder.Messages.Commit
  alias Replicant.Decoder.Messages.Delete
  alias Replicant.Decoder.Messages.Insert
  alias Replicant.Decoder.Messages.Relation
  alias Replicant.Decoder.Messages.Update

  @fixtures_dir "test/fixtures"

  # pglogical binary frames (term_to_binary of [binary()]): 'S' startup, 'B', 'R',
  # 'I', 'U'(K+N), 'D', 'C' — the OBSERVED native grammar.
  @pglogical_fixtures ~w(pglogical_pg96.bin pglogical_pg12.bin)
  # wal2json JSON documents, one per line: B/I/U/D/C per captured transaction.
  @wal2json_fixtures ~w(wal2json_pg96.jsons wal2json_pg12.jsons)

  # ---- known-good decode ----

  for file <- @pglogical_fixtures do
    test "pglogical real bytes (#{file}): every frame decodes, kinds as OBSERVED" do
      frames = :erlang.binary_to_term(File.read!(Path.join(@fixtures_dir, unquote(file))))

      assert length(frames) >= 8
      cache = Pglogical.init_cache(column_types: %{}, replica_identity: %{})

      {messages, _cache} =
        Enum.map_reduce(frames, cache, fn frame, cache_acc ->
          {:ok, msgs, cache2} = Decoder.decode(frame, decoder: :pglogical, cache: cache_acc)
          {msgs, cache2}
        end)

      kinds =
        messages
        |> List.flatten()
        |> Enum.map(& &1.__struct__)

      assert Begin in kinds
      assert Relation in kinds
      assert Insert in kinds
      assert Update in kinds
      assert Delete in kinds
      assert Commit in kinds
    end
  end

  for file <- @wal2json_fixtures do
    test "wal2json real bytes (#{file}): every document decodes, kinds as OBSERVED" do
      docs =
        unquote(file)
        |> then(&Path.join(@fixtures_dir, &1))
        |> File.read!()
        |> String.split("\n", trim: true)

      assert length(docs) >= 9

      {messages, _cache} =
        Enum.map_reduce(docs, Wal2json.init_cache([]), fn doc, cache_acc ->
          {:ok, msgs, cache2} = Decoder.decode(doc, decoder: :wal2json, cache: cache_acc)
          {msgs, cache2}
        end)

      kinds =
        messages
        |> List.flatten()
        |> Enum.map(& &1.__struct__)

      assert Begin in kinds
      assert Insert in kinds
      # the captured update changes the PK (a real old-key tuple on the wire)
      assert Update in kinds
      assert Delete in kinds
      assert Commit in kinds
    end
  end

  # ---- tamper evidence (machine-checked) ----

  describe "tamper-evidence: a meaningful byte flip diverges from the known-good decode" do
    for file <- @pglogical_fixtures do
      test "pglogical (#{file}): type byte + sampled payload byte each diverge" do
        frames = :erlang.binary_to_term(File.read!(Path.join(@fixtures_dir, unquote(file))))
        frame = Enum.at(frames, 3)

        {:ok, original, _} = Decoder.decode(frame, decoder: :pglogical, cache: pglogical_cache())
        # baseline guard: a mistyped fixture fails LOUD here
        assert original != []

        # flip the type byte
        <<_t, rest::binary>> = frame
        tampered_type = <<0x5A, rest::binary>>
        {:error, _} = Decoder.decode(tampered_type, decoder: :pglogical, cache: pglogical_cache())

        # flip a payload byte past the header
        mid = div(byte_size(frame), 2)
        <<head::binary-size(^mid), b, tail::binary>> = frame
        flipped = :erlang.bxor(b, 0x20)
        tampered_payload = <<head::binary, flipped, tail::binary>>

        case Decoder.decode(tampered_payload, decoder: :pglogical, cache: pglogical_cache()) do
          {:error, _} -> :ok
          {:ok, msgs, _} -> assert msgs != original
        end
      end
    end

    for file <- @wal2json_fixtures do
      test "wal2json (#{file}): action key + sampled value byte each diverge" do
        docs =
          unquote(file)
          |> then(&Path.join(@fixtures_dir, &1))
          |> File.read!()
          |> String.split("\n", trim: true)

        doc = Enum.find(docs, &String.contains?(&1, ~s("action":"I")))

        {:ok, original, _} = Decoder.decode(doc, decoder: :wal2json, cache: init_wal2json(doc))
        assert original != []

        # corrupt the action discriminator
        tampered_action = String.replace(doc, ~s("action":"I"), ~s("action":"X"), global: false)

        {:error, _} =
          Decoder.decode(tampered_action, decoder: :wal2json, cache: init_wal2json(doc))

        # flip a value byte in the middle of the document
        mid = div(byte_size(doc), 2)
        <<head::binary-size(^mid), b, tail::binary>> = doc
        flipped = :erlang.bxor(b, 0x01)
        tampered_payload = <<head::binary, flipped, tail::binary>>

        case Decoder.decode(tampered_payload, decoder: :wal2json, cache: init_wal2json(doc)) do
          {:error, _} -> :ok
          {:ok, msgs, _} -> assert msgs != original
        end
      end
    end
  end

  defp pglogical_cache do
    Pglogical.init_cache(column_types: %{}, replica_identity: %{})
  end

  defp init_wal2json(_doc), do: Wal2json.init_cache([])
end
