defmodule Replicant.Decoder.ConformanceTest do
  @moduledoc """
  Real-pgoutput byte conformance (spec §12.1).

  Every byte literal below is a REAL captured pgoutput message — the exact
  fixtures shipped in walex 4.8.0's test/walex/decoder/decoder_test.exs (MIT;
  captured from a live Postgres; credited in NOTICE). Plan 2 adds an independent
  fresh docker-PG16 capture to corroborate. These are NOT self-signed.
  """

  use ExUnit.Case, async: true

  import Bitwise

  alias Replicant.Decoder

  alias Replicant.Decoder.Messages.{
    Begin,
    Commit,
    Delete,
    Insert,
    Message,
    Origin,
    Relation,
    Relation.Column,
    Truncate,
    Type,
    Update
  }

  # walex's captured Begin LSN was the tuple {2, 2_817_828_992} → uint64 form:
  @begin_lsn bsl(2, 32) + 2_817_828_992

  # walex's captured Commit end_lsn was the tuple {2, 2_817_829_040} → uint64
  # form. Precomputed as a module attribute because `bsl/2` is a remote call and
  # cannot be invoked inside the assert pattern-match below.
  @commit_end_lsn bsl(2, 32) + 2_817_829_040

  describe "control messages (real bytes)" do
    test "Begin" do
      {:ok, expected_dt_base, 0} = DateTime.from_iso8601("2019-07-18T17:02:35Z")
      expected_dt = DateTime.add(expected_dt_base, 726_322, :microsecond)

      assert {:ok, %Begin{commit_timestamp: ^expected_dt, final_lsn: @begin_lsn, xid: 619}} =
               Decoder.decode(
                 <<66, 0, 0, 0, 2, 167, 244, 168, 128, 0, 2, 48, 246, 88, 88, 213, 242, 0, 0, 2,
                   107>>
               )
    end

    test "Commit" do
      {:ok, expected_dt_base, 0} = DateTime.from_iso8601("2019-07-18T17:02:35Z")
      expected_dt = DateTime.add(expected_dt_base, 726_322, :microsecond)

      assert {:ok,
              %Commit{
                flags: [],
                lsn: @begin_lsn,
                end_lsn: @commit_end_lsn,
                commit_timestamp: ^expected_dt
              }} =
               Decoder.decode(
                 <<67, 0, 0, 0, 0, 2, 167, 244, 168, 128, 0, 0, 0, 2, 167, 244, 168, 176, 0, 2,
                   48, 246, 88, 88, 213, 242>>
               )
    end

    test "Origin" do
      assert {:ok, %Origin{origin_commit_lsn: @begin_lsn, name: "Elmer Fud"}} =
               Decoder.decode(<<79, 0, 0, 0, 2, 167, 244, 168, 128>> <> "Elmer Fud")
    end

    test "Relation (two real captures)" do
      assert {:ok,
              %Relation{
                id: 24_576,
                namespace: "public",
                name: "foo",
                replica_identity: :default,
                columns: [
                  %Column{flags: [], name: "bar", type: "text", type_modifier: 4_294_967_295},
                  %Column{flags: [:key], name: "id", type: "int4", type_modifier: 4_294_967_295}
                ]
              }} =
               Decoder.decode(
                 <<82, 0, 0, 96, 0, 112, 117, 98, 108, 105, 99, 0, 102, 111, 111, 0, 100, 0, 2, 0,
                   98, 97, 114, 0, 0, 0, 0, 25, 255, 255, 255, 255, 1, 105, 100, 0, 0, 0, 0, 23,
                   255, 255, 255, 255>>
               )

      assert {:ok,
              %Relation{
                id: 18_268,
                namespace: "public",
                name: "temp",
                replica_identity: :default,
                columns: [
                  %Column{flags: [], name: "test", type: "numeric", type_modifier: 4_294_967_295}
                ]
              }} =
               Decoder.decode(
                 <<82, 0, 0, 71, 92, 112, 117, 98, 108, 105, 99, 0, 116, 101, 109, 112, 0, 100, 0,
                   1, 0, 116, 101, 115, 116, 0, 0, 0, 6, 164, 255, 255, 255, 255>>
               )
    end

    test "Type" do
      assert {:ok, %Type{id: 32_820, namespace: "public", name: "example_type"}} =
               Decoder.decode(
                 <<89, 0, 0, 128, 52, 112, 117, 98, 108, 105, 99, 0, 101, 120, 97, 109, 112, 108,
                   101, 95, 116, 121, 112, 101, 0>>
               )
    end
  end

  describe "Truncate (real bytes)" do
    test "plain" do
      assert {:ok, %Truncate{number_of_relations: 1, options: [], truncated_relations: [24_576]}} =
               Decoder.decode(<<84, 0, 0, 0, 1, 0, 0, 0, 96, 0>>)
    end

    test "cascade" do
      assert {:ok, %Truncate{options: [:cascade], truncated_relations: [24_576]}} =
               Decoder.decode(<<84, 0, 0, 0, 1, 1, 0, 0, 96, 0>>)
    end

    test "restart identity" do
      assert {:ok, %Truncate{options: [:restart_identity], truncated_relations: [24_576]}} =
               Decoder.decode(<<84, 0, 0, 0, 1, 2, 0, 0, 96, 0>>)
    end
  end

  describe "row changes — unchanged-TOAST + replica-identity against the REAL wire format (spec §7)" do
    test "Insert" do
      assert {:ok, %Insert{relation_id: 24_576, tuple_data: {"baz", "560"}}} =
               Decoder.decode(
                 <<73, 0, 0, 96, 0, 78, 0, 2, 116, 0, 0, 0, 3, 98, 97, 122, 116, 0, 0, 0, 3, 53,
                   54, 48>>
               )
    end

    test "Insert with a NULL column" do
      assert {:ok, %Insert{tuple_data: {nil, "560"}}} =
               Decoder.decode(<<73, 0, 0, 96, 0, 78, 0, 2, 110, 116, 0, 0, 0, 3, 53, 54, 48>>)
    end

    test "Insert with an unchanged-TOAST sentinel (the §7 sentinel, as it really appears on the wire)" do
      # The 'u' (0x75) byte IS the unchanged-TOAST sentinel Postgres sends. The
      # Assembler (Task 13) extracts it into Change.unchanged; here we assert the
      # decoder surfaces it verbatim as :unchanged_toast (not as a value).
      assert {:ok, %Insert{tuple_data: {:unchanged_toast, "560"}}} =
               Decoder.decode(<<73, 0, 0, 96, 0, 78, 0, 2, 117, 116, 0, 0, 0, 3, 53, 54, 48>>)
    end

    test "Update with DEFAULT replica identity (new tuple only)" do
      assert {:ok,
              %Update{
                relation_id: 24_576,
                changed_key_tuple_data: nil,
                old_tuple_data: nil,
                tuple_data: {"example", "560"}
              }} =
               Decoder.decode(
                 <<85, 0, 0, 96, 0, 78, 0, 2, 116, 0, 0, 0, 7, 101, 120, 97, 109, 112, 108, 101,
                   116, 0, 0, 0, 3, 53, 54, 48>>
               )
    end

    test "Update with FULL replica identity (old tuple carried)" do
      assert {:ok, %Update{old_tuple_data: {"baz", "560"}, tuple_data: {"example", "560"}}} =
               Decoder.decode(
                 <<85, 0, 0, 96, 0, 79, 0, 2, 116, 0, 0, 0, 3, 98, 97, 122, 116, 0, 0, 0, 3, 53,
                   54, 48, 78, 0, 2, 116, 0, 0, 0, 7, 101, 120, 97, 109, 112, 108, 101, 116, 0, 0,
                   0, 3, 53, 54, 48>>
               )
    end

    test "Update with USING INDEX replica identity (key tuple carried)" do
      assert {:ok,
              %Update{
                changed_key_tuple_data: {"baz", nil},
                old_tuple_data: nil,
                tuple_data: {"example", "560"}
              }} =
               Decoder.decode(
                 <<85, 0, 0, 96, 0, 75, 0, 2, 116, 0, 0, 0, 3, 98, 97, 122, 110, 78, 0, 2, 116, 0,
                   0, 0, 7, 101, 120, 97, 109, 112, 108, 101, 116, 0, 0, 0, 3, 53, 54, 48>>
               )
    end

    test "Delete with USING INDEX replica identity" do
      assert {:ok, %Delete{relation_id: 24_576, changed_key_tuple_data: {"example", nil}}} =
               Decoder.decode(
                 <<68, 0, 0, 96, 0, 75, 0, 2, 116, 0, 0, 0, 7, 101, 120, 97, 109, 112, 108, 101,
                   110>>
               )
    end

    test "Delete with FULL replica identity" do
      assert {:ok, %Delete{old_tuple_data: {"baz", "560"}}} =
               Decoder.decode(
                 <<68, 0, 0, 96, 0, 79, 0, 2, 116, 0, 0, 0, 3, 98, 97, 122, 116, 0, 0, 0, 3, 53,
                   54, 48>>
               )
    end
  end

  # A2 real-byte corroboration (spec §7.1 / §7.2, decision A2 / plan Task 11). Unlike the
  # walex-sourced fixtures above, these two 'M' (0x4D = 77) frames were captured fresh from a
  # live docker-PG16.14 via a Postgrex.ReplicationConnection (START_REPLICATION … messages 'true',
  # `pg_logical_emit_message/3`) — the independent real-byte second layer for the Message decode
  # path, which otherwise had only Task 5's hand-crafted bytes. Frame layout: 'M', flags (1 byte:
  # 0 = non-transactional, 1 = transactional), LSN (uint64), NUL-terminated prefix, content length
  # (uint32), content bytes.
  describe "logical-decoding Message (real bytes, fresh PG16 capture)" do
    test "non-transactional (flags=0) — pg_logical_emit_message(false, …)" do
      assert {:ok,
              %Message{
                transactional?: false,
                lsn: 14_463_526_072,
                prefix: "probe_prefix",
                content: "probe_content",
                xid: nil,
                ordinal: nil
              }} =
               Decoder.decode(
                 <<77, 0, 0, 0, 0, 3, 94, 23, 228, 184, 112, 114, 111, 98, 101, 95, 112, 114, 101,
                   102, 105, 120, 0, 0, 0, 0, 13, 112, 114, 111, 98, 101, 95, 99, 111, 110, 116,
                   101, 110, 116>>
               )
    end

    test "transactional (flags=1) — pg_logical_emit_message(true, …)" do
      assert {:ok,
              %Message{
                transactional?: true,
                lsn: 14_463_526_368,
                prefix: "txn_prefix",
                content: "txn_content",
                xid: nil,
                ordinal: nil
              }} =
               Decoder.decode(
                 <<77, 1, 0, 0, 0, 3, 94, 23, 229, 224, 116, 120, 110, 95, 112, 114, 101, 102,
                   105, 120, 0, 0, 0, 0, 11, 116, 120, 110, 95, 99, 111, 110, 116, 101, 110, 116>>
               )
    end
  end

  # Tamper-evidence (machine-checked): a meaningful byte flip is caught. The strict per-fixture
  # assertions above make the suite tamper-red BY CONSTRUCTION; this makes it tamper-red BY TEST.
  # For each message class, flipping the type byte or a sampled payload byte MUST diverge from the
  # known-good decode (error or a different decoded term), never silently the same. The fixtures
  # are the same REAL captured bytes used above (re-transcribed here so this describe is
  # self-contained; the baseline-decode guard fails LOUD if a byte is mistyped).
  describe "tamper-evidence (machine-checked: a meaningful byte flip is caught)" do
    @tamper_fixtures [
      {"Begin",
       <<66, 0, 0, 0, 2, 167, 244, 168, 128, 0, 2, 48, 246, 88, 88, 213, 242, 0, 0, 2, 107>>},
      {"Insert",
       <<73, 0, 0, 96, 0, 78, 0, 2, 116, 0, 0, 0, 3, 98, 97, 122, 116, 0, 0, 0, 3, 53, 54, 48>>},
      {"Update",
       <<85, 0, 0, 96, 0, 79, 0, 2, 116, 0, 0, 0, 3, 98, 97, 122, 116, 0, 0, 0, 3, 53, 54, 48, 78,
         0, 2, 116, 0, 0, 0, 7, 101, 120, 97, 109, 112, 108, 101, 116, 0, 0, 0, 3, 53, 54, 48>>},
      {"Delete",
       <<68, 0, 0, 96, 0, 79, 0, 2, 116, 0, 0, 0, 3, 98, 97, 122, 116, 0, 0, 0, 3, 53, 54, 48>>},
      {"Message",
       <<77, 1, 0, 0, 0, 3, 94, 23, 229, 224, 116, 120, 110, 95, 112, 114, 101, 102, 105, 120, 0,
         0, 0, 0, 11, 116, 120, 110, 95, 99, 111, 110, 116, 101, 110, 116>>}
    ]

    for {name, bytes} <- @tamper_fixtures do
      @name name
      @bytes bytes

      test "tamper: #{@name} — the type byte + a sampled payload byte each diverge from the known-good decode" do
        original = Decoder.decode(@bytes)
        # Baseline guard: a mistyped fixture byte fails LOUD here, not silently downstream.
        assert match?({:ok, _}, original),
               "#{@name} baseline must decode — check the fixture bytes"

        # The message-type byte: flipping the discriminator must NOT silently decode to the same
        # message (it routes to a different type or errors).
        refute Decoder.decode(flip_byte(@bytes, 0)) == original,
               "#{@name}: a type-byte flip must diverge from the known-good decode"

        # At least one payload byte is load-bearing: a sampled payload flip diverges (catches a
        # value-field tamper, not just the discriminator). ~6 evenly-spaced samples.
        last = byte_size(@bytes) - 1
        samples = Enum.take_every(1..last, max(1, div(last, 6)))

        assert Enum.any?(samples, fn p -> Decoder.decode(flip_byte(@bytes, p)) != original end),
               "#{@name}: at least one sampled payload-byte flip must diverge"
      end
    end
  end

  defp flip_byte(bytes, i) do
    <<head::binary-size(^i), b, rest::binary>> = bytes
    <<head::binary, bxor(b, 0xFF), rest::binary>>
  end
end
