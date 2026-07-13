defmodule Replicant.Decoder.DecoderTest do
  use ExUnit.Case, async: true

  alias Replicant.Decoder
  alias Replicant.Decoder.Messages

  describe "decode/1 parser mechanics (hand-crafted bytes — parser unit, not conformance)" do
    test "decodes an Insert into %Messages.Insert{}" do
      bytes =
        <<73, 0, 0, 96, 0, 78, 0, 2, 116, 0, 0, 0, 3, 98, 97, 122, 116, 0, 0, 0, 3, 53, 54, 48>>

      assert {:ok, %Messages.Insert{relation_id: 24_576, tuple_data: {"baz", "560"}}} =
               Decoder.decode(bytes)
    end

    test "decodes a Begin into %Messages.Begin{} with uint64 lsn" do
      # "B", final_lsn::binary-8 (file=0,offset=0x16E3778), timestamp::64, xid::32
      lsn_bytes = <<0::32, 0x16E3778::32>>
      bytes = <<"B", lsn_bytes::binary, 0::64, 42::32>>
      assert {:ok, %Messages.Begin{final_lsn: final_lsn, xid: 42}} = Decoder.decode(bytes)
      # decode_lsn/1 converts the {file, offset} byte pair to a single uint64 via
      # Bitwise.bsl(file, 32) + offset — always an integer, never a tuple. This
      # asserts the conversion result directly; a `refute match?({_, _}, final_lsn)`
      # would be vacuous (the value is provably an integer) and trips the Elixir 1.19
      # type-checker (integer can never match a 2-tuple).
      assert final_lsn == 0x16E3778
    end
  end

  describe "decode/1 value-free-error boundary (Critical Rule 1)" do
    test "a truncated tuple-data that RAISES inside the parser is caught → {:error, :decode_failure}, zero leaked bytes" do
      # Insert of relation 1, 1 column, then a 't' tuple-data entry claiming a
      # 99-byte column but providing only the bytes of "secret-row-value". The
      # vendored decode_tuple_data reads beyond the binary → raises. The boundary
      # MUST catch it and scrub the offending bytes (which carry row data).
      bytes = <<"I", 0, 0, 0, 1, "N", 0, 1, "t", 99::32, "secret-row-value">>
      {:error, err} = Decoder.decode(bytes)
      inspected = inspect(err) <> Exception.message(err)
      assert err.reason == :decode_failure
      refute inspected =~ "secret-row-value"
    end

    test "an unknown first byte (no clause matches) → {:error, :unsupported_message}, NO raw data" do
      # Unknown letters fall to the vendored catch-all %Unsupported{data: binary};
      # the boundary normalises it to a value-free error and drops `data`.
      {:error, err} = Decoder.decode(<<"Z", "more-secret-bytes", 9, 9, 9>>)
      inspected = inspect(err) <> Exception.message(err)
      assert err.reason == :unsupported_message
      refute inspected =~ "more-secret-bytes"
    end

    test "decode/1 is total: every binary returns a tagged result, never raises" do
      assert {:ok, %Messages.Begin{}} = Decoder.decode(<<"B", 0::32, 0x10::32, 0::64, 1::32>>)
      assert {:error, _} = Decoder.decode(<<>>)
      assert {:error, _} = Decoder.decode(<<"Z">>)
    end
  end

  describe "streaming message structs (spec §5)" do
    alias Replicant.Decoder.Messages.{
      Insert,
      Message,
      StreamAbort,
      StreamCommit,
      StreamStart,
      StreamStop,
      Truncate
    }

    test "the four stream-control structs exist with their documented fields" do
      assert %StreamStart{xid: 7, first_segment: true}.first_segment == true
      assert %StreamStop{} == %StreamStop{}

      assert %StreamCommit{xid: 7, commit_lsn: 100, end_lsn: 101, commit_timestamp: nil}.commit_lsn ==
               100

      assert %StreamAbort{xid: 7, subxid: 8}.subxid == 8
    end

    test "change structs carry an optional xid (nil for non-streamed)" do
      assert %Insert{relation_id: 1, tuple_data: {}}.xid == nil
      assert %Insert{relation_id: 1, tuple_data: {}, xid: 515_103}.xid == 515_103
    end

    test "decodes Stream Start / Stop / Commit / Abort (unambiguous by type byte)" do
      assert {:ok, %StreamStart{xid: 515_103, first_segment: true}} =
               Decoder.decode(<<"S", 515_103::32, 1::8>>)

      assert {:ok, %StreamStop{}} = Decoder.decode(<<"E">>)

      # c: xid, flags(0), commit_lsn(8B pg_lsn), end_lsn(8B), commit_ts(i64). LSN 0/100 = 100.
      assert {:ok, %StreamCommit{xid: 515_103, commit_lsn: 100, end_lsn: 101}} =
               Decoder.decode(<<"c", 515_103::32, 0::8, 0::32, 100::32, 0::32, 101::32, 0::64>>)

      assert {:ok, %StreamAbort{xid: 515_103, subxid: 515_104}} =
               Decoder.decode(<<"A", 515_103::32, 515_104::32>>)
    end

    test "a STREAMED Insert (xid-prefixed) decodes only under streaming?: true" do
      # streamed Insert = "I" <xid::32> <relation_oid::32> "N" <ncols::16> <tuple...>
      # one text column 't' with value "x": "t" <len::32> "x"
      streamed = <<"I", 515_103::32, 42::32, "N", 1::16, "t", 1::32, "x">>

      assert {:ok, %Insert{xid: 515_103, relation_id: 42, tuple_data: {"x"}}} =
               Decoder.decode(streamed, streaming: true)

      # Under the v1 (non-streaming) decoder the same bytes mis-frame: the leading xid is read as
      # relation_id and the remaining bytes don't match the v1 Insert body, so it falls to the
      # Unsupported catch-all — a value-free :unsupported_message, proving the streaming flag is
      # load-bearing (spec §7). No row values leak.
      assert {:error, %Replicant.Error{reason: :unsupported_message}} =
               Decoder.decode(streamed, streaming: false)
    end

    test "a non-streamed Insert still decodes without xid under the default (v1) path" do
      v1 = <<"I", 42::32, "N", 1::16, "t", 1::32, "x">>

      assert {:ok, %Insert{xid: nil, relation_id: 42, tuple_data: {"x"}}} =
               Decoder.decode(v1)
    end

    test "a STREAMED Truncate (xid-prefixed) strips the xid, decodes the v1 body, re-attaches xid" do
      # v1 Truncate wire (decode_message_impl): "T" <n_relations::32> <options::8> <oid::32...>.
      # options=0 → [], two relation oids 100 and 200. The streamed form prefixes the xid AFTER
      # the type byte: "T" <xid::32> <v1-truncate-body>. A structurally-different delegated type
      # than Insert (relation-count + option byte + oid list, no "N"/tuple-data) — exercises the
      # xid-strip + put_xid round-trip on a second shape.
      streamed_truncate = <<"T", 515_103::32, 2::32, 0::8, 100::32, 200::32>>

      assert {:ok,
              %Truncate{
                xid: 515_103,
                number_of_relations: 2,
                options: [],
                truncated_relations: [100, 200]
              }} = Decoder.decode(streamed_truncate, streaming: true)
    end

    test "byte-level tampering of a streamed frame's xid-prefixed body is caught value-free (spec §12.2, Rule 1)" do
      # A streamed Insert = "I" <xid::32> <rel::32> "N" <ncols::16> <col...>. Tamper the body so the
      # column length (99) overruns the actual bytes: the vendored parser raises reading past the end,
      # and decode/1's boundary rescue MUST scrub it to a value-free error carrying NO row bytes.
      sentinel = "SENTINEL_ROW_SECRET"
      tampered = <<"I", 515_103::32, 42::32, "N", 1::16, "t", 99::32, sentinel::binary>>

      assert {:error, %Replicant.Error{reason: reason} = err} =
               Decoder.decode(tampered, streaming: true)

      # value-free fail-closed: a tampered streamed frame surfaces as :decode_failure (a parser raise)
      # or :unsupported_message (a mis-frame to the catch-all) — never a silently mis-parsed struct.
      assert reason in [:decode_failure, :unsupported_message]
      # Rule 1: no raw row byte leaks through the error's rendered message or inspect.
      refute Exception.message(err) =~ sentinel
      refute inspect(err) =~ sentinel
    end

    test "a streamed change frame truncated WITHIN the xid field is caught value-free (spec §12.2)" do
      # Only 2 of the xid's 4 bytes present: the streaming delegation clause (which needs xid::32)
      # cannot match, and the v1 fallback cannot frame it either → a value-free error, never a crash
      # and never a mis-parsed struct.
      truncated = <<"I", 515_103::16>>

      assert {:error, %Replicant.Error{reason: reason}} =
               Decoder.decode(truncated, streaming: true)

      assert reason in [:decode_failure, :unsupported_message]
    end

    test "decodes a non-transactional Message (flags=0) under the v1 path" do
      # "M" <flags::8=0> <lsn::64> <prefix> <null> <length::32> <content>
      lsn_bytes = <<0::32, 0x16E3778::32>>
      bytes = <<"M", 0::8, lsn_bytes::binary, "outbox", 0, 7::32, "payload">>

      assert {:ok,
              %Message{
                transactional?: false,
                lsn: 0x16E3778,
                prefix: "outbox",
                content: "payload",
                xid: nil
              }} = Decoder.decode(bytes)
    end

    test "decodes a transactional Message (flags=1) under the v1 path" do
      lsn_bytes = <<0::32, 0x16E3778::32>>
      bytes = <<"M", 1::8, lsn_bytes::binary, "outbox", 0, 7::32, "payload">>

      assert {:ok,
              %Message{
                transactional?: true,
                lsn: 0x16E3778,
                prefix: "outbox",
                content: "payload",
                xid: nil
              }} = Decoder.decode(bytes)
    end

    test "decodes a STREAMED Message (xid-prefixed) only under streaming?: true" do
      # streamed: "M" <xid::32> <flags::8> <lsn::64> <prefix> <null> <length::32> <content>
      lsn_bytes = <<0::32, 0x16E3778::32>>
      streamed = <<"M", 515_103::32, 1::8, lsn_bytes::binary, "outbox", 0, 7::32, "payload">>

      assert {:ok,
              %Message{
                transactional?: true,
                lsn: 0x16E3778,
                prefix: "outbox",
                content: "payload",
                xid: 515_103
              }} = Decoder.decode(streamed, streaming: true)

      # Under v1 the same bytes mis-frame: the leading xid is read as flags, and the null-split
      # + length-prefix body is shape-flexible enough to still match (unlike the rigid "N"/ncols
      # Insert body, which falls to :unsupported_message). So v1 yields a valid-but-WRONG decode —
      # garbled prefix/lsn and xid: nil — which is exactly why the streaming flag is load-bearing
      # for xid attachment. The streamed xid (515_103) is provably absent under v1.
      assert {:ok, %Message{xid: nil, content: "payload"} = v1_msg} =
               Decoder.decode(streamed, streaming: false)

      refute v1_msg.prefix == "outbox"
      refute v1_msg.lsn == 0x16E3778
    end

    test "a malformed Message (truncated content) is caught value-free (Rule 1)" do
      # claims 99 bytes of content but provides only "secret-msg"
      bytes = <<"M", 0::8, 0::64, "p", 0, 99::32, "secret-msg">>
      {:error, err} = Decoder.decode(bytes)
      assert err.reason == :decode_failure
      refute inspect(err) <> Exception.message(err) =~ "secret-msg"
    end
  end
end
