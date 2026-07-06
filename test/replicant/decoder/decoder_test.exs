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
    alias Replicant.Decoder.Messages.{Insert, StreamAbort, StreamCommit, StreamStart, StreamStop}

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
  end
end
