defmodule Replicant.ErrorTest do
  use ExUnit.Case, async: true

  alias Replicant.Error

  describe "message/1" do
    test "carries reason, table, lsn class — never a value" do
      err = %Error{reason: :decode_failure, table: "orders", lsn: 0x16E3778}
      msg = Exception.message(err)
      assert msg =~ "decode_failure"
      assert msg =~ "orders"
      # lsn renders as its safe uppercase hex shape (a WAL position, not row data)
      assert msg =~ "0/16E3778"
    end
  end

  describe "Inspect" do
    test "never renders a value-bearing field even if one is set" do
      # A caller must NEVER put a row value on the struct, but if one slips through,
      # Inspect must not surface it. We model the leak defensively.
      err = %Error{reason: :sink_failed, table: "orders", lsn: 0x1, message: "secret-row-value"}
      inspected = inspect(err)
      assert inspected =~ "sink_failed"
      refute inspected =~ "secret-row-value"
    end
  end

  describe "decode_failure/1" do
    test "scrubs the underlying exception's message and raw bytes" do
      # The vendored decoder raises FunctionClauseError (carrying no row value) on
      # malformed bytes, but a %Postgrex.Error{} or an ArithmeticError CAN embed
      # bytes. The boundary must scrub both to a value-free reason.
      noisy = %ArgumentError{message: "bad bytes <<0,1,2,3>> leaked here"}
      err = Error.decode_failure(noisy)
      inspected = inspect(err)
      refute inspected =~ "bad bytes"
      refute inspected =~ "leaked"
      assert err.reason == :decode_failure
    end

    test "rejects a non-exception struct (fail loud, not silent mislabel)" do
      # decode_failure/1 scrubs a RAISED exception; a non-exception struct must
      # NOT be silently stamped :decode_failure (its @spec is Exception.t()).
      assert_raise FunctionClauseError, fn -> Error.decode_failure(~D[2024-01-01]) end
    end
  end

  describe "snapshot fault (spec §9, Critical Rule 1)" do
    test "a :snapshot_failed error renders reason + structural fields only, no value" do
      err = %Replicant.Error{reason: :snapshot_failed, table: "orders", shape: "Postgrex.Error"}
      msg = Exception.message(err)
      assert msg =~ "reason=snapshot_failed"
      assert msg =~ "table=orders"
      assert msg =~ "shape=Postgrex.Error"
    end
  end

  test "checkpoint-store reasons build and render structurally" do
    e = %Replicant.Error{reason: :checkpoint_store_failed}
    assert Replicant.Error.message(e) =~ "reason=checkpoint_store_failed"
    m = %Replicant.Error{reason: :checkpoint_store_schema_mismatch, shape: "commit_lsn=text"}
    assert Replicant.Error.message(m) =~ "shape=commit_lsn=text"
  end
end
