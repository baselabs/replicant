defmodule Replicant.SnapshotProgressTest do
  use ExUnit.Case, async: true

  alias Replicant.SnapshotProgress, as: SP

  defp tables do
    [
      %{
        schema: "public",
        table: "orders",
        qualified: "public.orders",
        pk_raw: ["id"],
        pk_quoted: [~s("id")]
      },
      %{schema: "public", table: "nopk", qualified: "public.nopk", pk_raw: [], pk_quoted: []}
    ]
  end

  test "new/2 queues all tables, not complete, carries the floor" do
    sp = SP.new(tables(), 1_000)
    assert sp.floor_lsn == 1_000
    refute SP.complete?(sp)
    assert {:table, %{qualified: "public.orders"}, nil, _sp} = SP.next(sp)
  end

  test "advance/2 records the bound; finish_table/1 moves on; completing all yields :complete" do
    sp = SP.new(tables(), 1)
    {:table, t1, nil, sp} = SP.next(sp)
    sp = SP.advance(sp, [42])
    # a re-read of next after advance resumes the SAME table at the recorded bound
    assert {:table, ^t1, [42], _} = SP.next(sp)
    sp = SP.finish_table(sp)
    {:table, t2, nil, sp} = SP.next(sp)
    assert t2.qualified == "public.nopk"
    sp = SP.finish_table(sp)
    assert :complete = SP.next(sp)
    assert SP.complete?(SP.mark_complete(sp))
  end

  test "encode/decode round-trips" do
    # `advance/2` records a bound for the IN-PROGRESS table, so establish one via
    # `next/1` first (an in-progress backfill with a recorded bound is the realistic
    # struct to round-trip; advancing before `next/1` has no current table).
    {:table, _t, nil, sp0} = tables() |> SP.new(7) |> SP.next()
    sp = SP.advance(sp0, [9])
    assert {:ok, ^sp} = sp |> SP.encode() |> SP.decode()
  end

  test "decode rejects tampered/truncated/foreign binaries value-free" do
    good = SP.encode(SP.new(tables(), 7))
    <<head::binary-size(10), _::binary>> = good
    assert {:error, :snapshot_progress_invalid} = SP.decode(head)
    assert {:error, :snapshot_progress_invalid} = SP.decode(<<131, 100, 0, 3, 102, 111, 111>>)
    assert {:error, :snapshot_progress_invalid} = SP.decode(:erlang.term_to_binary({:not, :ours}))
    # non-binary input hits the fail-closed decode(_other) clause
    assert {:error, :snapshot_progress_invalid} = SP.decode(nil)
  end

  test "decode rejects a well-versioned token with a malformed inner table_ref (value-free)" do
    forged =
      :erlang.term_to_binary(
        {:replicant_snapshot_progress, 1,
         %{floor_lsn: 5, pending: [%{}], current: nil, bound: nil, done: [], complete?: false}}
      )

    assert {:error, :snapshot_progress_invalid} = SP.decode(forged)
  end

  test "redo_table/1 clears the bound but keeps the in-progress table" do
    sp = tables() |> SP.new(1)
    {:table, t, nil, sp} = SP.next(sp)
    sp = SP.advance(sp, [99])
    sp = SP.redo_table(sp)
    assert sp.bound == nil
    assert {:table, ^t, nil, _} = SP.next(sp)
  end

  test "decode rejects a future version tag" do
    sp = SP.new(tables(), 7)
    forged = :erlang.term_to_binary({:replicant_snapshot_progress, 999, Map.from_struct(sp)})
    assert {:error, :snapshot_progress_invalid} = SP.decode(forged)
  end

  test "decode never uses unsafe binary_to_term (atom-forging binary rejected, atom NOT created)" do
    # An external-term binary encoding a NEW atom. Under binary_to_term/2 with [:safe]
    # this RAISES (atom not created); under plain binary_to_term it would silently
    # create the atom. Both paths currently reach {:error, :snapshot_progress_invalid},
    # so ALSO assert the atom does not exist — that assertion goes RED if [:safe] is dropped.
    name = "replicant_forged_atom_qz7x_4417"
    evil = <<131, 100, byte_size(name)::16>> <> name
    assert {:error, :snapshot_progress_invalid} = SP.decode(evil)
    assert_raise ArgumentError, fn -> String.to_existing_atom(name) end
  end
end
