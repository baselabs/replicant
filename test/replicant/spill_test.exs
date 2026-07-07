defmodule Replicant.SpillTest do
  use ExUnit.Case, async: true

  alias Replicant.{Change, Spill}

  setup do
    base =
      Path.join(System.tmp_dir!(), "replicant_spill_test_#{System.unique_integer([:positive])}")

    on_exit(fn -> File.rm_rf(base) end)
    %{base: base, slot: "slot_a"}
  end

  defp change(v), do: %Change{op: :insert, schema: "public", table: "t", record: %{"v" => v}}

  test "open creates a 0700 slot dir and a 0600 per-top-xid file; append writes framed bytes", %{
    base: base,
    slot: slot
  } do
    assert {:ok, h} = Spill.open(base, slot, 100)
    slot_dir = Path.join(base, slot)
    assert File.dir?(slot_dir)
    assert {:ok, %File.Stat{mode: dmode}} = File.stat(slot_dir)
    assert Bitwise.band(dmode, 0o777) == 0o700
    assert {:ok, %File.Stat{mode: fmode}} = File.stat(h.path)
    assert Bitwise.band(fmode, 0o777) == 0o600

    assert {:ok, n1} = Spill.append(h, 100, change(1))
    assert {:ok, n2} = Spill.append(h, 101, change(2))
    assert n1 > 0 and n2 > 0
    :ok = Spill.close(h)

    # frames are length-prefixed term_to_binary({subxid, %Change{}})
    raw = File.read!(h.path)
    <<len1::32, f1::binary-size(len1), rest::binary>> = raw
    assert {100, %Change{record: %{"v" => 1}}} = :erlang.binary_to_term(f1)
    <<len2::32, f2::binary-size(len2), <<>>::binary>> = rest
    assert {101, %Change{record: %{"v" => 2}}} = :erlang.binary_to_term(f2)
  end

  test "discard closes the device and removes the file", %{base: base, slot: slot} do
    {:ok, h} = Spill.open(base, slot, 100)
    {:ok, _} = Spill.append(h, 100, change(1))
    assert File.exists?(h.path)
    :ok = Spill.discard(h)
    refute File.exists?(h.path)
  end

  test "append to a closed/absent device returns a value-free :spill_io_failed (no row bytes)", %{
    base: base,
    slot: slot
  } do
    {:ok, h} = Spill.open(base, slot, 100)
    :ok = Spill.discard(h)

    assert {:error, %Replicant.Error{reason: :spill_io_failed} = err} =
             Spill.append(h, 100, change("SEKRET"))

    refute Exception.message(err) =~ "SEKRET"
    refute inspect(err) =~ "SEKRET"
  end

  test "sweep_slot removes a prior crash-orphan file for the slot, leaving OTHER slots untouched",
       %{base: base, slot: slot} do
    {:ok, ha} = Spill.open(base, slot, 100)
    {:ok, _} = Spill.append(ha, 100, change(1))
    :ok = Spill.close(ha)
    {:ok, hb} = Spill.open(base, "slot_b", 200)
    {:ok, _} = Spill.append(hb, 200, change(2))
    :ok = Spill.close(hb)

    :ok = Spill.sweep_slot(base, slot)
    # slot_a orphan swept
    refute File.exists?(ha.path)
    # concurrent slot_b untouched
    assert File.exists?(hb.path)
  end
end
