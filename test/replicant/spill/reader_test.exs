defmodule Replicant.Spill.ReaderTest do
  use ExUnit.Case, async: true

  alias Replicant.{Change, Spill}
  alias Replicant.Spill.Reader

  setup do
    base =
      Path.join(System.tmp_dir!(), "replicant_reader_test_#{System.unique_integer([:positive])}")

    on_exit(fn -> File.rm_rf(base) end)
    %{base: base}
  end

  defp change(v), do: %Change{op: :insert, schema: "public", table: "t", record: %{"v" => v}}

  defp spill_file(base, frames) do
    {:ok, h} = Spill.open(base, "s", 100)
    Enum.each(frames, fn {sx, c} -> {:ok, _} = Spill.append(h, sx, c) end)
    :ok = Spill.close(h)
    h.path
  end

  test "streams spilled frames THEN the in-memory tail, filtered by aborted subxids, ordinals + commit_lsn stamped",
       %{base: base} do
    # spilled (commit order): subxid 100 v=1, subxid 101 v=2 (101 will be aborted)
    path = spill_file(base, [{100, change(1)}, {101, change(2)}])
    # in-memory tail is stored newest-first (as the assembler holds it): [{100, v=4}, {100, v=3}]
    tail = [{100, change(4)}, {100, change(3)}]
    aborted = MapSet.new([101])

    reader = Reader.new(path, tail, aborted, 900)
    changes = Enum.to_list(reader)

    # 101 filtered out; commit order = spilled(1) then tail-reversed(3,4); ordinals 0,1,2; commit_lsn 900
    assert [
             %Change{record: %{"v" => 1}, ordinal: 0, commit_lsn: 900},
             %Change{record: %{"v" => 3}, ordinal: 1, commit_lsn: 900},
             %Change{record: %{"v" => 4}, ordinal: 2, commit_lsn: 900}
           ] = changes
  end

  test "a corrupted spill frame raises a value-free Spill.Error (no row bytes)", %{base: base} do
    {:ok, h} = Spill.open(base, "s", 100)
    {:ok, _} = Spill.append(h, 100, change("SEKRET-ROW"))
    :ok = Spill.close(h)
    # corrupt the frame body (flip bytes AFTER the 4-byte length prefix)
    raw = File.read!(h.path)
    <<len::32, body::binary-size(len)>> = raw

    corrupted =
      <<len::32, :binary.part(body, 0, 1)::binary, 0xFF, :binary.part(body, 2, len - 2)::binary>>

    File.write!(h.path, corrupted)

    err =
      assert_raise Replicant.Spill.Error, fn ->
        Enum.to_list(Reader.new(h.path, [], MapSet.new(), 900))
      end

    refute Exception.message(err) =~ "SEKRET"
    assert err.reason == :spill_io_failed
  end

  test "a :safe frame of the WRONG shape raises a value-free Spill.Error at the decode boundary (not a downstream MatchError)",
       %{base: base} do
    File.mkdir_p!(base)
    path = Path.join(base, "100.spill")

    # A valid :safe term (no atoms/funs, so binary_to_term SUCCEEDS) but NOT a {int, %Change{}} frame
    # — e.g. corruption or a tampered 0600 file. It must be rejected at the decode boundary as
    # :spill_io_failed, not pass through to a misattributed MatchError in raw_stream's {subxid, _} match.
    bin = :erlang.term_to_binary({1, 2, 3})
    File.write!(path, <<byte_size(bin)::32, bin::binary>>)

    err =
      assert_raise Replicant.Spill.Error, fn ->
        Enum.to_list(Reader.new(path, [], MapSet.new(), 900))
      end

    assert err.reason == :spill_io_failed
  end

  test "reading a Reader whose file was deleted raises a value-free Spill.Error (fail-loud, spec §8)",
       %{base: base} do
    path = spill_file(base, [{100, change(1)}])
    reader = Reader.new(path, [], MapSet.new(), 900)
    File.rm!(path)
    assert_raise Replicant.Spill.Error, fn -> Enum.to_list(reader) end
  end

  test "the Reader is a proper Enumerable (Enum.each / Enum.reduce work without forcing a List)",
       %{base: base} do
    path = spill_file(base, [{100, change(1)}, {100, change(2)}])
    reader = Reader.new(path, [], MapSet.new(), 900)
    parent = self()
    Enum.each(reader, fn %Change{record: %{"v" => v}} -> send(parent, {:seen, v}) end)
    assert_received {:seen, 1}
    assert_received {:seen, 2}
  end
end
