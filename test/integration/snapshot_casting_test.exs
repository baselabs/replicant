defmodule Replicant.SnapshotCastingTest do
  @moduledoc """
  v1 snapshot ↔ stream TYPE CONVERGENCE (spec §2; tracker D5).

  The v1 `snapshot: true` path previously shipped Postgrex's NATIVE row decode
  (`SELECT *`), so a `timestamp` column delivered `%NaiveDateTime{}` from the
  snapshot while the stream casts the same column through `Casting.Types.cast_record/2`
  to `%DateTime{}` — a sink branching on the runtime type sees two shapes for one
  column. The incremental snapshot was fixed; this test back-ports and locks the
  convergence for the v1 path: every snapshot value must be the type the stream's
  cast produces.
  """
  use ExUnit.Case, async: false
  @moduletag :integration

  alias Replicant.Test.PG16

  # Captures the FIRST delivered snapshot row's record into :persistent_term so the
  # test process can assert the runtime TYPE of each value. (Runs in the snapshotter
  # process, so it cannot `send` to a pid it doesn't know — a global key is the bridge.)
  defmodule CapturingSink do
    @behaviour Replicant.Sink
    @key {__MODULE__, :first_record}

    def checkpoint, do: {:ok, nil}

    def handle_snapshot([], _ctx), do: :ok

    def handle_snapshot([%Replicant.Change{} = c | _], _ctx) do
      if is_nil(:persistent_term.get(@key, nil)), do: :persistent_term.put(@key, c.record)
      :ok
    end

    def handle_snapshot_complete(lsn), do: {:ok, lsn}
    def handle_transaction(_txn), do: {:ok, 0}
  end

  @key {CapturingSink, :first_record}

  setup do
    {:ok, ctrl} = Postgrex.start_link(PG16.pg_opts() ++ [pool_size: 5])
    slot = "rep_snapcast_#{System.unique_integer([:positive])}"
    :persistent_term.erase(@key)
    reset_schema(ctrl)
    drop_slot(ctrl, slot)

    on_exit(fn ->
      Replicant.stop(slot)
      PG16.wait_until(fn -> Registry.lookup(Replicant.Registry, {slot, :pipeline}) == [] end, 200)

      {:ok, c} = Postgrex.start_link(PG16.pg_opts())
      drop_slot(c, slot)
      :persistent_term.erase(@key)
    end)

    %{ctrl: ctrl, slot: slot}
  end

  test "v1 snapshot delivers the SAME runtime types as the stream (cast convergence)",
       %{ctrl: ctrl, slot: slot} do
    # timestamp (without tz) is the canonical divergence: Postgrex native-decodes
    # NaiveDateTime; the stream casts DateTime. numeric -> Decimal, bool -> boolean,
    # jsonb -> map (all already converge, asserted for completeness).
    Postgrex.query!(
      ctrl,
      "CREATE TABLE cast_sample (" <>
        "id int PRIMARY KEY," <>
        "ts timestamp without time zone," <>
        "amt numeric(10,2)," <>
        "flag bool," <>
        "meta jsonb)",
      []
    )

    Postgrex.query!(ctrl, "CREATE PUBLICATION cast_pub FOR TABLE cast_sample", [])

    Postgrex.query!(
      ctrl,
      "INSERT INTO cast_sample (id, ts, amt, flag, meta) VALUES " <>
        "(1, '2024-01-15 10:30:00', '12.34', true, '{\"k\":1}'::jsonb)",
      []
    )

    {:ok, _} =
      Replicant.start_link(
        connection: PG16.pg_opts(),
        slot_name: slot,
        publication: "cast_pub",
        sink: CapturingSink,
        snapshot: true
      )

    record = poll_record(@key, 400)

    assert record != nil, "a snapshot row was never captured"
    # The convergence contract: each value is the type the STREAM's cast_record produces.
    assert %DateTime{} = record["ts"],
           "ts must be %DateTime{} (stream cast), got #{inspect(record["ts"])}"

    assert record["ts"] == DateTime.from_naive!(~N[2024-01-15 10:30:00], "Etc/UTC")
    assert %Decimal{} = record["amt"], "amt must be %Decimal{} (stream cast)"
    assert Decimal.equal?(record["amt"], Decimal.new("12.34"))
    assert record["flag"] == true
    assert record["meta"] == %{"k" => 1}
  end

  defp poll_record(_key, 0), do: nil

  defp poll_record(key, tries) do
    case :persistent_term.get(key, nil) do
      nil ->
        Process.sleep(25)
        poll_record(key, tries - 1)

      record ->
        record
    end
  end

  defp reset_schema(c) do
    Postgrex.query!(c, "DROP PUBLICATION IF EXISTS cast_pub", [])
    Postgrex.query!(c, "DROP TABLE IF EXISTS cast_sample", [])
    Postgrex.query!(c, "CREATE TABLE cast_sample (id int PRIMARY KEY)", [])
    Postgrex.query!(c, "DROP PUBLICATION IF EXISTS cast_pub", [])
    Postgrex.query!(c, "DROP TABLE IF EXISTS cast_sample", [])
  end

  defp drop_slot(c, slot), do: drop_slot(c, slot, 20)

  defp drop_slot(c, slot, 0) do
    Postgrex.query!(
      c,
      "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
      [slot]
    )
  end

  defp drop_slot(c, slot, tries) do
    Postgrex.query!(
      c,
      "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
      [slot]
    )
  rescue
    _ ->
      Process.sleep(50)
      drop_slot(c, slot, tries - 1)
  end
end
