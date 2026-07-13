defmodule Replicant.TelemetryTest do
  use ExUnit.Case, async: true

  alias Replicant.Telemetry

  test "allowed_meta_keys/0 returns the CDC structure-only set (incl. :kind)" do
    keys = Telemetry.allowed_meta_keys() |> MapSet.new()

    assert MapSet.subset?(
             MapSet.new([
               :commit_lsn,
               :change_count,
               :byte_size,
               :lag_ms,
               :duration,
               :table,
               :reason,
               :error_class,
               :kind
             ]),
             keys
           )
  end

  test "event/3 accepts the :kind meta key (used by schema_change events)" do
    # Does not raise — :kind is allowlisted (Task 13 emits %{kind: :additive|:destructive}).
    assert :ok =
             Telemetry.event([:replicant, :schema_change, :additive], %{}, %{
               kind: :additive,
               table: "orders"
             })
  end

  test "event/3 accepts allowlisted metadata" do
    :telemetry.attach_many(
      to_string(__MODULE__) <> "-ok",
      [[:replicant, :transaction, :assembled]],
      fn _event, _meas, meta, _cfg -> send(self(), {:emitted, meta}) end,
      nil
    )

    Telemetry.event([:replicant, :transaction, :assembled], %{change_count: 2, byte_size: 64}, %{
      commit_lsn: 0x10
    })

    assert_received {:emitted, %{commit_lsn: 0x10}}
  after
    :telemetry.detach(to_string(__MODULE__) <> "-ok")
  end

  test "event/3 RAISES on a value-bearing metadata key (Critical Rule 1)" do
    assert_raise ArgumentError, fn ->
      Telemetry.event([:replicant, :sink, :committed], %{duration: 1}, %{row_value: "secret"})
    end
  end

  test "slot_name is a permitted (value-free) metadata key" do
    assert %{slot_name: "rep_x", commit_lsn: 5} =
             Telemetry.validate!(%{slot_name: "rep_x", commit_lsn: 5})
  end

  test "TRIPWIRE: a non-allowlisted (row-value-shaped) key still raises" do
    assert_raise ArgumentError, ~r/value-free allowlist/, fn ->
      Telemetry.validate!(%{customer_email: "a@b.c"})
    end
  end

  test "attempt and max_retries are permitted (value-free) metadata keys" do
    assert %{slot_name: "s", attempt: 2, max_retries: 5} =
             Telemetry.validate!(%{slot_name: "s", attempt: 2, max_retries: 5})
  end

  test "TRIPWIRE: a row-value-shaped key alongside the retry keys still raises" do
    assert_raise ArgumentError, ~r/value-free allowlist/, fn ->
      Telemetry.validate!(%{attempt: 1, max_retries: 5, customer_email: "a@b.c"})
    end
  end

  test "transactional is an allowlisted meta key (A2 messages)" do
    assert :transactional in Replicant.Telemetry.allowed_meta_keys()
  end

  describe "span/3" do
    test "a stop-side off-allowlist metadata key raises (Critical Rule 1 on the merged meta)" do
      # span/3 merges start + stop meta and re-validates before the stop event —
      # a value-bearing key on the STOP side must raise, not ship.
      assert_raise ArgumentError, fn ->
        Telemetry.span(:transaction, %{commit_lsn: 0x10}, fn ->
          {:the_result, %{leaked_value: "secret"}}
        end)
      end
    end

    test "returns the function result when both start and stop metadata are allowlisted" do
      assert :the_result =
               Telemetry.span(:transaction, %{commit_lsn: 0x10}, fn ->
                 {:the_result, %{change_count: 3}}
               end)
    end
  end
end
