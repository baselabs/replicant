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

  describe "value-shape contract (R02)" do
    # Each allowlisted key now carries a type/shape contract, not just key-closure.
    # A row/column value smuggled into an allowlisted key as the WRONG shape (a string
    # where an LSN/count/duration/boolean belongs) must raise, not ship downstream.

    test "TRIPWIRE: a string in the LSN field (commit_lsn) raises" do
      assert_raise ArgumentError, fn -> Telemetry.validate!(%{commit_lsn: "0/16B3748"}) end
    end

    test "commit_lsn admits nil (checkpoint-store :read on a fresh slot = no checkpoint)" do
      assert %{commit_lsn: nil} = Telemetry.validate!(%{commit_lsn: nil})
    end

    test "TRIPWIRE: a NON-nil atom in the LSN field still raises (nil-admission is nil-specific)" do
      assert_raise ArgumentError, fn -> Telemetry.validate!(%{commit_lsn: :absent}) end
    end

    test "TRIPWIRE: a string in a count field (change_count) raises" do
      assert_raise ArgumentError, fn -> Telemetry.validate!(%{change_count: "5"}) end
    end

    test "TRIPWIRE: a string in the boolean field (transactional) raises" do
      assert_raise ArgumentError, fn -> Telemetry.validate!(%{transactional: "true"}) end
    end

    test "TRIPWIRE: a string in a duration field (lag_ms) raises" do
      assert_raise ArgumentError, fn -> Telemetry.validate!(%{lag_ms: "12"}) end
    end

    test "TRIPWIRE: a negative value in a non-negative count field (byte_size) raises" do
      assert_raise ArgumentError, fn -> Telemetry.validate!(%{byte_size: -1}) end
    end

    test "TRIPWIRE: a non-atom in the reason field raises" do
      assert_raise ArgumentError, fn -> Telemetry.validate!(%{reason: "sink_failed"}) end
    end

    test "TRIPWIRE: a string measurement value (duration) raises" do
      assert_raise ArgumentError, fn ->
        Telemetry.event([:replicant, :sink, :committed], %{duration: "1"}, %{commit_lsn: 0})
      end
    end

    test "TRIPWIRE: an off-allowlist MEASUREMENT key raises" do
      assert_raise ArgumentError, ~r/value-free allowlist/, fn ->
        Telemetry.event([:replicant, :sink, :committed], %{customer_balance: 1}, %{commit_lsn: 0})
      end
    end

    test "VALUE-FREE: a shape-violation error never renders the offending value (Rule 1)" do
      # The secret byte-string smuggled into an LSN field must NOT appear in the raised
      # message — only the key and the TYPE. inspect(value) here would be the exact leak.
      secret = "S3CR3T-row-bytes-do-not-leak"

      err =
        assert_raise ArgumentError, fn -> Telemetry.validate!(%{commit_lsn: secret}) end

      refute err.message =~ secret
      assert err.message =~ "commit_lsn"
    end

    test "VALUE-FREE: an off-allowlist metadata error never renders the value" do
      err =
        assert_raise ArgumentError, fn ->
          Telemetry.validate!(%{customer_email: "a@b.c"})
        end

      refute err.message =~ "a@b.c"
    end

    test "all current metadata shapes pass" do
      assert %{} =
               Telemetry.validate!(%{
                 commit_lsn: 0x10,
                 change_count: 0,
                 byte_size: 64,
                 lag_ms: 0,
                 duration: 5,
                 attempt: 2,
                 max_retries: 5,
                 transactional: false,
                 table: "public.orders",
                 slot_name: "rep_x",
                 reason: :sink_failed,
                 kind: :additive
               })
    end

    test "table admits nil (schema-change sites carry String.t() | nil, value-free)" do
      assert %{table: nil} = Telemetry.validate!(%{table: nil})
    end

    test "the lag measurement admits a NEGATIVE value (signed WAL-byte arithmetic)" do
      # connection.ex emits `%{lag: received - max(cp, floor) - spilled}` — signed; a
      # non-negative guard here would over-reject a legitimate current emission site.
      assert :ok =
               Telemetry.event([:replicant, :connection, :disconnected], %{lag: -3}, %{
                 reason: :sink_too_slow
               })
    end

    test "current measurement shapes (duration, byte_size, change_count) pass" do
      assert :ok =
               Telemetry.event(
                 [:replicant, :transaction, :assembled],
                 %{
                   duration: 7,
                   byte_size: 64,
                   change_count: 2
                 },
                 %{commit_lsn: 0x10}
               )
    end
  end
end
