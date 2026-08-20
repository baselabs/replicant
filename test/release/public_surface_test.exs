defmodule Replicant.ReleasePublicSurfaceTest do
  # The 1.2.0 candidate must actually carry the R01-R05 fixed surfaces, proven by
  # SEMANTIC assertions rather than source text greps: a comment or dead string can satisfy
  # `grep 'handle_session_identity'`, but cannot satisfy "the behaviour lists the callback"
  # or "the query builder emits version-tiered SQL". Each assertion reds if its surface is
  # removed, and none touches a live database. The artifact-derived counterpart runs the same
  # exercises against the extracted package bytes in scripts/release/consume_candidate.sh.
  use ExUnit.Case, async: true

  describe "R04 — typed slot-origin callback + D2 session identity (public Sink surface)" do
    test "handle_slot_origin/2 and handle_session_identity/2 are optional Sink callbacks" do
      callbacks = Replicant.Sink.behaviour_info(:callbacks)
      optional = Replicant.Sink.behaviour_info(:optional_callbacks)

      assert {:handle_slot_origin, 2} in callbacks
      assert {:handle_session_identity, 2} in callbacks
      assert {:handle_slot_origin, 2} in optional
      assert {:handle_session_identity, 2} in optional
    end

    test "SessionIdentity carries the four typed identity fields" do
      identity = %Replicant.SessionIdentity{
        system_identifier: 1,
        timeline_id: 1,
        current_lsn: 0,
        database: "postgres"
      }

      assert Map.keys(identity) |> Enum.sort() ==
               [:__struct__, :current_lsn, :database, :system_identifier, :timeline_id]
    end

    test "the identity query is IDENTIFY_SYSTEM (the actual replication-session identity)" do
      assert Replicant.QueryBuilder.identify_system() == "IDENTIFY_SYSTEM"
    end
  end

  describe "R05 — version-gated slot-invalidation query" do
    test "slot_invalidation_status/2 selects a version-tiered column set" do
      {:ok, pg15} = Replicant.QueryBuilder.slot_invalidation_status("s", 150_019)
      {:ok, pg16} = Replicant.QueryBuilder.slot_invalidation_status("s", 160_014)
      {:ok, pg17} = Replicant.QueryBuilder.slot_invalidation_status("s", 170_011)

      # PG15 has no `conflicting` column (added PG16); PG17 adds `invalidation_reason`.
      refute pg15 =~ "conflicting"
      assert pg16 =~ "conflicting"
      refute pg16 =~ "invalidation_reason"
      assert pg17 =~ "invalidation_reason"
    end
  end

  describe "R02/R03 — value-free telemetry boundary (no row/secret bytes escape)" do
    test "a wrong-shape value on an allowlisted key raises with the value elided" do
      secret = "SECRET-ROW-VALUE-9f3a"

      err =
        assert_raise ArgumentError, fn ->
          # commit_lsn's contract is :lsn (non-neg integer or nil); a string smuggling a row
          # value must be rejected, and the rejection must not echo the bytes.
          Replicant.Telemetry.validate!(%{commit_lsn: secret})
        end

      refute Exception.message(err) =~ secret
    end

    test "an off-allowlist key is rejected without echoing the arbitrary key/value" do
      err =
        assert_raise ArgumentError, fn ->
          Replicant.Telemetry.validate!(%{"row_password" => "hunter2"})
        end

      refute Exception.message(err) =~ "hunter2"
      refute Exception.message(err) =~ "row_password"
    end
  end

  describe "R01 — unknown checkpoint with absent slot halts fail-closed" do
    test "a fault checkpoint with no slot rows never emits CREATE_REPLICATION_SLOT" do
      state = %Replicant.Connection{
        step: :invalidation_check,
        slot_name: "audit_slot",
        publication: ["audit_pub"],
        snapshot: false,
        checkpoint_lsn: 0,
        checkpoint_state: :fault,
        failover: false
      }

      result = Replicant.Connection.handle_result([%Postgrex.Result{rows: []}], state)

      # The fix: a fail-closed data-gap halt, not a slot creation that would skip WAL.
      assert result == {:disconnect, :data_gap}

      refute match?({:query, "CREATE_REPLICATION_SLOT" <> _, _}, result)
    end
  end
end
