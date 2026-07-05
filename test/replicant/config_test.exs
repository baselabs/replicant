# A plain module (no @behaviour): checkpoint/0 is still a MANDATORY @callback at this
# task's commit point (Task 10 moves it to @optional_callbacks), so declaring @behaviour
# would fail `mix compile --warnings-as-errors`. Config.fetch_sink checks callbacks at
# RUNTIME via function_exported?/3, so a plain module is a faithful lib-mode-sink fixture.
# Defined at file top-level (NOT nested under Replicant.ConfigTest) so its fully-qualified
# name is exactly Replicant.Test.CpStubSink — a nested defmodule would be renamed
# Replicant.ConfigTest.Replicant.Test.CpStubSink and relativize the %Transaction{} match.
defmodule Replicant.Test.CpStubSink do
  def handle_transaction(%Replicant.Transaction{} = txn), do: {:ok, txn.commit_lsn}
end

defmodule Replicant.ConfigTest do
  use ExUnit.Case, async: true

  alias Replicant.Config

  # --- stub sinks (each a minimal valid Replicant.Sink) ---

  defmodule StateMirrorEmpty do
    @behaviour Replicant.Sink
    @impl true
    def checkpoint, do: {:ok, nil}
    @impl true
    def handle_transaction(_txn), do: {:ok, 0}
    # no sink_kind/0 → defaults to :state_mirror
  end

  defmodule StateMirrorPersisted do
    @behaviour Replicant.Sink
    @impl true
    def checkpoint, do: {:ok, 0x100}
    @impl true
    def handle_transaction(_txn), do: {:ok, 0x100}
  end

  defmodule AppendLogEmpty do
    @behaviour Replicant.Sink
    @impl true
    def checkpoint, do: {:ok, nil}
    @impl true
    def handle_transaction(_txn), do: {:ok, 0}
    @impl true
    def sink_kind, do: :append_log
  end

  defmodule RaisingCheckpoint do
    @behaviour Replicant.Sink
    @impl true
    def checkpoint, do: raise("checkpoint store unreachable")
    @impl true
    def handle_transaction(_txn), do: {:ok, 0}
  end

  defmodule InvalidKindEmpty do
    @behaviour Replicant.Sink
    @impl true
    def checkpoint, do: {:ok, nil}
    @impl true
    def handle_transaction(_txn), do: {:ok, 0}
    # A typo'd / invalid kind (neither :state_mirror nor :append_log). It must be
    # treated as the strict :state_mirror default so the go-forward guard still fires.
    @impl true
    def sink_kind, do: :stat_mirror
  end

  defmodule SnapshotCapableEmpty do
    @behaviour Replicant.Sink
    @impl true
    def checkpoint, do: {:ok, nil}
    @impl true
    def handle_transaction(_txn), do: {:ok, 0}
    @impl true
    def handle_snapshot(_changes, _ctx), do: :ok
    @impl true
    def handle_snapshot_complete(lsn), do: {:ok, lsn}
  end

  @base [
    connection: [
      hostname: "standby.internal",
      port: 5432,
      username: "u",
      password: "p",
      database: "d"
    ],
    slot_name: "replicant_orders",
    publication: "orders_pub"
  ]

  defp base_opts,
    do: [
      connection: [hostname: "h"],
      slot_name: "s",
      publication: "p",
      sink: Replicant.Test.RecordingSink
    ]

  describe "validate/1" do
    test "accepts a well-formed config and normalises go_forward_only" do
      assert {:ok, cfg} = Config.validate(@base ++ [sink: StateMirrorPersisted])
      assert cfg.slot_name == "replicant_orders"
      assert cfg.publication == "orders_pub"
      assert cfg.sink == StateMirrorPersisted
      assert cfg.go_forward_only == false
    end

    test "rejects an invalid slot identifier (no raw name reaches SQL)" do
      opts = Keyword.merge(@base, slot_name: "orders'; DROP", sink: StateMirrorPersisted)
      assert {:error, :invalid_identifier} = Config.validate(opts)
    end

    test "rejects an invalid publication identifier" do
      opts = Keyword.merge(@base, publication: "Orders Pub", sink: StateMirrorPersisted)
      assert {:error, :invalid_identifier} = Config.validate(opts)
    end

    test "rejects a sink missing the mandatory callbacks" do
      assert {:error, :invalid_sink} = Config.validate(@base ++ [sink: Enum])
    end

    test "rejects a missing connection" do
      opts = Keyword.delete(@base ++ [sink: StateMirrorPersisted], :connection)
      assert {:error, :config_invalid} = Config.validate(opts)
    end

    test "defaults max_inflight_lag to the Connection ceiling when omitted" do
      {:ok, cfg} = Config.validate(@base ++ [sink: StateMirrorPersisted])
      assert cfg.max_inflight_lag == Replicant.Connection.default_max_inflight_lag()
    end

    test "accepts an explicit positive-integer max_inflight_lag (§4 bounded window)" do
      {:ok, cfg} =
        Config.validate(@base ++ [sink: StateMirrorPersisted, max_inflight_lag: 4_096])

      assert cfg.max_inflight_lag == 4_096
    end

    test "rejects a non-positive-integer max_inflight_lag" do
      for bad <- [0, -1, "1024", 1.5] do
        opts = @base ++ [sink: StateMirrorPersisted, max_inflight_lag: bad]
        assert {:error, :config_invalid} = Config.validate(opts)
      end
    end
  end

  describe "guard/1 — go-forward-only start guard (spec §3/§6)" do
    # THE tripwire: a state-mirror sink resuming from an empty checkpoint without an
    # explicit go_forward_only would silently deliver partial data. It must refuse.
    test "REFUSES a :state_mirror sink from an empty checkpoint without go_forward_only" do
      {:ok, cfg} = Config.validate(@base ++ [sink: StateMirrorEmpty])
      assert cfg.go_forward_only == false
      assert {:error, :go_forward_required} = Config.guard(cfg)
    end

    test "allows the same sink when go_forward_only: true" do
      {:ok, cfg} = Config.validate(@base ++ [sink: StateMirrorEmpty, go_forward_only: true])
      assert :ok = Config.guard(cfg)
    end

    test "allows an :append_log sink from an empty checkpoint (guard is state-mirror-only)" do
      {:ok, cfg} = Config.validate(@base ++ [sink: AppendLogEmpty])
      assert :ok = Config.guard(cfg)
    end

    test "allows a :state_mirror sink that already has a checkpoint" do
      {:ok, cfg} = Config.validate(@base ++ [sink: StateMirrorPersisted])
      assert :ok = Config.guard(cfg)
    end

    test "a raising checkpoint/0 does NOT trip the guard (fail-open per spec §14.15)" do
      # A checkpoint READ fault is fail-open (dup-safe by the §6 idempotency contract);
      # the guard refuses ONLY on a definitive {:ok, nil}, never on a fault.
      {:ok, cfg} = Config.validate(@base ++ [sink: RaisingCheckpoint])
      assert :ok = Config.guard(cfg)
    end

    test "REFUSES a sink with an INVALID sink_kind from an empty checkpoint (fail-closed, not fail-open)" do
      # An unrecognized sink_kind must be treated as the strict :state_mirror default,
      # so a typo cannot silently bypass the guard and partial-deliver.
      {:ok, cfg} = Config.validate(@base ++ [sink: InvalidKindEmpty])
      assert {:error, :go_forward_required} = Config.guard(cfg)
    end
  end

  describe "snapshot start mode (spec §7)" do
    test "accepts snapshot: true for a snapshot-capable sink and normalises the flag" do
      {:ok, cfg} = Config.validate(@base ++ [sink: SnapshotCapableEmpty, snapshot: true])
      assert cfg.snapshot == true
    end

    test "snapshot: true satisfies the go-forward guard for an empty state-mirror sink" do
      {:ok, cfg} = Config.validate(@base ++ [sink: SnapshotCapableEmpty, snapshot: true])
      assert :ok = Config.guard(cfg)
    end

    test "rejects go_forward_only + snapshot both true (conflicting intents)" do
      opts = @base ++ [sink: SnapshotCapableEmpty, snapshot: true, go_forward_only: true]
      assert {:error, :conflicting_start_mode} = Config.validate(opts)
    end

    test "rejects snapshot: true when the sink lacks the snapshot callbacks" do
      opts = @base ++ [sink: StateMirrorEmpty, snapshot: true]
      assert {:error, :snapshot_unsupported} = Config.validate(opts)
    end

    test "snapshot defaults to false and does not affect a normal config" do
      {:ok, cfg} = Config.validate(@base ++ [sink: StateMirrorPersisted])
      assert cfg.snapshot == false
    end
  end

  describe "checkpoint_store (lib mode)" do
    test "a valid :checkpoint_store is normalised onto the config" do
      opts = base_opts() ++ [checkpoint_store: [connection: [hostname: "db"], table: "cp"]]
      assert {:ok, cfg} = Config.validate(opts)

      assert Keyword.take(cfg.checkpoint_store, [:connection, :table]) ==
               [connection: [hostname: "db"], table: "cp"]

      assert Keyword.get(cfg.checkpoint_store, :max_retries) == 5
    end

    test "retry opts default to max_retries 5 / retry_backoff_ms 1000 when omitted" do
      {:ok, cfg} =
        Config.validate(base_opts() ++ [checkpoint_store: [connection: [hostname: "db"]]])

      assert Keyword.get(cfg.checkpoint_store, :max_retries) == 5
      assert Keyword.get(cfg.checkpoint_store, :retry_backoff_ms) == 1000
    end

    test "explicit retry opts are preserved; max_retries: 0 (halt-now) is allowed" do
      opts =
        base_opts() ++
          [
            checkpoint_store: [
              connection: [hostname: "db"],
              max_retries: 0,
              retry_backoff_ms: 250
            ]
          ]

      {:ok, cfg} = Config.validate(opts)
      assert Keyword.get(cfg.checkpoint_store, :max_retries) == 0
      assert Keyword.get(cfg.checkpoint_store, :retry_backoff_ms) == 250
    end

    test "a negative max_retries or non-positive backoff is a config error" do
      bad_n = base_opts() ++ [checkpoint_store: [connection: [hostname: "db"], max_retries: -1]]
      assert {:error, :config_invalid} = Config.validate(bad_n)

      bad_b =
        base_opts() ++ [checkpoint_store: [connection: [hostname: "db"], retry_backoff_ms: 0]]

      assert {:error, :config_invalid} = Config.validate(bad_b)

      bad_type =
        base_opts() ++ [checkpoint_store: [connection: [hostname: "db"], retry_backoff_ms: "100"]]

      assert {:error, :config_invalid} = Config.validate(bad_type)

      bad_float =
        base_opts() ++ [checkpoint_store: [connection: [hostname: "db"], max_retries: 1.5]]

      assert {:error, :config_invalid} = Config.validate(bad_float)
    end

    test "lib mode does NOT require the sink to implement checkpoint/0" do
      # CpStubSink implements ONLY handle_transaction/1 (a plain module, no @behaviour).
      opts =
        [
          connection: [hostname: "h"],
          slot_name: "s",
          publication: "p",
          sink: Replicant.Test.CpStubSink
        ] ++
          [checkpoint_store: [connection: [hostname: "db"]]]

      assert {:ok, cfg} = Config.validate(opts)
      assert cfg.sink == Replicant.Test.CpStubSink
    end

    test "sink-owned mode still requires checkpoint/0" do
      opts = [
        connection: [hostname: "h"],
        slot_name: "s",
        publication: "p",
        sink: Replicant.Test.CpStubSink
      ]

      assert {:error, :invalid_sink} = Config.validate(opts)
    end

    test "an invalid checkpoint_store table identifier is rejected" do
      opts = base_opts() ++ [checkpoint_store: [connection: [hostname: "db"], table: "Bad Name"]]
      assert {:error, :invalid_identifier} = Config.validate(opts)
    end

    test "guard defers in lib mode (empty-checkpoint enforcement moves to connect)" do
      {:ok, cfg} =
        Config.validate(base_opts() ++ [checkpoint_store: [connection: [hostname: "db"]]])

      assert Config.guard(cfg) == :ok
    end
  end
end
