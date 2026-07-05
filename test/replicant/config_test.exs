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
end
