defmodule Replicant.DependencyTest do
  use ExUnit.Case, async: true

  # Boot smoke: the streaming core is built on Postgrex.ReplicationConnection.
  # This proves the dep resolved AND the exact callback surface Task 7 depends on
  # is present (init/1 required; handle_connect/1, handle_data/2, handle_info/2,
  # handle_result/2 the behaviour we implement).
  test "Postgrex.ReplicationConnection is available with the callbacks Task 7 uses" do
    assert Code.ensure_loaded?(Postgrex.ReplicationConnection)
    behaviours = Postgrex.ReplicationConnection.behaviour_info(:callbacks)
    assert {:init, 1} in behaviours
    assert {:handle_connect, 1} in behaviours
    assert {:handle_data, 2} in behaviours
    assert {:handle_info, 2} in behaviours
    assert {:handle_result, 2} in behaviours
  end

  test "decimal 3.x still resolves alongside postgrex (the dep-floor guard)" do
    assert Code.ensure_loaded?(Decimal)
    # postgrex ~> 0.20 would force decimal ~> 1.5/2.0 and break this; ~> 0.22 accepts 3.x.
    assert Version.match?(Version.parse!(to_string(Application.spec(:decimal, :vsn))), "~> 3.0")
  end
end
