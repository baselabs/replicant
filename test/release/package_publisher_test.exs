Code.require_file("../../scripts/release/package_publisher.exs", __DIR__)

defmodule Replicant.PackagePublisherTest do
  use ExUnit.Case, async: true

  alias Replicant.PackagePublisher

  defmodule ReleaseAPI do
    def publish(repository, bytes, auth, progress, replace?) do
      send(self(), {:publish, repository, bytes, auth, is_function(progress, 1), replace?})
      {:ok, {201, [], %{}}}
    end
  end

  defmodule Checksum do
    def verify!(version, digest, key) do
      send(self(), {:checksum, version, digest, key})
      :ok
    end
  end

  test "publishes the exact authorized bytes through the public Hex wrapper and verifies checksum" do
    bytes = "exact witnessed package bytes"
    digest = :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)

    env = fn
      "REPLICANT_PUBLISH_AUTHORIZED" -> "1.2.0:#{digest}"
      "HEX_API_KEY" -> "test-key"
    end

    assert {:ok, ^digest} =
             PackagePublisher.publish("1.2.0", bytes,
               env: env,
               release_api: ReleaseAPI,
               checksum: Checksum
             )

    assert_received {:publish, "hexpm", ^bytes, [key: "test-key"], true, false}
    assert_received {:checksum, "1.2.0", ^digest, "test-key"}
  end

  test "wrong authorization rejects before any publish call" do
    env = fn
      "REPLICANT_PUBLISH_AUTHORIZED" -> "1.2.0:wrong"
      "HEX_API_KEY" -> "test-key"
    end

    assert {:error, message} =
             PackagePublisher.publish("1.2.0", "bytes",
               env: env,
               release_api: ReleaseAPI,
               checksum: Checksum
             )

    assert message =~ "exact version:digest authorization"
    refute_received {:publish, _, _, _, _, _}
  end
end
