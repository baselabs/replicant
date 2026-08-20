Code.require_file("../../scripts/release/package_checksum.exs", __DIR__)

defmodule Replicant.PackageChecksumTest do
  use ExUnit.Case, async: true

  alias Replicant.PackageChecksum

  @digest String.duplicate("a", 64)

  test "accepts only the exact checksum returned for the published release" do
    assert :ok ==
             PackageChecksum.classify({:ok, {200, [], %{"checksum" => @digest}}}, @digest)

    assert {:error, message} =
             PackageChecksum.classify(
               {:ok, {200, [], %{"checksum" => String.duplicate("b", 64)}}},
               @digest
             )

    assert message =~ "does not match"
  end

  test "missing, malformed, or failed release reads fail closed" do
    assert {:error, _} = PackageChecksum.classify({:ok, {200, [], %{}}}, @digest)
    assert {:error, _} = PackageChecksum.classify({:ok, {503, [], %{}}}, @digest)
    assert {:error, _} = PackageChecksum.classify({:error, :timeout}, @digest)
  end
end
