defmodule Replicant.PackageChecksum do
  @moduledoc false

  def classify({:ok, {200, _headers, %{"checksum" => checksum}}}, checksum), do: :ok

  def classify({:ok, {200, _headers, %{"checksum" => actual}}}, expected)
      when is_binary(actual) do
    {:error, "published checksum #{actual} does not match uploaded artifact #{expected}"}
  end

  def classify({:ok, {200, _headers, _body}}, _expected),
    do: {:error, "published release response has no checksum"}

  def classify({:ok, {status, _headers, _body}}, _expected),
    do: {:error, "published release checksum read returned HTTP #{status}"}

  def classify({:error, _reason}, _expected),
    do: {:error, "published release checksum read failed"}

  def verify!(version, expected, key, attempts \\ 5) do
    result =
      1..attempts
      |> Enum.reduce_while(nil, fn attempt, _last ->
        response = apply(Hex.API.Release, :get, ["hexpm", "replicant", version, [key: key]])

        case classify(response, expected) do
          :ok ->
            {:halt, :ok}

          {:error, _} = error when attempt == attempts ->
            {:halt, error}

          {:error, _} = error ->
            Process.sleep(1_000)
            {:cont, error}
        end
      end)

    case result do
      :ok -> :ok
      {:error, message} -> raise message
    end
  end
end
