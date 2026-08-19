# Unpack an EXISTING Hex tarball with Hex checksum validation — NOT a rebuild.
#
# `mix hex.build --unpack` rebuilds the package in memory and unpacks THAT, so it cannot inspect a
# retained artifact. `:mix_hex_tarball.unpack/2` reads the exact tar bytes, recomputes the inner
# checksum, and compares it to the tar's CHECKSUM member — a tampered payload fails closed here.
#
# Usage: mix run scripts/release/unpack_validated.exs <tarball> <dest-dir>
Mix.ensure_application!(:hex)

[tar, dest] = System.argv()
bin = File.read!(tar)
File.rm_rf!(dest)
File.mkdir_p!(dest)

case :mix_hex_tarball.unpack(bin, String.to_charlist(dest)) do
  {:ok, meta} ->
    checksum = meta |> Map.get(:outer_checksum, "") |> Base.encode16(case: :lower)
    IO.puts("unpack_validated: OK — Hex checksum validated, extracted to #{dest} (outer_checksum #{checksum})")

  {:error, reason} ->
    IO.puts(:stderr, "::error::unpack_validated: Hex checksum validation FAILED: #{inspect(reason)}")
    System.halt(1)
end
