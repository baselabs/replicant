tools =
  Path.expand("../../.tool-versions", __DIR__)
  |> File.read!()
  |> String.split()
  |> Enum.chunk_every(2)
  |> Map.new(fn [name, version] -> {name, version} end)

[elixir_version, otp_major] = String.split(Map.fetch!(tools, "elixir"), "-otp-")

otp_version =
  Path.join([
    to_string(:code.root_dir()),
    "releases",
    to_string(:erlang.system_info(:otp_release)),
    "OTP_VERSION"
  ])
  |> File.read!()
  |> String.trim()

unless System.version() == elixir_version and otp_version == Map.fetch!(tools, "erlang") and
         to_string(:erlang.system_info(:otp_release)) == otp_major do
  raise "installed runtime differs from .tool-versions"
end
