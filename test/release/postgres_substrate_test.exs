defmodule Replicant.PostgresSubstrateTest do
  use ExUnit.Case, async: true

  @root Path.expand("../..", __DIR__)

  test "the reader derives engine and compatibility coverage from the declarations" do
    with_reader(fn lanes, reference, paths ->
      {output, 0, result} = read_lanes(lanes, reference, paths)
      assert output == ""
      matrix = Jason.decode!(result["matrix"])["pg"]
      [engine | compatibility] = matrix
      assert engine["kind"] == "engine"
      assert engine["major"] == result["engine-major"]
      refute Enum.any?(lanes, &(&1["major"] == engine["major"]))

      for lane <- lanes, role <- ["pgoutput", "decoders"], role in lane["roles"] do
        assert Enum.count(compatibility, fn cell ->
                 cell["major"] == lane["major"] and cell["role"] == role and
                   cell["image"] == lane["image"]
               end) == 1
      end

      source = Enum.find(lanes, &("legacy-source" in &1["roles"]))
      parity = Enum.filter(compatibility, &Map.has_key?(&1, "sidecar"))
      assert parity != []
      assert Enum.all?(parity, &(&1["sidecar"]["image"] == source["image"]))

      count =
        Enum.sum(
          Enum.map(lanes, fn lane ->
            Enum.count(lane["roles"], &(&1 in ["pgoutput", "decoders"]))
          end)
        )

      assert length(compatibility) == count
    end)
  end

  test "invalid declarations refuse without publishing partial outputs" do
    with_reader(fn lanes, reference, paths ->
      [first | rest] = lanes
      [_, engine_major] = Regex.run(~r/postgres:(\d+)\./, reference)

      mutations = [
        %{},
        [],
        lanes ++ [first],
        [Map.put(first, "unknown", true) | rest],
        [Map.delete(first, "image") | rest],
        [Map.put(first, "roles", ["unknown"]) | rest],
        [Map.put(first, "roles", ["decoders", "decoders"]) | rest],
        [Map.put(first, "roles", ["legacy-source"]) | rest],
        [Map.put(first, "major", engine_major) | rest],
        [Map.put(first, "image", first["image"] <> "bad") | rest],
        [
          Map.put(first, "image", String.replace(first["image"], "postgres:", "other:"))
          | rest
        ],
        [Map.put(first, "major", "999") | rest],
        [Map.put(first, "settings", ["arbitrary=setting"]) | rest],
        Enum.map(lanes, fn lane ->
          Map.update!(lane, "roles", &List.delete(&1, "legacy-source"))
        end)
      ]

      for mutation <- mutations do
        {_output, status, result} = read_lanes(mutation, reference, paths)
        assert status != 0
        assert result == %{}
      end

      for bad_ref <- [String.trim(reference), reference <> reference, "invalid\n"] do
        {_output, status, result} = read_lanes(lanes, bad_ref, paths)
        assert status != 0
        assert result == %{}
      end
    end)
  end

  test "workflows use one substrate action, declared matrices, and unconditional cleanup" do
    qualification = yaml(".github/workflows/qualification.yml")
    test_job = qualification["jobs"]["test"]
    assert test_job["needs"] == "toolchain"
    assert test_job["strategy"]["matrix"] =~ "fromJSON(needs.toolchain.outputs.matrix)"
    assert test_job["env"]["EXPECTED_PG_MAJOR"] == "${{ matrix.pg.expected_major }}"

    for name <- ["test", "reference-example"] do
      steps = qualification["jobs"][name]["steps"]
      action_steps = Enum.filter(steps, &(&1["uses"] == "./.github/actions/postgres"))
      assert length(action_steps) == 2
      stop = List.last(action_steps)
      assert stop["if"] == "always()"
      assert stop["with"]["mode"] == "stop"
    end

    for path <- Path.wildcard(Path.join(@root, ".github/workflows/*.yml")) do
      source = File.read!(path)
      refute source =~ ~r/postgres:[0-9]/
      refute source =~ ~r/ghcr\.io\/baselabs\/postgres:[0-9]/
      refute source =~ ~r/docker\s+(run|start|stop|rm|pull)\b/
      refute source =~ ~r/docker compose (up|down)\b/
      refute source =~ "POSTGRES_HOST_AUTH_METHOD"
      refute source =~ ~r/matrix\.pg\.major\s*==/
    end

    action = yaml(".github/actions/postgres/action.yml")
    [check, manage] = action["runs"]["steps"]
    assert check["name"] == "Check the public platform engine and pull its digest"
    assert check["run"] =~ ~S|engine="$(cat .github/postgres.ref)"|
    assert check["run"] =~ ~S|if [ "${engine##*@}" != "$current_digest" ]|
    assert check["run"] =~ ~s(docker pull "$engine")
    refute check["run"] =~ "docker pull ghcr"
    assert manage["if"] == "always() && (inputs.mode == 'stop' || success())"

    script = File.read!(Path.join(@root, "scripts/ci/postgres.sh"))
    assert script =~ "--entrypoint baselabs-test-postgres"
    assert script =~ "--tmpfs /var/lib/pg:uid=26,gid=26,mode=0750 --tmpfs /tmp"
    assert script =~ "pg_isready -h 127.0.0.1"
    assert script =~ "POSTGRES_INITDB_ARGS=\"--auth-host=$auth\""
    assert script =~ "docker rm -f -v"
    refute script =~ ~r/host replication.*trust/
  end

  test "the example uses disposable engine scratch and password connections" do
    compose = yaml("examples/replication_pipeline/docker-compose.yml")

    for name <- ["source-pg", "dest-pg"] do
      server = compose["services"][name]
      assert server["image"] =~ "${REPLICANT_POSTGRES_IMAGE:?"
      assert server["entrypoint"] == "baselabs-test-postgres"
      assert "/var/lib/pg:uid=26,gid=26,mode=0750" in server["tmpfs"]
      assert "/tmp" in server["tmpfs"]
      database = server["environment"]["TEST_PG_DATABASE"]
      assert is_binary(database) and database not in ["", "postgres"]
      refute Map.has_key?(server["environment"], "POSTGRES_HOST_AUTH_METHOD")
    end

    pipeline = compose["services"]["pipeline"]["environment"]
    assert is_binary(pipeline["SOURCE_PASSWORD"]) and pipeline["SOURCE_PASSWORD"] != ""
    assert is_binary(pipeline["DEST_PASSWORD"]) and pipeline["DEST_PASSWORD"] != ""
  end

  defp yaml(path) do
    {json, 0} =
      System.cmd("ruby", [
        "-ryaml",
        "-rjson",
        "-e",
        "puts JSON.generate(YAML.load_file(ARGV.fetch(0)))",
        Path.join(@root, path)
      ])

    Jason.decode!(json)
  end

  defp with_reader(fun) do
    dir = Path.join(System.tmp_dir!(), "replicant-lanes-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)

    try do
      lanes = Jason.decode!(File.read!(Path.join(@root, ".github/postgres-lanes.json")))
      reference = File.read!(Path.join(@root, ".github/postgres.ref"))
      fun.(lanes, reference, dir)
    after
      File.rm_rf!(dir)
    end
  end

  defp read_lanes(lanes, reference, dir) do
    input = Path.join(dir, "lanes.json")
    ref = Path.join(dir, "postgres.ref")
    output = Path.join(dir, "output")
    File.write!(input, Jason.encode!(lanes))
    File.write!(ref, reference)
    File.write!(output, "")

    {text, status} =
      System.cmd(Path.join(@root, "scripts/ci/postgres-lanes.sh"), [input, ref],
        env: [{"GITHUB_OUTPUT", output}],
        stderr_to_stdout: true
      )

    result =
      output
      |> File.read!()
      |> String.split("\n", trim: true)
      |> Map.new(fn line ->
        [key, value] = String.split(line, "=", parts: 2)
        {key, value}
      end)

    {text, status, result}
  end
end
