defmodule Replicant.PublishWorkflowTest do
  use ExUnit.Case, async: true

  @repo_root Path.expand("../..", __DIR__)
  @workflow Path.join(@repo_root, ".github/workflows/publish.yml")
  @tools Path.join(@repo_root, ".tool-versions")
  @qualification Path.join(@repo_root, ".github/workflows/qualification.yml")
  @checkout "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1"
  @beam "erlef/setup-beam@54075bcc5e249e4758d363f27d099f55d843f124"
  @artifact "actions/upload-artifact@cf430e030ddbb5b0abf93d22962f4752f3646cd9"
  @qualified_guards [
    {"workflow", ~S|"repos/$GH_REPO/actions/workflows/ci.yml/runs"|},
    {"pagination", "--paginate"},
    {"SHA query", ~S|-f head_sha="$SOURCE_SHA"|},
    {"push query", "-f event=push"},
    {"success query", "-f status=success"},
    {"run commit", ".head_sha == $sha"},
    {"landing branch", ~S|.head_branch == ("kci/" + $sha)|},
    {"push event", ~S|.event == "push"|},
    {"run success", ~S|.conclusion == "success"|},
    {"jobs API", ~S|"repos/$GH_REPO/actions/runs/$run_id/jobs?filter=latest&per_page=100"|},
    {"qualified name", ~S|select(.name == "qualified")|},
    {"one verdict", "length == 1"},
    {"qualified success", ~S|all(.[]; .conclusion == "success")|},
    {"verdict enforcement", ~S([[ "$qualified" == "true" ]] || continue)},
    {"fresh download directory", ~S|mktemp -d "$records/run-$run_id.XXXXXX"|},
    {"artifact download", ~S|gh run download "$run_id" -n package-authorization -D "$directory"|},
    {"record commit", ~S|"$commit" == "$SOURCE_SHA"|},
    {"record version", ~S|"$record_version" == "$version"|},
    {"record digest", ~S|"$digest" =~ ^[0-9a-f]{64}$|},
    {"three-line format", ~S|cmp -s "$record" <(printf 'commit: %s\nversion: %s\nsha256: %s\n'|},
    {"agreement", ~S([[ -z "$authorization" || "$authorization" == "$candidate" ]])},
    {"missing record", ~S([[ -n "$authorization" ]] || die)},
    {"step output", ~S|printf 'authorization=%s\n' "$authorization" >> "$GITHUB_OUTPUT"|},
    {"cleanup", ~S|trap 'rm -rf -- "$records"' EXIT|}
  ]
  @owners Enum.join(
            [
              ~S|contains(fromJSON('["corepay","palermo-git"]'), github.actor)|,
              ~S|contains(fromJSON('["corepay","palermo-git"]'), github.triggering_actor)|
            ],
            " && "
          )
  @authorization ~S"""
  version="$(grep -oE '@version "[^"]+"' mix.exs | head -1 | sed -E 's/@version "([^"]+)"/\1/')"
  receipt=".kimosabe/artifacts/replicant-$version-receipt.txt"
  digest="$(sed -n 's/^sha256: //p' "$receipt")"
  [[ "$digest" =~ ^[0-9a-f]{64}$ ]]
  [[ "$REPLICANT_PUBLISH_AUTHORIZED" == "$version:$digest" ]] || {
    echo "::error::publish requires exact version:digest authorization from the qualified run"
    exit 1
  }
  """
  @upload """
  mix run --no-start scripts/release/upload_candidate.exs --publish
  mix run --no-start scripts/release/upload_docs.exs --publish
  """

  test "all workflows enforce the exact toolchain, hosted runners, and no caches" do
    paths = Path.wildcard(Path.join(@repo_root, ".github/workflows/*.{yml,yaml}"))
    assert paths != []

    for path <- paths do
      assert_workflow!(read_yaml!(path), File.read!(@tools))
    end
  end

  test "publish is owner-dispatched with one environment secret and guarded exact-byte upload" do
    assert_publish!(read_yaml!(@workflow))
  end

  # Each mutation changes a parsed copy of the real workflow. Every refusal runs
  # the same assertion as the positive contract, so the negative cases cannot
  # pass merely because an unrelated check already fails on the original.
  for {name, path, value} <- [
        {"automatic trigger", ["on", "push"], %{}},
        {"missing dispatch", ["on"], %{}},
        {"dispatch input", ["on", "workflow_dispatch"],
         %{"inputs" => %{"authorization" => %{"required" => true, "type" => "string"}}}},
        {"wrong environment", ["jobs", "publish", "environment"], "production"},
        {"missing owner guard", ["jobs", "publish", "if"], nil},
        {"unguarded triggering actor", ["jobs", "publish", "if"], "github.actor == 'corepay'"},
        {"write permission", ["permissions", "contents"], "write"},
        {"job write permission", ["jobs", "publish", "permissions", "contents"], "write"},
        {"missing Actions permission", ["jobs", "publish", "permissions"],
         %{"contents" => "read"}},
        {"Actions write permission", ["jobs", "publish", "permissions", "actions"], "write"},
        {"extra job permission", ["jobs", "publish", "permissions", "packages"], "read"},
        {"extra workflow permission", ["permissions", "actions"], "read"},
        {"persisted key", ["jobs", "publish", "steps", 1, "with", "persist-credentials"], true},
        {"unpinned checkout", ["jobs", "publish", "steps", 1, "uses"], "actions/checkout@main"},
        {"unpinned source", ["jobs", "publish", "steps", 1, "with", "ref"], "main"},
        {"branch dispatch", ["jobs", "publish", "steps", 0, "run"], "true"},
        {"missing read step", ["jobs", "publish", "steps", 6, "run"], "true"},
        {"wrong read id", ["jobs", "publish", "steps", 6, "id"], "other"},
        {"wrong read token", ["jobs", "publish", "steps", 6, "env", "GH_TOKEN"], "ignored"},
        {"wrong read commit", ["jobs", "publish", "steps", 6, "env", "SOURCE_SHA"], "main"},
        {"wrong read repository", ["jobs", "publish", "steps", 6, "env", "GH_REPO"],
         "other/repo"},
        {"check instead of mint", ["jobs", "publish", "steps", 7, "run"],
         "bash scripts/release/build_candidate.sh --check"},
        {"missing authorization check", ["jobs", "publish", "steps", 8, "run"], "true"},
        {"wrong comparison source",
         ["jobs", "publish", "steps", 8, "env", "REPLICANT_PUBLISH_AUTHORIZED"],
         "ignored"},
        {"wrong upload authorization", ["jobs", "publish", "steps", 9, "env"],
         %{
           "HEX_API_KEY" => "${{ secrets.HEX_API_KEY }}",
           "REPLICANT_PUBLISH_AUTHORIZED" => "other"
         }},
        {"rebuilding upload", ["jobs", "publish", "steps", 9, "run"], "mix hex.publish --yes"},
        {"missing docs upload", ["jobs", "publish", "steps", 9, "run"],
         "mix run --no-start scripts/release/upload_candidate.exs --publish"},
        {"wrong secret", ["jobs", "publish", "steps", 9, "env", "HEX_API_KEY"],
         "${{ secrets.OTHER_KEY }}"},
        {"job credential", ["jobs", "publish", "env"],
         %{"HEX_API_KEY" => "${{ secrets.HEX_API_KEY }}"}},
        {"step credential leak", ["jobs", "publish", "steps", 7, "env"],
         %{"HEX_API_KEY" => "${{ secrets.HEX_API_KEY }}"}}
      ] do
    test "publish contract refuses #{name}" do
      original = read_yaml!(@workflow)
      assert_publish!(original)
      mutated = replace(original, unquote(path), unquote(Macro.escape(value)))
      assert_raise ExUnit.AssertionError, fn -> assert_publish!(mutated) end
    end
  end

  for {name, path, value} <- [
        {"Elixir drift", ["env", "ELIXIR_VERSION"], "1.20.2"},
        {"OTP drift", ["env", "OTP_VERSION"], "29"},
        {"job runtime override", ["jobs", "publish", "env"], %{"OTP_VERSION" => "28.0.0"}},
        {"step runtime override", ["jobs", "publish", "steps", 2, "env"],
         %{"ELIXIR_VERSION" => "1.20.2"}},
        {"missing strict version", ["jobs", "publish", "steps", 2, "with", "version-type"], nil},
        {"loose version", ["jobs", "publish", "steps", 2, "with", "version-type"], "loose"},
        {"Elixir override", ["jobs", "publish", "steps", 2, "with", "elixir-version"], "1.20.2"},
        {"literal OTP override", ["jobs", "publish", "steps", 2, "with", "otp-version"], "29"},
        {"moving runner", ["jobs", "publish", "runs-on"], "ubuntu-latest"},
        {"self-hosted runner", ["jobs", "publish", "runs-on"], ["self-hosted", "linux"]},
        {"build-box runner", ["jobs", "publish", "runs-on"], "devbox-ovh"},
        {"cache action", ["jobs", "publish", "steps", 3, "uses"], "actions/cache@v4"}
      ] do
    test "every workflow contract refuses #{name}" do
      original = read_yaml!(@workflow)
      tools = File.read!(@tools)
      assert_workflow!(original, tools)
      mutated = replace(original, unquote(path), unquote(Macro.escape(value)))
      assert_raise ExUnit.AssertionError, fn -> assert_workflow!(mutated, tools) end
    end
  end

  test "the Elixir OTP suffix must match the Erlang major" do
    workflow = read_yaml!(@workflow)
    tools = File.read!(@tools)
    assert_workflow!(workflow, tools)
    mutated = String.replace(tools, ~r/-otp-\d+/, "-otp-28")
    refute mutated == tools
    assert_raise ExUnit.AssertionError, fn -> assert_workflow!(workflow, mutated) end
  end

  for {name, first, second} <- [
        {"read after mint", 6, 7},
        {"read after upload", 6, 9},
        {"upload before comparison", 8, 9}
      ] do
    test "publish contract refuses #{name}" do
      original = read_yaml!(@workflow)
      assert_publish!(original)

      mutated =
        update_in(original, ["jobs", "publish", "steps"], fn steps ->
          List.replace_at(steps, unquote(first), Enum.at(steps, unquote(second)))
          |> List.replace_at(unquote(second), Enum.at(steps, unquote(first)))
        end)

      assert_raise ExUnit.AssertionError, fn -> assert_publish!(mutated) end
    end
  end

  for {name, guard} <- @qualified_guards do
    test "qualification read refuses missing #{name}" do
      original = read_yaml!(@workflow)
      assert_publish!(original)
      path = ["jobs", "publish", "steps", Access.at(6), "run"]
      script = get_in(original, path)
      mutated = put_in(original, path, String.replace(script, unquote(guard), ""))
      refute mutated == original
      assert_raise ExUnit.AssertionError, fn -> assert_publish!(mutated) end
    end
  end

  test "qualification records the package authorization after a successful check build" do
    assert_qualification!(read_yaml!(@qualification))
  end

  for {name, path, value} <- [
        {"missing output option", [8, "run"],
         "BUILD_DATE=ci bash scripts/release/build_candidate.sh --check"},
        {"wrong artifact", [9, "with", "name"], "other"},
        {"wrong record path", [9, "with", "path"], "other.txt"},
        {"ignored missing record", [9, "with", "if-no-files-found"], "ignore"},
        {"unpinned artifact upload", [9, "uses"], "actions/upload-artifact@main"}
      ] do
    test "qualification contract refuses #{name}" do
      original = read_yaml!(@qualification)
      assert_qualification!(original)
      path = ["jobs", "release-artifact", "steps"] ++ unquote(path)
      mutated = replace(original, path, unquote(value))
      assert_raise ExUnit.AssertionError, fn -> assert_qualification!(mutated) end
    end
  end

  test "qualification cannot upload the record before the build" do
    original = read_yaml!(@qualification)
    assert_qualification!(original)

    mutated =
      update_in(original, ["jobs", "release-artifact", "steps"], fn steps ->
        List.replace_at(steps, 8, Enum.at(steps, 9))
        |> List.replace_at(9, Enum.at(steps, 8))
      end)

    assert_raise ExUnit.AssertionError, fn -> assert_qualification!(mutated) end
  end

  test "qualification and publish install the same pinned Hex for reproducible bytes" do
    publish = read_yaml!(@workflow)
    qualification = read_yaml!(@qualification)
    assert_hex!(publish, qualification)

    mutated = put_in(publish, ["env", "HEX_VERSION"], "0.0.0")
    assert_raise ExUnit.AssertionError, fn -> assert_hex!(mutated, qualification) end

    mutated = replace(publish, ["jobs", "publish", "steps", 3, "run"], "mix local.hex --force")
    assert_raise ExUnit.AssertionError, fn -> assert_hex!(mutated, qualification) end
  end

  test "authorization shell accepts only the minted version and digest without uploads" do
    root = Path.join(System.tmp_dir!(), "replicant-publish-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(root) end)
    artifacts = Path.join(root, ".kimosabe/artifacts")
    File.mkdir_p!(artifacts)
    File.write!(Path.join(root, "mix.exs"), ~s(@version "1.2.0"\n))
    receipt = Path.join(artifacts, "replicant-1.2.0-receipt.txt")
    digest = String.duplicate("a", 64)
    File.write!(receipt, "sha256: #{digest}\n")
    script = read_yaml!(@workflow) |> get_in(["jobs", "publish", "steps"]) |> Enum.at(8)

    for {authorization, expected_status} <- [
          {"1.2.0:#{digest}", 0},
          {"", 1},
          {"1.2.1:#{digest}", 1},
          {"1.2.0:#{String.duplicate("b", 64)}", 1},
          {"1.2.0:wrong", 1},
          {"$(exit 99)", 1}
        ] do
      {_output, status} = authorization_run(root, script["run"], authorization)
      assert status == expected_status
    end

    for invalid_digest <- ["wrong", "#{digest}\nsha256: #{digest}"] do
      File.write!(receipt, "sha256: #{invalid_digest}\n")
      {_output, status} = authorization_run(root, script["run"], "1.2.0:#{digest}")
      assert status == 1
    end
  end

  test "record validation rejects every deviation from the qualified commit version and bytes" do
    root = Path.join(System.tmp_dir!(), "replicant-read-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(root) end)
    File.mkdir_p!(root)
    record = Path.join(root, "package-authorization.txt")
    commit = String.duplicate("a", 40)
    digest = String.duplicate("b", 64)
    content = "commit: #{commit}\nversion: 1.2.0\nsha256: #{digest}\n"
    read = read_yaml!(@workflow) |> get_in(["jobs", "publish", "steps"]) |> Enum.at(6)
    [die | _rest] = String.split(read["run"], "\n")
    start = ~s(commit="$(sed -n 's/^commit: //p' "$record")")
    [_, after_start] = String.split(read["run"], start, parts: 2)
    [validation, _rest] = String.split(after_start, ~s(authorization="$candidate"), parts: 2)
    script = die <> "\n" <> start <> validation

    for {bytes, authorization, expected_status} <- [
          {content, "", 0},
          {content, "1.2.0:#{digest}", 0},
          {content, "1.2.0:#{String.duplicate("c", 64)}", 1},
          {String.replace(content, commit, String.duplicate("c", 40)), "", 1},
          {String.replace(content, "1.2.0", "1.2.1"), "", 1},
          {String.replace(content, digest, String.upcase(digest)), "", 1},
          {String.replace(content, digest, "short"), "", 1},
          {String.replace(content, "commit:", "source_commit:"), "", 1},
          {"version: 1.2.0\ncommit: #{commit}\nsha256: #{digest}\n", "", 1},
          {content <> "sha256: #{digest}\n", "", 1},
          {content <> "extra: value\n", "", 1},
          {content <> "\n", "", 1},
          {String.trim_trailing(content), "", 1},
          {String.replace(content, "\n", "\r\n"), "", 1},
          {String.replace(content, digest, "$(exit 99)"), "", 1},
          {"", "", 1}
        ] do
      File.write!(record, bytes)

      {_output, status} =
        System.cmd("bash", ["--noprofile", "--norc", "-euo", "pipefail", "-c", script],
          env: [
            {"record", record},
            {"SOURCE_SHA", commit},
            {"version", "1.2.0"},
            {"authorization", authorization}
          ],
          stderr_to_stdout: true
        )

      assert status == expected_status
    end
  end

  test "publish mode still rejects path and witness overrides" do
    for option <- ["--artifact", "--receipt", "--witness-ref"] do
      {output, status} =
        System.cmd(
          "mix",
          [
            "run",
            "--no-start",
            "scripts/release/upload_candidate.exs",
            "--publish",
            option,
            "other"
          ],
          cd: @repo_root,
          stderr_to_stdout: true
        )

      assert status == 1
      assert output =~ "publish mode does not accept path or witness overrides"
    end
  end

  test "the workstation wrappers and credential loader are retired" do
    for name <- ["publish_candidate.sh", "publish_docs.sh", "credential_loader.sh"] do
      refute File.exists?(Path.join([@repo_root, "scripts", "release", name]))
    end
  end

  defp assert_workflow!(workflow, tools) do
    [_, elixir, suffix] = Regex.run(~r/^elixir (\d+\.\d+\.\d+)-otp-(\d+)$/m, tools)
    [_, otp, major] = Regex.run(~r/^erlang ((\d+)\.\d+\.\d+)$/m, tools)
    assert suffix == major
    maps = maps(workflow)

    for map <- maps, {key, expected} <- [{"ELIXIR_VERSION", elixir}, {"OTP_VERSION", otp}] do
      if Map.has_key?(map, key), do: assert(map[key] == expected)
    end

    for {_name, job} <- workflow["jobs"], not Map.has_key?(job, "uses") do
      assert job["runs-on"] == "ubuntu-24.04"
    end

    for map <- maps do
      refute to_string(map["uses"] || "") =~ ~r{actions/cache(?:/|@)}i

      if String.starts_with?(map["uses"] || "", "actions/checkout@") do
        assert map["uses"] == @checkout
      end

      if String.starts_with?(map["uses"] || "", "erlef/setup-beam@") do
        assert map["uses"] == @beam
        assert workflow["env"]["ELIXIR_VERSION"] == elixir
        assert workflow["env"]["OTP_VERSION"] == otp
        assert map["with"]["elixir-version"] == "${{ env.ELIXIR_VERSION }}"
        assert map["with"]["otp-version"] == "${{ env.OTP_VERSION }}"
        assert map["with"]["version-type"] == "strict"
      end
    end
  end

  defp assert_publish!(workflow) do
    assert_workflow!(workflow, File.read!(@tools))
    assert workflow["on"] == %{"workflow_dispatch" => nil}
    refute Enum.any?(maps(workflow), &Map.has_key?(&1, "inputs"))
    refute Jason.encode!(workflow) =~ "inputs."

    for map <- maps(workflow), is_binary(map["run"]) do
      refute map["run"] =~ "${{"
    end

    assert workflow["permissions"] == %{"contents" => "read"}
    assert Map.keys(workflow["jobs"]) == ["publish"]
    job = workflow["jobs"]["publish"]
    assert job["if"] == @owners
    assert job["environment"] == "publish"
    assert job["permissions"] == %{"contents" => "read", "actions" => "read"}
    assert job["timeout-minutes"] == 15
    steps = job["steps"]
    assert length(steps) == 10
    assert Enum.at(steps, 0)["run"] == ~s(test "$GITHUB_REF" = refs/heads/main)
    checkout = Enum.at(steps, 1)
    assert checkout["uses"] == @checkout
    assert checkout["with"]["persist-credentials"] == false
    assert checkout["with"]["ref"] == "${{ github.sha }}"
    assert checkout["with"]["fetch-depth"] == 0
    assert Enum.at(steps, 2)["uses"] == @beam

    assert Enum.at(steps, 4)["run"] == "mix local.rebar --force"
    assert Enum.at(steps, 5)["run"] == "mix deps.get --check-locked"
    read = Enum.at(steps, 6)
    assert read["id"] == "qualified"

    assert read["env"] == %{
             "GH_TOKEN" => "${{ github.token }}",
             "GH_REPO" => "${{ github.repository }}",
             "SOURCE_SHA" => "${{ github.sha }}"
           }

    for {_name, guard} <- @qualified_guards, do: assert(read["run"] =~ guard)
    assert Enum.at(steps, 7)["run"] == "bash scripts/release/build_candidate.sh"
    assert Enum.at(steps, 8)["run"] == @authorization

    assert Enum.at(steps, 8)["env"] == %{
             "REPLICANT_PUBLISH_AUTHORIZED" => "${{ steps.qualified.outputs.authorization }}"
           }

    assert Enum.at(steps, 9)["run"] == @upload

    assert Enum.at(steps, 9)["env"] == %{
             "HEX_API_KEY" => "${{ secrets.HEX_API_KEY }}",
             "REPLICANT_PUBLISH_AUTHORIZED" => "${{ steps.qualified.outputs.authorization }}"
           }

    without_secret =
      update_in(workflow, ["jobs", "publish", "steps", Access.at(9), "env"], fn env ->
        Map.delete(env, "HEX_API_KEY")
      end)

    refute Jason.encode!(without_secret) =~ "HEX_API_KEY"
  end

  defp assert_qualification!(workflow) do
    steps = workflow["jobs"]["release-artifact"]["steps"]
    assert length(steps) == 10

    assert Enum.at(steps, 8)["run"] ==
             "BUILD_DATE=ci bash scripts/release/build_candidate.sh --check " <>
               ~s(--authorization-out "$RUNNER_TEMP/package-authorization.txt")

    upload = Enum.at(steps, 9)
    assert upload["uses"] == @artifact

    assert upload["with"] == %{
             "name" => "package-authorization",
             "path" => "${{ runner.temp }}/package-authorization.txt",
             "if-no-files-found" => "error"
           }
  end

  defp assert_hex!(publish, qualification) do
    version = qualification["env"]["HEX_VERSION"]
    assert version =~ ~r/^\d+\.\d+\.\d+$/
    assert publish["env"]["HEX_VERSION"] == version

    for job <- [publish["jobs"]["publish"], qualification["jobs"]["release-artifact"]] do
      assert Enum.any?(job["steps"], &(&1["run"] == ~s(mix local.hex "$HEX_VERSION" --force)))
    end
  end

  defp maps(map) when is_map(map), do: [map | Enum.flat_map(Map.values(map), &maps/1)]
  defp maps(list) when is_list(list), do: Enum.flat_map(list, &maps/1)
  defp maps(_value), do: []

  defp replace(workflow, path, value) do
    access =
      Enum.map(path, fn key ->
        if is_integer(key), do: Access.at(key), else: Access.key(key)
      end)

    put_in(workflow, access, value)
  end

  defp authorization_run(root, script, authorization) do
    System.cmd("bash", ["--noprofile", "--norc", "-euo", "pipefail", "-c", script],
      cd: root,
      env: [{"REPLICANT_PUBLISH_AUTHORIZED", authorization}],
      stderr_to_stdout: true
    )
  end

  # Ruby's standard YAML parser is present on the hosted runner. Use a real
  # parser without adding a YAML runtime dependency to this Elixir library.
  defp read_yaml!(path) do
    parser = """
    data = YAML.safe_load(File.read(ARGV.fetch(0)), aliases: true)
    data['on'] = data.delete(true) if data.key?(true)
    puts JSON.generate(data)
    """

    {json, 0} = System.cmd("ruby", ["-ryaml", "-rjson", "-e", parser, path])
    Jason.decode!(json)
  end
end
