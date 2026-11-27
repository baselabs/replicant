defmodule Replicant.PublishWorkflowTest do
  use ExUnit.Case, async: true

  @repo_root Path.expand("../..", __DIR__)
  @workflow Path.join(@repo_root, ".github/workflows/publish.yml")
  @reader Path.join(@repo_root, "scripts/toolchain.sh")
  @toolchain_action Path.join(@repo_root, ".github/actions/toolchain/action.yml")
  @beam_action Path.join(@repo_root, ".github/actions/beam/action.yml")
  @toolchain "./.github/actions/toolchain"
  @beam_action_ref "./.github/actions/beam"
  @reader_sha256 "25bbfa023b7f031256428b498fe300fcc460988fea81277072355e4c0d933764"
  @installed ~S|"$GITHUB_WORKSPACE/scripts/toolchain.sh" --installed "elixir=$WANT_ELIXIR" "erlang=$WANT_OTP"|
  @qualification Path.join(@repo_root, ".github/workflows/qualification.yml")
  @checkout "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1"
  @beam "erlef/setup-beam@54075bcc5e249e4758d363f27d099f55d843f124"
  @artifact "actions/upload-artifact@cf430e030ddbb5b0abf93d22962f4752f3646cd9"
  @hex_pin "654fcd0ef63daf8e3f79dfeb4ca900adffd0d4e2"
  @hex_action "baselabs/.github/.github/actions/hex-publish@#{@hex_pin}"
  @hex_version_url "https://raw.githubusercontent.com/baselabs/.github/#{@hex_pin}/.github/actions/hex-publish/hex-version"
  @example_image ~r/\Ahexpm\/elixir:(\d+\.\d+\.\d+)-erlang-(\d+\.\d+\.\d+)-\S+@sha256:[0-9a-f]{64}\z/
  @owners Enum.join(
            [
              ~S|contains(fromJSON('["corepay","palermo-git"]'), github.actor)|,
              ~S|contains(fromJSON('["corepay","palermo-git"]'), github.triggering_actor)|
            ],
            " && "
          )
  @release_commit "${{ inputs.release_commit }}"
  @workflow_revision "${{ github.workflow_sha }}"
  @runtime_file "${{ runner.temp }}/replicant-release-versions"
  @release_assets "${{ runner.temp }}/release-assets"

  test "all workflows enforce the exact toolchain, hosted runners, and no caches" do
    paths = Path.wildcard(Path.join(@repo_root, ".github/workflows/*.{yml,yaml}"))
    assert paths != []

    for path <- paths do
      assert_workflow!(read_yaml!(path))
    end
  end

  test "test output pipelines cannot turn failed tests into a successful integration job" do
    original = read_yaml!(@qualification)
    assert_workflow!(original)

    pipelines =
      for {name, job} <- original["jobs"],
          {step, index} <- Enum.with_index(job["steps"]),
          (step["run"] || "") =~ ~r/\|\s*tee\b/,
          do: {name, index}

    assert pipelines != []

    for {name, index} <- pipelines,
        shell <- ["sh", "bash {0}"] do
      mutated = replace(original, ["jobs", name, "steps", index, "shell"], shell)
      assert_raise ExUnit.AssertionError, fn -> assert_workflow!(mutated) end
    end
  end

  test "every workflow runs its steps in bash with pipefail" do
    for path <- Path.wildcard(Path.join(@repo_root, ".github/workflows/*.{yml,yaml}")) do
      original = read_yaml!(path)
      assert_bash_shell!(original)

      for defaults <- [nil, %{"run" => %{"shell" => "sh"}}, %{"run" => %{}}] do
        mutated = Map.put(original, "defaults", defaults)
        assert_raise ExUnit.AssertionError, fn -> assert_bash_shell!(mutated) end
      end

      for {name, job} <- original["jobs"], Map.has_key?(job, "steps") do
        mutated =
          replace(original, ["jobs", name, "defaults", "run"], %{"working-directory" => "."})

        assert_raise ExUnit.AssertionError, fn -> assert_bash_shell!(mutated) end
      end

      for {name, job} <- original["jobs"],
          {step, index} <- Enum.with_index(job["steps"] || []),
          Map.has_key?(step, "run"),
          shell <- ["sh", "bash {0}", "pwsh"] do
        mutated = replace(original, ["jobs", name, "steps", index, "shell"], shell)
        assert_raise ExUnit.AssertionError, fn -> assert_bash_shell!(mutated) end
      end
    end
  end

  test "release scripts are bash with pipefail and never hide a failing command" do
    scripts = Path.wildcard(Path.join(@repo_root, "scripts/**/*.sh"))
    assert scripts != []
    workflows = Path.wildcard(Path.join(@repo_root, ".github/workflows/*.{yml,yaml}"))

    for path <- scripts do
      source = File.read!(path)
      assert_strict_script!(source)

      for {before, after_change} <- [
            {"#!/usr/bin/env bash", "#!/bin/sh"},
            {"set -euo pipefail", "set -eu"}
          ] do
        assert_raise ExUnit.AssertionError, fn ->
          assert_strict_script!(String.replace(source, before, after_change))
        end
      end
    end

    for path <- scripts ++ workflows do
      source = File.read!(path)
      assert_no_lost_status!(source)

      for line <- [
            "  mix test || true",
            ~S|  diff -u expected <(find . -type f)|,
            ~S{  [[ -z "$(git status --porcelain)" ]] || exit 1},
            ~S|  if [ -n "$(find . -type l)" ]; then exit 1; fi|
          ] do
        mutated = source <> "\n" <> line <> "\n"
        assert_raise ExUnit.AssertionError, fn -> assert_no_lost_status!(mutated) end
      end
    end
  end

  test "GitHub caller steps alone receive the job token and require read permission" do
    for path <- [@workflow, @qualification] do
      original = read_yaml!(path)
      assert_github_auth!(original)

      for {name, job} <- original["jobs"] do
        for scope <- [["env", "GH_TOKEN"], ["jobs", name, "env", "GH_TOKEN"]] do
          mutated = replace(original, scope, "${{ github.token }}")
          assert_raise ExUnit.AssertionError, fn -> assert_github_auth!(mutated) end
        end

        for {step, index} <- Enum.with_index(job["steps"]) do
          scope = ["jobs", name, "steps", index, "env", "GH_TOKEN"]
          value = if github_read?(step), do: nil, else: "${{ github.token }}"
          mutated = replace(original, scope, value)
          assert_raise ExUnit.AssertionError, fn -> assert_github_auth!(mutated) end

          if github_read?(step) do
            for value <- ["ignored", "${{ secrets.OTHER_KEY }}"] do
              mutated = replace(original, scope, value)
              assert_raise ExUnit.AssertionError, fn -> assert_github_auth!(mutated) end
            end

            for permissions <- [nil, %{"contents" => "write"}] do
              mutated = replace(original, ["jobs", name, "permissions"], permissions)
              assert_raise ExUnit.AssertionError, fn -> assert_github_auth!(mutated) end
            end
          end
        end
      end
    end
  end

  test "run scripts reject expressions and credentials in arguments, logs, or files" do
    for path <- [@workflow, @qualification] do
      original = read_yaml!(path)
      assert_github_auth!(original)

      for {_name, job} <- original["jobs"],
          step <- job["steps"],
          is_binary(step["run"]),
          command <- [
            "echo ${{ github.token }}",
            "echo ${{ secrets.HEX_API_KEY }}",
            ~S(gh api -H "Authorization: Bearer $GH_TOKEN" repos/baselabs/replicant),
            ~S(gh api -H "Authorization: Bearer ${GITHUB_TOKEN}" repos/baselabs/replicant),
            ~S(mix hex.publish --key "$HEX_API_KEY"),
            ~S(printf '%s' "$GH_TOKEN" > token.txt)
          ] do
        env = step["env"] || %{}
        assert_script_credentials!(step["run"], env)
        mutated = step["run"] <> "\n" <> command

        assert_raise ExUnit.AssertionError, fn -> assert_script_credentials!(mutated, env) end
      end
    end
  end

  test "every executable job documents its timeout and refuses invalid limits" do
    for path <- Path.wildcard(Path.join(@repo_root, ".github/workflows/*.{yml,yaml}")) do
      source = File.read!(path)
      assert_timeouts!(source)

      if path == @qualification do
        for {before, after_rule} <- [
              {"2x", "3x"},
              {"under 2.5 minutes get 5", "under 2.5 minutes get 2"}
            ] do
          mutated = String.replace(source, before, after_rule)
          refute mutated == source
          assert_raise ExUnit.AssertionError, fn -> assert_timeouts!(mutated) end
        end
      end

      lines = Regex.scan(~r/^    timeout-minutes: .+$/m, source)
      assert lines != []

      for [line] <- lines,
          replacement <- [
            "",
            line |> String.split(" #") |> hd(),
            Regex.replace(~r/Green run \d+|no green run yet/, line, "unknown"),
            String.replace(line, ~r/timeout-minutes: \d+/, "timeout-minutes: 1"),
            String.replace(line, ~r/timeout-minutes: \d+/, "timeout-minutes: 91")
          ] do
        mutated = String.replace(source, line, replacement)
        refute mutated == source
        assert_raise ExUnit.AssertionError, fn -> assert_timeouts!(mutated) end
      end
    end
  end

  test "a publish job can switch from its initial limit to a measured green limit" do
    source = File.read!(@workflow)
    initial = "timeout-minutes: 90 # no green run yet"
    measured = "timeout-minutes: 7 # Green run 12345: build 3m15s"
    assert_timeouts!(source)
    assert_timeouts!(String.replace(source, initial, measured))
  end

  test "committed projects fetch locked" do
    paths =
      Path.wildcard(Path.join(@repo_root, ".github/workflows/*.{yml,yaml}")) ++
        Path.wildcard(Path.join(@repo_root, "scripts/**/*.sh")) ++
        [Path.join(@repo_root, "examples/replication_pipeline/Dockerfile")]

    for path <- paths do
      source = File.read!(path)
      assert_locked_fetches!(source)

      for [command] <- Regex.scan(~r/\bmix\s+deps\.get\b[^\n]*/, source) do
        mutated = String.replace(source, command, String.replace(command, " --check-locked", ""))
        refute mutated == source
        assert_raise ExUnit.AssertionError, fn -> assert_locked_fetches!(mutated) end
      end
    end
  end

  test "the example image runs the exact .tool-versions runtime" do
    dockerfile = File.read!(Path.join(@repo_root, "examples/replication_pipeline/Dockerfile"))
    versions = toolchain_versions!()
    assert_example_image_runtime!(dockerfile, versions)

    for mutated <- [
          Regex.replace(~r/(hexpm\/elixir:)\d+\.\d+\.\d+/, dockerfile, "\\g{1}0.0.0"),
          Regex.replace(~r/(-erlang-)\d+\.\d+\.\d+/, dockerfile, "\\g{1}0.0.0"),
          Regex.replace(~r/^FROM hexpm\/elixir:/m, dockerfile, "FROM elixir:"),
          Regex.replace(
            ~r/^FROM base AS runtime$/m,
            dockerfile,
            "FROM debian:bookworm-slim AS runtime"
          )
        ] do
      refute mutated == dockerfile

      assert_raise ExUnit.AssertionError, fn ->
        assert_example_image_runtime!(mutated, versions)
      end
    end

    mutated = Map.put(versions, "erlang", "0.0.0")
    refute mutated == versions

    assert_raise ExUnit.AssertionError, fn ->
      assert_example_image_runtime!(dockerfile, mutated)
    end
  end

  test "example audits inspect its lockfile using the root's locked dependency audit tool" do
    original = read_yaml!(@qualification)
    assert_example_audits!(original)

    for {name, job} <- original["jobs"],
        {step, index} <- Enum.with_index(job["steps"]),
        is_binary(step["run"]),
        audit <- ["mix deps.audit --path examples/replication_pipeline", "mix hex.audit"],
        String.contains?(step["run"], audit) do
      path = ["jobs", name, "steps", Access.at(index), "run"]
      mutated = put_in(original, path, String.replace(step["run"], audit, "true"))
      assert_raise ExUnit.AssertionError, fn -> assert_example_audits!(mutated) end
    end

    mutated =
      put_in(original, ["jobs", "static", "defaults"], %{
        "run" => %{"working-directory" => "examples/replication_pipeline"}
      })

    assert_raise ExUnit.AssertionError, fn -> assert_example_audits!(mutated) end
  end

  test "every runtime installation consumes only shared reader outputs" do
    for path <- [@workflow, @qualification] do
      original = read_yaml!(path)
      assert_workflow!(original)

      installs =
        for {name, job} <- original["jobs"],
            {step, index} <- Enum.with_index(job["steps"] || []),
            step["uses"] == @beam_action_ref,
            do: {name, index}

      assert installs != []

      for {name, index} <- installs do
        for {key, value} <- [
              {"elixir", "1.20.3-otp-29"},
              {"otp", "29.0.5"},
              {"elixir", nil},
              {"otp", "${{ inputs.otp }}"},
              {"version-file", ".tool-versions"},
              {"version-type", "loose"}
            ] do
          mutated = replace(original, ["jobs", name, "steps", index, "with", key], value)
          assert_raise ExUnit.AssertionError, fn -> assert_workflow!(mutated) end
        end

        mutated = update_in(original, ["jobs", name, "steps"], &List.delete_at(&1, index))
        assert_raise ExUnit.AssertionError, fn -> assert_workflow!(mutated) end

        mutated = replace(original, ["jobs", name, "steps", index, "uses"], @beam)
        assert_raise ExUnit.AssertionError, fn -> assert_workflow!(mutated) end
      end

      for {name, job} <- original["jobs"],
          {step, index} <- Enum.with_index(job["steps"] || []),
          step["uses"] == @toolchain do
        for mutated <- [
              replace(original, ["jobs", name, "steps", index, "id"], "other"),
              update_in(original, ["jobs", name, "steps"], &List.delete_at(&1, index))
            ] do
          assert_raise ExUnit.AssertionError, fn -> assert_workflow!(mutated) end
        end
      end
    end

    original = read_yaml!(@qualification)

    for key <- ["elixir", "otp"] do
      mutated = replace(original, ["jobs", "toolchain", "outputs", key], "0.0.0")
      assert_raise ExUnit.AssertionError, fn -> assert_workflow!(mutated) end
    end

    for name <- ["static", "test", "release-artifact", "reference-example"] do
      mutated = replace(original, ["jobs", name, "needs"], nil)
      assert_raise ExUnit.AssertionError, fn -> assert_workflow!(mutated) end
    end

    for {path, value} <- [
          {["env", "WANT_ELIXIR"], "1.20.3-otp-29"},
          {["env", "WANT_OTP"], "29.0.5"},
          {["env", "elixir-version"], "1.20.3"},
          {["env", "otp-version"], "29.0.5"},
          {["env", "version-file"], ".tool-versions"}
        ] do
      mutated = replace(original, path, value)
      assert_raise ExUnit.AssertionError, fn -> assert_workflow!(mutated) end
    end

    mutated =
      update_in(original, ["jobs", "toolchain", "steps"], fn steps ->
        steps ++ [%{"run" => "awk '{print $2}' .tool-versions"}]
      end)

    assert_raise ExUnit.AssertionError, fn -> assert_workflow!(mutated) end
  end

  test "the shared reader action tests its parser before exposing versions by path" do
    original = read_yaml!(@toolchain_action)
    assert_toolchain_action!(original)

    for {path, value} <- [
          {["outputs", "elixir", "value"], "1.20.3-otp-29"},
          {["outputs", "otp", "value"], "29.0.5"},
          {["runs", "steps", 0, "run"], "true"},
          {["runs", "steps", 1, "id"], "other"},
          {["runs", "steps", 1, "run"], ~s(bash scripts/toolchain.sh >> "$GITHUB_OUTPUT")},
          {["runs", "steps", 1, "run"], ~s(awk '{print $2}' .tool-versions)},
          {["runs", "steps", 1, "shell"], "sh"}
        ] do
      mutated = replace(original, path, value)
      assert_raise ExUnit.AssertionError, fn -> assert_toolchain_action!(mutated) end
    end

    mutated = update_in(original, ["runs", "steps"], &Enum.reverse/1)
    assert_raise ExUnit.AssertionError, fn -> assert_toolchain_action!(mutated) end
  end

  test "the BEAM action installs strictly and immediately checks the installed runtime" do
    original = read_yaml!(@beam_action)
    assert_beam_action!(original)

    for {path, value} <- [
          {["inputs", "elixir", "default"], "1.20.3-otp-29"},
          {["inputs", "otp", "required"], false},
          {["runs", "steps", 0, "uses"], "erlef/setup-beam@main"},
          {["runs", "steps", 0, "with", "version-file"], ".tool-versions"},
          {["runs", "steps", 0, "with", "elixir-version"], "1.20.3"},
          {["runs", "steps", 0, "with", "otp-version"], "29.0.5"},
          {["inputs", "install-hex", "default"], "false"},
          {["runs", "steps", 0, "with", "install-hex"], "true"},
          {["runs", "steps", 0, "with", "version-type"], nil},
          {["runs", "steps", 0, "with", "version-type"], "loose"},
          {["runs", "steps", 1, "env", "WANT_ELIXIR"], "1.20.3-otp-29"},
          {["runs", "steps", 1, "env", "WANT_OTP"], "29"},
          {["runs", "steps", 1, "run"], "true"},
          {["runs", "steps", 1, "run"], String.replace(@installed, "--installed", "--self-test")},
          {["runs", "steps", 1, "run"], String.replace(@installed, "$GITHUB_WORKSPACE/", "")},
          {["runs", "steps", 1, "shell"], "sh"}
        ] do
      mutated = replace(original, path, value)
      assert_raise ExUnit.AssertionError, fn -> assert_beam_action!(mutated) end
    end

    for steps <- [
          Enum.take(original["runs"]["steps"], 1),
          Enum.reverse(original["runs"]["steps"])
        ] do
      mutated = put_in(original, ["runs", "steps"], steps)
      assert_raise ExUnit.AssertionError, fn -> assert_beam_action!(mutated) end
    end
  end

  test "the shared reader has the pinned bytes and executable mode" do
    source = File.read!(@reader)
    stat = File.stat!(@reader)
    assert stat.type == :regular
    assert_shared_reader!(source, stat.mode)

    for {contents, mode} <- [
          {source <> "\n", stat.mode},
          {source, 0o100644},
          {source, 0o104755},
          {source, 0o100777}
        ] do
      assert_raise ExUnit.AssertionError, fn -> assert_shared_reader!(contents, mode) end
    end
  end

  test "the shared refusal rejects Elixir, full OTP patch, and OTP build suffix drift" do
    assert_installed_runtime!()
  end

  test "the shared reader's parser self-test passes" do
    {output, status} = System.cmd(@reader, ["--self-test"], stderr_to_stdout: true)
    assert status == 0, output
    assert output =~ "toolchain self-test:"
  end

  test "qualification retains the archives required by the CI verdict" do
    ci = read_yaml!(Path.join(@repo_root, ".github/workflows/ci.yml"))
    assert ci["jobs"]["qualification"]["uses"] == "./.github/workflows/qualification.yml"
    assert ci["jobs"]["qualified"]["needs"] == ["qualification"]

    job = read_yaml!(@qualification)["jobs"]["release-artifact"]
    assert job["permissions"] == %{"contents" => "read"}
    assert [checkout, runtime, install_hex, qualify] = job["steps"]
    assert checkout["uses"] == @checkout
    assert checkout["with"] == %{"persist-credentials" => false}
    assert runtime["uses"] == @beam_action_ref
    assert runtime["with"]["install-hex"] == "false"
    assert_hex_install!(install_hex, "${{ needs.toolchain.outputs.hex }}")
    assert qualify["uses"] == @hex_action
    assert qualify["with"] == hex_inputs("qualify")
  end

  test "publish hands exact qualification provenance to keyless read-back" do
    workflow = read_yaml!(@workflow)
    assert_publish!(workflow)

    for {path, value} <- [
          {["jobs", "upload", "if"], nil},
          {["jobs", "upload", "permissions", "id-token"], nil},
          {["jobs", "upload", "outputs", "qualification-sha256"], nil},
          {["jobs", "read-back", "environment"], "publish"},
          {["jobs", "read-back", "steps", 7, "with", "qualification-run"], "123"},
          {["jobs", "read-back", "steps", 7, "with", "qualification-sha256"], "unbound"},
          {["jobs", "read-back", "steps", 8, "with", "name"], "temporary"},
          {["jobs", "upload", "steps", 6, "with", "fetch-depth"], 1},
          {["jobs", "upload", "steps", 1, "with", "ref"], @release_commit}
        ] do
      assert_raise ExUnit.AssertionError, fn ->
        assert_publish!(replace(workflow, path, value))
      end
    end
  end

  defp assert_workflow!(workflow) do
    maps = maps(workflow)
    refute_pull_request_trigger!(workflow)
    assert_github_auth!(workflow)

    for map <- maps do
      for key <- ["ELIXIR_VERSION", "OTP_VERSION", "version-file"] do
        refute Map.has_key?(map, key)
      end

      for key <- ["elixir", "otp", "elixir-version", "otp-version", "WANT_ELIXIR", "WANT_OTP"],
          Map.has_key?(map, key) do
        assert is_binary(map[key])
        assert map[key] =~ ~r/\A\$\{\{ (?:steps|needs|inputs)\.[\w.]+ \}\}\z/
      end
    end

    for {_name, job} <- workflow["jobs"], do: assert_job_host!(job)
    assert_runtime_steps!(workflow)

    assert_bash_shell!(workflow)

    for map <- maps, do: assert_action_pins!(map)
  end

  # A bare run: is `bash -e` without pipefail; the workflow default is `bash -eo pipefail`.
  # A job's own defaults.run replaces the workflow's entirely, so it must name bash too.
  defp assert_bash_shell!(workflow) do
    assert workflow["defaults"]["run"]["shell"] == "bash"

    for {_name, job} <- workflow["jobs"], Map.has_key?(job, "defaults") do
      assert job["defaults"]["run"]["shell"] == "bash"
    end

    for {_name, job} <- workflow["jobs"],
        step <- job["steps"] || [],
        Map.has_key?(step, "shell"),
        do: assert(step["shell"] == "bash")
  end

  defp assert_strict_script!(source) do
    assert String.starts_with?(source, "#!/usr/bin/env bash\n")
    assert source =~ ~r/^set -euo pipefail$/m
  end

  # A command whose output feeds a check is assigned first, so its failure fails
  # the step instead of reading as empty output.
  defp assert_no_lost_status!(source) do
    refute source =~ ~r/\|\|\s*true\b/
    refute source =~ ~r/<\(/
    refute source =~ ~r/(\[\[?|test) +-[zn] +"\$\(/
  end

  defp refute_pull_request_trigger!(workflow) do
    events = workflow["on"]
    events = if is_map(events), do: Map.keys(events), else: List.wrap(events)
    refute Enum.any?(events, &String.starts_with?(&1, "pull_request"))
  end

  defp assert_job_host!(%{"uses" => _} = job) do
    assert job == %{"uses" => "./.github/workflows/qualification.yml"}
  end

  defp assert_job_host!(job) do
    assert job["runs-on"] == "ubuntu-24.04"

    assert is_integer(job["timeout-minutes"]) and job["timeout-minutes"] >= 5 and
             job["timeout-minutes"] <= 90
  end

  defp assert_runtime_steps!(workflow) do
    for {_name, job} <- workflow["jobs"] do
      steps = job["steps"] || []
      install = Enum.find_index(steps, &(&1["uses"] == @beam_action_ref))

      for {step, index} <- Enum.with_index(steps) do
        assert_runtime_step!(workflow, job, steps, install, step, index)
      end
    end
  end

  defp assert_runtime_step!(workflow, job, steps, install, step, index) do
    refute String.starts_with?(step["uses"] || "", "erlef/setup-beam@")
    script = step["run"] || ""
    script = String.replace(script, ~S|show "$RELEASE_COMMIT:.tool-versions"|, "")
    refute script =~ ".tool-versions"

    if script =~ ~r/\b(?:mix|elixir|erl)\s/ do
      assert is_integer(install) and install < index
    end

    if step["uses"] == @beam_action_ref do
      source = runtime_output_source!(workflow, job, steps, index)

      assert Map.drop(step["with"], ["install-hex"]) == %{
               "elixir" => "${{ #{source}.elixir }}",
               "otp" => "${{ #{source}.otp }}"
             }
    end
  end

  defp runtime_output_source!(workflow, job, steps, index) do
    reader = Enum.find_index(steps, &(&1["uses"] == @toolchain))

    if is_integer(reader) do
      assert reader < index
      assert_reader_step!(steps)
      "steps.toolchain.outputs"
    else
      qualification_runtime_outputs!(workflow, job)
    end
  end

  defp qualification_runtime_outputs!(workflow, job) do
    assert "toolchain" in List.wrap(job["needs"])
    toolchain = workflow["jobs"]["toolchain"]
    assert is_map(toolchain)
    assert_reader_step!(toolchain["steps"])

    for key <- ["elixir", "otp"] do
      assert toolchain["outputs"][key] == "${{ steps.toolchain.outputs.#{key} }}"
    end

    "needs.toolchain.outputs"
  end

  defp assert_reader_step!(steps) do
    reader = Enum.find_index(steps, &(&1["uses"] == @toolchain))
    checkout = Enum.find_index(steps, &(&1["uses"] == @checkout))
    assert is_integer(reader) and is_integer(checkout) and checkout < reader
    step = Enum.at(steps, reader)
    assert Map.drop(step, ["with"]) == %{"uses" => @toolchain, "id" => "toolchain"}

    if Enum.any?(steps, &(&1["with"]["ref"] == @release_commit)) do
      assert step["with"] == %{"path" => @runtime_file}
    else
      refute step["with"]
    end
  end

  defp assert_toolchain_action!(action) do
    assert action["inputs"]["path"]["default"] == ".tool-versions"
    assert Enum.sort(Map.keys(action["outputs"])) == ["elixir", "hex", "otp"]

    for key <- ["elixir", "hex", "otp"] do
      assert action["outputs"][key]["value"] == "${{ steps.versions.outputs.#{key} }}"
    end

    assert action["runs"] == %{
             "using" => "composite",
             "steps" => [
               %{
                 "name" => "Test the shared toolchain reader",
                 "shell" => "bash",
                 "run" => "scripts/toolchain.sh --self-test"
               },
               %{
                 "name" => "Read the runtime versions",
                 "id" => "versions",
                 "shell" => "bash",
                 "env" => %{"TOOLCHAIN_PATH" => "${{ inputs.path }}"},
                 "run" => ~s(scripts/toolchain.sh "$TOOLCHAIN_PATH" >> "$GITHUB_OUTPUT")
               }
             ]
           }
  end

  defp assert_beam_action!(action) do
    assert Enum.sort(Map.keys(action["inputs"])) == ["elixir", "install-hex", "otp"]
    assert action["inputs"]["install-hex"]["default"] == "true"

    for key <- ["elixir", "otp"] do
      assert action["inputs"][key]["required"] == true
      refute Map.has_key?(action["inputs"][key], "default")
    end

    assert action["runs"]["using"] == "composite"
    assert [setup, refusal] = action["runs"]["steps"]
    assert_action_pins!(setup)
    assert setup["uses"] == @beam
    assert map_size(setup) == 2

    assert refusal == %{
             "name" => "Refuse a differing runtime",
             "shell" => "bash",
             "env" => %{
               "WANT_ELIXIR" => "${{ inputs.elixir }}",
               "WANT_OTP" => "${{ inputs.otp }}"
             },
             "run" => @installed <> "\n"
           }
  end

  defp assert_action_pins!(map) do
    uses = to_string(map["uses"] || "")
    refute uses =~ ~r{actions/cache(?:/|@)}i

    if uses != "" and not String.starts_with?(uses, "./"), do: assert(uses =~ ~r/@[0-9a-f]{40}$/)

    if String.starts_with?(uses, "actions/checkout@") do
      assert uses == @checkout
      assert map["with"]["persist-credentials"] == false
    end

    if String.starts_with?(uses, "erlef/setup-beam@") do
      assert uses == @beam

      assert map["with"] == %{
               "elixir-version" => "${{ inputs.elixir }}",
               "otp-version" => "${{ inputs.otp }}",
               "install-hex" => "${{ inputs.install-hex }}",
               "version-type" => "strict"
             }
    end
  end

  defp assert_publish!(workflow) do
    assert workflow["on"] == %{
             "workflow_dispatch" => %{
               "inputs" => %{
                 "release_commit" => %{
                   "description" => "Full lowercase SHA of the qualified commit to release",
                   "required" => true,
                   "type" => "string"
                 }
               }
             }
           }

    assert workflow["concurrency"] == %{"group" => "publish", "cancel-in-progress" => false}
    jobs = workflow["jobs"]
    assert Enum.sort(Map.keys(jobs)) == ["read-back", "upload"]
    assert Enum.all?(jobs, fn {_name, job} -> job["if"] == @owners end)

    upload = jobs["upload"]
    assert upload["environment"] == "publish"

    assert upload["permissions"] == %{
             "contents" => "read",
             "actions" => "read",
             "id-token" => "write"
           }

    assert upload["outputs"] == %{
             "qualification-run" => "${{ steps.release.outputs.qualification-run }}",
             "qualification-sha256" => "${{ steps.release.outputs.qualification-sha256 }}"
           }

    readback = jobs["read-back"]
    refute Map.has_key?(readback, "environment")
    assert readback["needs"] == "upload"
    assert readback["permissions"] == %{"contents" => "read", "actions" => "read"}
    assert readback["outputs"] == %{"commit" => "${{ steps.release.outputs.commit }}"}

    assert {setup, [publish, upload_cleanup]} = Enum.split(upload["steps"], 7)
    assert {^setup, [verify, artifact, readback_cleanup]} = Enum.split(readback["steps"], 7)
    assert [ref, tooling, versions, reader, runtime, install_hex, source] = setup
    assert ref["env"]["RELEASE_COMMIT"] == @release_commit
    assert ref["run"] =~ ~S|[[ "$GITHUB_REF" == "refs/heads/$DEFAULT_BRANCH" ]]|
    assert ref["run"] =~ ~S|[[ "$RELEASE_COMMIT" =~ ^[0-9a-f]{40}$ ]]|

    assert tooling["uses"] == @checkout
    assert tooling["with"]["ref"] == @workflow_revision
    assert tooling["with"]["fetch-depth"] == 0
    assert tooling["with"]["persist-credentials"] == false

    assert tooling["with"]["sparse-checkout"] ==
             "/.github/actions/beam/\n/.github/actions/toolchain/\n/scripts/toolchain.sh\n"

    assert versions["run"] =~ ~S|merge-base --is-ancestor "$RELEASE_COMMIT"|
    assert versions["run"] =~ ~S|show "$RELEASE_COMMIT:.tool-versions"|
    assert reader["uses"] == @toolchain
    assert reader["with"] == %{"path" => @runtime_file}
    assert runtime["uses"] == @beam_action_ref
    assert runtime["with"]["install-hex"] == "false"
    assert_hex_install!(install_hex, "${{ steps.toolchain.outputs.hex }}")

    assert source == %{
             "uses" => @checkout,
             "with" => %{
               "ref" => @release_commit,
               "fetch-depth" => 0,
               "persist-credentials" => false
             }
           }

    assert publish["uses"] == @hex_action
    assert publish["id"] == "release"

    assert publish["with"] ==
             Map.merge(hex_inputs("upload"), %{
               "release-commit" => @release_commit,
               "organization-read-key" => "${{ secrets.HEX_ORG_KEY_PUBLISH_API_READ }}"
             })

    assert verify["uses"] == @hex_action
    assert verify["id"] == "release"

    assert verify["with"] ==
             Map.merge(hex_inputs("read-back"), %{
               "release-commit" => @release_commit,
               "qualification-run" => "${{ needs.upload.outputs.qualification-run }}",
               "qualification-sha256" => "${{ needs.upload.outputs.qualification-sha256 }}",
               "release-assets-directory" => @release_assets
             })

    assert artifact["uses"] == @artifact

    assert artifact["with"] == %{
             "name" => "release-assets",
             "path" => @release_assets <> "/",
             "retention-days" => 7,
             "compression-level" => 0,
             "if-no-files-found" => "error",
             "overwrite" => true
           }

    assert upload_cleanup["if"] == "always()"
    assert readback_cleanup["if"] == "always()"
  end

  defp hex_inputs(operation) do
    %{"operation" => operation, "package" => "replicant", "package-directory" => "."}
  end

  defp assert_hex_install!(step, version) do
    assert step["working-directory"] == "${{ runner.temp }}"
    assert step["env"] == %{"HEX_VERSION" => version}
    assert step["run"] =~ @hex_version_url
    assert step["run"] =~ ~S|"$pin" == "$HEX_VERSION"|
    assert step["run"] =~ ~S|mix local.hex "$pin" --force|
  end

  defp assert_github_auth!(workflow) do
    refute_tokens!(workflow["env"])
    for {_name, job} <- workflow["jobs"], do: refute_tokens!(job["env"])

    for {_name, job} <- workflow["jobs"], step <- job["steps"] || [] do
      env = step["env"] || %{}
      refute Map.has_key?(env, "GITHUB_TOKEN")

      if github_read?(step) do
        assert job["permissions"]["contents"] == "read"
        assert env["GH_TOKEN"] == "${{ github.token }}"
      else
        refute Map.has_key?(env, "GH_TOKEN")
      end

      assert_script_credentials!(step["run"] || "", env)
    end
  end

  defp refute_tokens!(env) do
    for key <- ["GH_TOKEN", "GITHUB_TOKEN"], do: refute(Map.has_key?(env || %{}, key))
  end

  defp github_read?(step) do
    (step["run"] || "") =~ ~r/\bgh\s+api\b/
  end

  defp assert_script_credentials!(script, env) do
    refute script =~ "${{"
    refute script =~ ~r/\$\{?(?:GH_TOKEN|GITHUB_TOKEN|HEX_API_KEY)\b/

    for {name, value} <- env, is_binary(value), String.contains?(value, "secrets.") do
      refute script =~ Regex.compile!("\\$\\{?" <> Regex.escape(name) <> "\\b")
    end
  end

  defp assert_timeouts!(source) do
    workflow = parse_yaml!(source)

    if workflow["name"] == "Qualification" do
      assert source =~ "# Timeout rule for all workflows: 2x the measured green duration"
      assert source =~ "jobs measured under 2.5 minutes get 5"
    end

    for {name, job} <- workflow["jobs"], not Map.has_key?(job, "uses") do
      [_, body] = Regex.run(~r/^  #{Regex.escape(name)}:\n(.*?)(?=^  [\w-]+:|\z)/ms, source)

      if body =~ ~r/^    timeout-minutes: 90 # no green run yet$/m do
        assert job["timeout-minutes"] == 90
      else
        measurement =
          Regex.run(
            ~r/^    timeout-minutes: (\d+) # Green run \d+: .+? (\d+)m(\d{2})s$/m,
            body
          )

        assert measurement, "#{name} needs a timeout with its green run and duration"
        [_, limit, minutes, seconds] = measurement
        duration = String.to_integer(minutes) * 60 + String.to_integer(seconds)
        assert job["timeout-minutes"] == String.to_integer(limit)
        assert job["timeout-minutes"] == max(5, div(2 * duration + 59, 60))
      end

      assert job["timeout-minutes"] <= 90
    end
  end

  # Every external base image is the official Elixir image whose Elixir and OTP
  # versions equal .tool-versions; every other stage derives from it.
  defp assert_example_image_runtime!(dockerfile, versions) do
    [elixir, _otp_major] = String.split(Map.fetch!(versions, "elixir"), "-otp-")
    otp = Map.fetch!(versions, "erlang")
    stages = Regex.scan(~r/^FROM\s+\S+\s+AS\s+(\S+)$/m, dockerfile, capture: :all_but_first)
    sources = Regex.scan(~r/^FROM\s+(\S+)/m, dockerfile, capture: :all_but_first)
    stage_names = MapSet.new(List.flatten(stages))
    external = for [source] <- sources, not MapSet.member?(stage_names, source), do: source

    assert external != []

    for source <- external do
      assert [_, image_elixir, image_otp] = Regex.run(@example_image, source)
      assert image_elixir == elixir
      assert image_otp == otp
    end
  end

  defp assert_locked_fetches!(source) do
    for [_, arguments] <- Regex.scan(~r/\bmix\s+deps\.get\b([^\n]*)/, source) do
      assert "--check-locked" in String.split(arguments)
    end
  end

  defp assert_example_audits!(workflow) do
    static = workflow["jobs"]["static"]
    refute get_in(static, ["defaults", "run", "working-directory"])
    steps = static["steps"]
    fetch = Enum.find_index(steps, &(&1["run"] == "mix deps.get --check-locked"))

    audit =
      Enum.find_index(steps, fn step ->
        step["run"] == "mix deps.audit --path examples/replication_pipeline"
      end)

    assert is_integer(fetch) and is_integer(audit) and fetch < audit
    refute Enum.at(steps, audit)["working-directory"]
    example = workflow["jobs"]["reference-example"]

    assert get_in(example, ["defaults", "run", "working-directory"]) ==
             "examples/replication_pipeline"

    assert Enum.any?(example["steps"], fn step ->
             lines = String.split(step["run"] || "", "\n")
             fetch = Enum.find_index(lines, &(&1 == "mix deps.get --check-locked"))
             audit = Enum.find_index(lines, &(&1 == "mix hex.audit"))
             is_integer(fetch) and is_integer(audit) and fetch < audit
           end)
  end

  defp assert_shared_reader!(source, mode) do
    assert Base.encode16(:crypto.hash(:sha256, source), case: :lower) == @reader_sha256
    assert rem(mode, 0o10000) == 0o755
  end

  defp toolchain_versions! do
    {output, status} = System.cmd(@reader, [], cd: @repo_root, stderr_to_stdout: true)
    assert status == 0, output

    Map.new(String.split(output, "\n", trim: true), fn line ->
      [name, value] = String.split(line, "=", parts: 2)
      {name, value}
    end)
  end

  defp assert_installed_runtime! do
    versions = toolchain_versions!()
    elixir = Map.fetch!(versions, "elixir")
    otp = Map.fetch!(versions, "otp")
    [major, minor, patch] = String.split(otp, ".")
    wrong_patch = Enum.join([major, minor, String.to_integer(patch) + 1], ".")

    for {expected_elixir, expected_otp, success?} <- [
          {elixir, otp, true},
          {String.replace(elixir, ~r/^\d+\.\d+\.\d+/, "0.0.0"), otp, false},
          {elixir, wrong_patch, false},
          {elixir, major, false},
          {String.replace(elixir, "-otp-" <> major, "-otp-0"), otp, false}
        ] do
      {_output, status} =
        System.cmd(
          @reader,
          ["--installed", "elixir=#{expected_elixir}", "erlang=#{expected_otp}"],
          stderr_to_stdout: true
        )

      assert status == 0 == success?
    end
  end

  defp maps(map) when is_map(map), do: [map | Enum.flat_map(Map.values(map), &maps/1)]
  defp maps(list) when is_list(list), do: Enum.flat_map(list, &maps/1)
  defp maps(_value), do: []

  defp replace(workflow, path, value) do
    access =
      Enum.map(path, fn key ->
        if is_integer(key), do: Access.at(key), else: Access.key(key, %{})
      end)

    put_in(workflow, access, value)
  end

  # Ruby's standard YAML parser is present on the hosted runner. Use a real
  # parser without adding a YAML runtime dependency to this Elixir library.
  defp read_yaml!(path) do
    parse_yaml!(File.read!(path))
  end

  defp parse_yaml!(source) do
    parser = """
    data = YAML.safe_load(ARGV.fetch(0), aliases: true)
    data['on'] = data.delete(true) if data.key?(true)
    puts JSON.generate(data)
    """

    {json, 0} = System.cmd("ruby", ["-ryaml", "-rjson", "-e", parser, source])
    Jason.decode!(json)
  end
end
