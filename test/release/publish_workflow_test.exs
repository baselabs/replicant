defmodule Replicant.PublishWorkflowTest do
  use ExUnit.Case, async: true

  @repo_root Path.expand("../..", __DIR__)
  @workflow Path.join(@repo_root, ".github/workflows/publish.yml")
  @reader Path.join(@repo_root, "scripts/toolchain.sh")
  @toolchain_action Path.join(@repo_root, ".github/actions/toolchain/action.yml")
  @beam_action Path.join(@repo_root, ".github/actions/beam/action.yml")
  @toolchain "./.github/actions/toolchain"
  @beam_action_ref "./.github/actions/beam"
  @reader_sha256 "b29bfeb6ec4ec8f5b7e3ac3c963f04b6d3c73483172e5bd59ea40f20b102f1eb"
  @installed ~S|"$GITHUB_WORKSPACE/scripts/toolchain.sh" --installed "elixir=$WANT_ELIXIR" "erlang=$WANT_OTP"|
  @qualification Path.join(@repo_root, ".github/workflows/qualification.yml")
  @checkout "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1"
  @beam "erlef/setup-beam@54075bcc5e249e4758d363f27d099f55d843f124"
  @artifact "actions/upload-artifact@cf430e030ddbb5b0abf93d22962f4752f3646cd9"
  @retain ~S|mix run --no-mix-exs "$GITHUB_WORKSPACE/scripts/release/retain_release_assets.exs" "$PACKAGE_VERSION" "$RUNNER_TEMP/replicant-release/package.tar" "$RUNNER_TEMP/replicant-release/release-assets"|
  @example_image ~r/\Ahexpm\/elixir:(\d+\.\d+\.\d+)-erlang-(\d+\.\d+\.\d+)-\S+@sha256:[0-9a-f]{64}\z/
  @owners Enum.join(
            [
              ~S|contains(fromJSON('["corepay","palermo-git"]'), github.actor)|,
              ~S|contains(fromJSON('["corepay","palermo-git"]'), github.triggering_actor)|
            ],
            " && "
          )
  @upload ~S|mix run --no-mix-exs "$GITHUB_WORKSPACE/scripts/release/publish_hex_artifacts.exs" "$PACKAGE_VERSION" "$RUNNER_TEMP/replicant-release/package.tar" "$RUNNER_TEMP/replicant-release/docs.tar.gz"|

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

  test "every executable job has a measured timeout and rejects missing or invalid limits" do
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
            String.replace(line, ~r/timeout-minutes: \d+/, "timeout-minutes: 30")
          ] do
        mutated = String.replace(source, line, replacement)
        refute mutated == source
        assert_raise ExUnit.AssertionError, fn -> assert_timeouts!(mutated) end
      end
    end
  end

  test "committed projects fetch locked and the generated consumer resolves without a borrowed lock" do
    consumer_path = Path.join(@repo_root, "scripts/release/consume_candidate.sh")

    paths =
      Path.wildcard(Path.join(@repo_root, ".github/workflows/*.{yml,yaml}")) ++
        (Path.wildcard(Path.join(@repo_root, "scripts/**/*.sh")) -- [consumer_path]) ++
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

    source = File.read!(consumer_path)
    assert_consumer_fetch!(source)

    for mutated <- [
          String.replace(source, "mix deps.get >&2", "mix deps.get --check-locked >&2"),
          String.replace(source, ~s([[ ! -e "$consumer/mix.lock" ]]), "true"),
          source <> ~s(\ncp "$src/mix.lock" "$consumer/mix.lock"\n),
          String.replace(source, "mix hex.audit", "true"),
          String.replace(source, ~s(mix deps.audit --path "$consumer"), "true"),
          String.replace(source, "mix deps.get --check-locked", "mix deps.get")
        ] do
      refute mutated == source
      assert_raise ExUnit.AssertionError, fn -> assert_consumer_fetch!(mutated) end
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
      assert_raise ExUnit.AssertionError, fn -> assert_example_image_runtime!(mutated, versions) end
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

  test "publish is owner-dispatched with one environment secret and guarded exact-byte upload" do
    assert_publish!(read_yaml!(@workflow))
  end

  # Each mutation starts from the passing real workflow.
  for {name, path, value} <- [
        {"automatic trigger", ["on", "push"], %{}},
        {"missing dispatch", ["on"], %{}},
        {"dispatch input", ["on", "workflow_dispatch"], %{"inputs" => %{}}},
        {"wrong environment", ["jobs", "publish", "environment"], "production"},
        {"missing owner guard", ["jobs", "publish", "if"], nil},
        {"unguarded triggering actor", ["jobs", "publish", "if"], "github.actor == 'corepay'"},
        {"write permission", ["permissions", "contents"], "write"},
        {"job write permission", ["jobs", "publish", "permissions", "contents"], "write"},
        {"Actions permission", ["jobs", "publish", "permissions", "actions"], "read"},
        {"extra workflow permission", ["permissions", "actions"], "read"},
        {"persisted key", ["jobs", "publish", "steps", 2, "with", "persist-credentials"], true},
        {"unpinned checkout", ["jobs", "publish", "steps", 2, "uses"], "actions/checkout@main"},
        {"unpinned source", ["jobs", "publish", "steps", 2, "with", "ref"], "main"},
        {"branch dispatch", ["jobs", "publish", "steps", 0, "run"], "true"},
        {"missing package refusal", ["jobs", "publish", "steps", 1, "run"], "true"},
        {"missing archives", ["jobs", "publish", "steps", 8, "run"], "true"},
        {"unpinned release assets", ["jobs", "publish", "steps", 11, "uses"],
         "actions/upload-artifact@v7.0.2"},
        {"wrong artifact name", ["jobs", "publish", "steps", 11, "with", "name"], "other"},
        {"wrong artifact path", ["jobs", "publish", "steps", 11, "with", "path"], "."},
        {"missing artifact files", ["jobs", "publish", "steps", 11, "with", "if-no-files-found"],
         "warn"},
        {"assets on failed read-back", ["jobs", "publish", "steps", 11, "if"], "always()"},
        {"wrong secret", ["jobs", "publish", "steps", 9, "env", "HEX_API_KEY"],
         "${{ secrets.OTHER_KEY }}"},
        {"job credential", ["jobs", "publish", "env"],
         %{"HEX_API_KEY" => "${{ secrets.HEX_API_KEY }}"}}
      ] do
    test "publish contract refuses #{name}" do
      original = read_yaml!(@workflow)
      assert_publish!(original)
      mutated = replace(original, unquote(path), unquote(Macro.escape(value)))
      assert_raise ExUnit.AssertionError, fn -> assert_publish!(mutated) end
    end
  end

  test "the key cannot reach any step except the isolated uploader" do
    original = read_yaml!(@workflow)
    assert_publish!(original)

    for {_step, index} <- Enum.with_index(original["jobs"]["publish"]["steps"]), index != 9 do
      mutated =
        replace(original, ["jobs", "publish", "steps", index, "env", "HEX_API_KEY"], "leak")

      assert_raise ExUnit.AssertionError, fn -> assert_publish!(mutated) end
    end

    mutated = put_in(original, ["env", "HEX_API_KEY"], "leak")
    assert_raise ExUnit.AssertionError, fn -> assert_publish!(mutated) end
  end

  test "the upload step accepts only the no-project Mix invocation and both inspected paths" do
    original = read_yaml!(@workflow)
    assert_publish!(original)

    for command <- [
          "mix hex.publish --yes",
          String.replace(@upload, "--no-mix-exs", "--no-start"),
          @upload <> "\nmix docs",
          "mix deps.get --check-locked\n" <> @upload,
          String.replace(@upload, "package.tar", "other.tar"),
          String.replace(@upload, "docs.tar.gz", "other.tar.gz")
        ] do
      mutated = replace(original, ["jobs", "publish", "steps", 9, "run"], command)
      assert_raise ExUnit.AssertionError, fn -> assert_publish!(mutated) end
    end
  end

  test "the upload step runs outside the checkout, where no mix.exs exists" do
    original = read_yaml!(@workflow)
    assert_publish!(original)

    removed =
      update_in(
        original,
        ["jobs", "publish", "steps", Access.at(9)],
        &Map.delete(&1, "working-directory")
      )

    assert_raise ExUnit.AssertionError, fn -> assert_publish!(removed) end

    for directory <- [".", "${{ github.workspace }}", "scripts/release"] do
      mutated = replace(original, ["jobs", "publish", "steps", 9, "working-directory"], directory)
      assert_raise ExUnit.AssertionError, fn -> assert_publish!(mutated) end
    end

    relative =
      replace(
        original,
        ["jobs", "publish", "steps", 9, "run"],
        String.replace(
          @upload,
          ~s("$GITHUB_WORKSPACE/scripts/release/publish_hex_artifacts.exs"),
          "scripts/release/publish_hex_artifacts.exs"
        )
      )

    assert_raise ExUnit.AssertionError, fn -> assert_publish!(relative) end
  end

  test "publication refuses qualification lookups anywhere in the workflow" do
    original = read_yaml!(@workflow)
    assert_publish!(original)

    for command <- [
          "gh run view 123",
          "gh run list",
          "gh api repos/baselabs/replicant/actions/runs",
          "gh run download 123 -n package-authorization",
          "echo qualified"
        ] do
      mutated = replace(original, ["jobs", "publish", "steps", 12, "run"], command)
      assert_raise ExUnit.AssertionError, fn -> assert_publish!(mutated) end
    end
  end

  test "package refusal requires an anonymous 200 and fails closed on every other result" do
    original = read_yaml!(@workflow)
    assert_publish!(original)
    source = Enum.at(original["jobs"]["publish"]["steps"], 1)["run"]

    for {before, replacement} <- [
          {"https://hex.pm/api/packages/replicant", "https://hex.pm/api/packages/other"},
          {"200) ;;", "200|404) ;;"},
          {"404)", "400)"},
          {"*)", "503)"},
          {"exit 1", "true"},
          {"a first publication is the owner's decision", "absent"}
        ] do
      script = String.replace(source, before, replacement)
      refute script == source
      mutated = replace(original, ["jobs", "publish", "steps", 1, "run"], script)
      assert_raise ExUnit.AssertionError, fn -> assert_publish!(mutated) end
    end
  end

  for {name, path, value} <- [
        {"Elixir drift", ["env", "ELIXIR_VERSION"], "1.20.2"},
        {"OTP drift", ["env", "OTP_VERSION"], "29"},
        {"job runtime override", ["jobs", "publish", "env"], %{"OTP_VERSION" => "28.0.0"}},
        {"step runtime override", ["jobs", "publish", "steps", 3, "env"],
         %{"ELIXIR_VERSION" => "1.20.2"}},
        {"Elixir override", ["jobs", "publish", "steps", 4, "with", "elixir"], "1.20.2"},
        {"literal OTP override", ["jobs", "publish", "steps", 4, "with", "otp"], "29"},
        {"moving runner", ["jobs", "publish", "runs-on"], "ubuntu-" <> "latest"},
        {"self-hosted runner", ["jobs", "publish", "runs-on"], ["self-hosted", "linux"]},
        {"build-box runner", ["jobs", "publish", "runs-on"], "devbox-ovh"},
        {"cache action", ["jobs", "publish", "steps", 5, "uses"], "actions/cache@v4"},
        {"pull request trigger", ["on", "pull_request"], nil},
        {"privileged pull request trigger", ["on", "pull_request_target"], nil}
      ] do
    test "every workflow contract refuses #{name}" do
      original = read_yaml!(@workflow)
      assert_workflow!(original)
      mutated = replace(original, unquote(path), unquote(Macro.escape(value)))
      assert_raise ExUnit.AssertionError, fn -> assert_workflow!(mutated) end
    end
  end

  test "both archives are inspected before upload and cleaned even on failure" do
    original = read_yaml!(@workflow)
    assert_publish!(original)

    for {first, second} <- [{1, 8}, {8, 9}, {9, 10}, {10, 11}] do
      mutated =
        update_in(original, ["jobs", "publish", "steps"], fn steps ->
          List.replace_at(steps, first, Enum.at(steps, second))
          |> List.replace_at(second, Enum.at(steps, first))
        end)

      assert_raise ExUnit.AssertionError, fn -> assert_publish!(mutated) end
    end

    mutated = replace(original, ["jobs", "publish", "steps", 12, "if"], nil)
    assert_raise ExUnit.AssertionError, fn -> assert_publish!(mutated) end
  end

  test "release assets come only from the keyless served read-back after upload" do
    original = read_yaml!(@workflow)
    assert_publish!(original)

    for command <- [
          "true",
          String.replace(@retain, "retain_release_assets.exs", "build_docs_archive.exs")
        ] do
      mutated = replace(original, ["jobs", "publish", "steps", 10, "run"], command)
      assert_raise ExUnit.AssertionError, fn -> assert_publish!(mutated) end
    end

    original_build = Enum.at(original["jobs"]["publish"]["steps"], 8)["run"]

    for command <- ["cp docs.tar.gz release-assets/", "sha256sum docs.tar.gz > SHA256SUMS"] do
      mutated =
        replace(original, ["jobs", "publish", "steps", 8, "run"], original_build <> command)

      assert_raise ExUnit.AssertionError, fn -> assert_publish!(mutated) end
    end

    assert File.read!(@workflow) =~ @artifact <> " # v7.0.2"
  end

  test "qualification checks both archives without a publication authorization artifact" do
    original = read_yaml!(@qualification)
    steps = original["jobs"]["release-artifact"]["steps"]
    assert List.last(steps)["run"] == "bash scripts/release/build_candidate.sh --check"

    live = Enum.find(steps, &(&1["name"] == "Exercise the uploader's Hex reads without a key"))
    assert live["working-directory"] == "${{ runner.temp }}"

    assert live["run"] ==
             ~s|mix run --no-mix-exs "$GITHUB_WORKSPACE/scripts/release/test_publish_hex_artifacts.exs"|

    refute Map.has_key?(live, "env")
    refute Jason.encode!(original) =~ "authorization"
    refute Jason.encode!(original) =~ "upload-artifact"
  end

  test "qualification and publish install the same pinned Hex for reproducible bytes" do
    publish = read_yaml!(@workflow)
    qualification = read_yaml!(@qualification)
    assert_hex!(publish, qualification)

    mutated = put_in(publish, ["env", "HEX_VERSION"], "0.0.0")
    assert_raise ExUnit.AssertionError, fn -> assert_hex!(mutated, qualification) end

    mutated = replace(publish, ["jobs", "publish", "steps", 5, "run"], "mix local.hex --force")
    assert_raise ExUnit.AssertionError, fn -> assert_hex!(mutated, qualification) end
  end

  test "the old upload and authorization machinery is retired" do
    for name <- [
          "publish_candidate.sh",
          "publish_docs.sh",
          "credential_loader.sh",
          "upload_candidate.exs",
          "upload_docs.exs",
          "package_witness.exs",
          "package_publisher.exs",
          "package_checksum.exs"
        ] do
      refute File.exists?(Path.join([@repo_root, "scripts", "release", name]))
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
        refute String.starts_with?(step["uses"] || "", "erlef/setup-beam@")
        refute (step["run"] || "") =~ ".tool-versions"

        if (step["run"] || "") =~ ~r/\b(?:mix|elixir|erl)\s/ do
          assert is_integer(install) and install < index
        end

        if step["uses"] == @beam_action_ref do
          source = runtime_output_source!(workflow, job, steps, index)

          assert step["with"] == %{
                   "elixir" => "${{ #{source}.elixir }}",
                   "otp" => "${{ #{source}.otp }}"
                 }
        end
      end
    end
  end

  defp runtime_output_source!(workflow, job, steps, index) do
    reader = Enum.find_index(steps, &(&1["uses"] == @toolchain))

    if is_integer(reader) do
      assert reader < index
      assert_reader_step!(steps)
      "steps.toolchain.outputs"
    else
      assert "toolchain" in List.wrap(job["needs"])
      toolchain = workflow["jobs"]["toolchain"]
      assert is_map(toolchain)
      assert_reader_step!(toolchain["steps"])

      for key <- ["elixir", "otp"] do
        assert toolchain["outputs"][key] == "${{ steps.toolchain.outputs.#{key} }}"
      end

      "needs.toolchain.outputs"
    end
  end

  defp assert_reader_step!(steps) do
    reader = Enum.find_index(steps, &(&1["uses"] == @toolchain))
    checkout = Enum.find_index(steps, &(&1["uses"] == @checkout))
    assert is_integer(reader) and is_integer(checkout) and checkout < reader
    assert Enum.at(steps, reader) == %{"uses" => @toolchain, "id" => "toolchain"}
  end

  defp assert_toolchain_action!(action) do
    assert Enum.sort(Map.keys(action["outputs"])) == ["elixir", "otp"]

    for key <- ["elixir", "otp"] do
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
                 "run" => ~s(scripts/toolchain.sh >> "$GITHUB_OUTPUT")
               }
             ]
           }
  end

  defp assert_beam_action!(action) do
    assert Enum.sort(Map.keys(action["inputs"])) == ["elixir", "otp"]

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
               "version-type" => "strict"
             }
    end
  end

  defp assert_publish!(workflow) do
    assert_workflow!(workflow)
    assert workflow["on"] == %{"workflow_dispatch" => nil}
    refute Enum.any?(maps(workflow), &Map.has_key?(&1, "inputs"))
    serialized = Jason.encode!(workflow)
    refute serialized =~ ~r/qualified|authorization|actions\/runs|actions\/workflows|gh\s+run/
    refute serialized =~ ~r/gh\s+release|action-gh-release|create-release/
    assert workflow["permissions"] == %{"contents" => "read"}
    assert Map.keys(workflow["jobs"]) == ["publish"]
    job = workflow["jobs"]["publish"]
    assert job["if"] == @owners
    assert job["environment"] == "publish"
    assert job["permissions"] == %{"contents" => "read"}
    assert job["timeout-minutes"] == 90
    steps = job["steps"]
    assert length(steps) == 13
    assert Enum.at(steps, 0)["run"] == ~s(test "$GITHUB_REF" = refs/heads/main)
    refusal = Enum.at(steps, 1)
    assert refusal["name"] == "Refuse a first publication"
    assert refusal["run"] =~ "https://hex.pm/api/packages/replicant"
    assert refusal["run"] =~ "200) ;;"
    assert refusal["run"] =~ "404)"
    assert refusal["run"] =~ "*)"
    assert refusal["run"] =~ "the owner's decision"
    assert refusal["run"] =~ "a first publication is the owner's decision"
    assert length(Regex.scan(~r/exit 1/, refusal["run"])) == 3
    checkout = Enum.at(steps, 2)
    assert checkout["uses"] == @checkout
    assert checkout["with"]["persist-credentials"] == false
    assert checkout["with"]["ref"] == "${{ github.sha }}"
    assert checkout["with"]["fetch-depth"] == 0
    assert Enum.at(steps, 3)["uses"] == @toolchain
    assert Enum.at(steps, 4)["uses"] == @beam_action_ref
    assert Enum.at(steps, 6)["run"] == "mix local.rebar --force"
    assert Enum.at(steps, 7)["run"] == "mix deps.get --check-locked"
    build = Enum.at(steps, 8)
    assert build["id"] == "archives"
    assert build["env"] == %{"GH_TOKEN" => "${{ github.token }}"}

    assert build["run"] =~
             ~s(bash scripts/release/build_candidate.sh "$RUNNER_TEMP/replicant-release")

    assert build["run"] =~ ~s(printf 'version=%s\\n' "$version" >> "$GITHUB_OUTPUT")

    refute build["run"] =~ ~r/release-assets|SHA256SUMS|\bcp\b/
    upload = Enum.at(steps, 9)
    assert upload["run"] == @upload
    assert upload["working-directory"] == "${{ runner.temp }}"

    assert upload["env"] == %{
             "PACKAGE_VERSION" => "${{ steps.archives.outputs.version }}",
             "HEX_API_KEY" => "${{ secrets.HEX_API_KEY }}"
           }

    assert Enum.at(steps, 10) == %{
             "name" => "Read back served archives and prove docs provenance without a key",
             "id" => "read_back",
             "working-directory" => "${{ runner.temp }}",
             "env" => %{"PACKAGE_VERSION" => "${{ steps.archives.outputs.version }}"},
             "run" => @retain
           }

    assert Enum.at(steps, 11) == %{
             "name" => "Retain verified release assets for infra",
             "uses" => @artifact,
             "with" => %{
               "name" => "release-assets",
               "path" => "${{ runner.temp }}/replicant-release/release-assets/",
               "if-no-files-found" => "error"
             }
           }

    assert Enum.at(steps, 12)["if"] == "always()"
    assert Enum.at(steps, 12)["run"] == ~s(rm -rf -- "$RUNNER_TEMP/replicant-release")

    without_secret =
      update_in(workflow, ["jobs", "publish", "steps", Access.at(9), "env"], fn env ->
        Map.delete(env, "HEX_API_KEY")
      end)

    refute Jason.encode!(without_secret) =~ "HEX_API_KEY"
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
    script = step["run"] || ""

    script =~ ~r/\bgh\s+api\b/ or
      script =~
        ~r{scripts/release/(?:build_candidate\.sh|package_identity\.exs)\b}
  end

  defp assert_script_credentials!(script, env) do
    refute script =~ "${{"
    refute script =~ ~r/\$\{?(?:GH_TOKEN|GITHUB_TOKEN|HEX_API_KEY)\b/

    for {name, value} <- env, is_binary(value), String.contains?(value, "secrets.") do
      refute script =~ Regex.compile!("\\$\\{?" <> Regex.escape(name) <> "\\b")
    end
  end

  defp assert_hex!(publish, qualification) do
    version = qualification["env"]["HEX_VERSION"]
    assert version =~ ~r/^\d+\.\d+\.\d+$/
    assert publish["env"]["HEX_VERSION"] == version

    for job <- [publish["jobs"]["publish"], qualification["jobs"]["release-artifact"]] do
      assert Enum.any?(job["steps"], &(&1["run"] == ~s(mix local.hex "$HEX_VERSION" --force)))
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

      if workflow["name"] == "Publish to Hex" do
        assert body =~ "timeout-minutes: 90 # no green run yet"
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

  defp assert_consumer_fetch!(source) do
    [producer, consumer] = String.split(source, ~s(  cd "$consumer"), parts: 2)
    assert_locked_fetches!(producer)
    assert producer =~ ~s([[ ! -e "$consumer/mix.lock" ]])
    refute source =~ ~r/(?:cp|ln|install|cat)[^\n]*["$]consumer\/mix\.lock/
    fetches = Regex.scan(~r/mix deps\.get[^\n]*/, consumer) |> List.flatten()
    assert fetches == ["mix deps.get >&2"]
    assert consumer =~ "mix hex.audit"
    assert consumer =~ ~s(mix deps.audit --path "$consumer")
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
        System.cmd(@reader, ["--installed", "elixir=#{expected_elixir}", "erlang=#{expected_otp}"],
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
