defmodule Kino do
  @moduledoc """
  Headless test-only stub of the parts of [Kino](https://hexdocs.pm/kino) that
  `notebooks/getting_started.livemd` calls, so the notebook's code cells can be
  extracted and evaluated by `Replicant.Integration.LivebookGettingStartedTest`
  WITHOUT a running Livebook server.

  This stub exists ONLY in `test/support` (compiled in `:test`). A real Livebook
  reader gets the real `Kino` via the notebook's `Mix.install/1` setup cell; that
  cell is dropped by the extraction test, so in the test VM these modules ARE the
  `Kino.*` the cells resolve against. Only presentation is stubbed — the notebook's
  load-bearing CDC logic runs against the real `Replicant` pipeline + live Postgres.
  """
end

defmodule Kino.Input do
  @moduledoc "Headless stub of `Kino.Input` (see `Kino`)."

  @doc """
  A text input whose stored value is its `:default`. In the test VM the notebook
  defaults the DB URL to `REPLICANT_TEST_URL`, so `read/1` returns the live URL
  without any Livebook interaction.
  """
  def text(_label, opts \\ []) do
    %{__kino_stub__: :input, value: Keyword.get(opts, :default, "")}
  end

  @doc "Read a stubbed input's value (its `:default`)."
  def read(%{__kino_stub__: :input, value: value}), do: value
end

defmodule Kino.DataTable do
  @moduledoc "Headless stub of `Kino.DataTable` (see `Kino`)."

  @doc "Wrap tabular data for (stubbed) display; the notebook never reads this back."
  def new(data, _opts \\ []), do: %{__kino_stub__: :data_table, data: data}
end

defmodule Kino.Markdown do
  @moduledoc "Headless stub of `Kino.Markdown` (see `Kino`)."

  @doc "Wrap markdown for (stubbed) display."
  def new(markdown), do: %{__kino_stub__: :markdown, markdown: markdown}
end
