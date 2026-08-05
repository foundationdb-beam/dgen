defmodule Dgen.MixProject do
  use Mix.Project

  def project do
    [
      app: :dgen,
      version: File.read!("VERSION") |> String.trim(),
      elixir: "~> 1.15",
      elixirc_paths: elixirc_paths(Mix.env()),
      erlc_paths: erlc_paths(Mix.env()),
      erlc_options: erlc_options(Mix.env()),
      aliases: aliases(),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package(),
      name: "DGen",
      docs: docs()
    ]
  end

  def cli do
    [preferred_envs: [dst: :test]]
  end

  defp package() do
    [
      description: "Distributed gen_server backed by key-value stores",
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => "https://github.com/foundationdb-beam/dgen"
      },
      files: [
        "lib",
        "src",
        "include",
        "mix.exs",
        "rebar.config",
        "VERSION",
        "README.md",
        "CHANGELOG.md",
        "LICENSE.md"
      ]
    ]
  end

  defp docs do
    [
      main: "readme",
      source_url: "https://github.com/foundationdb-beam/dgen",
      extras: [
        "README.md",
        "CHANGELOG.md",
        "LICENSE.md",
        "docs/design/dgen_server_design.md",
        "docs/design/dgen_registry_design.md",
        "docs/getting_started/dgen-intro.livemd",
        "docs/getting_started/dgen-registry-intro.livemd",
        "docs/getting_started/presence-demo.livemd",
        {"formal/README.md",
         [filename: "dgen_registry_formal", title: "dgen_registry Formal Model"]}
      ],
      groups_for_extras: [
        "Getting Started": ~r/getting_started/,
        Design: ~r/design/,
        "Formal Methods": ~r/formal/
      ]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp erlc_paths(_), do: ["src"]

  defp erlc_options(:test), do: [:debug_info, {:d, :DST}] ++ mutation()
  defp erlc_options(_), do: [:debug_info]

  # A deliberately planted defect, for asking the eta framework to rediscover a bug
  # we already understand.
  #
  #     DGEN_MUTATION=partial_batch mix compile --force
  #     DGEN_MUTATION=partial_batch DGEN_BACKEND=dgen_mem mix test --only mutation
  #
  # `--force` is not optional. Mix does not reliably rebuild an Erlang module when
  # only its compiler options change, so toggling this without it leaves the suite
  # running yesterday's code — the same hazard the parse transforms have, and it
  # fails in the direction that looks like success.
  #
  # Test builds only, so a mutation can never reach a release.
  defp mutation do
    case System.get_env("DGEN_MUTATION") do
      nil -> []
      "" -> []
      name -> [{:d, :"MUTATION_#{String.upcase(name)}"}]
    end
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:erlfdb, "~> 1.0", optional: true},
      {:eta, git: "https://github.com/jessestimpson/eta.git", only: :test},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40", only: :dev, runtime: false}
    ]
  end

  defp aliases do
    [
      dst: &dst/1,
      lint: [
        "format --check-formatted",
        "cmd rebar3 fmt --check",
        "deps.unlock --check-unused",
        "dialyzer",
        "docs --warnings-as-errors"
      ]
    ]
  end

  # The deterministic simulation suite: every `:simulation` test, on the in-memory
  # backend.
  #
  #     mix dst                 # the whole simulation suite
  #     mix dst --seed 0        # extra arguments pass through to `mix test`
  defp dst(args) do
    System.put_env("DGEN_BACKEND", "dgen_mem")
    Mix.Task.run("test", ["--only", "simulation" | args])
  end
end
