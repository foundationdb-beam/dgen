defmodule Dgen.MixProject do
  use Mix.Project

  def project do
    [
      app: :dgen,
      version: File.read!("VERSION") |> String.trim(),
      elixir: "~> 1.15",
      elixirc_paths: elixirc_paths(Mix.env()),
      aliases: aliases(),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package(),
      name: "DGen",
      docs: docs()
    ]
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

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:erlfdb, "~> 1.0", optional: true},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40", only: :dev, runtime: false}
    ]
  end

  defp aliases do
    [
      lint: [
        "format --check-formatted",
        "cmd rebar3 fmt --check",
        "deps.unlock --check-unused",
        "dialyzer",
        "docs --warnings-as-errors"
      ]
    ]
  end
end
