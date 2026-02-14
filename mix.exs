defmodule Dgen.MixProject do
  use Mix.Project

  def project do
    [
      app: :dgen,
      version: "0.1.0",
      elixir: "~> 1.15",
      elixirc_paths: elixirc_paths(Mix.env()),
      aliases: aliases(),
      start_permanent: Mix.env() == :prod,
      deps: deps()
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
      {:erlfdb, "~> 0.3"},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false}
    ]
  end

  defp aliases do
    [
      lint: [
        "format --check-formatted",
        "cmd rebar3 fmt --check",
        "deps.unlock --check-unused",
        "dialyzer --format short"
      ]
    ]
  end
end
