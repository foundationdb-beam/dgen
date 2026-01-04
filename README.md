# Dgen

dgen provides a distributed gen_server.

## Motivation

I love gen_server. There are only 2 things stopping me from writing my entire app with them:

1. Durability: The state is lost when the process goes down.
2. High availability: The functionality is unavailable when the process goes down.

Let's try to solve this with a distributed system, and find out if an app actually can be
written with only gen_servers.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `dgen` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:dgen, "~> 0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/dgen>.
