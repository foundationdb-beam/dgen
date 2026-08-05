# DGen

[![CI](https://github.com/foundationdb-beam/dgen/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/foundationdb-beam/dgen/actions/workflows/ci.yml)
[![Formal Methods](https://github.com/foundationdb-beam/dgen/actions/workflows/formal.yml/badge.svg?branch=main)](https://github.com/foundationdb-beam/dgen/actions/workflows/formal.yml)
[![Hex.pm](https://img.shields.io/hexpm/v/dgen.svg)](https://hex.pm/packages/dgen)
[![Hex Docs](https://img.shields.io/badge/hex-docs-lightgrey.svg)](https://hexdocs.pm/dgen)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE.md)

DGen implements useful OTP building blocks — `gen_server`, a process registry,
and more to come — as durable, highly-available primitives for a distributed
system, with a focus on **ease of use** and **minimal operator setup**. If you
already know `gen_server` or a process registry like `:global` or `gproc`,
using DGen's equivalents should feel familiar; the difference is what's
underneath: a strongly-consistent database (FoundationDB) instead of process
memory, so state and coordination survive process, node, and even cluster
restarts.

## What's here

- **`dgen_server`** — the `gen_server` programming model (same callbacks, same
  return shapes), backed by durable state and a durable message queue instead
  of process memory. Any number of processes, on any number of nodes, can
  consume from the same server concurrently. See the
  [dgen_server design doc](docs/design/dgen_server_design.md).
- **`dgen_registry`** — an OTP-compatible process registry: give a running
  process a logical name and find or message it by name from anywhere in the
  cluster, via the standard `{via, dgen_registry, {RegistryName, LogicalName}}`
  contract. See the
  [dgen_registry design doc](docs/design/dgen_registry_design.md).

Both are built on the same idea: push state and coordination into a
strongly-consistent backend, so the OTP programming model you already know
keeps working even when a process, a node, or the whole cluster restarts.

## Installation

<!-- tabs-open -->

### Erlang

DGen can be installed by adding `dgen` to your list of dependencies in
`rebar.config`:

```erlang
{deps, [
    {dgen, "0.4.x"}
]}.
```

Find the most recent version number on [Hex](hex.pm/packages/dgen).

### Elixir

DGen can be installed by adding `dgen` to your list of dependencies in
`mix.exs`:

```elixir
def deps do
  [
    {:dgen, "~> 0.4"}
  ]
end
```

<!-- tabs-close -->

## Documentation

- [dgen_server design doc](docs/design/dgen_server_design.md) — programming
  model, message processing, locking, persisted state, crash behaviour,
  guarantees, and configuration.
- [dgen_registry design doc](docs/design/dgen_registry_design.md) —
  consistency model, replication, leadership handoff, guarantees, and
  configuration.
- [formal/README.md](formal/README.md) — the TLA+ model of `dgen_registry`'s
  replication protocol.
- [`eta`](https://github.com/foundationdb-beam/eta) — `dgen_registry` is partally
  tested with Deterministic Simulation Testing, to assist in verification
  of the actual implementation of the TLA+ model, including its invariants.
  The framework for driving the simulation was extracted to a separate
  library for general use.
- API reference and getting-started guides: <https://hexdocs.pm/dgen>.
- [CHANGELOG.md](CHANGELOG.md) — release notes.

## AI disclosure

DGen's development makes deliberate, disclosed use of AI tooling:

- **Implementation.** Substantial portions of the codebase were built with LLMs
  from initial implementation through refactoring and documentation.
- **Correctness.** LLM output is verified by various testing efforts, including
  unit and integration testing, simulation testing, and formal methods.
- **Design.** The architecture, API surface, and consistency trade-offs are
  human-opinionated, shaped by hands-on experience operating comparable
  distributed systems — leader election, durable queues, CP-oriented
  registries — not left to a model's defaults.
