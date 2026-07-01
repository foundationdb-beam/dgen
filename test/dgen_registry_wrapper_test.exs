defmodule DGen.RegistryWrapperTest do
  use ExUnit.Case, async: true

  # DGen.Registry is a thin Elixir facade over :dgen_registry — a defdelegate
  # for every public function. It has drifted out of sync with the Erlang
  # module before (the entire metadata/query API was added on the Erlang side
  # and simply never carried over), with nothing to catch it. This test walks
  # :dgen_registry's actual export list and asserts DGen.Registry exposes a
  # matching {name, arity} for each one, so a newly added Erlang-side function
  # can't be silently forgotten on the Elixir side again.
  #
  # @exclusions is a small, explicit list of *purposeful* deviations — things
  # :dgen_registry exports that are not meant to be part of the Elixir-facing
  # public API. Anything added here should be a deliberate, reviewed choice,
  # not a way to make a genuine gap disappear.
  @exclusions [
    # BEAM auto-generates these for every compiled module; reflection
    # utilities, not part of the registry's own API.
    {:module_info, 0},
    {:module_info, 1},
    # The supervisor init callback, invoked by the :supervisor behaviour
    # itself — never called directly by application code.
    {:init, 1}
  ]

  test "DGen.Registry delegates every public dgen_registry function" do
    Code.ensure_loaded!(:dgen_registry)
    Code.ensure_loaded!(DGen.Registry)

    expected =
      :dgen_registry.module_info(:exports)
      |> Enum.reject(&(&1 in @exclusions))

    missing =
      Enum.reject(expected, fn {name, arity} ->
        function_exported?(DGen.Registry, name, arity)
      end)

    assert missing == [],
           "DGen.Registry is missing a delegate for: #{inspect(missing)}. " <>
             "Add it to lib/dgen/registry.ex, or if it is a purposeful " <>
             "deviation, add it to @exclusions above with a comment explaining why."
  end
end
