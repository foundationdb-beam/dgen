# Which backend the suite runs against. `dgen_mem` is a pure-Erlang, in-memory
# implementation, so:
#
#     DGEN_BACKEND=dgen_mem mix test
#
# runs without FoundationDB installed at all. It is per-VM ETS rather than a shared
# database, so the `:cluster` tests — which start real peer nodes that open FDB
# themselves — cannot run against it and are excluded below.
backend = System.get_env("DGEN_BACKEND", "dgen_erlfdb") |> String.to_atom()
Application.put_env(:dgen, :backend, backend)

DGen.Case.init()

# Enable Erlang distribution so that cluster tests can spawn peer nodes via
# the :peer module.  Longnames with 127.0.0.1 avoid hostname-resolution
# issues.  Using the OS PID prevents collisions between concurrent runs.
if !Node.alive?() do
  System.cmd("epmd", ["-daemon"])
  node_name = :"dgen_test_#{:os.getpid()}@127.0.0.1"
  {:ok, _} = Node.start(node_name, :longnames)
end

# `:cluster` starts real peer nodes that open the backend themselves, and
# `:differential` compares a backend against `dgen_erlfdb` by opening the FDB
# sandbox directly. Both need FoundationDB, so both are excluded on any other
# backend — otherwise the run that is supposed to prove no FDB is needed is the
# one that hangs waiting for it.
# `:mem_only` is the mirror image: FoundationDB transactions expire on the real
# clock, so a process suspended mid-transaction dies of `tooslow`. Anything driven
# by `eta_sched` therefore needs the deterministic backend.
exclusions =
  if backend == :dgen_erlfdb, do: [:mem_only], else: [:cluster, :differential]

# `:mutation` needs a build with a defect deliberately compiled into it, so it is
# meaningless — and fails — in a normal one. This is the one place opting *in* is
# right: the tests do not describe the code in this build.
#
#     DGEN_MUTATION=partial_batch mix compile --force
#     DGEN_MUTATION=partial_batch DGEN_BACKEND=dgen_mem mix test --only mutation
exclusions = exclusions ++ [:mutation]

# The mirror-image hazard, guarded here because it actually happened: an
# interrupted mutation workflow leaves `_build/test` holding beams with the defect
# still compiled in — Mix does not recompile when only `DGEN_MUTATION` changes —
# and every subsequent plain `mix test` then runs against a bug planted on
# purpose. The mutation suite's own setup guards the opposite direction (flunks if
# the define is *absent*); this is the only place that can flunk its *presence*.
if System.get_env("DGEN_MUTATION") == nil do
  {:module, _} = Code.ensure_loaded(:dgen_registry_member)

  mutation_defines =
    for {:d, d} <-
          :dgen_registry_member.module_info(:compile) |> Keyword.get(:options, []),
        d |> Atom.to_string() |> String.starts_with?("MUTATION"),
        do: d

  if mutation_defines != [] do
    raise """
    _build/test holds a MUTATED build (#{inspect(mutation_defines)}) but DGEN_MUTATION \
    is not set — a previous mutation run left its planted defect in the build cache.
    Recompile clean before testing:

        MIX_ENV=test mix compile --force
    """
  end
end

# `:simulation` is the multi-seed fault-injection soak. It needs `dgen_mem` and
# takes minutes, so it has its own entry point rather than riding on `mix test`:
#
#     mix dst
exclusions = exclusions ++ [:simulation]

ExUnit.start(capture_log: true, exclude: exclusions)
