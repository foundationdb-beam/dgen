DGen.Case.init()

# Enable Erlang distribution so that cluster tests can spawn peer nodes via
# the :peer module.  Longnames with 127.0.0.1 avoid hostname-resolution
# issues.  Using the OS PID prevents collisions between concurrent runs.
if !Node.alive?() do
  node_name = :"dgen_test_#{:os.getpid()}@127.0.0.1"
  {:ok, _} = Node.start(node_name, :longnames)
end

ExUnit.start(capture_log: true)
