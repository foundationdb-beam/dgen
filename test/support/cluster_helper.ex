defmodule DGen.ClusterHelper do
  @moduledoc false

  @doc """
  Starts a `dgen_registry` on the calling node, connecting to the same FDB
  keyspace described by `cluster_file` and `dir_path`.

  Intended to be invoked on a peer node via `:erpc.call/4`.

  * `reg_name`     — atom name for the registry
  * `cluster_file` — absolute path to the FDB cluster file (binary)
  * `dir_path`     — serialisable erlfdb directory path as returned by
                     `:erlfdb_directory.get_path/1`: a list of
                     `{:utf8, binary}` tuples

  The `dir_path` must already exist in FDB (created by the primary during
  test setup); this function opens it with `create_or_open` so the peer
  shares the exact same key prefix.
  """
  def start_registry(reg_name, cluster_file, dir_path) do
    db = :erlfdb.open(cluster_file)
    root = :erlfdb_directory.root(node_prefix: <<0xFE>>, content_prefix: <<>>)
    dir = :erlfdb_directory.create_or_open(db, root, dir_path)

    # We MUST NOT call dgen_registry.start_link/2 directly here.
    # :erpc.call runs this function in a temporary process P that exits with
    # {Ref, :return, Result} when done.  supervisor:start_link links the
    # supervisor to its caller, so P's exit would propagate to the supervisor
    # and kill it.
    #
    # Instead we spawn a long-lived keeper process that owns the link.
    parent = self()
    ref = make_ref()

    spawn(fn ->
      result = :dgen_registry.start_link(reg_name, {db, dir})
      send(parent, {ref, result})
      # Remain alive so the supervisor stays linked to a live process.
      Process.sleep(:infinity)
    end)

    receive do
      {^ref, {:ok, _}} ->
        :ok

      {^ref, {:error, {:already_started, _}}} ->
        :ok

      {^ref, {:error, reason}} ->
        raise "start_registry failed: #{inspect(reason)}"
    after
      10_000 ->
        raise "start_registry timed out"
    end
  end

  @doc """
  Blocks until the registry's elector has elected a leader, then returns
  the `{node, member_name}` leader id.  Raises on timeout.
  """
  def await_leader!(reg_name, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_await_leader!(reg_name, deadline)
  end

  defp do_await_leader!(reg_name, deadline) do
    case :dgen_registry.get_leader(reg_name) do
      :undefined ->
        if System.monotonic_time(:millisecond) < deadline do
          Process.sleep(20)
          do_await_leader!(reg_name, deadline)
        else
          raise "timed out waiting for leader in #{inspect(reg_name)}"
        end

      leader ->
        leader
    end
  end

  @doc """
  Polls `fun.()` (on the calling node) until it returns truthy or the
  deadline passes.  Returns `true` on success, `false` on timeout.
  """
  def eventually(fun, timeout \\ 2_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_eventually(fun, deadline)
  end

  defp do_eventually(fun, deadline) do
    if fun.() do
      true
    else
      if System.monotonic_time(:millisecond) < deadline do
        Process.sleep(20)
        do_eventually(fun, deadline)
      else
        false
      end
    end
  end

  @doc """
  Blocks until the registry on the calling node is ready to accept
  registrations (i.e. the member knows its leader).  Uses a probe
  registration that is immediately unregistered on success.
  """
  def await_registry_ready!(reg_name, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    probe_pid = :erlang.spawn(:timer, :sleep, [:infinity])

    try do
      do_await_ready!(reg_name, probe_pid, deadline)
    after
      :erlang.exit(probe_pid, :kill)
    end
  end

  defp do_await_ready!(reg_name, probe_pid, deadline) do
    case :dgen_registry.register_name({reg_name, :__probe__}, probe_pid) do
      :yes ->
        :dgen_registry.unregister_name({reg_name, :__probe__})
        :ok

      _ ->
        if System.monotonic_time(:millisecond) < deadline do
          Process.sleep(20)
          do_await_ready!(reg_name, probe_pid, deadline)
        else
          raise "timed out waiting for #{inspect(reg_name)} to become ready"
        end
    end
  end

  @doc """
  Stops the peer node, swallowing any exit/noproc errors that arise from
  the test process already being torn down.
  """
  def stop_peer(peer_pid) do
    try do
      :peer.stop(peer_pid)
    catch
      :exit, _ -> :ok
    end
  end
end
