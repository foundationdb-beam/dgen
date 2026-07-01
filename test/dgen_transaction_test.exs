defmodule DGen.TransactionTest do
  use DGen.Case, async: true

  # ---------------------------------------------------------------------------
  # A minimal dgen_transaction callback module.
  #
  # The body is parameterised by the init args so each test drives a different
  # lifecycle path (commit / abort) through the same module.
  # ---------------------------------------------------------------------------
  defmodule Cb do
    @behaviour :dgen_transaction

    @impl true
    def init(args), do: {:ok, args}

    @impl true
    # Abort path: never touches the transaction, never commits.
    def handle_begin(_tx, %{action: :abort, reason: reason} = st) do
      {:stop, reason, st}
    end

    # Commit path: the directory is carried in the callback state, not passed by
    # dgen_transaction.  Write each {key, value} pair, then commit.
    def handle_begin(tx, %{action: :commit, dir: dir, writes: writes} = st) do
      b = :dgen_config.backend()

      Enum.each(writes, fn {key, val} ->
        b.set(tx, b.dir_pack(dir, {key}), val)
      end)

      {:commit, st}
    end

    # Read-then-commit path: a real read (so the pinned read version is actually
    # used and can raise transaction_too_old), then a write, then commit.  Counts
    # body attempts so a test can assert that a retry happened.
    def handle_begin(tx, %{action: :read_commit, dir: dir, key: key, val: val} = st) do
      :counters.add(st.attempts, 1, 1)
      b = :dgen_config.backend()
      _ = b.wait(b.get(tx, b.dir_pack(dir, {key})))
      b.set(tx, b.dir_pack(dir, {key}), val)
      {:commit, st}
    end

    @impl true
    def handle_committed(committed_version, _st) do
      {:ok, {:cv, committed_version}}
    end
  end

  # Callback that raises a retryable FDB error inside handle_begin on its first
  # attempt (as a stale pinned read version would), then commits on retry.  Used
  # to exercise body-level retry routing through handle_conflict/on_error.
  defmodule RetryCb do
    @behaviour :dgen_transaction

    @impl true
    def init(args), do: {:ok, args}

    @impl true
    def handle_begin(tx, %{dir: dir, key: key, val: val, attempts: attempts} = st) do
      n = :counters.get(attempts, 1)
      :counters.add(attempts, 1, 1)

      if n == 0 do
        # 1007 = transaction_too_old, a retryable error.
        :erlang.error({:erlfdb_error, 1007})
      else
        b = :dgen_config.backend()
        b.set(tx, b.dir_pack(dir, {key}), val)
        {:commit, st}
      end
    end

    @impl true
    def handle_committed(committed_version, _st) do
      {:ok, {:cv, committed_version}}
    end
  end

  # Read a key written under the case directory, outside the worker.
  defp read(tenant, key) do
    b = :dgen_config.backend()

    :dgen_backend.transactional(tenant, fn {tx, dir} ->
      case b.wait(b.get(tx, b.dir_pack(dir, {key}))) do
        :not_found -> :undefined
        val -> val
      end
    end)
  end

  describe "commit" do
    test "writes are durable and the committed version is returned", %{tenant: {db, dir} = tenant} do
      reply =
        :dgen_transaction.run(
          Cb,
          %{action: :commit, dir: dir, writes: [{<<"a">>, <<"1">>}, {<<"b">>, <<"2">>}]},
          db: db
        )

      assert {:committed, {:cv, cv}} = reply
      assert is_integer(cv) and cv > 0
      assert read(tenant, <<"a">>) == <<"1">>
      assert read(tenant, <<"b">>) == <<"2">>
    end

    test "a pinned (cached) read version still commits", %{tenant: {db, dir} = tenant} do
      # First commit gives us a recent, valid version to pin.
      {:committed, {:cv, cv}} =
        :dgen_transaction.run(
          Cb,
          %{action: :commit, dir: dir, writes: [{<<"seed">>, <<"x">>}]},
          db: db
        )

      reply =
        :dgen_transaction.run(
          Cb,
          %{action: :commit, dir: dir, writes: [{<<"pinned">>, <<"y">>}]},
          db: db,
          read_version: cv
        )

      assert {:committed, {:cv, cv2}} = reply
      assert is_integer(cv2)
      assert read(tenant, <<"pinned">>) == <<"y">>
    end
  end

  describe "abort" do
    test "stop directive aborts without writing", %{tenant: {db, _dir}} do
      reply =
        :dgen_transaction.run(
          Cb,
          %{action: :abort, reason: :nope},
          db: db
        )

      assert reply == {:aborted, :nope}
    end
  end

  describe "body-level retry" do
    test "a retryable error raised in handle_begin is retried, not fatal", %{
      tenant: {db, dir} = tenant
    } do
      attempts = :counters.new(1, [])

      reply =
        :dgen_transaction.run(
          RetryCb,
          %{dir: dir, key: <<"retried">>, val: <<"ok">>, attempts: attempts},
          db: db
        )

      assert {:committed, {:cv, _}} = reply
      # Body ran twice: the raising first attempt, then the committing retry.
      assert :counters.get(attempts, 1) == 2
      assert read(tenant, <<"retried">>) == <<"ok">>
    end

    # End-to-end version of the above against real FoundationDB: pin a read
    # version older than the ~5s MVCC window so the body's read genuinely raises
    # transaction_too_old, and assert the worker recovers with a fresh GRV.
    # Slow by construction (sleeps past the window); kept as a real regression for
    # the cached-GRV fallback the registry leader depends on.
    @tag :slow
    @tag timeout: 30_000
    test "a read pinned to a >5s-old version retries with a fresh GRV", %{
      tenant: {db, dir} = tenant
    } do
      # A real committed version to pin as the (soon-to-be-stale) read version.
      {:committed, {:cv, old_version}} =
        :dgen_transaction.run(
          Cb,
          %{action: :commit, dir: dir, writes: [{<<"grv_seed">>, <<"0">>}]},
          db: db
        )

      # Let it age past FoundationDB's ~5s MVCC window.
      Process.sleep(6_000)

      attempts = :counters.new(1, [])

      reply =
        :dgen_transaction.run(
          Cb,
          %{
            action: :read_commit,
            dir: dir,
            key: <<"grv_stale">>,
            val: <<"1">>,
            attempts: attempts
          },
          db: db,
          read_version: old_version
        )

      assert {:committed, {:cv, _}} = reply
      # First attempt read at the stale version (too_old) and retried; second
      # attempt read at a fresh GRV and committed.
      assert :counters.get(attempts, 1) == 2
      assert read(tenant, <<"grv_stale">>) == <<"1">>
    end
  end

  describe "result delivery" do
    test "the owner receives {dgen_transaction, ref, reply}", %{tenant: {db, dir}} do
      ref = make_ref()

      {:ok, _pid} =
        :dgen_transaction.start(
          Cb,
          %{action: :commit, dir: dir, writes: [{<<"c">>, <<"3">>}]},
          db: db,
          owner: self(),
          ref: ref
        )

      assert_receive {:dgen_transaction, ^ref, {:committed, {:cv, _}}}, 5_000
    end
  end
end
