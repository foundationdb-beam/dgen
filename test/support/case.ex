defmodule DGen.Case do
  @moduledoc false
  use ExUnit.CaseTemplate

  @test_sandbox_name "DGen.Case"
  @test_case_dir_name "DGen.Case"

  def init() do
    b = :dgen_config.backend()

    {db, dir} = b.sandbox_open(@test_sandbox_name, @test_case_dir_name)
    :persistent_term.put({__MODULE__, :db}, db)
    :persistent_term.put({__MODULE__, :dir}, dir)
  end

  # The backend a test's tags say it needs, or `nil` if it does not care.
  #
  # `:mem_only` is anything driven by `eta_sched`: FoundationDB transactions expire
  # on the real clock, so a member suspended between steps dies of `tooslow` and the
  # registry retries forever. `:cluster` and `:differential` are the mirror image —
  # real peer nodes and a differential comparison both need a shared database, which
  # per-VM ETS is not.
  @requires %{mem_only: :dgen_mem, cluster: :dgen_erlfdb, differential: :dgen_erlfdb}

  # Fail fast, and say what to type.
  #
  # `test_helper.exs` excludes these tags on the wrong backend, so this never fires
  # in an ordinary run. It exists for the case where that exclusion does not apply:
  # **naming a test by line**, or `--include`-ing its tag. ExUnit turns either into
  # a filter that replaces the tag exclusions outright, so the act of naming a test
  # removes the only guard between it and the wrong backend.
  #
  # Without this the symptom is a hang rather than a failure — the eta sweep spends
  # its 15-minute timeout retrying expired FoundationDB transactions.
  defp assert_backend!(context) do
    actual = :dgen_config.backend()

    case Enum.find(@requires, fn {tag, _} -> context[tag] end) do
      {_tag, ^actual} ->
        :ok

      nil ->
        :ok

      {tag, needed} ->
        raise """
        this test is tagged `#{inspect(tag)}` and needs the #{needed} backend, \
        but the suite is running against #{actual}.

            DGEN_BACKEND=#{needed} mix test #{Path.relative_to_cwd(context[:file])}:#{context[:line]}

        `test_helper.exs` normally excludes `#{inspect(tag)}` on this backend. Naming a \
        test by line replaces those tag exclusions with a location filter, so the \
        exclusion no longer applies — see `DGen.Case.assert_backend!/1`.
        """
    end
  end

  setup context do
    assert_backend!(context)

    b = :dgen_config.backend()
    db = :persistent_term.get({__MODULE__, :db})
    dir = :persistent_term.get({__MODULE__, :dir})

    id = Base.encode16(:crypto.strong_rand_bytes(16))
    case_dir = b.dir_create(db, dir, id)

    on_exit(fn ->
      b.dir_remove(db, dir, id)
    end)

    {:ok, [tenant: {db, case_dir}]}
  end
end
