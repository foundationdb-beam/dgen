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

  setup do
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
