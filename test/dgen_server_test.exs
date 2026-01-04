defmodule DGenServer.Test do
  use DGen.Case
  doctest DGenServer

  alias DGen.DCounter

  describe "dcounter" do
    test "count from 0 to 1", context do
      tenant = context[:tenant]
      {:ok, pid1} = DCounter.start_link(tenant)
      assert 0 = DCounter.get(pid1)

      {:ok, pid2} = DCounter.start_link(tenant)
      assert 0 = DCounter.get(pid2)

      DCounter.incr(pid1)

      assert 1 = DCounter.get(pid1)
      assert 1 = DCounter.get(pid2)

      {:ok, pid3} = DCounter.start_link(tenant)
      assert 1 = DCounter.get(pid3)
    end
  end
end
