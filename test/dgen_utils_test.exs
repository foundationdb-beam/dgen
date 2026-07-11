defmodule DGen.UtilsTest do
  use ExUnit.Case, async: true

  describe "node_reachable/1" do
    test "the local node is always reachable" do
      assert :dgen_utils.node_reachable(node())
    end

    test "a node that is neither local nor connected is not reachable" do
      absent = :"definitely_not_connected_#{:erlang.unique_integer([:positive])}@127.0.0.1"
      refute absent in [node() | Node.list()]
      refute :dgen_utils.node_reachable(absent)
    end
  end
end
