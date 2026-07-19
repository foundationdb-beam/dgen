defmodule DGen.ConfigTest do
  use ExUnit.Case, async: false

  # `connectivity/1` governs whether a registry runs its own proactive distribution
  # mesh (§4.6, §8). Resolution follows the standard knob precedence — per-registry
  # option, then `dgen` application env, then the built-in default — with the extra
  # rule that only `provided_externally` disables the mesh; anything else (including a
  # typo) resolves to `self_managed`, so a misconfiguration fails safe.

  setup do
    # The app env is a process-external global; make sure each test starts and ends
    # without a lingering `connectivity` default from another test.
    Application.delete_env(:dgen, :connectivity)
    on_exit(fn -> Application.delete_env(:dgen, :connectivity) end)
    :ok
  end

  describe "connectivity/1" do
    test "defaults to self_managed with no option and no app env" do
      assert :dgen_config.connectivity(%{}) == :self_managed
    end

    test "a per-registry option of provided_externally is honoured" do
      assert :dgen_config.connectivity(%{connectivity: :provided_externally}) ==
               :provided_externally
    end

    test "a per-registry option of self_managed is honoured" do
      assert :dgen_config.connectivity(%{connectivity: :self_managed}) == :self_managed
    end

    test "an unrecognised value fails safe to self_managed" do
      assert :dgen_config.connectivity(%{connectivity: :provided_externaly}) == :self_managed
      assert :dgen_config.connectivity(%{connectivity: true}) == :self_managed
    end

    test "falls back to the dgen application env when the option is unset" do
      Application.put_env(:dgen, :connectivity, :provided_externally)
      assert :dgen_config.connectivity(%{}) == :provided_externally
    end

    test "a per-registry option overrides the application env" do
      Application.put_env(:dgen, :connectivity, :provided_externally)
      assert :dgen_config.connectivity(%{connectivity: :self_managed}) == :self_managed
    end

    test "an unrecognised application-env value also fails safe" do
      Application.put_env(:dgen, :connectivity, :nonsense)
      assert :dgen_config.connectivity(%{}) == :self_managed
    end
  end
end
