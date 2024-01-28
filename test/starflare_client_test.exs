defmodule StarflareClientTest do
  use ExUnit.Case
  doctest StarflareClient

  test "greets the world" do
    assert StarflareClient.hello() == :world
  end
end
