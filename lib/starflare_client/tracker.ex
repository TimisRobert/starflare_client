defmodule StarflareClient.Tracker do
  @moduledoc false

  def new() do
    :ets.new(:tracker, [:private, :duplicate_bag])
  end

  def insert(table, %{packet_identifier: packet_identifier} = packet, from) do
    :ets.insert(table, {packet_identifier, packet, from})
  end

  def insert(table, %{packet_identifier: packet_identifier} = packet) do
    :ets.insert(table, {packet_identifier, packet})
  end

  defdelegate delete(table, packet_identifier), to: :ets
  defdelegate take(table, packet_identifier), to: :ets
  defdelegate lookup(table, packet_identifier), to: :ets
end
