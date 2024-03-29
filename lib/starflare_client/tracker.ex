defmodule StarflareClient.Tracker do
  @moduledoc false

  def new() do
    :ets.new(:tracker_table, [:duplicate_bag])
  end

  def insert(table, %{packet_identifier: packet_identifier} = packet, from) do
    :ets.insert(table, {packet_identifier, packet, from})
  end

  def insert(table, %{packet_identifier: packet_identifier} = packet) do
    :ets.insert(table, {packet_identifier, packet})
  end

  def lookup_packet(table, module) do
    acc = fn
      {_, %{__struct__: ^module} = packet, from}, acc -> [{packet, from} | acc]
      {_, %{__struct__: ^module} = packet}, acc -> [{packet} | acc]
      _, acc -> acc
    end

    :ets.foldl(acc, [], table)
  end

  defdelegate delete(table, packet_identifier), to: :ets
  defdelegate take(table, packet_identifier), to: :ets
  defdelegate lookup(table, packet_identifier), to: :ets
end
