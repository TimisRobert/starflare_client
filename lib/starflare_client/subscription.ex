defmodule StarflareClient.Subscription do
  @moduledoc false

  def new() do
    :ets.new(:subscription_table, [:duplicate_bag])
  end

  def insert_new(table, topic_name, from) do
    :ets.insert_new(table, {{topic_name, from}})
  end

  def delete(table, topic_name, from) do
    :ets.delete(table, {topic_name, from})
  end

  def find_matching(table, topic_name) do
    right = String.split(topic_name, "/")

    acc = fn {{k, t}}, acc ->
      left = String.split(k, "/")

      if topics_match?(left, right) do
        [t | acc]
      else
        acc
      end
    end

    :ets.foldl(acc, [], table)
  end

  defp topics_match?([], []), do: true
  defp topics_match?([], list) when length(list) > 0, do: false
  defp topics_match?(list, []) when length(list) > 0, do: false

  defp topics_match?([left | left_tail], [right | right_tail]) do
    case left do
      "#" ->
        true

      "+" ->
        topics_match?(left_tail, right_tail)

      _ ->
        if left === right do
          topics_match?(left_tail, right_tail)
        else
          false
        end
    end
  end
end
