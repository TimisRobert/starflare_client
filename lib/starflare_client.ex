defmodule StarflareClient do
  @moduledoc false

  alias StarflareClient.Connection

  def publish(pid, topic_name, payload, opts \\ []) do
    {qos_level, opts} = Keyword.pop(opts, :qos_level, :at_least_once)
    {retain, opts} = Keyword.pop(opts, :retain, false)

    publish = %ControlPacket.Publish{
      topic_name: topic_name,
      payload: payload,
      qos_level: qos_level,
      retain: retain,
      properties: Enum.into(opts, %{})
    }

    Connection.call(pid, {:send, publish})
  end
end
