defmodule StarflareClient do
  @moduledoc false

  alias StarflareClient.{Connection, Transport}

  def connect(uri, opts \\ []) do
    {:ok, transport, host, port} = get_protocol(uri)

    {port, opts} = Keyword.pop(opts, :port, port)

    connect = struct(ControlPacket.Connect, opts)

    Connection.start_link(
      connect: connect,
      transport: transport,
      host: host,
      port: port
    )
  end

  def async_publish(pid, topic_name, payload, opts \\ []) do
    publish = create_publish(topic_name, payload, opts)
    Connection.send_request(pid, {:send, publish})
  end

  def publish(pid, topic_name, payload, opts \\ []) do
    publish = create_publish(topic_name, payload, opts)
    Connection.call(pid, {:send, publish})
  end

  def subscribe(pid, topic_filters, opts \\ []) do
    subscribe = create_subscribe(topic_filters, opts)
    Connection.call(pid, {:send, subscribe})
  end

  def unsubscribe(pid, topic_filters, opts \\ []) do
    unsubscribe = create_unsubscribe(topic_filters, opts)
    Connection.call(pid, {:send, unsubscribe})
  end

  defp create_unsubscribe(topic_filters, opts) do
    %ControlPacket.Unsubscribe{
      topic_filters: topic_filters,
      properties: opts
    }
  end

  defp create_subscribe(topic_filters, opts) do
    %ControlPacket.Subscribe{
      topic_filters: topic_filters,
      properties: opts
    }
  end

  defp create_publish(topic_name, payload, opts) do
    {qos_level, opts} = Keyword.pop(opts, :qos_level, :at_least_once)
    {retain, opts} = Keyword.pop(opts, :retain, false)

    %ControlPacket.Publish{
      topic_name: topic_name,
      payload: payload,
      qos_level: qos_level,
      retain: retain,
      properties: opts
    }
  end

  defp get_protocol("mqtts://" <> host) do
    {:ok, Transport.Ssl, to_charlist(host), 8883}
  end

  defp get_protocol("mqtt://" <> host) do
    {:ok, Transport.Tcp, to_charlist(host), 1883}
  end

  defp get_protocol(host) do
    {:ok, Transport.Tcp, to_charlist(host), nil}
  end
end
