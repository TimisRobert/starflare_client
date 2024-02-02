defmodule StarflareClient do
  @moduledoc false

  alias StarflareClient.{Connection, Transport}

  def connect(uri, opts \\ []) do
    {:ok, transport, host, port} = get_protocol(uri)
    {port, opts} = Keyword.pop(opts, :port, port)

    with {:ok, connect} <- ControlPacket.Connect.new(opts) do
      Connection.start_link(
        connect: connect,
        transport: transport,
        host: host,
        port: port
      )
    end
  end

  def async_publish(pid, topic_name, payload, opts \\ []) do
    with {:ok, publish} <- ControlPacket.Publish.new(topic_name, payload, opts) do
      Connection.send_request(pid, {:send, publish})
    end
  end

  def publish(pid, topic_name, payload, opts \\ []) do
    with {:ok, publish} <- ControlPacket.Publish.new(topic_name, payload, opts) do
      Connection.call(pid, {:send, publish})
    end
  end

  def subscribe(pid, topic_filters, opts \\ []) do
    with {:ok, subscribe} <- ControlPacket.Subscribe.new(topic_filters, opts) do
      Connection.call(pid, {:send, subscribe})
    end
  end

  def unsubscribe(pid, topic_filters, opts \\ []) do
    with {:ok, unsubscribe} <- ControlPacket.Unsubscribe.new(topic_filters, opts) do
      Connection.call(pid, {:send, unsubscribe})
    end
  end

  defp get_protocol("mqtts://" <> host) do
    {:ok, Transport.Ssl, to_charlist(host), 8883}
  end

  defp get_protocol("mqtt://" <> host) do
    {:ok, Transport.Tcp, to_charlist(host), 1883}
  end
end
