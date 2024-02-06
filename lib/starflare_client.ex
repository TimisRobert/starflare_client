defmodule StarflareClient do
  @moduledoc false

  alias StarflareClient.Connection
  alias StarflareClient.Transport

  def connect(uri, opts \\ []) do
    {:ok, transport, host, port} = get_protocol(uri)
    {port, opts} = Keyword.pop(opts, :port, port)

    opts = Keyword.put_new_lazy(opts, :clientid, &random_clientid/0)

    with {:ok, connect} <- ControlPacket.Connect.new(opts),
         {:ok, _} <-
           DynamicSupervisor.start_child(
             StarflareClient.DynamicSupervisor,
             {Connection,
              connect: connect,
              transport: transport,
              host: host,
              port: port,
              name: via_tuple(connect.clientid)}
           ) do
      {:ok, connect.clientid}
    end
  end

  def async_publish(name, topic_name, payload, opts \\ []) do
    with {:ok, publish} <- ControlPacket.Publish.new(topic_name, payload, opts) do
      Connection.send_request(via_tuple(name), {:send, publish})
    end
  end

  def publish(name, topic_name, payload, opts \\ []) do
    with {:ok, publish} <- ControlPacket.Publish.new(topic_name, payload, opts) do
      Connection.call(via_tuple(name), {:send, publish})
    end
  end

  def subscribe(name, topic_filters, opts \\ []) do
    with {:ok, subscribe} <- ControlPacket.Subscribe.new(topic_filters, opts) do
      Connection.call(via_tuple(name), {:send, subscribe})
    end
  end

  def unsubscribe(name, topic_filters, opts \\ []) do
    with {:ok, unsubscribe} <- ControlPacket.Unsubscribe.new(topic_filters, opts) do
      Connection.call(via_tuple(name), {:send, unsubscribe})
    end
  end

  def disconnect(name, opts \\ []) do
    with {:ok, disconnect} <- ControlPacket.Disconnect.new(opts) do
      Connection.call(via_tuple(name), {:send, disconnect})

      [{pid, _}] = Registry.lookup(StarflareClient.Registry, name)
      DynamicSupervisor.terminate_child(StarflareClient.DynamicSupervisor, pid)
    end
  end

  defp get_protocol("mqtts://" <> host) do
    {:ok, Transport.Ssl, to_charlist(host), 8883}
  end

  defp get_protocol("mqtt://" <> host) do
    {:ok, Transport.Tcp, to_charlist(host), 1883}
  end

  defp random_clientid() do
    :crypto.strong_rand_bytes(10) |> Base.encode64(padding: false)
  end

  defp via_tuple(name) do
    {:via, Registry, {StarflareClient.Registry, name}}
  end
end
