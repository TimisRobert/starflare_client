defmodule StarflareClient do
  @moduledoc false

  alias StarflareClient.Connection
  alias StarflareClient.Transport

  def connect(uri, opts \\ []) do
    {:ok, transport, host, port} = get_protocol(uri)
    {port, opts} = Keyword.pop(opts, :port, port)

    opts = Keyword.put_new_lazy(opts, :clientid, &random_clientid/0)

    connect = struct!(ControlPacket.Connect, opts)

    with {:ok, _} <-
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
    opts = Keyword.merge(opts, topic_name: topic_name, payload: payload)
    publish = struct!(ControlPacket.Publish, opts)
    Connection.send_request(via_tuple(name), {:send, publish})
  end

  def publish(name, topic_name, payload, opts \\ []) do
    opts = Keyword.merge(opts, topic_name: topic_name, payload: payload)
    publish = struct!(ControlPacket.Publish, opts)
    Connection.call(via_tuple(name), {:send, publish})
  end

  def subscribe(name, topic_filters, opts \\ []) do
    topic_filters =
      Enum.map(topic_filters, fn
        topic_filter when is_tuple(topic_filter) -> topic_filter
        topic_filter -> {topic_filter, []}
      end)

    opts = Keyword.merge(opts, topic_filters: topic_filters)
    subscribe = struct!(ControlPacket.Subscribe, opts)
    Connection.call(via_tuple(name), {:send, subscribe})
  end

  def unsubscribe(name, topic_filters, opts \\ []) do
    opts = Keyword.merge(opts, topic_filters: topic_filters)
    unsubscribe = struct!(ControlPacket.Unsubscribe, opts)
    Connection.call(via_tuple(name), {:send, unsubscribe})
  end

  def disconnect(name, opts \\ []) do
    disconnect = struct!(ControlPacket.Disconnect, opts)
    Connection.call(via_tuple(name), {:send, disconnect})
    [{pid, _}] = Registry.lookup(StarflareClient.Registry, name)
    DynamicSupervisor.terminate_child(StarflareClient.DynamicSupervisor, pid)
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
