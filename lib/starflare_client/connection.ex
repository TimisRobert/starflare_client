defmodule StarflareClient.Connection do
  @moduledoc false

  alias StarflareClient.Subscription
  alias StarflareClient.Tracker
  require Logger

  @behaviour :gen_statem

  defstruct host: nil,
            port: nil,
            connect: nil,
            socket: nil,
            transport: nil,
            buffer: [],
            buffer_size: 0,
            packet_identifiers: [],
            tracker_table: nil,
            subscription_table: nil

  def child_spec(init_arg) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [init_arg]}
    }
  end

  def start_link(opts) do
    {connect, opts} = Keyword.pop!(opts, :connect)
    {transport, opts} = Keyword.pop!(opts, :transport)
    {host, opts} = Keyword.pop!(opts, :host)
    {port, opts} = Keyword.pop!(opts, :port)

    data = %__MODULE__{
      connect: connect,
      transport: transport,
      host: host,
      port: port
    }

    {name, opts} = Keyword.pop!(opts, :name)
    :gen_statem.start_link(name, __MODULE__, data, opts)
  end

  defdelegate call(pid, term), to: :gen_statem
  defdelegate send_request(pid, term), to: :gen_statem
  defdelegate get_state(pid), to: :sys

  @impl true
  def terminate(:shutdown, _state, data) do
    %__MODULE__{socket: socket, transport: transport} = data
    transport.close(socket)
    :ok
  end

  def terminate(_reason, _state, _data) do
    :ok
  end

  @impl true
  def callback_mode, do: [:handle_event_function, :state_enter]

  @impl true
  def init(data) do
    Process.flag(:trap_exit, true)

    tracker_table = Tracker.new()
    subscription_table = Subscription.new()
    data = %{data | tracker_table: tracker_table, subscription_table: subscription_table}

    {:ok, :disconnected, data}
  end

  @impl true
  def handle_event(:enter, :disconnected, :disconnected, _data) do
    {:keep_state_and_data, {:state_timeout, 0, :connect}}
  end

  def handle_event(:enter, _, :terminating, _data) do
    :keep_state_and_data
  end

  def handle_event(:enter, _state, :disconnected, data) do
    %__MODULE__{socket: socket, transport: transport} = data
    transport.close(socket)
    {:keep_state, data, {:state_timeout, :timer.seconds(1), :connect}}
  end

  def handle_event(:enter, :disconnected, :connecting, _) do
    {:keep_state_and_data, {:state_timeout, :timer.seconds(5), :disconnect}}
  end

  def handle_event(:enter, {:connected, :normal}, {:connected, :heartbeat}, _data) do
    {:keep_state_and_data, {:state_timeout, :timer.seconds(5), :disconnect}}
  end

  def handle_event(:enter, _, _, data) do
    %__MODULE__{connect: connect} = data
    {:keep_state_and_data, {:timeout, :timer.seconds(connect.keep_alive), :ping}}
  end

  def handle_event(:internal, :connect, :connecting, data) do
    %__MODULE__{
      connect: connect,
      transport: transport,
      host: host,
      port: port
    } = data

    opts = [:binary, packet: :raw, active: :once]

    case transport.connect(host, port, opts) do
      {:ok, socket} ->
        data = %{data | socket: socket}

        case send_packet(data, connect) do
          :ok ->
            {:next_state, :connecting, data}

          {:encoding_error, error} ->
            {:stop, error}

          {:connection_error, _error} ->
            {:next_state, :disconnected, data}
        end

      {:error, reason} ->
        {:stop, reason, data}
    end
  end

  def handle_event(:internal, {:send, packet}, {:connected, _}, data) do
    case send_packet(data, packet) do
      :ok -> :repeat_state_and_data
      {:encoding_error, _error} -> :repeat_state_and_data
      {:connection_error, _error} -> {:next_state, :disconnected, data, :postpone}
    end
  end

  def handle_event(:internal, {:send, packet, from}, {:connected, _}, data) do
    case send_packet(data, packet) do
      :ok -> {:repeat_state_and_data, {:reply, from, :ok}}
      {:encoding_error, error} -> {:repeat_state_and_data, {:reply, from, {:error, error}}}
      {:connection_error, _error} -> {:next_state, :disconnected, data, :postpone}
    end
  end

  def handle_event(:internal, {:send, _, _}, _state, _data) do
    {:keep_state_and_data, :postpone}
  end

  def handle_event(:state_timeout, :connect, :disconnected, data) do
    {:next_state, :connecting, data, {:next_event, :internal, :connect}}
  end

  def handle_event(:state_timeout, :disconnect, _, data) do
    {:next_state, :disconnected, data}
  end

  def handle_event(:timeout, :ping, {:connected, :normal}, data) do
    pingreq = %ControlPacket.Pingreq{}

    case send_packet(data, pingreq) do
      :ok -> {:next_state, {:connected, :heartbeat}, data}
      {:encoding_error, error} -> {:stop, error}
      {:connection_error, _error} -> {:next_state, :disconnected, data}
    end
  end

  def handle_event(:info, {:tcp_closed, socket}, _, %{socket: socket} = data) do
    {:next_state, :disconnected, data}
  end

  def handle_event(:info, {:ssl_closed, socket}, _, %{socket: socket} = data) do
    {:next_state, :disconnected, data}
  end

  def handle_event(:info, {proto, socket, packet}, state, %{socket: socket} = data)
      when proto in [:tcp, :ssl] do
    %__MODULE__{
      buffer: buffer,
      buffer_size: buffer_size,
      transport: transport,
      socket: socket
    } = data

    transport.setopts(socket, active: :once)

    case ControlPacket.decode_buffer([buffer | [packet]]) do
      {:ok, packets, _} ->
        handle_packets(packets, state, %{data | buffer: [], buffer_size: 0})

      {:error, :incomplete_packet, packets, size} ->
        packet_size = byte_size(packet)
        buffer_size = packet_size + buffer_size

        start = buffer_size - size
        length = packet_size - start
        buffer = binary_part(packet, start, length)

        handle_packets(packets, state, %{data | buffer: buffer, buffer_size: length})

      {:error, error, packets, _} ->
        case handle_packets(packets, state, data) do
          {_, _, actions} -> {:stop_and_reply, error, actions}
          {_, _} -> {:stop, error}
        end
    end
  end

  def handle_event(
        {:call, from},
        {:send, %ControlPacket.Publish{qos_level: :at_most_once} = publish},
        {:connected, :normal},
        _data
      ) do
    {:keep_state_and_data, {:next_event, :internal, {:send, publish, from}}}
  end

  def handle_event(
        {:call, from},
        {:send, %ControlPacket.Publish{} = publish},
        {:connected, :normal},
        data
      ) do
    %__MODULE__{
      tracker_table: tracker_table,
      packet_identifiers: packet_identifiers
    } = data

    case packet_identifiers do
      [] ->
        {:next_state, {:connected, :out_of_identifiers}, data, :postpone}

      [packet_identifier | packet_identifiers] ->
        publish = %{publish | packet_identifier: packet_identifier}
        Tracker.insert(tracker_table, publish, from)

        data = %{data | packet_identifiers: packet_identifiers}
        {:keep_state, data, {:next_event, :internal, {:send, publish}}}
    end
  end

  def handle_event(
        {:call, {pid, _} = from},
        {:send, %ControlPacket.Subscribe{} = subscribe},
        {:connected, :normal},
        data
      ) do
    %__MODULE__{
      tracker_table: tracker_table,
      subscription_table: subscription_table,
      packet_identifiers: packet_identifiers
    } = data

    %ControlPacket.Subscribe{
      topic_filters: topic_filters
    } = subscribe

    case packet_identifiers do
      [] ->
        {:next_state, {:connected, :out_of_identifiers}, data, :postpone}

      [packet_identifier | packet_identifiers] ->
        results =
          for {topic_name, _} <- topic_filters do
            Subscription.insert_new(subscription_table, topic_name, pid)
          end

        if Enum.all?(results, &Function.identity/1) do
          subscribe = %{subscribe | packet_identifier: packet_identifier}
          Tracker.insert(tracker_table, subscribe, from)

          data = %{data | packet_identifiers: packet_identifiers}
          {:keep_state, data, {:next_event, :internal, {:send, subscribe}}}
        else
          {:keep_state, data, {:reply, from, {:error, :already_subscribed}}}
        end
    end
  end

  def handle_event(
        {:call, from},
        {:send, %ControlPacket.Unsubscribe{} = unsubscribe},
        {:connected, :normal},
        data
      ) do
    %__MODULE__{
      tracker_table: tracker_table,
      packet_identifiers: packet_identifiers
    } = data

    case packet_identifiers do
      [] ->
        {:next_state, {:connected, :out_of_identifiers}, data, :postpone}

      [packet_identifier | packet_identifiers] ->
        unsubscribe = %{unsubscribe | packet_identifier: packet_identifier}
        Tracker.insert(tracker_table, unsubscribe, from)

        data = %{data | packet_identifiers: packet_identifiers}
        {:keep_state, data, {:next_event, :internal, {:send, unsubscribe}}}
    end
  end

  def handle_event(
        {:call, from},
        {:send, %ControlPacket.Disconnect{} = disconnect},
        {:connected, :normal},
        data
      ) do
    with :ok <- send_packet(data, disconnect) do
      {:next_state, :terminating, data, {:reply, from, :ok}}
    end
  end

  def handle_event({:call, _from}, {:send, _packet}, state, _data)
      when state !== {:connected, :normal} do
    {:keep_state_and_data, :postpone}
  end

  defp handle_packets(packets, state, data) do
    handle_packets(packets, state, data, [])
  end

  defp handle_packets([], state, data, actions) do
    actions = Enum.reverse(actions)

    case state do
      {:connected, :normal} -> {:repeat_state, data, actions}
      {:connected, :out_of_identifiers} -> {:next_state, {:connected, :normal}, data, actions}
    end
  end

  defp handle_packets([packet | tail], state, data, actions) do
    case handle_packet(packet, state, data) do
      {:ok, transition} when is_tuple(transition) -> transition
      {:ok, data, action} -> handle_packets(tail, state, data, [action | actions])
      {:ok, data} -> handle_packets(tail, state, data, actions)
      {:error, error} -> {:stop, error}
    end
  end

  defp handle_packet(%ControlPacket.Connack{} = connack, :connecting, data) do
    %ControlPacket.Connack{
      properties: properties
    } = connack

    %__MODULE__{connect: connect} = data

    keep_alive = Keyword.get(properties, :server_keep_alive, connect.keep_alive)
    clientid = Keyword.get(properties, :assigned_client_identifier, connect.clientid)
    receive_maximum = Keyword.get(properties, :receive_maximum, 0xFFFF)

    connect =
      connect
      |> Map.put(:keep_alive, keep_alive)
      |> Map.put(:clientid, clientid)
      |> Map.put(:clean_start, false)

    packet_identifiers = Enum.into(1..receive_maximum, [])

    data = %{data | connect: connect, packet_identifiers: packet_identifiers}

    {:ok, {:next_state, {:connected, :normal}, data}}
  end

  defp handle_packet(_, :connecting, _data) do
    {:error, :protocol_error}
  end

  defp handle_packet(%ControlPacket.Pingresp{} = _, {:connected, :heartbeat}, data) do
    {:ok, {:next_state, {:connected, :normal}, data}}
  end

  defp handle_packet(%ControlPacket.Pingresp{} = _, _state, _data) do
    {:error, :protocol_error}
  end

  defp handle_packet(%ControlPacket.Puback{} = puback, {:connected, _}, data) do
    %ControlPacket.Puback{
      packet_identifier: packet_identifier,
      reason_code: reason_code
    } = puback

    %__MODULE__{
      tracker_table: tracker_table,
      packet_identifiers: packet_identifiers
    } = data

    [{^packet_identifier, _, from}] = Tracker.take(tracker_table, packet_identifier)
    data = %{data | packet_identifiers: [packet_identifier | packet_identifiers]}

    if reason_code === :success do
      {:ok, data, {:reply, from, :ok}}
    else
      {:ok, data, {:reply, from, {:error, reason_code}}}
    end
  end

  defp handle_packet(%ControlPacket.Pubrec{} = pubrec, {:connected, _}, data) do
    %ControlPacket.Pubrec{
      packet_identifier: packet_identifier,
      reason_code: reason_code
    } = pubrec

    %__MODULE__{
      tracker_table: tracker_table,
      packet_identifiers: packet_identifiers
    } = data

    [{^packet_identifier, _, from}] = Tracker.lookup(tracker_table, packet_identifier)

    if reason_code === :success do
      pubrel = %ControlPacket.Pubrel{packet_identifier: packet_identifier}
      {:ok, data, {:next_event, :internal, {:send, pubrel}}}
    else
      Tracker.delete(tracker_table, packet_identifier)
      data = %{data | packet_identifiers: [packet_identifier | packet_identifiers]}
      {:ok, data, {:reply, from, {:error, reason_code}}}
    end
  end

  defp handle_packet(%ControlPacket.Pubrel{} = pubrel, {:connected, _}, data) do
    %ControlPacket.Pubrel{
      packet_identifier: packet_identifier,
      reason_code: _reason_code
    } = pubrel

    %__MODULE__{
      tracker_table: tracker_table
    } = data

    [{^packet_identifier, _}] = Tracker.take(tracker_table, packet_identifier)

    pubcomp = %ControlPacket.Pubcomp{packet_identifier: packet_identifier}
    {:ok, data, {:next_event, :internal, {:send, pubcomp}}}
  end

  defp handle_packet(%ControlPacket.Pubcomp{} = pubcomp, {:connected, _}, data) do
    %ControlPacket.Pubcomp{
      packet_identifier: packet_identifier,
      reason_code: reason_code
    } = pubcomp

    %__MODULE__{
      tracker_table: tracker_table,
      packet_identifiers: packet_identifiers
    } = data

    [{^packet_identifier, _, from}] = Tracker.take(tracker_table, packet_identifier)
    data = %{data | packet_identifiers: [packet_identifier | packet_identifiers]}

    if reason_code === :success do
      {:ok, data, {:reply, from, :ok}}
    else
      {:ok, data, {:reply, from, {:error, reason_code}}}
    end
  end

  defp handle_packet(%ControlPacket.Suback{} = suback, {:connected, _}, data) do
    %ControlPacket.Suback{
      packet_identifier: packet_identifier,
      reason_codes: reason_codes
    } = suback

    %__MODULE__{
      tracker_table: tracker_table,
      packet_identifiers: packet_identifiers
    } = data

    [{^packet_identifier, subscribe, from}] =
      Tracker.take(tracker_table, packet_identifier)

    %ControlPacket.Subscribe{topic_filters: topic_filters} = subscribe

    {accepted_topic_filters, rejected_topic_filters} =
      Enum.zip(reason_codes, topic_filters)
      |> Enum.split_with(fn {reason_code, _} ->
        reason_code in [:granted_qos_0, :granted_qos_1, :granted_qos_2]
      end)

    data = %{data | packet_identifiers: [packet_identifier | packet_identifiers]}
    {:ok, data, {:reply, from, {:ok, accepted_topic_filters, rejected_topic_filters}}}
  end

  defp handle_packet(%ControlPacket.Unsuback{} = unsuback, {:connected, _}, data) do
    %ControlPacket.Unsuback{
      packet_identifier: packet_identifier,
      reason_codes: reason_codes
    } = unsuback

    %__MODULE__{
      tracker_table: tracker_table,
      subscription_table: subscription_table,
      packet_identifiers: packet_identifiers
    } = data

    [{^packet_identifier, unsubscribe, {pid, _} = from}] =
      Tracker.take(tracker_table, packet_identifier)

    %ControlPacket.Unsubscribe{topic_filters: topic_filters} = unsubscribe

    {accepted_topic_filters, rejected_topic_filters} =
      Enum.zip(reason_codes, topic_filters)
      |> Enum.split_with(fn {reason_code, _} -> reason_code === :success end)

    for {_, topic_filter} <- accepted_topic_filters do
      Subscription.delete(subscription_table, topic_filter, pid)
    end

    data = %{data | packet_identifiers: [packet_identifier | packet_identifiers]}
    {:ok, data, {:reply, from, {:ok, accepted_topic_filters, rejected_topic_filters}}}
  end

  defp handle_packet(%ControlPacket.Publish{} = publish, {:connected, _}, data) do
    %ControlPacket.Publish{
      packet_identifier: packet_identifier,
      topic_name: topic_name,
      payload: payload,
      qos_level: qos_level
    } = publish

    %__MODULE__{
      tracker_table: tracker_table,
      subscription_table: subscription_table
    } = data

    for from <- Subscription.find_matching(subscription_table, topic_name) do
      send(from, {topic_name, payload})
    end

    case qos_level do
      :at_most_once ->
        {:ok, data}

      :at_least_once ->
        puback = %ControlPacket.Puback{packet_identifier: packet_identifier}
        {:ok, data, {:next_event, :internal, {:send, puback}}}

      :exactly_once ->
        pubrec = %ControlPacket.Pubrec{packet_identifier: packet_identifier}
        Tracker.insert(tracker_table, pubrec)
        {:ok, data, {:next_event, :internal, {:send, pubrec}}}
    end
  end

  defp handle_packet(%ControlPacket.Disconnect{} = disconnect, {:connected, _}, _data) do
    %ControlPacket.Disconnect{
      reason_code: reason_code,
      properties: properties
    } = disconnect

    reason_string = Keyword.get(properties, :reason_string, "unknown")
    Logger.info("Client disconnected, code: #{reason_code}, reason: #{reason_string}")

    {:ok, {:stop, reason_code}}
  end

  defp send_packet(data, packet) do
    %__MODULE__{
      transport: transport,
      socket: socket
    } = data

    case ControlPacket.encode(packet) do
      {:ok, packet, _size} ->
        case transport.send(socket, packet) do
          :ok -> :ok
          error -> {:connection_error, error}
        end

      error ->
        {:encoding_error, error}
    end
  end
end
