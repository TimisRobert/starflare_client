defmodule StarflareClient.Connection do
  @moduledoc false

  require Logger

  @behaviour :gen_statem

  defstruct host: nil,
            port: nil,
            connect: nil,
            socket: nil,
            transport: nil,
            buffer: [],
            packet_identifiers: [],
            tracking_table: nil

  def start_link(opts) do
    {connect, opts} = Keyword.pop!(opts, :connect)
    {transport, opts} = Keyword.pop!(opts, :transport)
    {host, opts} = Keyword.pop!(opts, :host)
    {port, opts} = Keyword.pop!(opts, :port)

    data = %__MODULE__{
      connect: connect,
      transport: transport,
      host: to_charlist(host),
      port: port
    }

    :gen_statem.start_link(__MODULE__, data, opts)
  end

  defdelegate call(pid, term), to: :gen_statem
  defdelegate send_request(pid, term), to: :gen_statem
  defdelegate get_state(pid), to: :sys

  @impl true
  def callback_mode, do: [:handle_event_function, :state_enter]

  @impl true
  def init(data) do
    tracking_table = :ets.new(:tracking_table, [:private, :set])

    {:ok, :disconnected, %{data | tracking_table: tracking_table}}
  end

  @impl true
  def handle_event(:enter, :disconnected, :disconnected, _data) do
    {:keep_state_and_data, {:state_timeout, 0, :connect}}
  end

  def handle_event(:enter, _state, :disconnected, _data) do
    {:keep_state_and_data, {:state_timeout, :timer.seconds(1), :connect}}
  end

  def handle_event(:enter, :disconnected, :connecting, _) do
    {:keep_state_and_data, {:state_timeout, :timer.seconds(5), :disconnect}}
  end

  def handle_event(:enter, {:connected, :normal}, {:connected, :heartbeat}, _data) do
    {:keep_state_and_data, {:state_timeout, :timer.seconds(5), :disconnect}}
  end

  def handle_event(:enter, :connecting, {:connected, :normal}, data) do
    %__MODULE__{connect: connect} = data
    {:keep_state_and_data, {:timeout, :timer.seconds(connect.keep_alive), :ping}}
  end

  def handle_event(:enter, {:connected, :normal}, {:connected, :normal}, data) do
    %__MODULE__{connect: connect} = data
    {:keep_state_and_data, {:timeout, :timer.seconds(connect.keep_alive), :ping}}
  end

  def handle_event(:enter, {:connected, :heartbeat}, {:connected, :normal}, data) do
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
        with :ok <- send_packet(transport, socket, connect) do
          data = %{data | socket: socket}
          {:next_state, :connecting, data}
        end

      _ ->
        {:next_state, :disconnected, data}
    end
  end

  def handle_event(:state_timeout, :connect, :disconnected, data) do
    {:next_state, :connecting, data, {:next_event, :internal, :connect}}
  end

  def handle_event(:state_timeout, :disconnect, _, data) do
    %__MODULE__{socket: socket, transport: transport} = data

    transport.close(socket)

    {:next_state, :disconnected, %{data | socket: nil}}
  end

  def handle_event(:timeout, :ping, {:connected, :normal}, data) do
    %__MODULE__{socket: socket, transport: transport} = data

    with :ok <- send_packet(transport, socket, %ControlPacket.Pingreq{}) do
      {:next_state, {:connected, :heartbeat}, data}
    end
  end

  def handle_event(:info, {:tcp_closed, socket}, {:connected, _}, %{socket: socket} = data) do
    {:next_state, :disconnected, %{data | socket: nil}}
  end

  def handle_event(:info, {:tcp, socket, packet}, state, %{socket: socket} = data) do
    %__MODULE__{buffer: buffer, transport: transport, socket: socket} = data

    transport.setopts(socket, active: :once)

    case ControlPacket.decode_buffer([buffer | [packet]]) do
      {:ok, packets} ->
        handle_packets(packets, state, %{data | buffer: []})

      {:error, :incomplete_packet, packets} ->
        handle_packets(packets, state, %{data | buffer: [packet]})

      {:error, error, packets} ->
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
        data
      ) do
    %__MODULE__{
      socket: socket,
      transport: transport
    } = data

    with :ok <- send_packet(transport, socket, publish) do
      {:keep_state_and_data, {:reply, from, :ok}}
    end
  end

  def handle_event(
        {:call, from},
        {:send, %ControlPacket.Publish{} = publish},
        {:connected, :normal},
        data
      ) do
    %__MODULE__{
      socket: socket,
      transport: transport,
      tracking_table: tracking_table,
      packet_identifiers: packet_identifiers
    } = data

    [packet_identifier | packet_identifiers] = packet_identifiers
    :ets.insert_new(tracking_table, {packet_identifier, from})

    publish = %{publish | packet_identifier: packet_identifier}
    data = %{data | packet_identifiers: packet_identifiers}

    with :ok <- send_packet(transport, socket, publish) do
      {:keep_state, data}
    end
  end

  def handle_event({:call, _from}, {:send, _packet}, state, _data)
      when state !== {:connected, :normal} do
    {:keep_state_and_data, :postpone}
  end

  defp handle_packets(packets, state, data) do
    handle_packets(packets, state, data, [])
  end

  defp handle_packets([], _state, data, actions) do
    {:repeat_state, data, Enum.reverse(actions)}
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

    connect =
      connect
      |> Map.put(:keep_alive, Map.get(properties, :server_keep_alive, connect.keep_alive))
      |> Map.put(:clientid, Map.get(properties, :assigned_client_identifier, connect.clientid))
      |> Map.put(:clean_start, false)

    packet_identifiers = 1..Map.get(properties, :receive_maximum, 0xFFFF) |> Enum.into([])

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

  defp handle_packet(%ControlPacket.Puback{} = puback, {:connected, :normal}, data) do
    %ControlPacket.Puback{
      packet_identifier: packet_identifier,
      reason_code: _reason_code
    } = puback

    %__MODULE__{
      tracking_table: tracking_table,
      packet_identifiers: packet_identifiers
    } = data

    [{^packet_identifier, from}] = :ets.take(tracking_table, packet_identifier)
    data = %{data | packet_identifiers: [packet_identifier | packet_identifiers]}

    {:ok, data, {:reply, from, :ok}}
  end

  defp handle_packet(%ControlPacket.Pubrec{} = pubrec, {:connected, :normal}, data) do
    %ControlPacket.Pubrec{
      packet_identifier: packet_identifier,
      reason_code: _reason_code
    } = pubrec

    %__MODULE__{
      transport: transport,
      socket: socket
    } = data

    # [{^packet_identifier, from}] = :ets.take(tracking_table, packet_identifier)

    pubrel = %ControlPacket.Pubrel{packet_identifier: packet_identifier}

    with :ok <- send_packet(transport, socket, pubrel) do
      {:ok, data}
    end
  end

  defp handle_packet(%ControlPacket.Pubcomp{} = pubcomp, {:connected, :normal}, data) do
    %ControlPacket.Pubcomp{
      packet_identifier: packet_identifier,
      reason_code: _reason_code
    } = pubcomp

    %__MODULE__{
      tracking_table: tracking_table,
      packet_identifiers: packet_identifiers
    } = data

    [{^packet_identifier, from}] = :ets.take(tracking_table, packet_identifier)
    data = %{data | packet_identifiers: [packet_identifier | packet_identifiers]}

    {:ok, data, {:reply, from, :ok}}
  end

  defp send_packet(transport, socket, packet) do
    with {:ok, packet, _size} <- ControlPacket.encode(packet) do
      transport.send(socket, packet)
    end
  end
end
