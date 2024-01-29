defmodule StarflareClient.Connection do
  @moduledoc false

  require Logger

  @behaviour :gen_statem

  @type t() :: %__MODULE__{
          host: charlist(),
          port: :inet.port_number(),
          connect: any(),
          socket: :inet.socket() | :ssl.sslsocket() | nil,
          transport: StarflareClient.Transport,
          buffer: iodata()
        }

  defstruct host: nil,
            port: nil,
            connect: nil,
            socket: nil,
            transport: nil,
            buffer: []

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

  def call(pid, packet) do
    :gen_statem.call(pid, {:send, packet})
  end

  def send_request(pid, packet) do
    :gen_statem.send_request(pid, {:send, packet})
  end

  def get_state(pid) do
    :sys.get_state(pid)
  end

  @impl true
  def callback_mode, do: [:handle_event_function, :state_enter]

  @impl true
  def init(data) do
    {:ok, :disconnected, data}
  end

  @impl true
  def handle_event(:enter, :disconnected, :disconnected, _data) do
    {:keep_state_and_data, {:state_timeout, 0, :connect}}
  end

  def handle_event(:enter, :connected, :disconnected, _data) do
    {:keep_state_and_data, {:state_timeout, :timer.seconds(1), :connect}}
  end

  def handle_event(:enter, :connecting, :disconnected, _data) do
    {:keep_state_and_data, {:state_timeout, :timer.seconds(1), :connect}}
  end

  def handle_event(:enter, :disconnected, :connecting, _) do
    {:keep_state_and_data, {:state_timeout, :timer.seconds(5), :disconnect}}
  end

  def handle_event(:enter, :connecting, :connected, _) do
    {:keep_state_and_data, {:timeout, :timer.seconds(5), :ping}}
  end

  def handle_event(:internal, {:connect_response, connack}, :connecting, data) do
    %ControlPacket.Connack{} = connack

    {:next_state, :connected, data}
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
        with {:ok, packet, _size} <- ControlPacket.encode(connect),
             :ok <- transport.send(socket, packet) do
          data = %{data | socket: socket}
          {:next_state, :connecting, data}
        end

      _ ->
        {:next_state, :disconnected, data}
    end
  end

  def handle_event(:internal, :ping, :connected, data) do
    %__MODULE__{socket: socket, transport: transport} = data

    with {:ok, packet, _size} <- ControlPacket.encode(%ControlPacket.Pingreq{}),
         :ok <- transport.send(socket, packet) do
      :keep_state_and_data
    end
  end

  def handle_event(:state_timeout, :connect, :disconnected, data) do
    {:next_state, :connecting, data, {:next_event, :internal, :connect}}
  end

  def handle_event(:state_timeout, :disconnect, :connecting, _data) do
    {:stop, :connection_timeout}
  end

  def handle_event(:timeout, :ping, :connected, _data) do
    {:keep_state_and_data, {:next_event, :internal, :ping}}
  end

  def handle_event(:info, {:tcp_closed, socket}, :connected, %{socket: socket} = data) do
    data = %{data | socket: nil}

    {:next_state, :disconnected, data}
  end

  def handle_event(:info, {:tcp, socket, packet}, state, %{socket: socket} = data) do
    %__MODULE__{buffer: buffer, transport: transport, socket: socket} = data

    transport.setopts(socket, active: :once)

    case ControlPacket.decode_buffer([buffer | [packet]]) do
      {:ok, packets} ->
        with {:ok, actions} <- handle_packets(packets, state, data) do
          {:keep_state, %{data | buffer: []}, actions}
        end

      {:error, :incomplete_packet, packets} ->
        with {:ok, actions} <- handle_packets(packets, state, data) do
          {:keep_state, %{data | buffer: [packet]}, actions}
        end

      {:error, error, packets} ->
        with {:ok, _actions} <- handle_packets(packets, state, data) do
          {:stop, error}
        end
    end
  end

  def handle_event({:call, _from}, {:send, packet}, :connected, data) do
    %__MODULE__{
      socket: socket,
      transport: transport
    } = data

    with {:ok, packet, _size} <- ControlPacket.encode(packet),
         :ok <- transport.send(socket, packet) do
      :keep_state_and_data
    end
  end

  def handle_event({:call, _from}, {:send, _packet}, state, _data)
      when state in [:connecting, :disconnected] do
    {:keep_state_and_data, :postpone}
  end

  defp handle_packets(packets, state, data) do
    handle_packets(packets, state, data, [])
  end

  defp handle_packets([], _state, _data, actions) do
    {:ok, Enum.reverse(actions)}
  end

  defp handle_packets([packet | tail], state, data, actions) do
    with {:ok, action} <- handle_packet(packet, state, data) do
      handle_packets(tail, state, data, [action | actions])
    end
  end

  defp handle_packet(%ControlPacket.Connack{} = connack, :connecting, _data) do
    {:ok, {:next_event, :internal, {:connect_response, connack}}}
  end

  defp handle_packet(_, :connecting, _data) do
    {:stop, :out_of_order_packet}
  end

  defp handle_packet(%ControlPacket.Pingresp{} = _, :connected, _data) do
    {:ok, {:timeout, :timer.seconds(5), :ping}}
  end
end
