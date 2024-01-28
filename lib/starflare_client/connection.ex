defmodule StarflareClient.Connection do
  @moduledoc false

  require Logger
  alias ControlPacket

  @behaviour :gen_statem

  @type t() :: %__MODULE__{
          host: charlist(),
          port: :inet.port_number(),
          connect: any(),
          socket: :inet.socket() | :ssl.sslsocket() | nil,
          transport: StarflareClient.Transport,
          buffer: iodata(),
          backoff: integer()
        }

  defstruct host: nil,
            port: nil,
            connect: nil,
            socket: nil,
            transport: nil,
            buffer: <<>>,
            backoff: 0

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
    {:keep_state_and_data, {:next_event, :internal, :connect}}
  end

  def handle_event(:enter, :connected, :disconnected, data) do
    backoff = data.backoff + :rand.uniform(1000)
    data = %{data | backoff: backoff}

    {:keep_state, data, {:next_event, :internal, :connect}}
  end

  def handle_event(:enter, _, _, _) do
    :keep_state_and_data
  end

  def handle_event(:internal, _state, :connect, data) do
    %__MODULE__{
      connect: connect,
      transport: transport,
      host: host,
      port: port
    } = data

    opts = [:binary, packet: :raw, active: :once]

    with {:ok, socket} <- transport.connect(host, port, opts),
         {:ok, packet, _size} <- ControlPacket.encode(connect),
         :ok <- transport.send(socket, packet) do
      data = %{data | socket: socket}
      {:next_state, data, :connecting, {:state_timeout, 5000, :disconnect}}
    end
  end

  def handle_event(:info, {:tcp_closed, socket}, :connected, data) do
  end

  def handle_event(:info, {:tcp, socket, packet}, state, %{socket: socket} = data) do
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
end
