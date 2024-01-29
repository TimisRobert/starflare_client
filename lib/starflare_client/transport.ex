defmodule StarflareClient.Transport do
  @moduledoc false

  @callback connect(
              address :: :inet.socket_address() | :inet.hostname(),
              port :: :inet.port_number(),
              opts :: [
                :inet.inet_backend() | :gen_tcp.connect_option() | :ssl.tls_client_option()
              ]
            ) ::
              {:ok, socket :: :inet.socket() | :ssl.sslsocket()}
              | {:error, :timeout | :inet.posix()}

  @callback close(socket :: :inet.socket() | :ssl.sslsocket()) :: :ok

  @callback send(socket :: :inet.socket() | :ssl.sslsocket(), data :: iodata()) ::
              :ok | {:error, {:timeout, binary()} | :inet.posix()}

  @callback setopts(socket :: :inet.socket() | :ssl.sslsocket(), opts :: [:gen_tcp.option()]) ::
              :ok | {:error, :inet.posix()}

  @callback controlling_process(socket :: :inet.socket() | :ssl.sslsocket(), owner :: pid()) ::
              :ok | {:error, :closed | :not_owner | :badarg | :inet.posix()}
end
