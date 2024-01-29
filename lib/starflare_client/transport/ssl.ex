defmodule StarflareClient.Transport.Ssl do
  @moduledoc false

  @behaviour StarflareClient.Transport

  defdelegate connect(address, port, opts), to: :ssl
  defdelegate close(socket), to: :ssl
  defdelegate send(socket, data), to: :ssl
  defdelegate setopts(socket, opts), to: :ssl
  defdelegate controlling_process(socket, owner), to: :ssl
end
