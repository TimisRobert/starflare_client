defmodule StarflareClient.Transport.Tcp do
  @moduledoc false

  @behaviour StarflareClient.Transport

  defdelegate connect(address, port, opts), to: :gen_tcp
  defdelegate send(socket, data), to: :gen_tcp
  defdelegate setopts(socket, opts), to: :inet
  defdelegate controlling_process(socket, owner), to: :gen_tcp
end
