defmodule StarflareClient.Transport.Ssl do
  @moduledoc false

  @behaviour StarflareClient.Transport

  def connect(address, port, opts) do
    opts =
      Keyword.put_new(opts, :verify, :verify_peer)
      |> Keyword.put_new(:cacertfile, CAStore.file_path())

    :ssl.connect(address, port, opts)
  end

  defdelegate close(socket), to: :ssl
  defdelegate send(socket, data), to: :ssl
  defdelegate setopts(socket, opts), to: :ssl
  defdelegate controlling_process(socket, owner), to: :ssl
end
