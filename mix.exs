defmodule StarflareClient.MixProject do
  use Mix.Project

  def project do
    [
      app: :starflare_client,
      version: "0.1.1",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package(),
      name: "StarflareClient",
      description: "MQTT 5 client",
      source_url: "https://github.com/TimisRobert/starflare_client"
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :ssl],
      mod: {StarflareClient.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:control_packet, "~> 1.0"},
      {:castore, "~> 1.0"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.14", only: :dev, runtime: false}
    ]
  end

  defp package() do
    [
      licenses: ["AGPL-3.0-or-later"],
      links: %{"GitHub" => "https://github.com/TimisRobert/starflare_client"}
    ]
  end
end
