Code.eval_file("mess.exs", (if File.exists?("../../lib/mix/mess.exs"), do: "../../lib/mix/"))

defmodule Bonfire.Data.Edges.MixProject do
  use Mix.Project

  def project do
    if File.exists?("../../.is_umbrella.exs") do
      [
        build_path: "../../_build",
        config_path: "../../config/config.exs",
        deps_path: "../../deps",
        lockfile: "../../mix.lock"
      ]
    else
      []
    end
    ++
    [
      app: :bonfire_data_edges,
      version: "0.1.0",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application, do: [extra_applications: [:logger]]

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    Mess.deps [
      # {:pointers, "~> 0.5.0"}
      {:pointers,
       git: "https://github.com/bonfire-networks/pointers.git", branch: "main"}
      # {:pointers, path: "../pointers"}
    ]
  end
end
