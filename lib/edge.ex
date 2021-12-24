defmodule Bonfire.Data.Edges.Edge do

  use Pointers.Mixin,
    otp_app: :bonfire_data_edges,
    source: "bonfire_data_edges_edge"

  require Pointers.Changesets
  alias Bonfire.Data.Edges.Edge
  alias Ecto.Changeset
  alias Pointers.{Pointer, Table}

  mixin_schema do
    belongs_to :subject, Pointer
    belongs_to :object,  Pointer
    belongs_to :table,   Table
  end

  @cast     [:subject_id, :object_id, :table_id]
  @required @cast

  def changeset(edge \\ %Edge{}, params) do
    edge
    |> Changeset.cast(params, @cast)
    |> Changeset.validate_required(@required)
    |> Changeset.assoc_constraint(:subject)
    |> Changeset.assoc_constraint(:object)
    |> Changeset.assoc_constraint(:table)
  end

end
defmodule Bonfire.Data.Edges.Edge.Migration do

  import Ecto.Migration
  import Pointers.Migration
  alias Bonfire.Data.Edges.Edge

  @edge_table Edge.__schema__(:source)

  # create_edge_table/{0,1}

  defp make_edge_table(exprs) do
    quote do
      require Pointers.Migration
      Pointers.Migration.create_mixin_table(Bonfire.Data.Edges.Edge) do
        Ecto.Migration.add :subject_id,
          Pointers.Migration.strong_pointer(), null: false
        Ecto.Migration.add :object_id,
          Pointers.Migration.strong_pointer(), null: false
        Ecto.Migration.add :table_id,
          Pointers.Migration.strong_pointer(), null: false
        unquote_splicing(exprs)
      end
    end
  end

  defmacro create_edge_table(), do: make_edge_table([])
  defmacro create_edge_table([do: {_, _, body}]), do: make_edge_table(body)

  # drop_edge_table/0

  def drop_edge_table(), do: drop_pointable_table(Edge)

  def migrate_edge_subject_index(dir \\ direction(), opts \\ [])
  def migrate_edge_subject_index(:up, opts),
    do: create_if_not_exists(index(@edge_table, [:subject_id], opts))
  def migrate_edge_subject_index(:down, opts),
    do: drop_if_exists(index(@edge_table, [:subject_id], opts))

  def migrate_edge_object_index(dir \\ direction(), opts \\ [])
  def migrate_edge_object_index(:up, opts),
    do: create_if_not_exists(index(@edge_table, [:object_id], opts))
  def migrate_edge_object_index(:down, opts),
    do: drop_if_exists(index(@edge_table, [:object_id], opts))

  def migrate_edge_table_index(dir \\ direction(), opts \\ [])
  def migrate_edge_table_index(:up, opts),
    do: create_if_not_exists(index(@edge_table, [:table_id], opts))
  def migrate_edge_table_index(:down, opts),
    do: drop_if_exists(index(@edge_table, [:table_id], opts))

  # migrate_edge/{0,1}

  defp me(:up) do
    quote do
      Bonfire.Data.Edges.Edge.Migration.create_edge_table()
      Bonfire.Data.Edges.Edge.Migration.migrate_edge_subject_index()
      Bonfire.Data.Edges.Edge.Migration.migrate_edge_object_index()
      Bonfire.Data.Edges.Edge.Migration.migrate_edge_table_index()
    end
  end
  defp me(:down) do
    quote do
      Bonfire.Data.Edges.Edge.Migration.migrate_edge_table_index()
      Bonfire.Data.Edges.Edge.Migration.migrate_edge_object_index()
      Bonfire.Data.Edges.Edge.Migration.migrate_edge_subject_index()
      Bonfire.Data.Edges.Edge.Migration.drop_edge_table()
    end
  end

  defmacro migrate_edge() do
    quote do
      if direction() == :up,
        do: unquote(me(:up)),
        else: unquote(me(:down))
    end
  end

  defmacro migrate_edge(dir), do: me(dir)

  def migrate_type_unique_index(dir \\ direction(), schema)
  def migrate_type_unique_index(:up, schema) do
    name = schema.__schema__(:source)
    id = schema.__pointers__(:table_id)
    |> Pointers.ULID.dump()
    |> elem(1)
    |> Ecto.UUID.cast!()
    create_if_not_exists unique_index(@edge_table, [:subject_id, :object_id, :table_id],
      where: "table_id = '#{id}'",
      name: "#{@edge_table}_#{name}_unique_index")
  end

  def migrate_type_unique_index(:down, schema) do
    name = schema.__schema__(:source)
    drop_if_exists unique_index(@edge_table, [:subject_id, :object_id, :table_id],
      name: "#{@edge_table}_#{name}_unique_index")
  end

end
