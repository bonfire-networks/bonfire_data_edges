defmodule Bonfire.Data.Edges.EdgeTotal do
  use Needle.Mixin,
    otp_app: :bonfire_data_edges,
    source: "bonfire_data_edges_edge_total"

  require Needle.Changesets
  alias Bonfire.Data.Edges.EdgeTotal
  alias Ecto.Changeset
  alias Needle.Table

  mixin_schema do
    field(:subject_count, :integer, default: 0)
    field(:object_count, :integer, default: 0)
    belongs_to(:table, Table)
  end

  # @cast [:subject_count, :object_count, :table_id]
  @required [:table_id]

  def changeset(me \\ %EdgeTotal{}, params) do
    Changeset.cast(me, params, @required)
  end
end

defmodule Bonfire.Data.Edges.EdgeTotal.Migration do
  @moduledoc false
  import Ecto.Migration
  import Needle.Migration
  alias Bonfire.Data.Edges.Edge
  alias Bonfire.Data.Edges.EdgeTotal

  @table EdgeTotal.__schema__(:source)

  # @edge_table Edge.__schema__(:source)
  # create_edge_total_table/{0,1}

  defp make_edge_total_table(exprs) do
    quote do
      require Needle.Migration

      Needle.Migration.create_mixin_table Bonfire.Data.Edges.EdgeTotal do
        Ecto.Migration.add(:subject_count, :bigint, null: false)
        Ecto.Migration.add(:object_count, :bigint, null: false)

        Ecto.Migration.add(:table_id, Needle.Migration.strong_pointer(), primary_key: true)

        unquote_splicing(exprs)
      end
    end
  end

  defmacro create_edge_total_table(), do: make_edge_total_table([])

  defmacro create_edge_total_table(do: {_, _, body}),
    do: make_edge_total_table(body)

  # drop_edge_total_table/0

  def drop_edge_total_table(), do: drop_mixin_table(EdgeTotal)

  defp make_edge_total_subject_count_index(opts) do
    quote do
      Ecto.Migration.create_if_not_exists(
        Ecto.Migration.index(unquote(@table), [:subject_count], unquote(opts))
      )
    end
  end

  defmacro create_edge_total_subject_count_index(opts \\ [])

  defmacro create_edge_total_subject_count_index(opts),
    do: make_edge_total_subject_count_index(opts)

  def drop_edge_total_subject_count_index(opts \\ []) do
    drop_if_exists(index(@table, [:subject_count], opts))
  end

  defp make_edge_total_object_count_index(opts) do
    quote do
      Ecto.Migration.create_if_not_exists(
        Ecto.Migration.index(unquote(@table), [:object_count], unquote(opts))
      )
    end
  end

  defmacro create_edge_total_object_count_index(opts \\ [])

  defmacro create_edge_total_object_count_index(opts),
    do: make_edge_total_object_count_index(opts)

  def drop_edge_total_object_count_index(opts \\ []) do
    drop_if_exists(index(@table, [:object_count], opts))
  end

  defp make_edge_total_table_index(opts) do
    quote do
      Ecto.Migration.create_if_not_exists(
        Ecto.Migration.index(unquote(@table), [:table_id], unquote(opts))
      )
    end
  end

  defmacro create_edge_total_table_index(opts \\ [])

  defmacro create_edge_total_table_index(opts),
    do: make_edge_total_table_index(opts)

  def drop_edge_total_table_index(opts \\ []) do
    drop_if_exists(index(@table, [:table_id], opts))
  end

  @create_trigger_fun """
  create or replace function "#{@table}_update" ()
  returns trigger
  language plpgsql
  as $$
  begin
    if (TG_OP = 'INSERT') then
      insert into "#{@table}" values (NEW.subject_id, 1, 0, NEW.table_id)
      on conflict (id, table_id) do update
        set subject_count = EXCLUDED.subject_count + 1
        where "#{@table}".id = NEW.subject_id;

      insert into "#{@table}" values (NEW.object_id, 0, 1, NEW.table_id)
      on conflict (id, table_id) do update
        set object_count = EXCLUDED.object_count + 1
        where "#{@table}".id = NEW.object_id;

    elsif (TG_OP = 'DELETE') then
      update "#{@table}" set subject_count = GREATEST(0, subject_count - 1)
      where id = OLD.subject_id and table_id = OLD.table_id;

      update "#{@table}" set object_count = GREATEST(0, object_count - 1)
      where id = OLD.object_id and table_id = OLD.table_id;

    end if;
    return null;
  end;
  $$;
  """

  @drop_trigger_fun "drop function if exists #{@table}_update cascade"

  @create_trigger """
  create trigger "#{@table}_trigger"
  after insert or delete on "#{Edge.__schema__(:source)}"
  for each row execute procedure "#{@table}_update"();
  """

  @drop_trigger """
  drop trigger if exists "#{@table}_trigger" on "#{Edge.__schema__(:source)}" cascade
  """

  def migrate_edge_total_trigger do
    IO.inspect(@create_trigger_fun)
    Ecto.Migration.execute(@create_trigger_fun, @drop_trigger_fun)
    # to replace if changed
    Ecto.Migration.execute(@drop_trigger, @drop_trigger)
    Ecto.Migration.execute(@create_trigger, @drop_trigger)
  end

  @doc false
  def migrate_edge_total_view(schema) when is_atom(schema) do
    source = schema.__schema__(:source)
    id = schema.__pointers__(:table_id)
    migrate_edge_total_view(direction(), source, id)
  end

  @doc false
  def migrate_edge_total_view(source, id)
      when is_binary(source) and is_binary(id),
      do: migrate_edge_total_view(direction(), source, id)

  @doc false
  def migrate_edge_total_view(:up, source, id)
      when is_binary(source) and is_binary(id) do
    {:ok, id} = Needle.ULID.dump(Needle.ULID.cast!(id))

    execute("""
    create or replace view "#{source}_total" as
      select
        id, subject_count, object_count, table_id
      from #{@table}
        where table_id = ('#{Ecto.UUID.cast!(id)}' :: uuid)
    """)
  end

  def migrate_edge_total_view(:down, source, _id) do
    execute("""
    drop view if exists "#{source}_total"
    """)
  end

  # migrate_edge_total/{0,1}

  defp met(:up) do
    quote do
      unquote(make_edge_total_table([]))
      unquote(make_edge_total_subject_count_index([]))
      unquote(make_edge_total_object_count_index([]))
      unquote(make_edge_total_table_index([]))
      Bonfire.Data.Edges.EdgeTotal.Migration.migrate_edge_total_trigger()
    end
  end

  defp met(:down) do
    quote do
      Bonfire.Data.Edges.EdgeTotal.Migration.migrate_edge_total_trigger()
      Bonfire.Data.Edges.EdgeTotal.Migration.drop_edge_total_table_index()

      Bonfire.Data.Edges.EdgeTotal.Migration.drop_edge_total_object_count_index()

      Bonfire.Data.Edges.EdgeTotal.Migration.drop_edge_total_subject_count_index()

      Bonfire.Data.Edges.EdgeTotal.Migration.drop_edge_total_table()
    end
  end

  defmacro migrate_edge_total() do
    quote do
      if Ecto.Migration.direction() == :up,
        do: unquote(met(:up)),
        else: unquote(met(:down))
    end
  end

  defmacro migrate_edge_total(dir), do: met(dir)
end
