import sqlalchemy as sql

from ._settings import settings


class schema:
    """Inspect the schema."""

    @classmethod
    def draw(cls, view=True):
        """Make a diagram of entity relationships."""
        import erdiagram

        engine = settings.instance.db_engine()
        metadata = sql.MetaData(bind=engine)
        metadata.reflect()
        graph = erdiagram.create_schema_graph(
            metadata=metadata,
            show_datatypes=False,
            show_indexes=False,  # ditto for indexes
            rankdir="TB",
            concentrate=False,  # Don't try to join the relation lines together
        )
        if view:
            erdiagram.view(graph)
        else:
            return graph

    @classmethod
    def list_entities(cls):
        """Return all entities in the db."""
        metadata = sql.MetaData()
        engine = settings.instance.db_engine()
        metadata.reflect(bind=engine)
        table_names = [table.name for table in metadata.sorted_tables]
        return table_names
