"""Clone a database to a test database, with built in fetch-depth."""
# This starts out with https://stackoverflow.com/questions/70392123
from typing import Optional

import sqlalchemy as sa
from sqlalchemy import MetaData, create_engine, func, select

from .._settings import settings
from ._settings_instance import InstanceSettings
from ._testdb import (
    setup_local_test_postgres,
    setup_local_test_postgres_supabase,
    setup_local_test_sqlite_file,
)


def clone_schema(
    schema, src_conn, src_metadata, tgt_conn, tgt_metadata, tgt_engine, depth: int
):
    # switch off foreign key integrity
    if src_conn.dialect.name == "postgresql":
        tgt_conn.execute(sa.sql.text("SET session_replication_role = replica;"))

    # create all tables in target database
    for table in src_metadata.sorted_tables:
        if not sa.inspect(tgt_engine).has_table(table.name, table.schema):
            table.create(bind=tgt_engine)
    # refresh metadata before copying data
    tgt_metadata.clear()
    tgt_metadata.reflect(bind=tgt_engine, schema=schema)

    # copy data
    print("Cloning: ", end="")
    for table in tgt_metadata.sorted_tables:
        if table.schema != schema:
            continue
        src_table = src_metadata.tables[f"{schema}.{table.name}"]
        n_rows = -1  # indicates no rows
        if len(list(src_table.primary_key)) > 0:
            pk_col = getattr(src_table.c, list(src_table.primary_key)[0].name)
            n_rows = int(
                src_conn.execute(
                    select([func.count(pk_col)]).select_from(src_table)
                ).scalar()
            )
        print(f"{table.name} ({n_rows})", end=", ")
        offset = max(n_rows - depth, 0)
        rows = src_conn.execute(src_table.select().offset(offset))
        values = [row._asdict() for index, row in enumerate(rows)]
        if len(values) > 0:
            tgt_conn.execute(table.insert(), values)
            tgt_conn.commit()


def clone_test(
    src_settings: Optional[InstanceSettings] = None,
    depth: int = 10,
    supabase: bool = False,
):
    """Clone from current instance to a test instance."""
    if src_settings is None:
        src_settings = settings.instance
    src_engine = create_engine(src_settings.db)
    src_metadata = MetaData()

    if src_settings.dialect == "sqlite":
        tgt_db = setup_local_test_sqlite_file(src_settings)
    elif supabase:
        tgt_db = setup_local_test_postgres_supabase()
    else:
        tgt_db = setup_local_test_postgres()

    assert tgt_db != src_settings.db

    tgt_engine = create_engine(tgt_db, future=True)
    tgt_metadata = MetaData()
    src_conn = src_engine.connect()
    src_metadata.reflect(bind=src_engine)
    tgt_conn = tgt_engine.connect()

    # create all schemas in target database
    src_schemas = src_conn.dialect.get_schema_names(src_conn)
    for schemaname in src_schemas:
        if schemaname not in tgt_conn.dialect.get_schema_names(tgt_conn):
            tgt_conn.execute(sa.schema.CreateSchema(schemaname))
        tgt_conn.commit()

    # only relevant for postgres, clean out some default schemas
    for schema in [
        "information_schema",
        "auth",
        "graphql",
        "graphql_public",
        "realtime",
        "extensions",
        "storage",
    ]:
        if schema in src_schemas:
            src_schemas.remove(schema)

    for schema in src_schemas:
        src_metadata.reflect(bind=src_engine, schema=schema)
        tgt_metadata.reflect(bind=tgt_engine, schema=schema)
        if src_engine.dialect.name != "sqlite":
            print(f"\nSchema: {schema}")
        clone_schema(
            schema, src_conn, src_metadata, tgt_conn, tgt_metadata, tgt_engine, depth
        )

    return tgt_db
