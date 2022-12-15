"""Clone a database to a test database, with built in fetch-depth."""
# This starts out with https://stackoverflow.com/questions/70392123
import time
from pathlib import Path
from subprocess import run
from typing import Optional

import sqlalchemy as sa
from lamin_logger import logger
from sqlalchemy import MetaData, create_engine, func, select

from ._settings import settings
from ._settings_instance import InstanceSettings


def setup_local_test_sqlite_file(
    src_settings: InstanceSettings, return_dir: bool = False
):
    path = src_settings._sqlite_file_local
    new_stem = path.stem + "_test"
    tgt_sqlite_dir = Path.cwd() / new_stem
    if return_dir:
        return tgt_sqlite_dir
    tgt_sqlite_file = tgt_sqlite_dir / f"{new_stem}{path.suffix}"
    tgt_sqlite_file.parent.mkdir(exist_ok=True)
    if tgt_sqlite_file.exists():
        tgt_sqlite_file.unlink()
    tgt_db = f"sqlite:///{tgt_sqlite_file}"
    return tgt_db


def setup_local_test_postgres():
    process = run(
        "docker run --name pgtest -e POSTGRES_PASSWORD=pwd"
        " -e POSTGRES_DB=pgtest -d -p 5432:5432 postgres",  # noqa
        shell=True,
    )
    if process.returncode == 0:
        logger.info(
            "Created Postgres test instance. It runs in docker container 'pgtest'."
        )
    else:
        raise RuntimeError("Failed to set up postgres test instance.")
    time.sleep(2)
    return "postgresql://postgres:pwd@0.0.0.0:5432/pgtest"


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


def clone_test(src_settings: Optional[InstanceSettings] = None, depth: int = 10):
    """Clone from current instance to a test instance."""
    if src_settings is None:
        src_settings = settings.instance
    if src_settings.storage_root is None:
        raise RuntimeError("Please run `lndb init` to configure an instance.")
    src_engine = create_engine(src_settings.db)
    src_metadata = MetaData()

    if src_settings._dbconfig == "sqlite":
        tgt_db = setup_local_test_sqlite_file(src_settings)
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

    # only relevant for postgres
    if "information_schema" in src_schemas:
        src_schemas.remove("information_schema")

    for schema in src_schemas:
        src_metadata.reflect(bind=src_engine, schema=schema)
        tgt_metadata.reflect(bind=tgt_engine, schema=schema)
        if src_engine.dialect.name != "sqlite":
            print(f"\nSchema: {schema}")
        clone_schema(
            schema, src_conn, src_metadata, tgt_conn, tgt_metadata, tgt_engine, depth
        )

    return tgt_db
