"""Clone a database to a test database, with built in fetch-depth."""
# This starts out with https://stackoverflow.com/questions/70392123
from pathlib import Path

from sqlalchemy import MetaData, create_engine, func, select

from ._settings import settings
from ._settings_instance import InstanceSettings


def get_local_test_sqlite_file(src_settings: InstanceSettings):
    path = src_settings._sqlite_file_local
    new_stem = path.stem + "_test"
    tgt_sqlite_file = Path.cwd() / new_stem / f"{new_stem}{path.suffix}"
    tgt_sqlite_file.parent.mkdir(exist_ok=True)
    if tgt_sqlite_file.exists():
        tgt_sqlite_file.unlink()
    tgt_db = f"sqlite:///{tgt_sqlite_file}"
    return tgt_db


def clone_to_test_instance(depth: int = 10):
    """Clone from current instance to test instance."""
    src_settings = settings.instance
    if src_settings.storage_root is None:
        raise RuntimeError("Please run `lndb init` to configure an instance.")
    src_engine = create_engine(src_settings.db)
    src_metadata = MetaData(bind=src_engine)

    if src_settings._dbconfig == "sqlite":
        tgt_db = get_local_test_sqlite_file(src_settings)
        print(f"Target db is: {tgt_db}")
    else:
        raise NotImplementedError

    tgt_engine = create_engine(tgt_db, future=True)
    tgt_metadata = MetaData(bind=tgt_engine)
    src_conn = src_engine.connect()
    tgt_conn = tgt_engine.connect()
    tgt_metadata.reflect()
    src_metadata.reflect()

    # create all tables in target database
    for table in src_metadata.sorted_tables:
        table.create(bind=tgt_engine)
    # refresh metadata before copying data
    tgt_metadata.clear()
    tgt_metadata.reflect()

    # copy data
    print("Cloning:")
    for table in tgt_metadata.sorted_tables:
        src_table = src_metadata.tables[table.name]
        pk_col = getattr(src_table.c, list(src_table.primary_key)[0].name)
        n_rows = int(
            src_conn.execute(
                select([func.count(pk_col)]).select_from(src_table)
            ).scalar()
        )
        print(f"{table.name} ({n_rows})", end=", ")
        offset = max(n_rows - depth, 0)
        rows = src_table.select().offset(offset).execute()
        values = [row._asdict() for index, row in enumerate(rows)]
        if len(values) > 0:
            tgt_conn.execute(table.insert(), values)
            tgt_conn.commit()
