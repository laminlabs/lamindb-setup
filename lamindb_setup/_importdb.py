from __future__ import annotations

from importlib import import_module
from pathlib import Path

from ._exportdb import MODELS


def import_registry(registry, directory, connection):
    import pandas as pd

    table_name = registry._meta.db_table
    df = pd.read_parquet(directory / f"{table_name}.parquet")
    old_foreign_key_columns = [
        column for column in df.columns if column.endswith("_old")
    ]
    for column in old_foreign_key_columns:
        df.drop(column, axis=1, inplace=True)
    df.to_sql(table_name, connection, if_exists="append", index=False)


def importdb() -> None:
    # import data from parquet files
    directory = Path("./lamindb_export/")
    if directory.exists():
        response = input(
            f"\n\nDo you want to import registries from here: {directory}? (y/n)\n"
        )
        if response != "y":
            return None
    from sqlalchemy import create_engine, text

    import lamindb_setup as ln_setup

    engine = create_engine(ln_setup.settings.instance.db, echo=False)
    with engine.begin() as connection:
        if ln_setup.settings.instance.dialect == "postgresql":
            connection.execute(text("SET CONSTRAINTS ALL DEFERRED;"))
        for schema_name, models in MODELS.items():
            for model_name in models.keys():
                print(model_name)
                schema_module = import_module(f"lnschema_{schema_name}")
                registry = getattr(schema_module, model_name)
                import_registry(registry, directory, connection)
                many_to_many_names = [
                    field.name for field in registry._meta.many_to_many
                ]
                for many_to_many_name in many_to_many_names:
                    link_orm = getattr(registry, many_to_many_name).through
                    import_registry(link_orm, directory, connection)
