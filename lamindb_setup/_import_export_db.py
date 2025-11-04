from __future__ import annotations

import warnings
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
from django.db import models, transaction

if TYPE_CHECKING:
    from collections.abc import Sequence
    from typing import Literal


def _get_registries(module_name: str) -> list[str]:
    """Get registry class names from a lnschema module."""
    schema_module = import_module(module_name)
    exclude = {"SQLRecord"}

    if module_name == "lamindb":
        module_filter = (
            lambda cls, name: cls.__module__ == f"{module_name}.models.{name.lower()}"
        )
    else:
        module_filter = (
            lambda cls, name: cls.__module__ == f"{module_name}.models"
            and name in dir(schema_module)
        )

    return [
        name
        for name in dir(schema_module.models)
        if (
            name[0].isupper()
            and isinstance(cls := getattr(schema_module.models, name, None), type)
            and issubclass(cls, models.Model)
            and module_filter(cls, name)
            and name not in exclude
        )
    ]


def _export_registry_to_parquet(registry: type[models.Model], directory: Path) -> None:
    """Export a single registry table to parquet."""
    import lamindb_setup as ln_setup

    table_name = registry._meta.db_table
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="Skipped unsupported reflection")
        df = pd.read_sql_table(table_name, ln_setup.settings.instance.db)
    df.to_parquet(directory / f"{table_name}.parquet", compression=None)


def export_db(
    module_names: Sequence[str] | None = None,
    *,
    output_dir: str | Path = "./lamindb_export/",
) -> None:
    """Export registry tables and many-to-many link tables to parquet files.

    Args:
        module_names: Module names to export (e.g., ["lamindb", "bionty", "wetlab"]).
            Defaults to "lamindb" if not provided.
        output_dir: Directory path for exported parquet files.
    """
    directory = Path(output_dir)
    directory.mkdir(parents=True, exist_ok=True)

    module_names = module_names or ["lamindb"]
    modules = {name: _get_registries(name) for name in module_names}

    for module_name, model_names in modules.items():
        schema_module = import_module(module_name)
        for model_name in model_names:
            registry = getattr(schema_module, model_name)
            _export_registry_to_parquet(registry, directory)

            for field in registry._meta.many_to_many:
                link_orm = getattr(registry, field.name).through
                _export_registry_to_parquet(link_orm, directory)


def _import_registry(
    registry: type[models.Model],
    directory: Path,
    if_exists: Literal["fail", "replace", "append"] = "replace",
) -> None:
    """Import a single registry table from parquet.

    Uses raw SQL export instead of django to later circumvent FK constraints.
    """
    table_name = registry._meta.db_table
    parquet_file = directory / f"{table_name}.parquet"

    if not parquet_file.exists():
        print(f"Skipped {table_name} (file not found)")
        return

    df = pd.read_parquet(parquet_file)

    old_foreign_key_columns = [col for col in df.columns if col.endswith("_old")]
    if old_foreign_key_columns:
        df = df.drop(columns=old_foreign_key_columns)

    from django.db import connection

    df.to_sql(table_name, connection.connection, if_exists=if_exists, index=False)


def import_db(
    module_names: Sequence[str] | None = None,
    *,
    input_dir: str | Path = "./lamindb_export/",
    if_exists: Literal["fail", "replace", "append"] = "replace",
) -> None:
    """Import registry and link tables from parquet files.

    Temporarily disables FK constraints to allow insertion in arbitrary order.
    Requires superuser/RDS admin privileges for postgres databases.

    Args:
        input_dir: Directory containing parquet files to import.
        module_names: Module names to import (e.g., ["lamindb", "bionty", "wetlab"]).
        if_exists: How to behave if table exists: 'fail', 'replace', or 'append'.
    """
    from django.db import connection

    import lamindb_setup as ln_setup

    directory = Path(input_dir)

    if not directory.exists():
        raise ValueError(f"Directory does not exist: {directory}")

    if module_names is None:
        parquet_files = list(directory.glob("*.parquet"))
        detected_modules = {
            f.name.split("_")[0] for f in parquet_files if "_" in f.name
        }
        module_names = sorted(detected_modules)

    modules = {name: _get_registries(name) for name in module_names}

    # Disable FK constraints to allow insertion in arbitrary order
    if ln_setup.settings.instance.dialect == "sqlite":
        with connection.cursor() as cursor:
            if ln_setup.settings.instance.dialect == "postgresql":
                cursor.execute("SET session_replication_role = 'replica'")
            elif ln_setup.settings.instance.dialect == "sqlite":
                cursor.execute("PRAGMA foreign_keys = OFF")

    with transaction.atomic():
        if ln_setup.settings.instance.dialect == "postgresql":
            with connection.cursor() as cursor:
                cursor.execute("SET CONSTRAINTS ALL DEFERRED")

        for module_name, model_names in modules.items():
            schema_module = import_module(module_name)
            for model_name in model_names:
                registry = getattr(schema_module, model_name)
                _import_registry(registry, directory, if_exists=if_exists)

                for field in registry._meta.many_to_many:
                    link_orm = getattr(registry, field.name).through
                    _import_registry(link_orm, directory, if_exists=if_exists)

    # Re-enable FK constraints again
    if ln_setup.settings.instance.dialect == "sqlite":
        with connection.cursor() as cursor:
            if ln_setup.settings.instance.dialect == "postgresql":
                cursor.execute("SET session_replication_role = 'origin'")
            elif ln_setup.settings.instance.dialect == "sqlite":
                cursor.execute("PRAGMA foreign_keys = ON")
