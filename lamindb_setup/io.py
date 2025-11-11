from __future__ import annotations

import io
import json
import warnings
from concurrent.futures import ProcessPoolExecutor, as_completed
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
from django.db import models, transaction
from rich.progress import Progress

if TYPE_CHECKING:
    from collections.abc import Sequence
    from typing import Literal


def _get_registries(module_name: str) -> list[str]:
    """Get registry class names from a module."""
    schema_module = import_module(module_name)
    exclude = {"SQLRecord", "BaseSQLRecord"}

    if module_name == "lamindb":
        module_filter = lambda cls, name: cls.__module__.startswith(
            f"{module_name}.models."
        ) and name in dir(schema_module)
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


def _export_full_table(
    registry_info: tuple[str, str, str | None],
    directory: Path,
    chunk_size: int,
) -> list[tuple[str, Path]] | str:
    """Export a registry table to parquet.

    For PostgreSQL, uses COPY TO which streams the table directly to CSV format,
    bypassing query planner overhead and row-by-row conversion (10-50x faster than SELECT).

    For SQLite with large tables, reads in chunks to avoid memory issues when tables exceed available RAM.

    Args:
        registry_info: Tuple of (module_name, model_name, field_name) where field_name
            is None for regular tables or the field name for M2M link tables.
        directory: Output directory for parquet files.
        chunk_size: Maximum rows per chunk for SQLite large tables.

    Returns:
        String identifier for single-file exports, or list of (table_name, chunk_path) tuples for chunked exports that need merging.
    """
    from django.db import connection

    import lamindb_setup as ln_setup

    module_name, model_name, field_name = registry_info
    schema_module = import_module(module_name)
    registry = getattr(schema_module, model_name)

    if field_name:
        registry = getattr(registry, field_name).through

    table_name = registry._meta.db_table

    try:
        if ln_setup.settings.instance.dialect == "postgresql":
            buffer = io.StringIO()
            with connection.cursor() as cursor:
                cursor.copy_expert(
                    f'COPY "{table_name}" TO STDOUT WITH (FORMAT CSV, HEADER TRUE)',
                    buffer,
                )
            buffer.seek(0)
            df = pd.read_csv(buffer)
            df.to_parquet(directory / f"{table_name}.parquet", compression=None)
            return (
                f"{module_name}.{model_name}.{field_name}"
                if field_name
                else f"{module_name}.{model_name}"
            )
        else:
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore", message="Skipped unsupported reflection"
                )
                row_count = pd.read_sql(
                    f"SELECT COUNT(*) as count FROM {table_name}",
                    ln_setup.settings.instance.db,
                ).iloc[0]["count"]

                if row_count > chunk_size:
                    chunk_files = []
                    num_chunks = (row_count + chunk_size - 1) // chunk_size
                    for chunk_id in range(num_chunks):
                        offset = chunk_id * chunk_size
                        df = pd.read_sql(
                            f"SELECT * FROM {table_name} LIMIT {chunk_size} OFFSET {offset}",
                            ln_setup.settings.instance.db,
                        )
                        chunk_file = (
                            directory / f"{table_name}_chunk_{chunk_id}.parquet"
                        )
                        df.to_parquet(chunk_file, compression=None)
                        chunk_files.append((table_name, chunk_file))
                    return chunk_files
                else:
                    df = pd.read_sql_table(table_name, ln_setup.settings.instance.db)
                    df.to_parquet(directory / f"{table_name}.parquet", compression=None)
                    return (
                        f"{module_name}.{model_name}.{field_name}"
                        if field_name
                        else f"{module_name}.{model_name}"
                    )
    except (ValueError, pd.errors.DatabaseError):
        raise ValueError(
            f"Table '{table_name}' was not found. The instance might need to be migrated."
        ) from None


def export_db(
    module_names: Sequence[str] | None = None,
    *,
    output_dir: str | Path = "./lamindb_export/",
    max_workers: int = 8,
    chunk_size: int = 500_000,
) -> None:
    """Export registry tables and many-to-many link tables to parquet files.

    Ensure that you connect to postgres instances using `use_root_db_user=True`.

    Args:
        module_names: Module names to export (e.g., ["lamindb", "bionty", "wetlab"]).
            Defaults to "lamindb" if not provided.
        output_dir: Directory path for exported parquet files.
        max_workers: Number of parallel processes.
        chunk_size: Number of rows per chunk for large tables.
    """
    directory = Path(output_dir)
    directory.mkdir(parents=True, exist_ok=True)

    module_names = module_names or ["lamindb"]
    modules = {name: _get_registries(name) for name in module_names}

    tasks = []
    for module_name, model_names in modules.items():
        schema_module = import_module(module_name)
        for model_name in model_names:
            registry = getattr(schema_module, model_name)
            tasks.append((module_name, model_name, None))
            for field in registry._meta.many_to_many:
                tasks.append((module_name, model_name, field.name))

    chunk_files_by_table: dict[str, list[Path]] = {}

    with Progress() as progress:
        task_id = progress.add_task("Exporting", total=len(tasks))
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_export_full_table, task, directory, chunk_size): task
                for task in tasks
            }

            for future in as_completed(futures):
                result = future.result()
                if isinstance(result, list):
                    for table_name, chunk_file in result:
                        chunk_files_by_table.setdefault(table_name, []).append(
                            chunk_file
                        )
                progress.advance(task_id)

    for table_name, chunk_files in chunk_files_by_table.items():
        merged_df = pd.concat([pd.read_parquet(f) for f in sorted(chunk_files)])
        merged_df.to_parquet(directory / f"{table_name}.parquet", compression=None)
        for chunk_file in chunk_files:
            chunk_file.unlink()


def _serialize_value(val):
    """Convert value to JSON string if it's a dict, list, or numpy array, otherwise return as-is."""
    if isinstance(val, (dict, list, np.ndarray)):
        return json.dumps(
            val, default=lambda o: o.tolist() if isinstance(o, np.ndarray) else None
        )
    return val


def _import_registry(
    registry: type[models.Model],
    directory: Path,
    if_exists: Literal["fail", "replace", "append"] = "replace",
) -> None:
    """Import a single registry table from parquet.

    For PostgreSQL, uses COPY FROM which bypasses SQL parsing and writes directly to
    table pages (20-50x faster than multi-row INSERTs).

    For SQLite, uses multi-row INSERTs with dynamic chunking to stay under the 999
    variable limit (2-5x faster than single-row INSERTs).
    """
    from django.db import connection

    table_name = registry._meta.db_table
    parquet_file = directory / f"{table_name}.parquet"

    if not parquet_file.exists():
        print(f"Skipped {table_name} (file not found)")
        return

    df = pd.read_parquet(parquet_file)

    old_foreign_key_columns = [col for col in df.columns if col.endswith("_old")]
    if old_foreign_key_columns:
        df = df.drop(columns=old_foreign_key_columns)

    for col in df.columns:
        if df[col].dtype == "object":
            mask = df[col].apply(lambda x: isinstance(x, (dict, list, np.ndarray)))
            if mask.any():
                df.loc[mask, col] = df.loc[mask, col].map(_serialize_value)

    if df.empty:
        return

    if connection.vendor == "postgresql":
        columns = df.columns.tolist()
        column_names = ", ".join(f'"{col}"' for col in columns)

        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False, sep="\t", na_rep="\\N")
        buffer.seek(0)

        with connection.cursor() as cursor:
            if if_exists == "replace":
                cursor.execute(f'DELETE FROM "{table_name}"')
            elif if_exists == "fail":
                cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
                if cursor.fetchone()[0] > 0:
                    raise ValueError(f"Table {table_name} already contains data")

            cursor.copy_expert(
                f"COPY \"{table_name}\" ({column_names}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')",
                buffer,
            )
    else:
        num_cols = len(df.columns)
        max_vars = 900  # SQLite has a limit of 999 variables per statement
        chunksize = max(1, max_vars // num_cols)

        df.to_sql(
            table_name,
            connection.connection,
            if_exists=if_exists,
            index=False,
            method="multi",
            chunksize=chunksize,
        )


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
    total_models = sum(len(models) for models in modules.values())

    is_sqlite = ln_setup.settings.instance.dialect == "sqlite"

    try:
        with connection.cursor() as cursor:
            if ln_setup.settings.instance.dialect == "postgresql":
                cursor.execute("SET session_replication_role = 'replica'")
            elif is_sqlite:
                cursor.execute("PRAGMA foreign_keys = OFF")
                # Disables fsync - OS buffers writes to disk, 10-50x faster but can corrupt DB on crash
                cursor.execute("PRAGMA synchronous = OFF")
                # Keeps rollback journal in RAM - 2-5x faster but cannot rollback on crash
                cursor.execute("PRAGMA journal_mode = MEMORY")
                # 64MB page cache for better performance on large imports
                cursor.execute("PRAGMA cache_size = -64000")

        with transaction.atomic():
            if ln_setup.settings.instance.dialect == "postgresql":
                with connection.cursor() as cursor:
                    cursor.execute("SET CONSTRAINTS ALL DEFERRED")

            with Progress() as progress:
                task = progress.add_task("Importing", total=total_models)
                for module_name, model_names in modules.items():
                    schema_module = import_module(module_name)
                    for model_name in model_names:
                        progress.update(
                            task, description=f"[cyan]{module_name}.{model_name}"
                        )
                        registry = getattr(schema_module, model_name)
                        _import_registry(registry, directory, if_exists=if_exists)
                        for field in registry._meta.many_to_many:
                            link_orm = getattr(registry, field.name).through
                            _import_registry(link_orm, directory, if_exists=if_exists)
                        progress.advance(task)
    finally:
        with connection.cursor() as cursor:
            if ln_setup.settings.instance.dialect == "postgresql":
                cursor.execute("SET session_replication_role = 'origin'")
            elif is_sqlite:
                cursor.execute("PRAGMA synchronous = FULL")
                cursor.execute("PRAGMA journal_mode = DELETE")
                cursor.execute("PRAGMA foreign_keys = ON")
