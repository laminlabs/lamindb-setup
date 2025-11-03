from __future__ import annotations

import warnings
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
from django.db import models

if TYPE_CHECKING:
    from collections.abc import Sequence


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


def exportdb(
    module_names: Sequence[str] | None = None,
    output_dir: str | Path = "./lamindb_export/",
) -> None:
    """Export registry tables and many-to-many link tables to parquet files.

    Args:
        module_names: List of module names to export (e.g., ["lamindb", "bionty", "wetlab"]).
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
