from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
import pytest
from lamindb_setup._exportdb import _get_registries, exportdb

if TYPE_CHECKING:
    from collections.abc import Callable, Generator
    from pathlib import Path


@pytest.fixture
def cleanup_export_dir(tmp_path) -> Generator[Path, None, None]:
    output_dir = tmp_path / "test_export"
    yield output_dir


def test_get_registries_lamindb(create_myinstance: Callable):
    registries = _get_registries("lamindb")

    assert "Artifact" in registries
    assert "Collection" in registries
    assert "Run" in registries
    assert "Transform" in registries
    assert "Record" in registries

    assert "SQLRecord" not in registries


def test_get_registries_bionty(create_myinstance: Callable):
    registries = _get_registries("bionty")

    assert "Gene" in registries
    assert "Protein" in registries
    assert "CellType" in registries
    # ... and a few more
    assert len(registries) > 0


def test_exportdb_creates_directory(
    create_myinstance: Callable, cleanup_export_dir: Path
):
    exportdb(output_dir=cleanup_export_dir, module_names=["lamindb"])

    assert cleanup_export_dir.exists()
    assert cleanup_export_dir.is_dir()


def test_exportdb_exports_parquet_files(
    create_myinstance: Callable, cleanup_export_dir: Path
):
    exportdb(output_dir=cleanup_export_dir, module_names=["lamindb"])

    parquet_files = list(cleanup_export_dir.glob("*.parquet"))
    assert len(parquet_files) > 0

    for file in parquet_files:
        df = pd.read_parquet(file)
        assert isinstance(df, pd.DataFrame)


def test_exportdb_multiple_modules(
    create_myinstance: Callable, cleanup_export_dir: Path
):
    import bionty as bt

    gene = bt.Gene.from_source(symbol="TCF7").save()

    exportdb(output_dir=cleanup_export_dir, module_names=["lamindb", "bionty"])

    lamindb_files = list(cleanup_export_dir.glob("lamindb_*.parquet"))
    bionty_files = list(cleanup_export_dir.glob("bionty_*.parquet"))

    assert len(lamindb_files) > 0
    assert len(bionty_files) > 0

    gene_df = pd.read_parquet(cleanup_export_dir / "bionty_gene.parquet")
    assert "TCF7" in gene_df["symbol"].values

    gene.delete(permanent=True)


def test_exportdb_default_module(create_myinstance: Callable, cleanup_export_dir: Path):
    exportdb(output_dir=cleanup_export_dir)

    lamindb_files = list(cleanup_export_dir.glob("lamindb_*.parquet"))
    assert len(lamindb_files) > 0


def test_exportdb_exports_link_tables(
    create_myinstance: Callable, cleanup_export_dir: Path
):
    exportdb(output_dir=cleanup_export_dir, module_names=["lamindb"])

    parquet_files = [f.name for f in cleanup_export_dir.glob("*.parquet")]
    link_tables = [f for f in parquet_files if "_" in f and "artifact" in f.lower()]

    assert len(link_tables) > 0
