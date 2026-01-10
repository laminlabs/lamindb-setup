from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
import pytest
from lamindb_setup.io import _get_registries, export_db, import_db

if TYPE_CHECKING:
    from collections.abc import Callable, Generator
    from pathlib import Path


@pytest.fixture
def cleanup_export_dir(tmp_path) -> Generator[Path, None, None]:
    output_dir = tmp_path / "test_export"
    yield output_dir


def test_get_registries_lamindb(simple_instance: Callable):
    registries = _get_registries("lamindb")

    assert "Artifact" in registries
    assert "Collection" in registries
    assert "Run" in registries
    assert "Transform" in registries
    assert "Record" in registries

    assert "SQLRecord" not in registries


def test_get_registries_bionty(simple_instance: Callable):
    registries = _get_registries("bionty")

    assert "Gene" in registries
    assert "Protein" in registries
    assert "CellType" in registries
    # ... and a few more
    assert len(registries) > 0


def test_exportdb_creates_directory(
    simple_instance: Callable, cleanup_export_dir: Path
):
    export_db(
        module_names=["lamindb"],
        output_dir=cleanup_export_dir,
    )

    assert cleanup_export_dir.exists()
    assert cleanup_export_dir.is_dir()


def test_exportdb_exports_parquet_files(
    simple_instance: Callable, cleanup_export_dir: Path
):
    export_db(
        module_names=["lamindb"],
        output_dir=cleanup_export_dir,
    )

    parquet_files = list(cleanup_export_dir.glob("*.parquet"))
    assert len(parquet_files) > 0

    for file in parquet_files:
        df = pd.read_parquet(file)
        assert isinstance(df, pd.DataFrame)


def test_exportdb_multiple_modules(simple_instance: Callable, cleanup_export_dir: Path):
    import bionty as bt
    import lamindb as ln

    artifact = ln.Artifact.from_dataframe(
        pd.DataFrame({"col": [1, 2, 3]}), key="test_artifact.parquet"
    ).save()
    gene = bt.Gene.from_source(symbol="TCF7", organism="human").save()
    artifact.genes.add(gene)
    feature = ln.Feature(name="temperature", dtype=int).save()
    artifact.features.add_values({"temperature": 10})

    export_db(
        module_names=["lamindb", "bionty"],
        output_dir=cleanup_export_dir,
    )

    lamindb_files = list(cleanup_export_dir.glob("lamindb_*.parquet"))
    bionty_files = list(cleanup_export_dir.glob("bionty_*.parquet"))

    assert len(lamindb_files) > 0
    assert len(bionty_files) > 0

    gene_df = pd.read_parquet(cleanup_export_dir / "bionty_gene.parquet")
    assert "TCF7" in gene_df["symbol"].values

    jsonvalue_df = pd.read_parquet(cleanup_export_dir / "lamindb_jsonvalue.parquet")
    assert len(jsonvalue_df) == 1
    assert jsonvalue_df.iloc[0]["feature_id"] == feature.id
    assert jsonvalue_df.iloc[0]["value"] == "10"

    artifact_df = pd.read_parquet(cleanup_export_dir / "lamindb_artifact.parquet")
    assert artifact.uid in artifact_df["uid"].values

    link_df = pd.read_parquet(cleanup_export_dir / "bionty_artifactgene.parquet")
    assert (
        len(
            link_df[
                (link_df["artifact_id"] == artifact.id)
                & (link_df["gene_id"] == gene.id)
            ]
        )
        == 1
    )

    artifact.delete(permanent=True)
    gene.delete(permanent=True)
    feature.delete(permanent=True)


def test_exportdb_default_module(simple_instance: Callable, cleanup_export_dir: Path):
    export_db(output_dir=cleanup_export_dir)

    lamindb_files = list(cleanup_export_dir.glob("lamindb_*.parquet"))
    assert len(lamindb_files) > 0


def test_exportdb_exports_link_tables(
    simple_instance: Callable, cleanup_export_dir: Path
):
    export_db(module_names=["lamindb"], output_dir=cleanup_export_dir)

    parquet_files = [f.name for f in cleanup_export_dir.glob("*.parquet")]
    link_tables = [f for f in parquet_files if "_" in f and "artifact" in f.lower()]

    assert len(link_tables) > 0


def test_exportdb_handles_mixed_null_and_string_values(
    simple_instance: Callable, cleanup_export_dir: Path
):
    import bionty as bt
    import lamindb_setup as ln_setup

    organism = bt.Organism.get(name="human")

    # Create one gene with ncbi_gene_ids populated
    gene1 = bt.Gene(
        symbol="GENE1",
        ensembl_gene_id="ENSG00000001",
        ncbi_gene_ids="12345,67890",  # String value
        organism=organism,
        source_id=1,
        created_by_id=ln_setup.settings.user.id,
    ).save()

    # Create one gene with ncbi_gene_ids NULL
    gene2 = bt.Gene(
        symbol="GENE2",
        ensembl_gene_id="ENSG00000002",
        ncbi_gene_ids=None,  # NULL value
        organism=organism,
        source_id=1,
        created_by_id=ln_setup.settings.user.id,
    ).save()

    # This would crash with ArrowTypeError without keep_default_na=False
    export_db(module_names=["bionty"], output_dir=cleanup_export_dir)

    gene1.delete(permanent=True)
    gene2.delete(permanent=True)


def test_import_db_from_parquet(simple_instance: Callable, tmp_path: Path):
    """Tests imports of a parquet file.

    Implicitly also tests whether `import_db` can deal with FK constraints.
    """
    import bionty as bt
    import lamindb as ln
    import lamindb_setup as ln_setup

    export_dir = tmp_path / "export"
    export_dir.mkdir()

    artifact_data = pd.DataFrame(
        {
            "id": [888],
            "uid": ["test_artifact_uid"],
            "key": ["test_key"],
            "_key_is_virtual": [False],
            "_overwrite_versions": [False],
            "description": ["Test artifact"],
            "suffix": [".txt"],
            "kind": ["dataset"],
            "size": [1024],
            "hash": ["testhash123"],
            "is_latest": [True],
            "is_locked": [False],
            "storage_id": [1],
            "created_by_id": [ln_setup.settings.user.id],
            "created_at": [pd.Timestamp.now()],
            "updated_at": [pd.Timestamp.now()],
        }
    )
    artifact_data.to_parquet(export_dir / "lamindb_artifact.parquet", index=False)

    gene_data = pd.DataFrame(
        {
            "id": [999],
            "uid": ["test_uid_999"],
            "symbol": ["TESTGENE"],
            "ensembl_gene_id": ["ENSG00000999"],
            "ncbi_gene_ids": [None],
            "biotype": ["protein_coding"],
            "description": ["Test gene for import"],
            "synonyms": [None],
            "organism_id": [1],
            "source_id": [1],
            "created_by_id": [ln_setup.settings.user.id],
            "created_at": [pd.Timestamp.now()],
            "updated_at": [pd.Timestamp.now()],
        }
    )
    gene_data.to_parquet(export_dir / "bionty_gene.parquet", index=False)

    link_data = pd.DataFrame(
        {
            "id": [1],
            "artifact_id": [888],
            "gene_id": [999],
            "feature_id": [None],
            "created_at": [pd.Timestamp.now()],
            "created_by_id": [ln_setup.settings.user.id],
            "run_id": [None],
        }
    )
    link_data.to_parquet(export_dir / "bionty_artifactgene.parquet", index=False)

    import_db(
        input_dir=export_dir,
        module_names=["lamindb", "bionty"],
        if_exists="append",
    )

    # gene and artifact should exist after the import
    imported_gene = bt.Gene.get(id=999)
    assert imported_gene.symbol == "TESTGENE"
    assert imported_gene.ensembl_gene_id == "ENSG00000999"
    imported_artifact = ln.Artifact.get(id=888)
    assert imported_artifact.key == "test_key"
    assert imported_artifact.genes.count() == 1

    # they should also be linked
    linked_gene = imported_artifact.genes.first()
    assert linked_gene.id == 999
    assert linked_gene.symbol == "TESTGENE"

    # Verify PRIMARY KEY constraint is preserved for "append" mode that we used here
    from django.db import connection

    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='lamindb_artifact'"
        )
        create_stmt = cursor.fetchone()[0]
        assert "PRIMARY KEY" in create_stmt


def test_import_db_converts_boolean_strings(simple_instance: Callable, tmp_path: Path):
    import lamindb as ln
    import lamindb_setup as ln_setup

    export_dir = tmp_path / "export"
    export_dir.mkdir()

    artifact_data = pd.DataFrame(
        {
            "id": [777],
            "uid": ["test_bool_uid"],
            "key": ["test_bool_key"],
            "_key_is_virtual": ["t"],
            "_overwrite_versions": ["f"],
            "description": ["Test boolean conversion"],
            "suffix": [".txt"],
            "kind": ["dataset"],
            "size": [1024],
            "hash": ["testhash456"],
            "is_latest": ["t"],
            "is_locked": ["f"],
            "storage_id": [1],
            "created_by_id": [ln_setup.settings.user.id],
            "created_at": [pd.Timestamp.now()],
            "updated_at": [pd.Timestamp.now()],
        }
    )
    artifact_data.to_parquet(export_dir / "lamindb_artifact.parquet", index=False)

    import_db(input_dir=export_dir, module_names=["lamindb"], if_exists="append")

    imported = ln.Artifact.get(id=777)
    assert imported._key_is_virtual is True
    assert imported._overwrite_versions is False
    assert imported.is_latest is True
    assert imported.is_locked is False


def test_import_db_converts_empty_strings_to_none(
    simple_instance: Callable, tmp_path: Path
):
    import lamindb as ln
    import lamindb_setup as ln_setup

    export_dir = tmp_path / "export"
    export_dir.mkdir()

    artifact_data = pd.DataFrame(
        {
            "id": [666],
            "uid": ["test_empty_str_uid"],
            "key": ["test_empty_str_key"],
            "_key_is_virtual": ["f"],
            "_overwrite_versions": ["f"],
            "_real_key": [""],
            "description": [""],
            "suffix": [".txt"],
            "kind": ["dataset"],
            "size": [1024],
            "hash": ["testhash789"],
            "is_latest": ["t"],
            "is_locked": ["f"],
            "storage_id": [1],
            "created_by_id": [ln_setup.settings.user.id],
            "created_at": [pd.Timestamp.now()],
            "updated_at": [pd.Timestamp.now()],
        }
    )
    artifact_data.to_parquet(export_dir / "lamindb_artifact.parquet", index=False)

    import_db(input_dir=export_dir, module_names=["lamindb"], if_exists="append")

    imported = ln.Artifact.get(id=666)
    assert imported._real_key is None
    assert imported.description is None


def test_import_db_converts_numeric_strings(simple_instance: Callable, tmp_path: Path):
    import lamindb as ln
    import lamindb_setup as ln_setup

    export_dir = tmp_path / "export"
    export_dir.mkdir()

    artifact_data = pd.DataFrame(
        {
            "id": ["555"],
            "uid": ["test_numeric_uid"],
            "key": ["test_numeric_key"],
            "_key_is_virtual": ["f"],
            "_overwrite_versions": ["f"],
            "description": ["Test numeric conversion"],
            "suffix": [".txt"],
            "kind": ["dataset"],
            "size": ["2048"],
            "hash": ["testhash999"],
            "is_latest": ["t"],
            "is_locked": ["f"],
            "storage_id": ["1"],
            "created_by_id": [str(ln_setup.settings.user.id)],
            "created_at": [pd.Timestamp.now()],
            "updated_at": [pd.Timestamp.now()],
        }
    )
    artifact_data.to_parquet(export_dir / "lamindb_artifact.parquet", index=False)

    import_db(input_dir=export_dir, module_names=["lamindb"], if_exists="append")

    imported = ln.Artifact.get(id=555)
    assert isinstance(imported.size, int)
    assert imported.size == 2048
