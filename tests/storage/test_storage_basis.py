from __future__ import annotations

from pathlib import Path

from lamindb_setup.core.upath import extract_suffix_from_path


def test_extract_suffix_from_path():
    # this is a collection of path, stem, suffix tuples
    collection = [
        ("a", ""),
        ("a.txt", ".txt"),
        ("a.123", ""),  # digits are no valid suffixes
        ("archive.tar.gz", ".tar.gz"),
        ("directory/file", ""),
        ("d.x.y.z/f.b.c", ".c"),
        ("d.x.y.z/f.a.b.c", ".c"),
        ("logs/date.log.txt", ".txt"),
        ("logs/date.log.123", ""),  # digits are no valid suffixes
        ("salmon.merged.gene_counts.tsv", ".tsv"),
        ("salmon.merged.gene_counts.tsv.gz", ".tsv.gz"),
        ("filename.v1.1.0.spatialdata.zarr", ".spatialdata.zarr"),
    ]
    for path, suffix in collection:
        filepath = Path(path)
        assert suffix == extract_suffix_from_path(filepath)
