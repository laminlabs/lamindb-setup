from __future__ import annotations

import mimetypes
from pathlib import Path

from lamindb_setup.core.canonical_suffix import SIMPLE_FORMATS, CanonicalSuffix


def test_extract_from_path():
    # this is a collection of path, stem, suffix tuples
    collection = [
        # no / unknown suffix
        ("a", ""),
        ("a.txt", ".txt"),
        ("a.123", ""),
        ("directory/file", ""),
        ("d.x.y.z/f.b.c", ".c"),
        ("d.x.y.z/f.a.b.c", ".c"),
        ("logs/date.log.txt", ".txt"),
        ("logs/date.log.123", ""),
        ("some.unknown.suffix", ""),
        # simple suffix with dotted stem
        ("salmon.merged.gene_counts.tsv", ".tsv"),
        ("filename.h5ad.zarr", ".zarr"),
        # newly whitelisted simple suffixes
        ("reads.bam", ".bam"),
        ("variants.vcf", ".vcf"),
        ("store.h5", ".h5"),
        ("array.npy", ".npy"),
        ("model.onnx", ".onnx"),
        ("notebook.ipynb", ".ipynb"),
        # composite suffixes take precedence over their last simple suffix
        ("filename.v1.1.0.anndata.zarr", ".anndata.zarr"),
        ("sample.ome.zarr", ".ome.zarr"),
        ("sample.ome.h5", ".ome.h5"),
        ("sample.ome.hdf5", ".ome.hdf5"),
        ("dashboard.vitessce.json", ".vitessce.json"),
        # a plain .json / .zarr is not treated as composite
        ("data.config.json", ".json"),
        # .gz compression handling
        ("plain.gz", ".gz"),
        ("archive.tar.gz", ".tar.gz"),
        ("salmon.merged.gene_counts.tsv.gz", ".tsv.gz"),
        ("variants.vcf.gz", ".vcf.gz"),
        ("filename.h5ad.tar.gz", ".h5ad.tar.gz"),
        # unknown suffix preceding .gz falls back safely
        ("file.random.gz", ".gz"),
        ("foo.bar.tar.gz", ".tar.gz"),
        # other compression suffixes handled like .gz
        ("plain.bz2", ".bz2"),
        ("plain.xz", ".xz"),
        ("plain.zst", ".zst"),
        ("data.csv.bz2", ".csv.bz2"),
        ("data.tsv.xz", ".tsv.xz"),
        ("archive.tar.bz2", ".tar.bz2"),
        ("archive.tar.zst", ".tar.zst"),
        ("filename.h5ad.tar.xz", ".h5ad.tar.xz"),
        ("file.random.zst", ".zst"),
        # uppercase suffixes are normalized to canonical lowercase
        ("scan.TIFF", ".tiff"),
        ("image.PNG", ".png"),
        ("photo.JPG", ".jpg"),
        ("sample.OME.ZARR", ".ome.zarr"),
        ("variants.VCF.GZ", ".vcf.gz"),
        ("unknown.XYZ", ""),
    ]
    for path, canonical_suffix in collection:
        filepath = Path(path)
        # from_path calls extract_from_path
        assert canonical_suffix == CanonicalSuffix.from_path(filepath)


def test_mime_extensions_are_included():
    mime_suffixes = {
        suffix.lower()
        for suffix in (
            set(mimetypes.types_map.keys()) | set(mimetypes.common_types.keys())
        )
    }
    # ensure we test MIME-driven behavior and not only ADD_SIMPLE_FORMATS
    mime_only = sorted(mime_suffixes - SIMPLE_FORMATS)
    assert len(mime_only) > 0
    mime_suffix = mime_only[0]
    assert CanonicalSuffix.from_path(Path(f"file{mime_suffix}")) == mime_suffix


def test_runtime_extension_simple_suffixes():
    custom_suffix = ".myformat"
    assert CanonicalSuffix.from_path(Path(f"file{custom_suffix}")) == ""
    CanonicalSuffix.simple_formats.add(custom_suffix)
    assert CanonicalSuffix.from_path(Path(f"file{custom_suffix}")) == custom_suffix
    CanonicalSuffix.simple_formats.discard(custom_suffix)
    assert CanonicalSuffix.from_path(Path(f"file{custom_suffix}")) == ""
