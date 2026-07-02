from __future__ import annotations

from pathlib import Path

from lamindb_setup.core.upath import extract_suffix_from_path


def test_extract_suffix_from_path():
    # this is a collection of path, stem, suffix tuples
    collection = [
        # no / unknown suffix
        ("a", ""),
        ("a.txt", ".txt"),
        ("a.123", ""),
        ("directory/file", ""),
        ("d.x.y.z/f.b.c", ""),
        ("d.x.y.z/f.a.b.c", ""),
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
        # uppercase suffixes are normalized to canonical lowercase
        ("scan.TIFF", ".tiff"),
        ("image.PNG", ".png"),
        ("photo.JPG", ".jpg"),
        ("sample.OME.ZARR", ".ome.zarr"),
        ("variants.VCF.GZ", ".vcf.gz"),
        ("unknown.XYZ", ""),
    ]
    for path, suffix in collection:
        filepath = Path(path)
        assert suffix == extract_suffix_from_path(filepath)
