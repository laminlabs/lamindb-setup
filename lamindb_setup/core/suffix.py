from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from lamindb_setup.types import AnyPath

# also see https://gist.github.com/securifera/e7eed730cbe1ce43d0c29d7cd2d582f4
# .gz is handled separately
VALID_SIMPLE_SUFFIXES = {
    #
    # without readers
    #
    ".fasta",
    ".fastq",
    ".jpg",
    ".mtx",
    ".obo",
    ".pdf",
    ".png",
    ".tar",
    ".tiff",
    ".txt",
    ".tsv",
    ".zip",
    ".xml",
    ".qs",  # https://cran.r-project.org/web/packages/qs/vignettes/vignette.html
    ".rds",
    ".pt",
    ".pth",
    ".ckpt",
    ".state_dict",
    ".keras",
    ".pb",
    ".pbtxt",
    ".savedmodel",
    ".pkl",
    ".pickle",
    ".bin",
    ".safetensors",
    ".model",
    ".mlmodel",
    ".mar",
    ".tiledbsoma",
    ".soma",
    ".tiledb",
    ".data",
    # .gz also but handled separately
    #
    # with readers (see below)
    #
    ".h5ad",
    ".parquet",
    ".csv",
    ".fcs",
    ".xlsx",
    ".zarr",
    ".json",
    #
    # added by opus 4.8
    #
    # bioinformatics / genomics
    ".bam",
    ".sam",
    ".cram",
    ".vcf",
    ".bcf",
    ".bed",
    ".gff",
    ".gff3",
    ".gtf",
    ".bigwig",
    ".bw",
    ".bedgraph",
    ".h5",
    ".hdf5",
    ".loom",
    ".gb",
    ".genbank",
    ".embl",
    ".nwk",
    ".newick",
    # tabular / data
    ".arrow",
    ".feather",
    ".orc",
    ".avro",
    ".ndjson",
    ".jsonl",
    ".yaml",
    ".yml",
    ".duckdb",
    ".db",
    ".sqlite",
    # images / microscopy
    ".svg",
    ".gif",
    ".bmp",
    ".webp",
    ".dcm",
    ".nd2",
    ".czi",
    ".lif",
    ".svs",
    ".tif",
    ".jpeg",
    # ml / arrays
    ".npy",
    ".npz",
    ".onnx",
    ".gguf",
    ".tflite",
    ".joblib",
    # documents / misc
    ".md",
    ".html",
    ".ipynb",
}


VALID_COMPOSITE_SUFFIXES = {
    ".anndata.zarr",
    ".vitessce.json",
    ".ome.zarr",
    # ".spatialdata.zarr" ?
    #
    # added by opus 4.8
    #
    ".ome.h5",
    ".ome.hdf5",
}


class VALID_SUFFIXES:
    """Valid suffixes."""

    SIMPLE: set[str] = VALID_SIMPLE_SUFFIXES
    """Simple suffixes."""
    COMPOSITE: set[str] = VALID_COMPOSITE_SUFFIXES
    """Composite suffixes."""


# this extracts only valid suffixes from the lists above and handles compression suffixes
def extract_suffix_from_path(path: AnyPath) -> str:
    # normalize to lowercase so uppercase variants (e.g. instrument output like
    # .TIFF, .CZI, .DCM) are recognized and returned in canonical lowercase form
    suffixes = [suffix.lower() for suffix in path.suffixes]
    total_suffix = "".join(suffixes)

    if len(suffixes) < 2:
        if total_suffix in VALID_SIMPLE_SUFFIXES or total_suffix == ".gz":
            return total_suffix
        return ""

    # further composite suffixes cases

    if total_suffix.endswith(tuple(VALID_COMPOSITE_SUFFIXES)):
        # below seems slow but OK for now
        for suffix in VALID_COMPOSITE_SUFFIXES:
            if total_suffix.endswith(suffix):
                break
        return suffix

    # after listed composite suffixes are checked
    last_suffix = suffixes[-1]
    if last_suffix in VALID_SIMPLE_SUFFIXES:
        return last_suffix

    # compression suffixes

    # Alex thought about adding logic along the lines of path.suffixes[-1]
    # in COMPRESSION_SUFFIXES to detect something like .random.gz and then
    # add ".random.gz" but concluded it's too dangerous it's safer to just
    # use ".gz" in such a case
    if last_suffix == ".gz":
        suffix = "".join(suffixes[-2:])
        if suffix == ".tar.gz":
            # if the suffix preceding the compression suffixes is a valid suffix,
            # we account for it; otherwise we don't.
            # i.e. we should have .h5ad.tar.gz or .csv.tar.gz, not just .tar.gz
            if (
                len(suffixes) > 2
                and (suffix_3 := suffixes[-3]) in VALID_SIMPLE_SUFFIXES
            ):
                return suffix_3 + suffix
            return ".tar.gz"
        elif suffixes[-2] in VALID_SIMPLE_SUFFIXES:
            return suffix
        return ".gz"

    return ""
