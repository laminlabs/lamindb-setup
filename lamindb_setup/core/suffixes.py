from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from lamindb_setup.types import AnyPath

# also see https://gist.github.com/securifera/e7eed730cbe1ce43d0c29d7cd2d582f4
# compression suffixes (see COMPRESSION_SUFFIXES) are handled separately
# suffixes marked "# loader" have an artifact loader in lamindb.core.loaders
VALID_SIMPLE_SUFFIXES = {
    #
    # bioinformatics / genomics
    #
    ".fasta",  # loader
    ".fastq",
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
    ".gb",
    ".genbank",
    ".embl",
    ".nwk",
    ".newick",
    ".obo",
    ".fcs",  # loader
    #
    # single-cell / omics
    #
    ".h5ad",  # loader
    ".h5mu",  # loader
    ".loom",
    #
    # tabular / dataframes
    #
    ".csv",  # loader
    ".tsv",  # loader
    ".parquet",  # loader
    ".xlsx",
    ".arrow",
    ".feather",
    ".orc",
    ".avro",
    #
    # arrays / matrices
    #
    ".mtx",
    ".npy",
    ".npz",
    ".h5",
    ".hdf5",
    #
    # array stores
    #
    ".zarr",  # loader
    ".tiledb",
    ".tiledbsoma",
    ".soma",
    #
    # serialization / config
    #
    ".json",  # loader
    ".jsonl",
    ".ndjson",
    ".yaml",  # loader
    ".yml",
    ".xml",
    ".pkl",
    ".pickle",
    ".qs",  # https://cran.r-project.org/web/packages/qs/vignettes/vignette.html
    ".rds",  # loader
    #
    # ml models & weights
    #
    ".pt",
    ".pth",
    ".ckpt",
    ".state_dict",
    ".keras",
    ".pb",
    ".pbtxt",
    ".savedmodel",
    ".bin",
    ".safetensors",
    ".model",
    ".mlmodel",
    ".mar",
    ".onnx",
    ".gguf",
    ".tflite",
    ".joblib",
    #
    # images / microscopy
    #
    ".jpg",  # loader
    ".jpeg",
    ".png",  # loader
    ".gif",  # loader
    ".svg",  # loader
    ".bmp",
    ".webp",
    ".tif",
    ".tiff",
    ".dcm",
    ".nd2",
    ".czi",
    ".lif",
    ".svs",
    #
    # documents / text
    #
    ".txt",  # loader
    ".md",
    ".html",  # loader
    ".pdf",
    ".ipynb",
    #
    # archives
    #
    ".tar",  # compression suffixes are handled separately
    ".zip",
    ".rar",
    ".7z",
    #
    # databases
    #
    ".db",
    ".duckdb",
    ".sqlite",
    #
    # misc
    #
    ".data",
}


VALID_COMPOSITE_SUFFIXES = {
    ".anndata.zarr",  # loader
    ".vitessce.json",  # loader
    ".ome.zarr",
    ".ome.h5",
    ".ome.hdf5",
}


# stream-compression suffixes that are appended on top of another suffix
# (e.g. .csv.gz, .tar.xz); handled specially rather than listed as simple suffixes
COMPRESSION_SUFFIXES = {
    ".gz",
    ".bz2",
    ".xz",
    ".zst",
}


class VALID_SUFFIXES:
    """Valid values for the `.suffix` field of the `Artifact` registry.

    Defines sets of valid suffixes and the logic to extract a valid suffix from a path,
    including composite (e.g. `.csv.gz`) suffixes.

    Only valid suffixes can populate the `suffix` field of the `Artifact` registry.
    """

    SIMPLE: set[str] = VALID_SIMPLE_SUFFIXES
    """Single-part suffixes such as `.csv`, `.h5ad` or `.parquet`.

    These correspond to the last component of a filename (`path.suffix`).
    """
    COMPOSITE: set[str] = VALID_COMPOSITE_SUFFIXES
    """Multi-part suffixes such as `.anndata.zarr` or `.ome.zarr`.

    Their meaning is carried by the combination of parts, so they take
    precedence over the trailing simple suffix (e.g. `.anndata.zarr` is
    preferred over `.zarr`).
    """
    COMPRESSION: set[str] = COMPRESSION_SUFFIXES
    """Stream-compression suffixes such as `.gz`, `.bz2`, `.xz` or `.zst`.

    These are appended to another suffix (e.g. `.csv.gz`, `.h5ad.tar.gz`).
    """


def extract_suffixes_from_path(path: AnyPath) -> tuple[str, str]:
    """Extract valid suffix and raw suffix from a path, including composite (e.g. `.csv.gz`) suffixes.

    Args:
        path: The path to extract the suffix from.

    Returns:
        A tuple of the valid suffix and the raw suffix.
    """
    # normalize to lowercase so uppercase variants (e.g. instrument output like
    # .TIFF, .CZI, .DCM) are recognized and returned in canonical lowercase form
    suffixes = [suffix.lower() for suffix in path.suffixes]
    last_suffix = suffixes[-1] if suffixes else ""
    total_suffix = "".join(suffixes)

    if len(suffixes) < 2:
        if (
            total_suffix in VALID_SIMPLE_SUFFIXES
            or total_suffix in COMPRESSION_SUFFIXES
        ):
            return total_suffix, total_suffix
        return "", last_suffix

    # further composite suffixes cases
    if total_suffix.endswith(tuple(VALID_COMPOSITE_SUFFIXES)):
        # below seems slow but OK for now
        for suffix in VALID_COMPOSITE_SUFFIXES:
            if total_suffix.endswith(suffix):
                break
        return suffix, suffix

    # after listed composite suffixes are checked
    if last_suffix in VALID_SIMPLE_SUFFIXES:
        return last_suffix, last_suffix

    # compression suffixes
    if last_suffix in COMPRESSION_SUFFIXES:
        suffix = "".join(suffixes[-2:])  # e.g. ".tar.gz", ".csv.bz2"
        if suffixes[-2] == ".tar":
            # if the suffix preceding ".tar.<compression>" is a valid suffix,
            # we account for it; otherwise we don't.
            # i.e. we should have .h5ad.tar.gz or .csv.tar.gz, not just .tar.gz
            if (
                len(suffixes) > 2
                and (suffix_3 := suffixes[-3]) in VALID_SIMPLE_SUFFIXES
            ):
                compression_suffix = suffix_3 + suffix
                return compression_suffix, compression_suffix
            return suffix, suffix
        elif suffixes[-2] in VALID_SIMPLE_SUFFIXES:
            return suffix, suffix
        return last_suffix, last_suffix

    return "", last_suffix
