"""Canonical artifact suffix utilities."""

from __future__ import annotations

from pathlib import PurePosixPath
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from lamindb_setup.types import AnyPath

# also see https://gist.github.com/securifera/e7eed730cbe1ce43d0c29d7cd2d582f4
# compression suffixes (see COMPRESSION_FORMATS) are handled separately
# suffixes marked "# loader" have an artifact loader in lamindb.core.loaders
SIMPLE_FORMATS = {
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


COMPOSITE_FORMATS = {
    ".anndata.zarr",  # loader
    ".vitessce.json",  # loader
    ".ome.zarr",
    ".ome.h5",
    ".ome.hdf5",
}


# stream-compression suffixes that are appended on top of another suffix
# (e.g. .csv.gz, .tar.xz); handled specially rather than listed as simple suffixes
COMPRESSION_FORMATS = {
    ".gz",
    ".bz2",
    ".xz",
    ".zst",
}


class CanonicalSuffix(str):
    """Strings that inform a storage format.

    Canonical suffixes populate the `.suffix` field of the `Artifact` registry
    based on the set of known storage formats in `SIMPLE`, `COMPOSITE`, and `COMPRESSION`.

    For unknown storage formats, the canonical suffix is the empty string.

    Examples:

        Construct from a path::

            CanonicalSuffix.from_path("data/sample.csv")
            #> CanonicalSuffix('.csv')

            CanonicalSuffix.extract_from_path("myfile.abcdedf")
            #> CanonicalSuffix('')

        Typically, you're interested in the canonical suffix and the raw string suffix::

            CanonicalSuffix.extract_from_path("data/sample.csv")
            #> CanonicalSuffix('.csv'), ".csv"

            CanonicalSuffix.extract_from_path("myfile.abcdedf")
            #> CanonicalSuffix(''), ".abcdedf"

            CanonicalSuffix.extract_from_path("data/sample.csv.gz")
            #> CanonicalSuffix('.csv.gz'), ".csv.gz"

            CanonicalSuffix.extract_from_path("data/sample.anndata.zarr")
            #> CanonicalSuffix('.anndata.zarr'), ".anndata.zarr"

    """

    SIMPLE: set[str] = SIMPLE_FORMATS
    """Formats such as `.csv`, `.h5ad` or `.parquet`.

    These correspond to the last component of a filename (`path.suffix`).
    """
    COMPOSITE: set[str] = COMPOSITE_FORMATS
    """Formats denoted by composite suffixes such as `.anndata.zarr` or `.ome.zarr`.

    Their meaning is carried by the combination of parts, so they take
    precedence over the trailing simple suffix (e.g. `.anndata.zarr` is
    preferred over `.zarr`).
    """
    COMPRESSION: set[str] = COMPRESSION_FORMATS
    """Stream-compression formats such as `.gz`, `.bz2`, `.xz` or `.zst`.

    These are appended to another suffix (e.g. `.csv.gz`, `.h5ad.tar.gz`).
    """
    _skip_validation = False

    def __new__(cls, value: str) -> CanonicalSuffix:
        canonical_value = value.lower()
        if cls._skip_validation:
            return super().__new__(cls, canonical_value)
        if canonical_value != "":
            cls._skip_validation = True
            try:
                extracted, _ = cls.extract_from_path(
                    PurePosixPath(f"file{canonical_value}")
                )
            finally:
                cls._skip_validation = False
            if extracted != canonical_value:
                raise ValueError(f"Invalid canonical suffix: {value!r}")
        return super().__new__(cls, canonical_value)

    @classmethod
    def from_path(cls, path: AnyPath) -> CanonicalSuffix:
        """Construct a canonical suffix from a path.

        Note that this returns the empty string if the path doesn't
        contain a suffix that maps on a known formats.

        Args:
            path: The path to extract the suffix from.
        """
        return cls.extract_from_path(path)[0]

    @classmethod
    def extract_from_path(cls, path: AnyPath) -> tuple[CanonicalSuffix, str]:
        """Extract a validated canonical suffix and a raw suffix from a path.

        This also treats composite (e.g. `.csv.gz`) suffixes.

        Args:
            path: The path to extract the suffix from.

        Returns:
            A tuple consisting in the canonical suffix and a raw string suffix.
        """
        # normalize to lowercase so uppercase variants (e.g. instrument output like
        # .TIFF, .CZI, .DCM) are recognized and returned in canonical lowercase form
        suffixes = [suffix.lower() for suffix in path.suffixes]
        last_suffix = suffixes[-1] if suffixes else ""
        total_suffix = "".join(suffixes)

        if len(suffixes) < 2:
            if total_suffix in SIMPLE_FORMATS or total_suffix in COMPRESSION_FORMATS:
                return cls(total_suffix), total_suffix
            return cls(""), last_suffix

        # further composite suffixes cases
        if total_suffix.endswith(tuple(COMPOSITE_FORMATS)):
            # below seems slow but OK for now
            for suffix in COMPOSITE_FORMATS:
                if total_suffix.endswith(suffix):
                    break
            return cls(suffix), suffix

        # after listed composite suffixes are checked
        if last_suffix in SIMPLE_FORMATS:
            return cls(last_suffix), last_suffix

        # compression suffixes
        if last_suffix in COMPRESSION_FORMATS:
            suffix = "".join(suffixes[-2:])  # e.g. ".tar.gz", ".csv.bz2"
            if suffixes[-2] == ".tar":
                # if the suffix preceding ".tar.<compression>" is a valid suffix,
                # we account for it; otherwise we don't.
                # i.e. we should have .h5ad.tar.gz or .csv.tar.gz, not just .tar.gz
                if len(suffixes) > 2 and (suffix_3 := suffixes[-3]) in SIMPLE_FORMATS:
                    compression_suffix = suffix_3 + suffix
                    return cls(compression_suffix), compression_suffix
                return cls(suffix), suffix
            elif suffixes[-2] in SIMPLE_FORMATS:
                return cls(suffix), suffix
            return cls(last_suffix), last_suffix

        return cls(""), last_suffix


__all__ = [
    "CanonicalSuffix",
]
