"""The `CanonicalSuffix` class."""
# the main class of this module is re-exported and documented in lamindb.base.types
# the location of canonical_suffix.py in lamindb_setup is for internal use
# or for developers who want to avoid importing lamindb
# this is the same rationale as for the upath.py module in lamindb_setup/core

from __future__ import annotations

import mimetypes
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from lamindb_setup.types import AnyPath

# these are simple formats that are not part of the MIME registry and should be
# recognized during validation of CanonicalSuffix instances
SIMPLE_FORMATS = {
    #
    # MIMETYPE suffixes
    #
    ".3g2",
    ".3gp",
    ".3gpp",
    ".3gpp2",
    ".a",
    ".aac",
    ".adts",
    ".ai",
    ".aif",
    ".aifc",
    ".aiff",
    ".ass",
    ".au",
    ".avi",
    ".avif",
    ".bat",
    ".bcpio",
    ".bin",
    ".bmp",
    ".c",
    ".cdf",
    ".cpio",
    ".csh",
    ".css",
    ".csv",
    ".dll",
    ".doc",
    ".dot",
    ".dvi",
    ".eml",
    ".eps",
    ".etx",
    ".exe",
    ".gif",
    ".gtar",
    ".h",
    ".h5",
    ".hdf",
    ".heic",
    ".heif",
    ".htm",
    ".html",
    ".ico",
    ".ief",
    ".jpe",
    ".jpeg",
    ".jpg",
    ".js",
    ".json",
    ".ksh",
    ".latex",
    ".loas",
    ".m1v",
    ".m3u",
    ".m3u8",
    ".man",
    ".markdown",
    ".md",
    ".me",
    ".mht",
    ".mhtml",
    ".mid",
    ".midi",
    ".mif",
    ".mjs",
    ".mov",
    ".movie",
    ".mp2",
    ".mp3",
    ".mp4",
    ".mpa",
    ".mpe",
    ".mpeg",
    ".mpg",
    ".ms",
    ".n3",
    ".nc",
    ".nq",
    ".nt",
    ".nws",
    ".o",
    ".obj",
    ".oda",
    ".opus",
    ".p12",
    ".p7c",
    ".pbm",
    ".pct",
    ".pdf",
    ".pfx",
    ".pgm",
    ".pic",
    ".pict",
    ".pl",
    ".png",
    ".pnm",
    ".pot",
    ".ppa",
    ".ppm",
    ".pps",
    ".ppt",
    ".ps",
    ".pwz",
    ".py",
    ".pyc",
    ".pyo",
    ".qt",
    ".ra",
    ".ram",
    ".ras",
    ".rdf",
    ".rgb",
    ".roff",
    ".rtf",
    ".rtx",
    ".sgm",
    ".sgml",
    ".sh",
    ".shar",
    ".snd",
    ".so",
    ".src",
    ".srt",
    ".sv4cpio",
    ".sv4crc",
    ".svg",
    ".swf",
    ".t",
    ".tar",
    ".tcl",
    ".tex",
    ".texi",
    ".texinfo",
    ".tif",
    ".tiff",
    ".tr",
    ".trig",
    ".tsv",
    ".txt",
    ".ustar",
    ".vcf",
    ".vtt",
    ".wasm",
    ".wav",
    ".webm",
    ".webmanifest",
    ".webp",
    ".wiz",
    ".wsdl",
    ".xbm",
    ".xlb",
    ".xls",
    ".xml",
    ".xpdl",
    ".xpm",
    ".xsl",
    ".xul",
    ".xwd",
    ".zip",
    #
    # bioinformatics / genomics
    #
    ".fasta",  # loader
    ".fastq",
    ".bam",
    ".sam",
    ".cram",
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
    ".fcs",
    #
    # single-cell / omics
    #
    ".h5ad",
    ".h5mu",
    ".loom",
    #
    # tabular / dataframes
    #
    ".parquet",
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
    ".hdf5",
    #
    # array stores
    #
    ".zarr",
    ".tiledb",
    ".tiledbsoma",
    ".soma",
    #
    # serialization / config
    #
    ".jsonl",
    ".ndjson",
    ".yaml",
    ".yml",
    ".pkl",
    ".pickle",
    ".qs",  # https://cran.r-project.org/web/packages/qs/vignettes/vignette.html
    ".rds",
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
    ".dcm",
    ".nd2",
    ".czi",
    ".lif",
    ".svs",
    #
    # documents / text
    #
    ".ipynb",
    #
    # archives
    #
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


# stream-encoding suffixes that are appended on top of another suffix
# (e.g. .csv.gz, .tar.xz); handled specially rather than listed as simple suffixes
ENCODING_FORMATS = {
    ".gz",
    ".bz2",
    ".xz",
    ".zst",
}


class CanonicalSuffixMeta(type):
    @property
    def simple_formats(cls) -> set[str]:
        typed_cls = cast("type[CanonicalSuffix]", cls)
        typed_cls._ensure_format_sets()
        assert typed_cls._simple_formats is not None
        return typed_cls._simple_formats

    @property
    def composite_formats(cls) -> set[str]:
        typed_cls = cast("type[CanonicalSuffix]", cls)
        typed_cls._ensure_format_sets()
        assert typed_cls._composite_formats is not None
        return typed_cls._composite_formats

    @property
    def encoding_formats(cls) -> set[str]:
        typed_cls = cast("type[CanonicalSuffix]", cls)
        typed_cls._ensure_format_sets()
        assert typed_cls._encoding_formats is not None
        return typed_cls._encoding_formats


class CanonicalSuffix(str, metaclass=CanonicalSuffixMeta):
    """Strings that inform a storage format.

    Canonical suffixes populate the `.suffix` field of the `Artifact` registry
    based on the known storage formats defined in this class.

    For unknown storage formats, the canonical suffix is the empty string.

    The known formats extend the international MIMETYPE registry based on
    `mimetypes` in the Python standard library.

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

            CanonicalSuffix.extract_from_path("image.PNG")
            #> CanonicalSuffix('.png'), ".png"

            CanonicalSuffix.extract_from_path("archive.tar.gz")
            #> CanonicalSuffix('.tar.gz'), ".tar.gz"

            CanonicalSuffix.extract_from_path("filename.h5ad.tar.xz")
            #> CanonicalSuffix('.h5ad.tar.xz'), ".h5ad.tar.xz"

            CanonicalSuffix.extract_from_path("file.random.gz")
            #> CanonicalSuffix('.gz'), ".gz"

            CanonicalSuffix.extract_from_path("sample.OME.ZARR")
            #> CanonicalSuffix('.ome.zarr'), ".ome.zarr"

            CanonicalSuffix.extract_from_path("unknown.XYZ")
            #> CanonicalSuffix(''), ".xyz"

        Extend simple suffixes dynamically in a Python session::

            CanonicalSuffix.simple_formats.add(".myformat")
            CanonicalSuffix.from_path("data/sample.myformat")
            #> CanonicalSuffix('.myformat')

    """

    _simple_formats: set[str] | None = None
    _composite_formats: set[str] | None = None
    _encoding_formats: set[str] | None = None

    @classmethod
    def _ensure_format_sets(cls) -> None:
        if cls._simple_formats is not None:
            return
        mime_simple_formats = {
            suffix.lower()
            for suffix in (
                set(mimetypes.types_map.keys()) | set(mimetypes.common_types.keys())
            )
        }
        cls._simple_formats = mime_simple_formats | SIMPLE_FORMATS
        cls._composite_formats = set(COMPOSITE_FORMATS)
        cls._encoding_formats = set(ENCODING_FORMATS)

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
        simple_formats = cls.simple_formats
        composite_formats = cls.composite_formats
        encoding_formats = cls.encoding_formats

        if len(suffixes) < 2:
            if total_suffix in simple_formats or total_suffix in encoding_formats:
                return cls(total_suffix), total_suffix
            return cls(""), last_suffix

        # further composite suffixes cases
        if total_suffix.endswith(tuple(composite_formats)):
            # below seems slow but OK for now
            for suffix in composite_formats:
                if total_suffix.endswith(suffix):
                    break
            return cls(suffix), suffix

        # after listed composite suffixes are checked
        if last_suffix in simple_formats:
            return cls(last_suffix), last_suffix

        # additional encoding
        if last_suffix in encoding_formats:
            suffix = "".join(suffixes[-2:])  # e.g. ".tar.gz", ".csv.bz2"
            if suffixes[-2] == ".tar":
                # if the suffix preceding ".tar.<compression>" is a valid suffix,
                # we account for it; otherwise we don't.
                # i.e. we should have .h5ad.tar.gz or .csv.tar.gz, not just .tar.gz
                if len(suffixes) > 2 and (suffix_3 := suffixes[-3]) in simple_formats:
                    compression_suffix = suffix_3 + suffix
                    return cls(compression_suffix), compression_suffix
                return cls(suffix), suffix
            elif suffixes[-2] in simple_formats:
                return cls(suffix), suffix
            return cls(last_suffix), last_suffix

        return cls(""), last_suffix


__all__ = [
    "CanonicalSuffix",
]
