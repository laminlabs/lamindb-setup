"""Hashing.

.. autosummary::
   :toctree: .

   hash_set
   hash_file

"""

from __future__ import annotations

import base64
import hashlib
import json
import math
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING

import psutil

HASH_LENGTH = 22

if TYPE_CHECKING:
    from collections.abc import Iterable

    from lamindb_setup.types import AnyPathStr, Path


def hash_and_encode_as_b62(s: str) -> str:
    # bionty.uids.encode_base62 uses the same hash for entity uids.
    from .base62 import encodebytes

    return encodebytes(hashlib.md5(s.encode()).digest())


def to_b64_str(bstr: bytes):
    b64 = base64.urlsafe_b64encode(bstr).decode().strip("=")
    return b64


def b16_to_b64(s: str):
    return to_b64_str(base64.b16decode(s.strip('"'), casefold=True))


# a lot to read about this: lamin-notes/2022/hashing
def hash_set(s: set[str]) -> str:
    join_s = ":".join(sorted(s))
    return hash_string(join_s)[:HASH_LENGTH]


def hash_dict(d: dict) -> str:
    return to_b64_str(hashlib.md5(json.dumps(d, sort_keys=True).encode()).digest())[
        :HASH_LENGTH
    ]


def hash_from_hashes_list(hashes: Iterable[str]) -> str:
    # need to sort below because we don't want the order of parsing the dir to
    # affect the hash
    digests = b"".join(
        hashlib.md5(hash.encode("utf-8")).digest() for hash in sorted(hashes)
    )
    digest = hashlib.md5(digests).digest()
    return to_b64_str(digest)[:HASH_LENGTH]


# below is only used when comparing with git's sha1 hashes
# we don't use it for our own hashes
def hash_code(file_path: AnyPathStr) -> hashlib._Hash:
    with open(file_path, "rb") as fp:
        data = fp.read()
    data_size = len(data)
    header = f"blob {data_size}\0".encode()
    blob = header + data
    return hashlib.sha1(blob)


def hash_small_bytes(data: bytes) -> str:
    return to_b64_str(hashlib.md5(data).digest())


# this is equivalent with hash_file for small files
def hash_string(string: str) -> str:
    # as we're truncating (not here) at 22 b64, we choose md5 over sha512
    return to_b64_str(hashlib.md5(string.encode("utf-8")).digest())[:HASH_LENGTH]


MiB = 1024**2


def s3_chunk_size(file_size: int) -> int:
    """Part size for a multipart upload to S3 through `upload_from()`.

    50 MiB as in s3fs, raised in steps of 5 MiB if the file would otherwise
    need more than the 10,000 parts that S3 allows.
    """
    chunk_size = 50 * MiB
    if file_size > 10_000 * chunk_size:
        step = 5 * MiB
        chunk_size = math.ceil(math.ceil(file_size / 10_000) / step) * step
    return chunk_size


def hash_file(
    file_path: Path,
    file_size: int | None = None,
    chunk_size: int | None = None,
) -> tuple[int, str, str]:
    """Hash the full content of a file, reproducing the S3 ETag.

    Files smaller than two chunks get the md5 of their content (type `"md5"`),
    larger files the md5 of the md5 digests of all chunks (type
    `"md5-{n_chunks}"`), which is what S3 computes for an upload with
    `upload_from()`.

    Args:
        file_path: Path to a local file.
        file_size: Size of the file, if already known.
        chunk_size: Defaults to the part size of `upload_from()`,
            see `s3_chunk_size()`.
    """
    with open(file_path, "rb") as fp:
        if file_size is None:
            fp.seek(0, 2)
            file_size = fp.tell()
            fp.seek(0, 0)
        if chunk_size is None:
            chunk_size = s3_chunk_size(file_size)
        if file_size < 2 * chunk_size:  # s3fs uploads this in a single part
            digest = hashlib.file_digest(fp, "md5").digest()
            hash_type = "md5"
        else:
            chunk_digests = []
            while chunk := fp.read(chunk_size):
                chunk_digests.append(hashlib.md5(chunk).digest())
            digest = hashlib.md5(b"".join(chunk_digests)).digest()
            hash_type = f"md5-{len(chunk_digests)}"
    return file_size, to_b64_str(digest)[:HASH_LENGTH], hash_type


# the hash_file() of earlier versions, which only covers the first and the last
# chunk of larger files ("sha1-fl"); keep it for about a year so that records
# saved with it can be found and updated
def hash_file_legacy(
    file_path: Path,
    file_size: int | None = None,
    chunk_size: int | None = 50 * MiB,
) -> tuple[int, str, str]:
    with open(file_path, "rb") as fp:
        if file_size is None:
            fp.seek(0, 2)
            file_size = fp.tell()
            fp.seek(0, 0)
        if chunk_size is None:
            chunk_size = file_size
        first_chunk = fp.read(chunk_size)
        if file_size <= chunk_size:
            digest = hashlib.md5(first_chunk).digest()
            hash_type = "md5"
        else:
            fp.seek(-chunk_size, 2)
            last_chunk = fp.read(chunk_size)
            digest = hashlib.sha1(
                hashlib.sha1(first_chunk).digest() + hashlib.sha1(last_chunk).digest()
            ).digest()
            hash_type = "sha1-fl"
    return file_size, to_b64_str(digest)[:HASH_LENGTH], hash_type


def hash_dir(path: Path) -> tuple[int, str, str, int]:
    files = (subpath for subpath in path.rglob("*") if subpath.is_file())

    def hash_size(file):
        # the per-file hashes of earlier versions keep "md5-d" unchanged
        size, hash, _ = hash_file_legacy(file)
        return hash, size

    try:
        n_workers = len(psutil.Process().cpu_affinity())
    except AttributeError:
        n_workers = psutil.cpu_count()
    if n_workers > 1:
        with ThreadPoolExecutor(n_workers) as pool:
            hashes_sizes = pool.map(hash_size, files)
    else:
        hashes_sizes = map(hash_size, files)
    hashes, sizes = zip(*hashes_sizes, strict=False)

    hash, hash_type = hash_from_hashes_list(hashes), "md5-d"
    n_files = len(hashes)
    size = sum(sizes)
    return size, hash, hash_type, n_files
