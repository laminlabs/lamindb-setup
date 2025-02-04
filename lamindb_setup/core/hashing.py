from __future__ import annotations

"""Hashing.

.. autosummary::
   :toctree: .

   hash_set
   hash_file

"""

import base64
import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING

import psutil

HASH_LENGTH = 22

if TYPE_CHECKING:
    from collections.abc import Iterable

    from .types import Path, UPathStr


def hash_and_encode_as_b62(s: str) -> str:
    from lamin_utils._base62 import encodebytes

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
def hash_code(file_path: UPathStr) -> hashlib._Hash:
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


def hash_file(
    file_path: Path,
    file_size: int | None = None,
    chunk_size: int | None = 50 * 1024 * 1024,
) -> tuple[str, str]:
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
    return to_b64_str(digest)[:HASH_LENGTH], hash_type


def hash_dir(path: Path):
    files = (subpath for subpath in path.rglob("*") if subpath.is_file())

    def hash_size(file):
        file_size = file.stat().st_size
        return hash_file(file, file_size)[0], file_size

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
