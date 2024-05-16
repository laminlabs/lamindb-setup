from __future__ import annotations

"""Hashing.

.. autosummary::
   :toctree: .

   hash_set
   hash_file

"""

import base64
import hashlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
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
    bstr = ":".join(sorted(s)).encode("utf-8")
    # as we're truncating at 20 b64, we choose md5 over sha512
    return to_b64_str(hashlib.md5(bstr).digest())[:20]


def hash_md5s_from_dir(hashes: list[str]) -> tuple[str, str]:
    # need to sort below because we don't want the order of parsing the dir to
    # affect the hash
    digests = b"".join(
        hashlib.md5(hash.encode("utf-8")).digest() for hash in sorted(hashes)
    )
    digest = hashlib.md5(digests).digest()
    return to_b64_str(digest)[:22], "md5-d"


def hash_code(file_path: UPathStr):
    with open(file_path, "rb") as fp:
        data = fp.read()
    data_size = len(data)
    header = f"blob {data_size}\0".encode()
    blob = header + data
    return hashlib.sha1(blob)


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
    return to_b64_str(digest)[:22], hash_type
