from __future__ import annotations

import base64
from pathlib import Path
from uuid import UUID

from lamindb_setup.core.hashing import (
    HASH_LENGTH,
    b16_to_b64,
    hash_and_encode_as_b62,
    hash_code,
    hash_dir,
    hash_file,
    hash_file_legacy,
    hash_from_hashes_list,
    hash_string,
    s3_chunk_size,
    to_b64_str,
)


def test_compute_hash():
    files = [
        # file_content, hash, chunk_size, hash_type, file_size
        ("a", "DMF1ucDxtqgxw5niaXcmYQ", None, "md5", 1),
        ("b", "kutf_uauL-w61xx3dTFXjw", None, "md5", 1),
        ("abc", "kAFQmDzST7DWlj99KOF_cg", None, "md5", 3),
        ("a", "DMF1ucDxtqgxw5niaXcmYQ", 1, "md5", 1),
        ("b", "kutf_uauL-w61xx3dTFXjw", 1, "md5", 1),
        # below two chunks it's still a single md5 as for an S3 single-part upload
        ("abc", "kAFQmDzST7DWlj99KOF_cg", 2, "md5", 3),
        # the last case here triggers multi-chunk compute like an S3 multipart upload
        ("abc", "QFTG93h-y3syMyMVWwwFgw", 1, "md5-3", 3),
    ]
    for content, hash, chunk_size, hash_type, file_size in files:
        assert len(hash) == HASH_LENGTH
        filepath = Path("file_1")
        filepath.write_text(content)
        if hash_type == "md5":
            computed_hash_from_string = hash_string(content)
            assert computed_hash_from_string == hash
        computed_size, computed_hash, computed_hash_type = hash_file(
            filepath, chunk_size=chunk_size
        )
        assert computed_hash == hash
        assert computed_hash_type == hash_type
        assert computed_size == file_size
        assert computed_size == filepath.stat().st_size
        filepath.unlink()


def test_hash_file_full_content(tmp_path):
    # two 120 MiB files that only differ in the middle, written sparse
    size = 120 * 1024**2
    for byte in (b"a", b"b"):
        with open(tmp_path / byte.decode(), "wb") as f:
            f.truncate(size)
            f.seek(size // 2)
            f.write(byte)
    hashes = [hash_file(tmp_path / name) for name in ("a", "b")]
    legacy_hashes = [hash_file_legacy(tmp_path / name) for name in ("a", "b")]
    # the ETags of S3 multipart uploads with parts of 50, 50 and 20 MiB
    assert hashes == [
        (size, "_gTEPv_XLqeWz7hTrthMIg", "md5-3"),
        (size, "mhzSGw7qZleZH0mk43EoFA", "md5-3"),
    ]
    # the legacy hash only covers the first and last 50 MiB
    assert legacy_hashes[0] == legacy_hashes[1]
    assert legacy_hashes[0][2] == "sha1-fl"
    # folders keep the legacy per-file hashes until "md5-d2"
    assert hash_dir(tmp_path)[1] == hash_from_hashes_list([legacy_hashes[0][1]] * 2)


def test_hash_file_legacy():
    filepath = Path("file_1")
    filepath.write_text("abc")
    # the hash of earlier versions for chunk_size=1
    assert hash_file_legacy(filepath, chunk_size=1)[1:] == (
        "p0EbDbQEP1wS-Tw6TuBjKS",
        "sha1-fl",
    )
    # up to one chunk it's the same md5 as before
    assert hash_file_legacy(filepath) == hash_file(filepath)
    filepath.unlink()


def test_s3_chunk_size():
    MiB = 1024**2
    assert s3_chunk_size(10_000 * 50 * MiB) == 50 * MiB
    # more than 10,000 parts would be needed, round up to the next 5 MiB
    assert s3_chunk_size(10_000 * 50 * MiB + 1) == 55 * MiB


def test_base64():
    mytest = b"test"
    b64_str = to_b64_str(mytest)
    b64_str_padded = f"{b64_str}=="
    assert base64.urlsafe_b64decode(b64_str_padded.encode()).hex() == mytest.hex()


def test_b16_to_b64():
    assert b16_to_b64("9b89c8c1acf79dba5b5341d1fff9806f") == "m4nIwaz3nbpbU0HR__mAbw"


def test_hash_code():
    sha1 = hash_code("./LICENSE")
    assert sha1.hexdigest() == "b09cd7856d58590578ee1a4f3ad45d1310a97f87"


def test_hash_and_encode_as_b62():
    id = UUID("10075f07-0b0b-48b0-9006-18724fb3be62")
    assert hash_and_encode_as_b62(id.hex) == "7clAMMtTbqlKVQPmUbqnIq"
