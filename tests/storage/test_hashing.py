from __future__ import annotations

import base64
from pathlib import Path
from uuid import UUID

from lamindb_setup.core.hashing import (
    HASH_LENGTH,
    b16_to_b64,
    hash_and_encode_as_b62,
    hash_code,
    hash_file,
    hash_string,
    to_b64_str,
)


def test_compute_hash():
    files = [
        # file_content, hash, chunk_size, hash_type
        ("a", "DMF1ucDxtqgxw5niaXcmYQ", None, "md5"),
        ("b", "kutf_uauL-w61xx3dTFXjw", None, "md5"),
        ("abc", "kAFQmDzST7DWlj99KOF_cg", None, "md5"),
        ("a", "DMF1ucDxtqgxw5niaXcmYQ", 1, "md5"),
        ("b", "kutf_uauL-w61xx3dTFXjw", 1, "md5"),
        # the last case here triggers multi-chunk compute with sha1
        ("abc", "p0EbDbQEP1wS-Tw6TuBjKS", 1, "sha1-fl"),
    ]
    for content, hash, chunk_size, hash_type in files:
        assert len(hash) == HASH_LENGTH
        filepath = Path("file_1")
        filepath.write_text(content)
        if hash_type == "md5":
            computed_hash_from_string = hash_string(content)
            assert computed_hash_from_string == hash
        computed_hash, computed_hash_type = hash_file(filepath, chunk_size=chunk_size)
        assert computed_hash == hash
        assert computed_hash_type == hash_type
        filepath.unlink()


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
