import os

from lamindb_setup.core._hub_client import httpx_client


def test_proxy_from_env():
    with httpx_client() as client:
        assert client._mounts == {}

    os.environ["HTTP_PROXY"] = "http://localhost:8080"
    os.environ["HTTPS_PROXY"] = "http://localhost:8080"

    with httpx_client() as client:
        patterns = {p.scheme for p in client._mounts}
    assert patterns == {"http", "https"}

    del os.environ["HTTP_PROXY"]
    del os.environ["HTTPS_PROXY"]

    with httpx_client() as client:
        assert client._mounts == {}
