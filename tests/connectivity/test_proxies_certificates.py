import os
import socket
from pathlib import Path

import httpx
import lamindb_setup as ln_setup
import pytest


def test_setup():
    # see .github/workflows/build.yml connectivity and noxfile.py connectivity
    # check that the env varibales are properly setup
    assert os.getenv("HTTP_PROXY") == "http://127.0.0.1:8080"
    assert os.getenv("HTTPS_PROXY") == "http://127.0.0.1:8080"
    assert os.getenv("NO_PROXY") == "localhost,127.0.0.1"
    # check that mitmproxy is running
    s = socket.socket()
    s.settimeout(2)
    try:
        s.connect(("127.0.0.1", 8080))
    finally:
        s.close()
    # check that direct requests are blocked
    httpx.get("https://hub.lamin.ai", timeout=2, trust_env=False)


def test_connect_without_certificate():
    with pytest.raises(Exception) as e:
        ln_setup.connect("laminlabs/lamindata")
    assert "SSL: CERTIFICATE_VERIFY_FAILED" in str(e)


def test_connect_with_certificate():
    cert_file = Path("./mitmproxy-ca.pem")
    assert cert_file.exists()

    os.environ["SSL_CERT_FILE"] = cert_file.resolve().as_posix()

    try:
        ln_setup.connect("laminlabs/lamindata")
    finally:
        del os.environ["SSL_CERT_FILE"]
