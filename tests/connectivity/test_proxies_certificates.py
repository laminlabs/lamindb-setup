import os
import socket
from pathlib import Path

import httpx
import lamindb_setup as ln_setup
import pytest
from lamindb_setup.core._hub_client import httpx_client
from lamindb_setup.core._hub_core import access_db


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
    with pytest.raises(httpx.ConnectError):
        httpx.get("https://hub.lamin.ai", timeout=2, trust_env=False)


def test_httpx_client_proxy():
    # check httpx client internals
    # existing ._mounts means that proxies are properly configured
    with httpx_client() as client:
        patterns = {p.scheme for p in client._mounts}
    assert patterns == {"http", "https", ""}


def test_connect_without_certificate():
    # this fails because mitmproxy requires the certificate it generated
    with pytest.raises(Exception) as e:
        ln_setup.connect("laminlabs/lamindata")
    assert "SSL: CERTIFICATE_VERIFY_FAILED" in str(e)


def test_connect_with_certificate():
    cert_file = Path("./mitmproxy-ca.pem")
    assert cert_file.exists()
    # here we provide the certificate
    os.environ["SSL_CERT_FILE"] = cert_file.resolve().as_posix()

    try:
        # direct requests are blocked so if this succeeds
        # we are sure all requests went through the proxy and the certificate was used
        ln_setup.connect("laminlabs/lamindata")
        # check with a direct request to a lamin endpoint, uses httpx_client
        access_db(ln_setup.settings.instance)
    finally:
        del os.environ["SSL_CERT_FILE"]
