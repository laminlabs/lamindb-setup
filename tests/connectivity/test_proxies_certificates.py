import os
import socket


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
