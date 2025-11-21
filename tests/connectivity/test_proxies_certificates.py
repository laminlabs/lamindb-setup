import os


def test_env():
    assert "HTTPS_PROXY" in os.environ
    assert "HTTP_PROXY" in os.environ
    assert "NO_PROXY" in os.environ
