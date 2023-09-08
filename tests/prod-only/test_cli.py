import os


def test_entrypoint():
    exit_status = os.system("lamin --help")
    assert exit_status == 0
