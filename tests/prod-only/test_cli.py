import os


def test_entrypoint():
    exit_status = os.system("lamin --help")
    assert exit_status == 0


def test_migrate_create():
    exit_status = os.system("lamin migrate create")
    assert exit_status == 0


def test_migrate_squash():
    exit_status = os.system(
        "yes | lamin migrate squash --package-name lnschema_core --end-number 0019"
    )
    assert exit_status == 0
