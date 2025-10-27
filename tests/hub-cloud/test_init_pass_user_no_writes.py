import os
import subprocess

import lamindb_setup as ln_setup
from lamindb_setup.core._hub_client import connect_hub_with_auth
from upath import UPath


def test_init_no_writes():
    ln_setup.login("testuser1")
    assert ln_setup.settings.user.handle == "testuser1"

    # cleanup from failed runs
    subprocess.run("lamin delete testuser1/test-init-no-writes --force", shell=True)

    root = UPath(f"s3://lamindb-ci/{os.environ['LAMIN_ENV']}_test/test-init-no-writes")
    (root / ".lamindb/storage_uid.txt").unlink(missing_ok=True)
    client = connect_hub_with_auth()
    client.table("storage").delete().eq("root", root.as_posix()).eq(
        "created_by", ln_setup.settings.user._uuid.hex
    ).execute()
    client.table("instance").delete().eq("name", "test-init-no-writes").eq(
        "account_id", ln_setup.settings.user._uuid.hex
    ).execute()
    client.auth.sign_out(options={"scope": "local"})

    # calls logout
    result = subprocess.run(
        "python ./tests/hub-cloud/scripts/script-init-pass-user-no-writes.py",
        shell=True,
        capture_output=True,
    )
    if result.returncode != 0:
        raise Exception("stderr: " + result.stderr.decode())

    result = subprocess.run("lamin login testuser1", shell=True, capture_output=True)
    if result.returncode != 0:
        raise Exception("stderr: " + result.stderr.decode())

    result = subprocess.run(
        "lamin delete testuser1/test-init-no-writes --force",
        shell=True,
        capture_output=True,
    )

    if result.returncode != 0:
        raise Exception("stderr: " + result.stderr.decode())
