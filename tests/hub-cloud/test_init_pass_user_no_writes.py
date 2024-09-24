import subprocess


def test_init_no_writes():
    subprocess.run("lamin login testuser1", shell=True)
    subprocess.run("lamin delete testuser1/test-init-no-writes --force", shell=True)

    # calls logout
    result = subprocess.run(
        "python ./tests/hub-cloud/scripts/script-init-pass-user-no-writes.py",
        shell=True,
        capture_output=True,
    )
    assert result.returncode == 0

    subprocess.run("lamin login testuser1", shell=True)
    subprocess.run("lamin delete testuser1/test-init-no-writes --force", shell=True)
