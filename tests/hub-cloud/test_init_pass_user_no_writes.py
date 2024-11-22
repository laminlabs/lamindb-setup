import subprocess


def test_init_no_writes():
    result = subprocess.run("lamin login testuser1", shell=True, capture_output=True)
    if result.returncode != 0:
        raise Exception("stderr: " + result.stderr.decode())

    subprocess.run("lamin delete testuser1/test-init-no-writes --force", shell=True)

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
