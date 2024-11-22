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
    if result.returncode != 0:
        raise Exception(
            "stdout: " + result.stdout.decode()
            if result.stdout is not None
            else "" + "\n" + "stderr: " + result.stderr.decode()
            if result.stderr is not None
            else ""
        )

    subprocess.run("lamin login testuser1", shell=True)
    result = subprocess.run(
        "lamin delete testuser1/test-init-no-writes --force", shell=True
    )
    if result.returncode != 0:
        raise Exception(
            "stdout: " + result.stdout.decode()
            if result.stdout is not None
            else "" + "\n" + "stderr: " + result.stderr.decode()
            if result.stderr is not None
            else ""
        )
