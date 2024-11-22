import os
import subprocess


def test_fail_managed_storage():
    if os.environ["LAMIN_ENV"] == "prod":
        result = subprocess.run(
            "python ./tests/hub-cloud/scripts/script-to-fail-managed-storage.py",
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
