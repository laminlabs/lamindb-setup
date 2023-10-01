import subprocess
import os


def test_track():
    env = os.environ
    env["LAMIN_TESTING"] = "true"
    result = subprocess.run(
        "lamin track ./tests/notebooks/test-notebooks/no-title.ipynb",
        shell=True,
        capture_output=True,
        env=env,
    )
    assert result.returncode == 0
    assert "updated notebook metadata" in result.stdout.decode()
