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
    # see lamindb_setup/_notebooks.py::update_notebook_metadata
    assert "updated notebook metadata" in result.stdout.decode()
