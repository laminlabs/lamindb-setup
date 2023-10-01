import subprocess
import os
import nbproject_test
from pathlib import Path


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


def test_save_no_title():
    env = os.environ
    env["LAMIN_TESTING"] = "true"
    result = subprocess.run(
        "lamin save ./tests/notebooks/test-notebooks/no-title.ipynb",
        shell=True,
        capture_output=True,
        env=env,
    )
    assert result.returncode == 1
    assert "No title!" in result.stdout.decode()


def test_save_non_consecutive():
    env = os.environ
    env["LAMIN_TESTING"] = "true"
    result = subprocess.run(
        "lamin save"
        " ./tests/notebooks/test-notebooks/with-title-and-initialized-non-consecutive.ipynb",  # noqa
        shell=True,
        capture_output=True,
        env=env,
    )
    assert result.returncode == 1
    assert "Aborted (non-consecutive)!" in result.stdout.decode()


def test_save_consecutive():
    notebook_path = Path.cwd() / Path(
        "tests/notebooks/test-notebooks/with-title-and-initialized-consecutive.ipynb"
    )
    env = os.environ
    env["LAMIN_TESTING"] = "true"
    result = subprocess.run(
        f"lamin save {str(notebook_path)}",
        shell=True,
        capture_output=True,
        env=env,
    )
    assert result.returncode == 1
    assert (
        "didn't find notebook in transform registry, did you run ln.track() in it?"
        in result.stdout.decode()
    )
    # now, re-run this notebook
    nbproject_test.execute_notebooks(notebook_path)
    result = subprocess.run(
        f"lamin save {str(notebook_path)}",
        shell=True,
        capture_output=True,
        env=env,
    )
    print(result.stderr)
    print(result.stdout)
    assert result.returncode == 0
    assert (
        "saved notebook and wrote source_file and html report" in result.stdout.decode()
    )
