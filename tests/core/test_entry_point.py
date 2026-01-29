import shlex
import subprocess
import sys

import pytest

pyproject_toml = """\
[project]
name = "my_custom_module"
version = "0.0.1"

# pyproject.toml
[project.entry-points."lamindb_setup.on_import"]
my_custom_function = "my_custom_module:my_custom_function"
"""

my_custom_module = """\
import os

def my_custom_function():
    os.environ["MY_CUSTOM_ENV_VAR"] = "1"
"""


@pytest.fixture
def installed_custom_module(tmp_path):
    (tmp_path / "pyproject.toml").write_text(pyproject_toml)
    (tmp_path / "my_custom_module.py").write_text(my_custom_module)
    cmd_install = [sys.executable, "-m", "pip", "install", str(tmp_path)]
    cmd_cleanup = [sys.executable, "-m", "pip", "uninstall", "my_custom_module", "-y"]

    subprocess.run(shlex.join(cmd_install), shell=True, check=True)
    try:
        yield
    finally:
        subprocess.run(shlex.join(cmd_cleanup), shell=True, check=True)


@pytest.mark.parametrize(
    "output,install_custom",
    [
        pytest.param(b"0", False, id="no-entry-point"),
        pytest.param(b"1", True, id="with-entry-point"),
    ],
)
def test_on_import_entry_point(output, install_custom, request):
    if install_custom:
        request.getfixturevalue("installed_custom_module")

    cmd = [
        sys.executable,
        "-c",
        "import lamindb_setup; import os; print(os.environ.get('MY_CUSTOM_ENV_VAR', '0'), end='')",
    ]
    out = subprocess.run(
        shlex.join(cmd),
        shell=True,
        capture_output=True,
        env={"PYTHONWARNINGS": "error"},
    )
    assert out.returncode == 0
    assert out.stderr == b""
    assert out.stdout == output


def test_call_registered_entry_points(installed_custom_module):
    from lamindb_setup._entry_points import call_registered_entry_points

    call_registered_entry_points("lamindb_setup.on_import")


def test_broken_entry_points_does_not_crash(installed_custom_module):
    from lamindb_setup._entry_points import call_registered_entry_points

    with pytest.warns(RuntimeWarning, match="Error loading entry point"):
        call_registered_entry_points("lamindb_setup.on_import", broken_kwarg=object())
