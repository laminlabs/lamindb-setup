from __future__ import annotations
from pathlib import Path

import nbproject_test as test


def test_notebooks():
    test.execute_notebooks(Path(__file__).parent, write=True)
