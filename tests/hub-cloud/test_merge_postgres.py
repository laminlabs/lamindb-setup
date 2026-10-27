import os
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from uuid import uuid4

from laminci.db import setup_local_test_postgres


def test_merge_postgres(tmp_path):
    # This job provides Docker; hub-prod initializes SQLite and skips RLS tests.
    container = f"merge-preflight-{uuid4().hex[:8]}"
    pgurl = setup_local_test_postgres(name=container)
    try:
        test_file = Path(__file__).resolve().parents[1] / "hub-prod/test_merge.py"
        env = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith("LAMIN")
        }
        env.update(LAMIN_SETTINGS_DIR=str(tmp_path), LAMINDB_MODULES="")
        # A fresh process isolates Django models and settings from other hub tests.
        subprocess.run(
            [
                sys.executable,
                "-c",
                """
import sys
import lamindb_setup
import pytest

lamindb_setup.init(storage="./storage", db=sys.argv[1], name="merge-preflight")
raise SystemExit(pytest.main([
    "--noconftest", sys.argv[2],
    "--junitxml=results.xml",
]))
""",
                pgurl,
                str(test_file),
            ],
            cwd=tmp_path,
            env=env,
            check=True,
            timeout=180,
        )
        cases = ET.parse(tmp_path / "results.xml").findall(".//testcase")
        assert len(cases) == 25
        assert all(case.find("skipped") is None for case in cases)
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", container], check=True)
