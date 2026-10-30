import os
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from uuid import uuid4

import pytest
from laminci.db import setup_local_test_postgres


@pytest.mark.parametrize("guarded", [False, True])
def test_merge_postgres(tmp_path, guarded):
    # This job provides Docker; hub-prod initializes SQLite and skips RLS tests.
    container = f"merge-atomic-{uuid4().hex[:8]}"
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
from pathlib import Path
import lamindb_setup
import pytest

lamindb_setup.init(storage="./storage", db=sys.argv[1], name="merge-atomic")
args = ["--noconftest", sys.argv[2], "--junitxml=results.xml"]
if sys.argv[3] == "True":
    import hubmodule
    from hubmodule.utils import _execute_sql_file

    _execute_sql_file(
        sys.argv[1],
        Path(hubmodule.__file__).parent / "sql/0008_setup_utils-branch-merge.sql",
    )
else:
    # Guarded RLS behavior is exercised by the other run.
    args += ["-k", "not test_merge_rolls_back_rls_restrictions and not test_merge_handles_hidden_records and not test_merge_prevents_concurrent_guard_removal and not test_merge_rejects_stale_guard_state"]
raise SystemExit(pytest.main(args))
""",
                pgurl,
                str(test_file),
                str(guarded),
            ],
            cwd=tmp_path,
            env=env,
            check=True,
            timeout=180,
        )
        cases = ET.parse(tmp_path / "results.xml").findall(".//testcase")
        assert cases
        assert all(case.find("skipped") is None for case in cases)
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", container], check=True)
