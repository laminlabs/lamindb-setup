#!/usr/bin/env python
"""Recreate lamin.db.gz - a seed SQLite database.

This script:
1. Runs `lamin init --storage initdb --modules bionty`
2. Deletes the storage location and user from the lamindb tables
3. Compresses the SQLite file and saves it to lamindb_setup/core/lamin.db.gz

Run from the lamindb-setup directory: python scripts/recreate_lamin_db_gz.py
"""

import gzip
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path


def main() -> None:
    # Ensure we run from lamindb-setup directory
    script_dir = Path(__file__).parent
    lamindb_setup_dir = script_dir.parent
    initdb_path = lamindb_setup_dir / "initdb"
    sqlite_path = initdb_path / ".lamindb" / "lamin.db"
    output_path = lamindb_setup_dir / "lamindb_setup" / "core" / "lamin.db.gz"

    print("Step 1: Running lamin init --storage initdb --modules bionty...")
    result = subprocess.run(
        ["lamin", "init", "--storage", "initdb", "--modules", "bionty"],
        cwd=str(lamindb_setup_dir),
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"lamin init failed:\n{result.stderr}", file=sys.stderr)
        sys.exit(1)
    print("lamin init completed.")

    if not sqlite_path.exists():
        print(f"SQLite file not found at {sqlite_path}", file=sys.stderr)
        sys.exit(1)

    print("Step 2: Deleting storage and user records from lamindb tables...")
    conn = sqlite3.connect(str(sqlite_path))
    try:
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute("DELETE FROM lamindb_storage")
        conn.execute("DELETE FROM lamindb_user")
        conn.commit()
    finally:
        conn.close()
    print("Records deleted.")

    print("Step 3: Compressing SQLite file and saving to lamin.db.gz...")
    temp_gz_path = sqlite_path.with_suffix(".db.gz")
    with (
        open(sqlite_path, "rb") as f_in,
        gzip.open(temp_gz_path, "wb") as f_out,
    ):
        shutil.copyfileobj(f_in, f_out)
    shutil.move(str(temp_gz_path), str(output_path))
    print(f"Saved to {output_path}")

    # Cleanup: disconnect and remove initdb
    print("Step 4: Cleaning up...")
    subprocess.run(
        ["lamin", "delete", "--force"], cwd=str(lamindb_setup_dir), capture_output=True
    )
    shutil.rmtree(initdb_path, ignore_errors=True)
    print("Done.")


if __name__ == "__main__":
    main()
