import time
from pathlib import Path
from subprocess import PIPE, run

from lamin_logger import logger

from ._settings_instance import InstanceSettings


def setup_local_test_sqlite_file(
    src_settings: InstanceSettings, return_dir: bool = False
):
    path = src_settings._sqlite_file_local
    new_stem = path.stem + "_test"
    tgt_sqlite_dir = Path.cwd() / new_stem
    if return_dir:
        return tgt_sqlite_dir
    tgt_sqlite_file = tgt_sqlite_dir / f"{new_stem}{path.suffix}"
    tgt_sqlite_file.parent.mkdir(exist_ok=True)
    if tgt_sqlite_file.exists():
        tgt_sqlite_file.unlink()
    tgt_db = f"sqlite:///{tgt_sqlite_file}"
    return tgt_db


def setup_local_test_postgres(name: str = "pgtest"):
    process = run(
        f"docker run --name {name} -e POSTGRES_PASSWORD=pwd"
        f" -e POSTGRES_DB={name} -d -p 5432:5432 postgres",  # noqa
        shell=True,
    )
    if process.returncode == 0:
        logger.info(
            f"Created Postgres test instance. It runs in docker container '{name}'."
        )
    else:
        raise RuntimeError("Failed to set up postgres test instance.")
    time.sleep(2)
    return f"postgresql://postgres:pwd@0.0.0.0:5432/{name}"


def setup_local_test_postgres_supabase():
    process = run(
        f"""supabase start | grep 'anon key'|cut -f2 -d ":" | sed -e 's/^[[:space:]]*//'""",  # noqa
        shell=True,
        stdout=PIPE,
    )
    anon_key = process.stdout.decode("UTF-8").replace("\n", "")
    open(".supabase_local_anon_key", "w").write(anon_key)
    if process.returncode == 0:
        logger.info("Created Supabase test instance.")
    else:
        raise RuntimeError("Failed to set up Supabase test instance.")
    time.sleep(2)
    return "postgresql://postgres:postgres@localhost:54322/postgres"
