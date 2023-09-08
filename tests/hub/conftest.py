from typing import Dict
from subprocess import DEVNULL, run
from subprocess import getoutput
import pytest
import os


# this function is duplicated across laminhub-rest and lamindb-setup
def create_local_env() -> Dict[str, str]:
    start_supabase = """supabase start -x realtime,storage-api,imgproxy,pgadmin-schema-diff,migra,postgres-meta,studio,edge-runtime,logflare,vector,pgbouncer"""  # noqa
    # unfortuantely, supabase status -o env does not work with
    # a reduced number of containers
    get_anon_key = """grep 'anon key'|cut -f2 -d ":" | sed -e 's/^[[:space:]]*//'"""
    anon_key = getoutput(f"{start_supabase}|{get_anon_key}").split("\n")[-1]
    env = {
        "LAMIN_ENV": "local",
        "POSTGRES_DSN": "postgresql://postgres:postgres@localhost:54322/postgres",
        "SUPABASE_API_URL": "http://localhost:54321",
        "SUPABASE_ANON_KEY": anon_key,
    }
    return env


def pytest_sessionstart(session: pytest.Session):
    env = create_local_env()
    for key, value in env.items():
        os.environ[key] = value
    run(
        "lnhub alembic upgrade head",
        shell=True,
        env=os.environ,
        cwd="./laminhub-rest",
        check=True,
    )


def pytest_sessionfinish(session: pytest.Session):
    run("supabase stop", shell=True, stdout=DEVNULL)
