from subprocess import DEVNULL, run
from subprocess import getoutput
import os


# this function is duplicated across laminhub-rest and lamindb-setup
#
def create_and_set_local_supabase_env():
    start_supabase = """supabase start -x realtime,storage-api,imgproxy,pgadmin-schema-diff,migra,postgres-meta,studio,edge-runtime,logflare,vector,pgbouncer"""  # noqa
    # unfortuantely, supabase status -o env does not work with
    # a reduced number of containers, hence, we need this ugly regex
    get_anon_key = """grep 'anon key'|cut -f2 -d ":" | sed -e 's/^[[:space:]]*//'"""
    anon_key = getoutput(f"{start_supabase}|{get_anon_key}").split("\n")[-1]
    env = {
        "LAMIN_ENV": "local",
        "POSTGRES_DSN": "postgresql://postgres:postgres@localhost:54322/postgres",
        "SUPABASE_API_URL": "http://localhost:54321",
        "SUPABASE_ANON_KEY": anon_key,
    }
    # now, update the environment with these values
    # this will not overwrite existing environment variables
    # for the reason detailed below
    for key, value in env.items():
        # can only set it once because this function might be called several times
        # leading to differing output (anon_key showing a trivial message)
        # Alex doesn't understand why it's called several times and logging
        # doesn't indicate otherwise
        if key not in os.environ:
            os.environ[key] = value
        else:
            print(f"WARNING: env variable {key} is already set to {os.environ[key]}")


def pytest_configure():
    create_and_set_local_supabase_env()
    run(
        "lnhub alembic upgrade head",
        shell=True,
        env=os.environ,
        cwd="./laminhub-rest",
        check=True,
    )


def pytest_unconfigure():
    print(" tear down supabase")
    run("supabase stop", shell=True, stdout=DEVNULL)
