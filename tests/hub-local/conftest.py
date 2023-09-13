import os
from pathlib import Path
from subprocess import DEVNULL, getoutput, run


# this function is duplicated across laminhub-rest and lamindb-setup
#
def create_and_set_local_supabase_env():
    start_supabase = """supabase start -x realtime,storage-api,imgproxy,pgadmin-schema-diff,migra,postgres-meta,studio,edge-runtime,logflare,vector,pgbouncer"""  # noqa
    # unfortunately, supabase status -o env does not work with
    # a reduced number of containers (running supabase CLI version 1.38.6 & 1.96.4)
    # hence, we need this ugly regex
    get_anon_key = """grep 'anon key'|cut -f2 -d ":" | sed -e 's/^[[:space:]]*//'"""
    anon_key = getoutput(f"{start_supabase}|{get_anon_key}").split("\n")[-1]
    env = {
        "LAMIN_ENV": "local",
        "POSTGRES_DSN": "postgresql://postgres:postgres@localhost:54322/postgres",
        "SUPABASE_API_URL": "http://localhost:54321",
        "SUPABASE_ANON_KEY": anon_key,
    }
    # update environment variables with these values
    for key, value in env.items():
        # the following will not overwrite existing environment variables
        # the reason is that create_and_set_local_supabase_env seems to be called
        # multiple times; for any but the 1st time the supabase CLI is called,
        # we see a trivial output message and cannot parse the anon_key
        # (Alex doesn't understand why it's called several times and extensive
        #  debugging with logging didn't yield a conclusion)
        if key not in os.environ:
            os.environ[key] = value
        else:
            print(f"WARNING: env variable {key} is already set to {os.environ[key]}")


def pytest_configure():
    create_and_set_local_supabase_env()
    import laminhub_rest

    run(
        "lnhub alembic upgrade head",
        shell=True,
        env=os.environ,
        cwd=Path(laminhub_rest.__file__).parent.parent,
        check=True,
    )


def pytest_unconfigure():
    print("tear down supabase")
    run("supabase stop", shell=True, stdout=DEVNULL)
