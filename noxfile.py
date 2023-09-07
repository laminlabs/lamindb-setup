from typing import Dict

import nox
from laminci.nox import build_docs, login_testuser1, login_testuser2, run_pre_commit

nox.options.default_venv_backend = "none"
COVERAGE_ARGS = "--cov=lamindb_setup --cov-append --cov-report=term-missing"


# this function is duplicated across laminhub-rest and lamindb-setup
def get_local_env() -> Dict[str, str]:
    supabase_api_url = "http://localhost:54321"
    with open("anon_key") as f:
        supabase_anon_key = f.readline().strip()
    env = {
        "LAMIN_ENV": "local",
        "POSTGRES_DSN": "postgresql://postgres:postgres@localhost:54322/postgres",
        "SUPABASE_API_URL": supabase_api_url,
        "SUPABASE_ANON_KEY": supabase_anon_key,
    }
    return env


@nox.session
def lint(session: nox.Session) -> None:
    run_pre_commit(session)


@nox.session
@nox.parametrize(
    "group",
    ["hub", "one-env", "two-envs", "noaws"],
)
def install(session: nox.Session, group: str) -> None:
    if group in {"two-envs"}:
        session.run(*"pip install git+https://github.com/laminlabs/bionty".split())
        session.run(
            *"pip install --no-deps git+https://github.com/laminlabs/lnschema-bionty"
            .split()
        )
        session.run(
            *"pip install --no-deps git+https://github.com/laminlabs/lnschema-core"
            .split()
        )
        session.run(*"pip install ./laminhub-rest[server]".split())
        session.run(*"pip install -e .[aws,dev]".split())
    elif group == "noaws":
        session.run(*"pip install -e .[aws,dev]".split())
    elif group == "one-env":
        session.run(
            *"pip install git+https://github.com/laminlabs/lnschema-bionty".split()
        )
        session.run(*"pip install -e .[aws,dev]".split())
    elif group == "hub":
        session.run(*"pip install -e .[aws,dev,hub]".split())
        session.run(*"pip install ./laminhub-rest[server]".split())
        # grab directories & files from laminhub-rest repo
        session.run(*"cp -r laminhub-rest/supabase .".split())


@nox.session
@nox.parametrize(
    "group",
    ["hub", "one-env", "two-envs"],
)
@nox.parametrize(
    "lamin_env",
    ["staging", "prod", "local"],
)
def build(session: nox.Session, group: str, lamin_env: str):
    env = {"LAMIN_ENV": lamin_env}
    if group != "hub":
        login_testuser1(session, env=env)
        login_testuser2(session, env=env)
    if group == "one-env":
        session.run(
            *f"pytest {COVERAGE_ARGS} ./tests/one-env".split(),
            env=env,
        )
        session.run(*f"pytest -s {COVERAGE_ARGS} ./docs/one-env".split(), env=env)
    elif group == "two-envs":
        session.run(
            *f"pytest {COVERAGE_ARGS} ./tests/two-envs".split(),
            env=env,
        )
        session.run(*f"pytest -s {COVERAGE_ARGS} ./docs/two-envs".split(), env=env)


@nox.session
def hub(session: nox.Session):
    # only run for local environment
    env = get_local_env()
    laminhub_rest_path = "./"  # locally used /Users/falexwolf/repos/
    with session.chdir(f"{laminhub_rest_path}laminhub-rest/"):
        session.run(*"lnhub alembic upgrade head".split(), env=env)
    session.run(
        *f"cp {laminhub_rest_path}laminhub-rest/tests/conftest.py tests/".split()
    )
    # the -n 1 is to ensure that supabase thread exits properly
    session.run(
        *f"pytest {COVERAGE_ARGS} ./tests/hub".split(),
        env=env,
    )


@nox.session
@nox.parametrize(
    "lamin_env",
    ["staging", "prod"],
)
def docs(session: nox.Session, lamin_env: str):
    env = {"LAMIN_ENV": lamin_env}
    if lamin_env == "staging":  # make sure CI is running against staging
        session.run(
            *"lamin login testuser1.staging@lamin.ai --password password".split(" "),
            external=True,
            env=env,
        )
    login_testuser1(session, env=env)
    session.run(*"lamin init --storage ./docsbuild".split(), env=env)
    if lamin_env != "staging":
        build_docs(session)


@nox.session
def noaws(session: nox.Session):
    login_testuser1(session)
    session.run(
        *f"pytest {COVERAGE_ARGS} ./tests/test_load_persistent_instance.py".split()
    )
