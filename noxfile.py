import os
from typing import Dict

import nox
from laminci.nox import build_docs, login_testuser1, login_testuser2, run_pre_commit

nox.options.default_venv_backend = "none"
COVERAGE_ARGS = "--cov=lamindb_setup --cov-append --cov-report=term-missing"


# this function is duplicated across lnhub-rest and lamindb-setup
def get_local_env() -> Dict[str, str]:
    env = {
        "LAMIN_ENV": "local",
        "POSTGRES_DSN": os.environ["DB_URL"].replace('"', ""),
        "SUPABASE_API_URL": os.environ["API_URL"].replace('"', ""),
        "SUPABASE_ANON_KEY": os.environ["ANON_KEY"].replace('"', ""),
        "SUPABASE_SERVICE_ROLE_KEY": os.environ["SERVICE_ROLE_KEY"].replace('"', ""),
    }
    return env


@nox.session
def lint(session: nox.Session) -> None:
    run_pre_commit(session)


@nox.session
@nox.parametrize(
    "group",
    ["unit", "hub", "docs", "noaws"],
)
def install(session: nox.Session, group: str) -> None:
    if group in {"unit", "docs"}:
        session.run(*"pip install git+https://github.com/laminlabs/bionty".split())
        session.run(
            *"pip install --no-deps git+https://github.com/laminlabs/lnschema-bionty"
            .split()
        )
        session.run(
            *"pip install --no-deps git+https://github.com/laminlabs/lnschema-core"
            .split()
        )
        session.run(*"pip install -e .[aws,dev]".split())
    elif group == "noaws":
        session.run(*"pip install -e .[aws,dev]".split())
    elif group == "hub":
        session.run(*"pip install -e .[aws,dev,hub]".split())
        session.run(*"pip install ./lnhub-rest[server]".split())
        # grab directories & files from lnhub-rest repo
        session.run(*"cp -r lnhub-rest/supabase .".split())


@nox.session
@nox.parametrize(
    "group",
    ["unit", "hub", "docs"],
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
    if group == "unit":
        session.run(
            *f"pytest {COVERAGE_ARGS} ./tests/unit".split(),
            env=env,
        )
    elif group == "docs":
        session.run(*f"pytest -s {COVERAGE_ARGS} ./docs".split(), env=env)
    elif group == "hub":
        # only run for local environment
        assert lamin_env == "local"
        env = get_local_env()
        with session.chdir("./lnhub-rest"):
            session.run(*"lnhub alembic upgrade head".split(), env=env)
        session.run(*"cp lnhub-rest/tests/conftest.py tests/".split())
        # the -n 1 is to ensure that supabase thread exits properly
        session.run(
            *f"pytest -n 1 {COVERAGE_ARGS} ./tests/hub".split(),
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
