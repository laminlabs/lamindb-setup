import nox
import os
from laminci.nox import build_docs, login_testuser1, login_testuser2, run_pre_commit

nox.options.default_venv_backend = "none"
COVERAGE_ARGS = "--cov=lamindb_setup --cov-append --cov-report=term-missing"


@nox.session
def lint(session: nox.Session) -> None:
    run_pre_commit(session)


@nox.session
@nox.parametrize(
    "group",
    ["hub-local", "hub-prod", "hub-cloud", "storage"],
)
def install(session: nox.Session, group: str) -> None:
    if group in {"hub-cloud"}:
        # TODO: get rid of the bionty duplication asap
        session.run(*"pip install bionty".split())
        session.run(*"pip install git+https://github.com/laminlabs/bionty-base".split())
        session.run(
            *"pip install --no-deps git+https://github.com/laminlabs/lnschema-bionty"
            .split()
        )
        session.run(
            *"pip install --no-deps git+https://github.com/laminlabs/lnschema-core"
            .split()
        )
        session.run(*"pip install ./laminhub/rest-hub".split())
        session.run(*"pip install -e .[aws,dev]".split())
        # need for CLI, but this is bad because it's downstream
        session.run(*"git clone https://github.com/laminlabs/lamin-cli".split())
        session.run(*"pip install lamin-cli".split())
    elif group == "storage":
        session.run(*"pip install -e .[aws,dev]".split())
    elif group == "hub-prod":
        # TODO: get rid of the bionty duplication asap
        session.run(*"pip install bionty".split())
        session.run(*"pip install git+https://github.com/laminlabs/bionty-base".split())
        session.run(
            *"pip install git+https://github.com/laminlabs/lnschema-core".split()
        )
        session.run(
            *"pip install git+https://github.com/laminlabs/lnschema-bionty".split()
        )
        session.run(*"pip install git+https://github.com/laminlabs/wetlab".split())
        session.run(*"pip install -e .[aws,dev]".split())
        # need for CLI, but this is bad because it's downstream
        session.run(*"git clone https://github.com/laminlabs/lamin-cli".split())
        session.run(*"pip install lamin-cli".split())
    elif group == "hub-local":
        session.run(*"pip install -e .[aws,dev,hub]".split())
        session.run(*"pip install -e ./laminhub/rest-hub".split())
        session.run(*"pip install lamin-cli".split())


@nox.session
@nox.parametrize(
    "group",
    ["hub-prod", "hub-cloud"],
)
@nox.parametrize(
    "lamin_env",
    ["staging", "prod"],
)
def build(session: nox.Session, group: str, lamin_env: str):
    env = {"LAMIN_ENV": lamin_env}
    login_testuser1(session, env=env)
    login_testuser2(session, env=env)
    if group == "hub-prod":
        session.run(
            *f"pytest {COVERAGE_ARGS} ./tests/hub-prod".split(),
            env=env,
        )
        session.run(*f"pytest -s {COVERAGE_ARGS} ./docs/hub-prod".split(), env=env)
    elif group == "hub-cloud":
        session.run(
            *f"pytest {COVERAGE_ARGS} ./tests/hub-cloud".split(),
            env=env,
        )
        session.run(*f"pytest -s {COVERAGE_ARGS} ./docs/hub-cloud".split(), env=env)


@nox.session
def hub_local(session: nox.Session):
    # the -n 1 is to ensure that supabase thread exits properly
    session.run(*f"pytest -n 1 {COVERAGE_ARGS} ./tests/hub-local".split())


@nox.session
def storage(session: nox.Session):
    # we need AWS to retrieve credentials for testuser1, but want to eliminate
    # them after that
    os.environ["AWS_ACCESS_KEY_ID"] = os.environ["TMP_AWS_ACCESS_KEY_ID"]
    os.environ["AWS_SECRET_ACCESS_KEY"] = os.environ["TMP_AWS_SECRET_ACCESS_KEY"]
    login_testuser1(session)
    # mimic anonymous access
    del os.environ["AWS_ACCESS_KEY_ID"]
    del os.environ["AWS_SECRET_ACCESS_KEY"]
    session.run(
        *f"pytest {COVERAGE_ARGS} ./tests/storage".split(),
        env=os.environ,
    )


@nox.session
def docs(session: nox.Session):
    import lamindb_setup as ln_setup

    login_testuser1(session)
    ln_setup.init(storage="./docsbuild")
    build_docs(session, strip_prefix=True)
