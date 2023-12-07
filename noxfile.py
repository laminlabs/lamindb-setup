import nox
from laminci.nox import build_docs, login_testuser1, login_testuser2, run_pre_commit

nox.options.default_venv_backend = "none"
COVERAGE_ARGS = "--cov=lamindb_setup --cov-append --cov-report=term-missing"


@nox.session
def lint(session: nox.Session) -> None:
    run_pre_commit(session)


@nox.session
@nox.parametrize(
    "group",
    ["hub-local", "prod-only", "prod-staging", "storage", "vault"],
)
def install(session: nox.Session, group: str) -> None:
    if group in {"prod-staging"}:
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
        # need for CLI, but this is bad because it's downstream
        session.run(*"git clone https://github.com/laminlabs/lamin-cli".split())
        session.run(*"pip install lamin-cli".split())
    elif group == "storage":
        session.run(*"pip install -e .[aws,dev]".split())
    elif group == "vault":
        session.run(*"pip install -e .[aws,dev]".split())
    elif group == "prod-only":
        session.run(
            *"pip install git+https://github.com/laminlabs/lnschema-bionty".split()
        )
        session.run(*"pip install -e .[aws,dev]".split())
        # need for CLI, but this is bad because it's downstream
        session.run(*"git clone https://github.com/laminlabs/lamin-cli".split())
        session.run(*"pip install lamin-cli".split())
    elif group == "hub-local":
        session.run(*"pip install -e .[aws,dev,hub]".split())
        session.run(*"pip install ./laminhub-rest[server]".split())
        # grab directories & files from laminhub-rest repo
        session.run(*"cp -r laminhub-rest/supabase .".split())


@nox.session
@nox.parametrize(
    "group",
    ["prod-only", "prod-staging"],
)
@nox.parametrize(
    "lamin_env",
    ["staging", "prod"],
)
def build(session: nox.Session, group: str, lamin_env: str):
    env = {"LAMIN_ENV": lamin_env}
    login_testuser1(session, env=env)
    login_testuser2(session, env=env)
    if group == "prod-only":
        session.run(
            *f"pytest {COVERAGE_ARGS} ./tests/prod-only".split(),
            env=env,
        )
        session.run(*f"pytest -s {COVERAGE_ARGS} ./docs/prod-only".split(), env=env)
    elif group == "prod-staging":
        session.run(
            *f"pytest {COVERAGE_ARGS} ./tests/prod-staging".split(),
            env=env,
        )
        session.run(*f"pytest -s {COVERAGE_ARGS} ./docs/prod-staging".split(), env=env)


@nox.session
def hub_local(session: nox.Session):
    # the -n 1 is to ensure that supabase thread exits properly
    session.run(*f"pytest -n 1 {COVERAGE_ARGS} ./tests/hub-local".split())


@nox.session
def docs(session: nox.Session):
    import lamindb_setup as ln_setup

    login_testuser1(session)
    ln_setup.init(storage="./docsbuild")
    build_docs(session)


@nox.session
def storage(session: nox.Session):
    login_testuser1(session)
    session.run(*f"pytest {COVERAGE_ARGS} ./tests/test_storage_access.py".split())


@nox.session
def vault(session: nox.Session):
    env = {"LAMIN_ENV": "staging"}
    login_testuser1(session, env=env)
    session.run(
        *f"pytest {COVERAGE_ARGS} ./tests/test_vault.py".split(),
        env=env,
    )
