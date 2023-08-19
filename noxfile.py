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
        session.run(*"pip install .[aws,dev]".split())
    elif group == "noaws":
        session.run(*"pip install .[aws,dev]".split())
    elif group == "hub":
        session.run(*"pip install .[aws,dev,hub]".split())


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
    if group.startswith("unit"):
        session.run(
            *f"pytest -s {COVERAGE_ARGS} ./tests/unit".split(),
            env=env,
        )
    elif group.startswith("hub"):
        session.run(
            *f"pytest -s {COVERAGE_ARGS} ./tests/hub".split(),
            env=env,
        )
    elif group.startswith("docs"):
        session.run(*f"pytest -s {COVERAGE_ARGS} ./docs".split(), env=env)


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
        *f"pytest -s {COVERAGE_ARGS} ./tests/test_load_persistent_instance.py".split()
    )
