import os

import nox
from laminci import upload_docs_artifact
from laminci.nox import build_docs, login_testuser1, login_testuser2, run_pre_commit

nox.options.default_venv_backend = "none"


LAMIN_ENV = "prod"
if "GITHUB_REF_NAME" in os.environ:
    if os.environ["GITHUB_REF_NAME"] == "main":
        LAMIN_ENV = "prod"
    elif os.environ["GITHUB_REF_NAME"] == "staging":
        LAMIN_ENV = "staging"
elif "LAMIN_ENV" in os.environ:
    LAMIN_ENV = os.environ["LAMIN_ENV"]

env = {"LAMIN_ENV": LAMIN_ENV}


@nox.session
def lint(session: nox.Session) -> None:
    run_pre_commit(session)


@nox.session
@nox.parametrize(
    "group",
    ["unit", "docs"],
)
def install(session: nox.Session, group: str) -> None:
    session.run(*"pip install git+https://github.com/laminlabs/bionty".split())
    session.run(
        *"pip install --no-deps git+https://github.com/laminlabs/lnschema-bionty"
        .split()
    )
    # session.run(
    #     *"pip install --no-deps git+https://github.com/laminlabs/lnschema-lamin1"
    #     .split()
    # )
    # install lnschema-core from sub-module
    session.run(*"pip install --no-deps ./lnschema-core".split())
    # install lamindb-setup without deps
    session.run(*"pip install .[aws,test]".split())


@nox.session
@nox.parametrize(
    "group",
    ["unit", "docs"],
)
def build(session: nox.Session, group: str):
    login_testuser1(session, env=env)
    login_testuser2(session, env=env)
    coverage_args = "--cov=lamindb_setup --cov-append --cov-report=term-missing"  # noqa
    if group.startswith("unit"):
        session.run(*f"pytest -s {coverage_args} ./tests".split(), env=env)
    elif group.startswith("docs"):
        session.run(*f"pytest -s {coverage_args} ./docs".split(), env=env)


@nox.session
def docs(session: nox.Session):
    session.run(*"lamin init --storage ./docsbuild".split())
    build_docs(session)
    upload_docs_artifact()
