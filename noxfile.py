import os
import sys

import nox
from laminci import move_built_docs_to_docs_slash_project_slug, upload_docs_artifact
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
    ["unit", "docs", "unit-django", "docs-django"],
)
def install(session: nox.Session, group: str) -> None:
    if "django" in group:
        session.run(*"pip install django dj_database_url".split())
    # install lnschema-core from sub-module
    session.run(*"pip install --no-deps ./lnschema-core".split())
    # install lamindb-setup without deps
    session.run(*"pip install --no-deps .".split())
    # install lamindb
    session.run(*"git clone https://github.com/laminlabs/lamindb --depth 1".split())
    if sys.platform.startswith("linux"):  # remove version pin when running on CI
        session.run(*"sed -i /lndb==/d ./lamindb/pyproject.toml".split())
        session.run(*"sed -i /lnschema_core/d ./lamindb/pyproject.toml".split())
    session.run(*"pip install ./lamindb[bionty,lamin1,aws,test]".split())


@nox.session
@nox.parametrize(
    "group",
    ["unit", "docs", "unit-django", "docs-django"],
)
def build(session: nox.Session, group: str):
    login_testuser1(session, env=env)
    login_testuser2(session, env=env)
    if "django" in group:
        os.environ["LAMINDB_USE_DJANGO"] = "1"
        env["LAMINDB_USE_DJANGO"] = "1"
    coverage_args = "--cov=lndb --cov-append --cov-report=term-missing"  # noqa
    if group.startswith("unit"):
        session.run(*f"pytest -s {coverage_args} ./tests".split(), env=env)
    elif group.startswith("docs"):
        session.run(*f"pytest -s {coverage_args} ./docs".split(), env=env)


@nox.session
def docs(session: nox.Session):
    build_docs(session)
    upload_docs_artifact()
    move_built_docs_to_docs_slash_project_slug()
