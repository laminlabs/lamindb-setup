import os
import sys

import nox
from laminci import move_built_docs_to_docs_slash_project_slug, upload_docs_artifact
from laminci.nox import (
    build_docs,
    login_testuser1,
    login_testuser2,
    run_pre_commit,
    run_pytest,
)

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
def install(session: nox.Session) -> None:
    session.run(*"pip install --no-deps .".split())
    if os.getenv("GITHUB_EVENT_NAME") not in (None, "push"):
        session.run(*"pip install --no-deps ./lnschema-core".split())
    session.run(*"git clone https://github.com/laminlabs/lamindb --depth 1".split())
    if sys.platform.startswith("linux"):  # remove version pin when running on CI
        session.run(*"sed -i /lndb==/d ./lamindb/pyproject.toml".split())
    session.run(*"pip install ./lamindb[bionty,lamin1,aws,test]".split())


@nox.session
def build(session: nox.Session):
    login_testuser1(session, env=env)
    login_testuser2(session, env=env)
    run_pytest(session, env=env)
    build_docs(session)
    upload_docs_artifact()
    move_built_docs_to_docs_slash_project_slug()
