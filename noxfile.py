import os

import nox
from laminci import move_built_docs_to_docs_slash_project_slug, upload_docs_artifact
from laminci.nox import (
    build_docs,
    login_testuser1,
    login_testuser2,
    run_pre_commit,
    run_pytest,
)

nox.options.reuse_existing_virtualenvs = True


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


@nox.session(python=["3.7", "3.8", "3.9", "3.10", "3.11"])
def build(session):
    session.install(".[dev,test]")
    login_testuser1(session, env=env)
    login_testuser2(session, env=env)
    run_pytest(session, env=env)
    build_docs(session)
    upload_docs_artifact()
    move_built_docs_to_docs_slash_project_slug()
