import os
from pathlib import Path

from nox import Session

from lndb_setup._clone import setup_local_test_postgres


def login_testuser1(session: Session):
    login_user_1 = "lndb login testuser1@lamin.ai --password cEvcwMJFX4OwbsYVaMt2Os6GxxGgDUlBGILs2RyS"  # noqa
    session.run(*(login_user_1.split(" ")), external=True)


def setup_test_instances_from_main_branch(session: Session):
    # spin up a postgres test instance
    pgurl = setup_local_test_postgres()
    # switch to the main branch
    if "GITHUB_BASE_REF" in os.environ and os.environ["GITHUB_BASE_REF"] != "":
        session.run("git", "checkout", os.environ["GITHUB_BASE_REF"], external=True)
    session.install(".")  # install current package from main branch
    # init a postgres instance
    session.run(*f"lndb init --storage pgtest --db {pgurl}".split(" "), external=True)
    # go back to the PR branch
    if "GITHUB_HEAD_REF" in os.environ and os.environ["GITHUB_HEAD_REF"] != "":
        session.run("git", "checkout", os.environ["GITHUB_HEAD_REF"], external=True)


def build_docs(session: Session):
    prefix = "." if Path("./lndocs").exists() else ".."
    session.install(f"{prefix}/lndocs")
    session.run("lndocs")
