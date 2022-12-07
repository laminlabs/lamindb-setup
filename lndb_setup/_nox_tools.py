import os

from nox import Session

from ._clone import setup_local_test_postgres


def setup_test_instances_from_main_branch(session: Session):
    login_user_1 = "lndb login testuser1@lamin.ai --password cEvcwMJFX4OwbsYVaMt2Os6GxxGgDUlBGILs2RyS"  # noqa
    session.run(*(login_user_1.split(" ")))
    # init a test instance from the main branch
    if "GITHUB_BASE_REF" in os.environ and os.environ["GITHUB_BASE_REF"] != "":
        session.run("git", "checkout", os.environ["GITHUB_BASE_REF"], external=True)
    # install the current package from main
    session.install(".")
    # sqlite test instance - currently not needed, because we only test postgres here
    # session.run(*"lndb init --storage testdb".split(" "))
    # postgres test instance
    session.run(
        *f"lndb init --storage pgtest --db {setup_local_test_postgres()}".split(" ")
    )  # noqa
    # go back to the PR branch
    if "GITHUB_HEAD_REF" in os.environ and os.environ["GITHUB_HEAD_REF"] != "":
        session.run("git", "checkout", os.environ["GITHUB_HEAD_REF"], external=True)
