import os
from pathlib import Path
from typing import Optional

from nox import Session

from lndb_setup._clone import setup_local_test_postgres

from ._env import get_package_name


def get_schema_handle() -> Optional[str]:
    package_name = get_package_name()
    if package_name.startswith("lnschema_"):
        return package_name.replace("lnschema_", "")
    else:
        return None


def login_testuser1(session: Session):
    login_user_1 = "lndb login testuser1@lamin.ai --password cEvcwMJFX4OwbsYVaMt2Os6GxxGgDUlBGILs2RyS"  # noqa
    session.run(*(login_user_1.split(" ")), external=True)


def setup_test_instances_from_main_branch(session: Session, schema: str = None):
    # spin up a postgres test instance
    pgurl = setup_local_test_postgres()
    # switch to the main branch
    if "GITHUB_BASE_REF" in os.environ and os.environ["GITHUB_BASE_REF"] != "":
        session.run("git", "checkout", os.environ["GITHUB_BASE_REF"], external=True)
    session.install(".[test]")  # install current package from main branch
    # init a postgres instance
    init_instance = f"lndb init --storage pgtest --db {pgurl}"
    schema_handle = get_schema_handle()
    if schema is None and schema_handle not in {None, "core"}:
        init_instance += f" --schema {schema_handle}"
    elif schema is not None:
        init_instance += f" --schema {schema}"
    session.run(*init_instance.split(" "), external=True)
    # go back to the PR branch
    if "GITHUB_HEAD_REF" in os.environ and os.environ["GITHUB_HEAD_REF"] != "":
        session.run("git", "checkout", os.environ["GITHUB_HEAD_REF"], external=True)


def run_pre_commit(session: Session):
    session.install("pre-commit")
    session.run("pre-commit", "install")
    session.run("pre-commit", "run", "--all-files")


def run_pytest(session: Session):
    package_name = get_package_name()
    session.run(
        "pytest",
        "-s",
        f"--cov={package_name}",
        "--cov-append",
        "--cov-report=term-missing",
    )
    session.run("coverage", "xml")


def build_docs(session: Session):
    prefix = "." if Path("./lndocs").exists() else ".."
    session.install(f"{prefix}/lndocs")
    session.run("lndocs")
