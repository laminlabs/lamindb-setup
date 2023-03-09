import os
from pathlib import Path

import nox
from laminci import __version__, upload_docs_dir

if __version__ < "0.2.4":
    raise RuntimeError("Upgrade laminci version.")

nox.options.reuse_existing_virtualenvs = True

lamin_env = "prod"
if "GITHUB_REF_NAME" in os.environ:
    if os.environ["GITHUB_REF_NAME"] == "main":
        lamin_env = "prod"
    elif os.environ["GITHUB_REF_NAME"] == "staging":
        lamin_env = "staging"
elif "LAMIN_ENV" in os.environ:
    lamin_env = os.environ["LAMIN_ENV"]

env = {"LAMIN_ENV": lamin_env}


@nox.session
def lint(session: nox.Session) -> None:
    session.install("pre-commit")
    session.run("pre-commit", "install")
    session.run("pre-commit", "run", "--all-files")


@nox.session(python=["3.7", "3.8", "3.9", "3.10", "3.11"])
def build(session):
    session.install(".[dev,test]")
    login_user_1 = "lndb login testuser1@lamin.ai --password cEvcwMJFX4OwbsYVaMt2Os6GxxGgDUlBGILs2RyS"  # noqa
    session.run(*(login_user_1.split(" ")), env=env)
    login_user_2 = "lndb login testuser2@lamin.ai --password goeoNJKE61ygbz1vhaCVynGERaRrlviPBVQsjkhz"  # noqa
    session.run(*(login_user_2.split(" ")), external=True, env=env)
    session.run(
        "pytest",
        "-s",
        "--cov=lndb",
        "--cov-append",
        "--cov-report=term-missing",
        env=env,
    )
    session.run("coverage", "xml")
    prefix = "." if Path("./lndocs").exists() else ".."
    session.install(f"{prefix}/lndocs")
    session.run("lndocs")
    upload_docs_dir()
