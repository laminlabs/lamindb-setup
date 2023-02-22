import os
import shutil
from pathlib import Path

import nox

nox.options.reuse_existing_virtualenvs = True


def upload_docs_dir():
    import lndb

    if os.environ["GITHUB_EVENT_NAME"] != "push":
        return
    package_name = "lndb"
    filestem = f"{package_name}_docs"
    filename = shutil.make_archive(filestem, "zip", "./docs")
    lndb.load("testuser1/lamin-site-assets", migrate=True)

    import lamindb as ln
    import lamindb.schema as lns

    with ln.Session() as ss:
        dobject = ss.select(ln.DObject, name=filestem).one_or_none()
        pipeline = ln.add(lns.Pipeline, name=f"CI {package_name}")
        run = lns.Run(pipeline=pipeline)
        if dobject is None:
            dobject = ln.DObject(filename, source=run)
        else:
            dobject.source = run
        ss.add(dobject)


@nox.session
def lint(session: nox.Session) -> None:
    session.install("pre-commit")
    session.run("pre-commit", "install")
    session.run("pre-commit", "run", "--all-files")


@nox.session(python=["3.7", "3.8", "3.9", "3.10", "3.11"])
def build(session):
    session.install(".[dev,test]")
    login_user_1 = "lndb login testuser1@lamin.ai --password cEvcwMJFX4OwbsYVaMt2Os6GxxGgDUlBGILs2RyS"  # noqa
    session.run(*(login_user_1.split(" ")))
    login_user_2 = "lndb login testuser2@lamin.ai --password goeoNJKE61ygbz1vhaCVynGERaRrlviPBVQsjkhz"  # noqa
    session.run(*(login_user_2.split(" ")), external=True)
    session.run(
        "pytest",
        "-s",
        "--cov=lndb",
        "--cov-append",
        "--cov-report=term-missing",
    )
    session.run("coverage", "xml")
    prefix = "." if Path("./lndocs").exists() else ".."
    session.install(f"{prefix}/lndocs")
    session.run("lndocs")
    upload_docs_dir()
