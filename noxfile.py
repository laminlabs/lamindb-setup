from __future__ import annotations

import os

import nox
from laminci.nox import (
    build_docs,
    login_testuser1,
    login_testuser2,
    run,
    run_pre_commit,
)

nox.options.default_venv_backend = "none"

COVERAGE_ARGS = "--cov=lamindb_setup --cov-append --cov-report=term-missing"


@nox.session
def lint(session: nox.Session) -> None:
    run_pre_commit(session)


@nox.session
@nox.parametrize(
    "group",
    ["hub-local", "hub-prod", "hub-cloud", "storage", "docs"],
)
def install(session: nox.Session, group: str) -> None:
    no_deps_packages = "git+https://github.com/laminlabs/lamindb git+https://github.com/laminlabs/wetlab git+https://github.com/laminlabs/lamin-cli"
    modules_deps = f"""uv pip install --system --no-deps {no_deps_packages}
uv pip install --system git+https://github.com/laminlabs/bionty
"""
    if group == "hub-cloud":
        cmds = (
            modules_deps
            + "uv pip install --system sentry_sdk line_profiler wheel==0.45.1 flit"
            + "\nuv pip install --system ./laminhub/rest-hub --no-build-isolation"
        )
    elif group == "docs":
        cmds = modules_deps.strip()
    elif group == "storage":
        cmds = modules_deps + "uv pip install --system gcsfs huggingface_hub"
    elif group == "hub-prod":
        # cmds = "git clone --depth 1 https://github.com/django/django\n"
        # cmds += "uv pip install --system -e ./django\n"
        cmds = ""
        cmds += modules_deps.strip()
        cmds += """\nuv pip install --system gcsfs huggingface_hub"""
    elif group == "hub-local":
        cmds = modules_deps.strip()
    # current package
    cmds += """\nuv pip install --system -e '.[aws,dev]'"""

    # above downgrades django, I don't understand why, try this
    if group == "hub-local":
        cmds += "\nuv pip install --system sentry_sdk line_profiler wheel==0.45.1 flit"
        cmds += "\nuv pip install --system -e ./laminhub/rest-hub --no-build-isolation"
        # check that just installing psycopg (psycopg3) doesn't break fine-grained access
        cmds += "\nuv pip install --system psycopg[binary]"

    run(session, "uv pip install --system pandera")  # needed to import lamindb
    [run(session, line) for line in cmds.splitlines()]


@nox.session
@nox.parametrize(
    "group",
    ["hub-prod", "hub-cloud"],
)
@nox.parametrize(
    "lamin_env",
    ["staging", "prod"],
)
def build(session: nox.Session, group: str, lamin_env: str):
    env = {"LAMIN_ENV": lamin_env, "LAMIN_TESTING": "true"}
    login_testuser1(session, env=env)
    login_testuser2(session, env=env)
    if group == "hub-prod":
        run(session, f"pytest {COVERAGE_ARGS} ./tests/hub-prod", env=env)
        run(session, f"pytest -s {COVERAGE_ARGS} ./docs/hub-prod", env=env)
    elif group == "hub-cloud":
        run(session, f"pytest {COVERAGE_ARGS} ./tests/hub-cloud", env=env)
        run(session, f"pytest -s {COVERAGE_ARGS} ./docs/hub-cloud", env=env)


@nox.session
def hub_local(session: nox.Session):
    os.environ["AWS_SECRET_ACCESS_KEY_DEV_S3"] = os.environ["AWS_ACCESS_KEY_ID"]
    # the -n 1 is to ensure that supabase thread exits properly
    run(
        session,
        f"pytest {COVERAGE_ARGS} ./tests/hub-local",
        env=os.environ,
    )


@nox.session
def storage(session: nox.Session):
    # we need AWS to retrieve credentials for testuser1, but want to eliminate
    # them after that
    os.environ["AWS_ACCESS_KEY_ID"] = os.environ["TMP_AWS_ACCESS_KEY_ID"]
    os.environ["AWS_SECRET_ACCESS_KEY"] = os.environ["TMP_AWS_SECRET_ACCESS_KEY"]
    login_testuser1(session)
    # mimic anonymous access
    del os.environ["AWS_ACCESS_KEY_ID"]
    del os.environ["AWS_SECRET_ACCESS_KEY"]
    run(session, f"pytest {COVERAGE_ARGS} ./tests/storage", env=os.environ)


@nox.session
def docs(session: nox.Session):
    import lamindb_setup as ln_setup

    login_testuser1(session)
    ln_setup.init(storage="./docsbuild")
    build_docs(session, strip_prefix=True)
