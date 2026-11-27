"""Shared helpers for connecting to a LaminDB instance.

All instance connections in this package go through :func:`safe_connect` so the
environment schema modules are configured *before* connecting. Otherwise lamindb
warns that the instance `has non-configured modules` and its registries (e.g.
`bionty` / `pertdb`) are not queryable.

Connecting is always explicit: :func:`no_default_instance` hides whichever
instance the user selected with `lamin connect` for the duration of a run, so
that only :func:`safe_connect` ever opens a connection.

Because every command that touches an instance goes through :func:`safe_connect`,
it is also where the logged-in Lamin user is announced (see
:func:`echo_current_user`), so it is always visible which account a run acts
under.
"""

from __future__ import annotations

import contextlib
import os
import re
from typing import TYPE_CHECKING, Any

from ..console import echo, install_query_logger

if TYPE_CHECKING:
    from collections.abc import Iterator

#: Environment schema modules configured before connecting.
#:
#: Kept as a module-level default (and exposed via `safe_connect`'s `modules`
#: parameter) so this can later be made per-instance configurable without
#: touching call sites.
DEFAULT_MODULES = "bionty,pertdb"

#: The environment variable that names the instance lamindb connects to on
#: import, taking precedence over the settings file written by `lamin connect`.
CURRENT_INSTANCE_ENV_VAR = "LAMIN_CURRENT_INSTANCE"

#: Whether the logged-in user has already been announced in the current run.
_user_announced = False


class LaminSetup:
    """The slice of `lamindb_setup` this package drives.

    Every attribute this package reads or writes on `lamindb_setup` goes
    through one of these methods, so a caller -- in practice a test -- can hand
    in a stand-in rather than rewriting properties on a third-party singleton
    that the rest of the process shares.

    The imports stay inside the methods because `lamindb_setup` must not be
    pulled in merely to import this module.
    """

    @staticmethod
    def _setup() -> Any:
        import lamindb_setup as ln_setup

        return ln_setup

    def user(self) -> Any:
        """The logged-in Lamin user; raises when nobody is logged in."""
        return self._setup().settings.user

    def get_modules(self) -> Any:
        """The currently configured environment schema modules."""
        return self._setup().settings.modules

    def set_modules(self, value: str) -> None:
        """Configure the environment schema modules used by the next connect."""
        self._setup().settings.modules = value

    def connect(self, instance: str) -> None:
        """Open a connection to `instance`."""
        self._setup().connect(instance)

    def instance(self) -> Any:
        """The connected instance's settings, carrying its canonical identity."""
        return self._setup().settings.instance

    def is_connected(self) -> bool:
        """Whether an instance is already connected in this process."""
        from lamindb_setup.core import django as ln_django

        return bool(getattr(ln_django, "IS_SETUP", False))


#: The adapter used whenever a caller does not pass one.
DEFAULT_SETUP = LaminSetup()


def reset_user_announcement() -> None:
    """Forget that the user was announced, so the next connect prints it again.

    Called once per CLI invocation (from the top-level callback) so that the
    announcement is per run rather than per process: several commands invoked in
    the same process -- as the tests do -- each announce the user once.
    """
    global _user_announced
    _user_announced = False


def current_user_label(*, setup: LaminSetup | None = None) -> str:
    """Return a human-readable label for the Lamin user this run acts as."""
    setup = DEFAULT_SETUP if setup is None else setup

    try:
        user = setup.user()
    except Exception:
        # `settings.user` raises when no user is logged in, and may reach out
        # to the hub when `LAMIN_API_KEY` is set.  Naming the account is
        # advisory, so a failure here must never be what fails a command.
        return "unknown (not logged in; run 'lamin login')"
    handle = getattr(user, "handle", None) or "anonymous"
    name = (getattr(user, "name", None) or "").strip()
    return f"{handle} ({name})" if name and name != handle else handle


def echo_current_user(*, setup: LaminSetup | None = None) -> None:
    """Print which Lamin user this run acts as, once per run.

    Written to stderr so it does not end up in the Markdown or JSON a command
    writes to stdout, and printed only once even when a run connects to several
    instances (a `ci` run over many plans), since the account is the same for
    all of them.
    """
    global _user_announced
    if _user_announced:
        return
    _user_announced = True
    echo(f"Lamin user: {current_user_label(setup=setup)}", err=True)


@contextlib.contextmanager
def no_default_instance(
    *, modules: str | None = None, setup: LaminSetup | None = None
) -> Iterator[None]:
    """Hide the user's default instance from lamindb for the duration.

    LaminDB connects to the default instance -- the one selected by `lamin
    connect`, a `.lamin/current_instance` marker, or the
    `LAMIN_CURRENT_INSTANCE` environment variable -- the first time `lamindb`
    is imported. Most of this package's commands import lamindb without wanting
    an instance at all (`validate` resolves dtypes purely offline), and the
    ones that do want one name it with `--instance`; connecting to whatever the
    user happens to have selected is at best surprising and at worst a
    connection to the wrong database.

    Inside this context lamindb sees *no* default instance, exactly as for a user
    who never ran `lamin connect`: importing it sets Django up against the
    empty `none/none` instance, offline and without touching any database.
    :func:`safe_connect` then remains the only way a connection is opened. The
    `none/none` instance is given :data:`DEFAULT_MODULES` so that the
    `bionty` / `pertdb` registries a plan's dtypes refer to stay importable.

    Nothing is written to disk: the default instance is only masked in this
    process (in-memory settings plus the environment variable) and restored on
    exit, so a concurrent `lamin` invocation is unaffected. If a connection was
    opened while the context was open -- which is what `plan` and `apply`
    do -- that connection is left in place rather than masked away again.

    This is a no-op when lamindb is already connected, so that embedding a
    command in an already-connected process (as the tests do) keeps working.
    """
    setup = DEFAULT_SETUP if setup is None else setup
    if setup.is_connected():
        yield
        return

    from uuid import UUID

    from lamindb_setup import settings as ln_settings
    from lamindb_setup.core import InstanceSettings

    # Mirrors the instance lamindb_setup itself falls back to when no default
    # instance is configured (see its `load_instance_settings`).
    placeholder = InstanceSettings(
        id=UUID(int=0),
        owner="none",
        name="none",
        storage=None,
        modules=DEFAULT_MODULES if modules is None else modules,
    )
    previous_instance = ln_settings._instance_settings
    previous_env = os.environ.pop(CURRENT_INSTANCE_ENV_VAR, None)
    ln_settings._instance_settings = placeholder
    try:
        yield
    finally:
        if ln_settings._instance_settings is placeholder:
            ln_settings._instance_settings = previous_instance
        if previous_env is not None:
            os.environ[CURRENT_INSTANCE_ENV_VAR] = previous_env


def safe_connect(
    instance: str, *, modules: str | None = None, setup: LaminSetup | None = None
) -> str:
    """Connect to `instance` with schema modules configured; return its slug.

    Before connecting, the environment schema modules are set (default
    :data:`DEFAULT_MODULES`) so registries from those modules are queryable and
    lamindb does not warn about non-configured modules. `modules` is exposed as
    a parameter so callers can later configure modules per instance.

    The Lamin user the run acts as is announced first (see
    :func:`echo_current_user`), so the account is named even when the connection
    itself then fails because that account lacks access.

    Per-statement query logging is attached afterwards (a no-op unless
    `--log-queries` is on): Django's connection handler only exists once an
    instance is connected, and re-attaching after every connect keeps the log
    running across the instance switches a `ci` command makes.

    The returned slug is derived from the *connected* instance's canonical
    identity rather than the raw `instance` argument, so different ways of
    referencing the same instance resolve to the same lock file.

    `setup` is the :class:`LaminSetup` adapter the connection is made through;
    it defaults to the real `lamindb_setup`.
    """
    setup = DEFAULT_SETUP if setup is None else setup

    echo_current_user(setup=setup)
    previous_modules = setup.get_modules()
    setup.set_modules(DEFAULT_MODULES if modules is None else modules)
    try:
        setup.connect(instance)
    except BaseException:
        setup.set_modules(previous_modules)
        raise
    install_query_logger()
    settings_instance = setup.instance()
    raw = (
        getattr(settings_instance, "slug", None)
        or getattr(settings_instance, "uid", None)
        or instance
    )
    slug = re.sub(r"[^A-Za-z0-9._-]+", "-", str(raw)).strip("-._")
    return slug or "instance"
