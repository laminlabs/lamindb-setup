"""Log every SQL statement a run issues against an instance to stderr.

Turned on with the global `lamin evolve --log-queries` flag.  A run that
reads or writes many plans spends most of its wall clock inside LaminDB's ORM,
and until a stage finishes there is nothing on screen to say *which* query it is
waiting on -- the symptom being a command that looks hung.  The log answers that
statement by statement, and prints a total when the command closes.

The logger is a Django *execute wrapper*, so it sees every statement any layer
issues -- LaminDB's own reads included -- without patching the ORM.  It is
attached from :func:`~lamindb_setup.evolve.engine._connect.safe_connect` because
Django's connection handler only exists once an instance is connected, and it
re-attaches through the `connection_created` signal so that switching
instances mid-run (a `ci` command connects to each configured instance in
turn, which rebuilds the connection) keeps logging.
"""

from __future__ import annotations

import re
import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = [
    "configure_query_logging",
    "echo_query_log_summary",
    "install_query_logger",
    "query_log_totals",
]

#: How much of a statement is printed before it is elided.
_MAX_SQL_CHARS = 300

#: LaminDB authenticates a connection by prepending `SELECT set_token(<jwt>,
#: true);` to every statement issued outside an atomic block (see
#: `lamindb_setup.core.django.DBTokenManager`).  Its wrapper sits outside
#: ours, so what we are handed is the rewritten statement.  The prefix is
#: dropped rather than printed: it carries a credential, and at several hundred
#: characters it would fill the line and elide the statement actually being run.
#: The token is base64url, so it cannot contain the `);` this stops at.
_SET_TOKEN_PREFIX = re.compile(r"^SELECT set_token\(.*?\);\s*", re.IGNORECASE)

_enabled = False
_signal_connected = False
_count = 0
_seconds = 0.0


def configure_query_logging(enabled: bool) -> None:
    """Turn per-statement logging on or off, resetting the totals.

    Called once per CLI invocation from the top-level callback, so that several
    commands run in one process -- as the tests do -- each report their own
    totals rather than a running sum.
    """
    global _enabled, _count, _seconds
    _enabled = enabled
    _count = 0
    _seconds = 0.0


def query_log_totals() -> tuple[int, float]:
    """Return the `(statements, seconds)` logged so far this run."""
    return _count, _seconds


def install_query_logger() -> None:
    """Attach the logger to the current Django connections and to future ones.

    A no-op unless `--log-queries` is on.  Idempotent -- the signal is
    connected once and each connection is only wrapped once -- so calling it
    after every connect, which is what :func:`safe_connect` does, costs nothing.
    """
    if not _enabled:
        return
    global _signal_connected
    from django.db import connections
    from django.db.backends.signals import connection_created

    if not _signal_connected:
        connection_created.connect(_attach, weak=False)
        _signal_connected = True
    for connection in connections.all():
        _attach(None, connection)


def _attach(sender: Any, connection: Any = None, **_kwargs: Any) -> None:
    """Wrap `connection`'s cursor unless it is already wrapped."""
    if connection is None:
        return
    if _log_query not in connection.execute_wrappers:
        connection.execute_wrappers.append(_log_query)


def _log_query(
    execute: Callable[[Any, Any, bool], Any],
    sql: Any,
    params: Any,
    many: bool,
    context: Any,
) -> Any:
    """Django execute wrapper: time the statement, then print it to stderr.

    The enabled check lives here rather than only at attach time because the
    wrapper outlives a single CLI invocation: it stays on the connection, and a
    later command in the same process may not have asked for the log.
    """
    global _count, _seconds
    if not _enabled:
        return execute(sql, params, many)
    start = time.perf_counter()
    try:
        return execute(sql, params, many)
    finally:
        elapsed = time.perf_counter() - start
        _count += 1
        _seconds += elapsed
        _emit(_count, str(sql), elapsed, many)


def _emit(index: int, sql: str, elapsed: float, many: bool) -> None:
    """Print one statement as a single dimmed line on the shared stderr console."""
    from rich.markup import escape

    from ._console import stderr_console

    statement = _SET_TOKEN_PREFIX.sub("", " ".join(sql.split()))
    if len(statement) > _MAX_SQL_CHARS:
        statement = statement[: _MAX_SQL_CHARS - 1] + "\u2026"
    suffix = " (executemany)" if many else ""
    # `escape`: a statement can contain square brackets (array literals, quoted
    # identifiers) that rich would otherwise read as markup tags.  `soft_wrap`
    # keeps one statement on one line so the log stays greppable.
    stderr_console().print(
        f"[dim]sql {index:>6} {elapsed * 1000:8.1f}ms[/dim] "
        f"{escape(statement)}{suffix}",
        highlight=False,
        soft_wrap=True,
    )


def echo_query_log_summary() -> None:
    """Print the run's statement count and total query time.

    Registered as a context-close hook by the CLI, so it is printed even when
    the command exits non-zero -- which is often when the timing is wanted.
    """
    if not _enabled or _count == 0:
        return
    from ._console import stderr_console

    # `highlight=False` like the per-statement line: rich's default
    # highlighter would give the count and the "(s)" their own colors, turning
    # one dim summary into a ransom note.
    stderr_console().print(
        f"[dim]sql: {_count} statement(s) in {_seconds * 1000:.0f}ms[/dim]",
        highlight=False,
    )
