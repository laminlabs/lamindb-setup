"""The single stderr console this package writes its human-facing output to.

Progress displays, milestone lines, warnings and the `--log-queries` SQL log
all render to stderr, so stdout stays the clean Markdown / JSON stream callers
pipe.  They must share *one* :class:`rich.console.Console`: rich only lifts a
live progress display out of the way for output printed through the same
console, so a second `Console(stderr=True)` printing while a progress bar is
up tears the display apart.

Plain lines -- a note, a warning, the document a command writes to stdout -- go
through :func:`echo` instead.
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from rich.console import Console

__all__ = ["echo", "stderr_console"]

_console: Any = None


def echo(message: str = "", *, err: bool = False) -> None:
    """Write one line to the selected output stream."""
    print(message, file=sys.stderr if err else sys.stdout)


def stderr_console() -> Console:
    """Return the process-wide stderr console, creating it on first use.

    Created lazily so importing this module does not pull `rich` in; the CLI
    imports it from the top-level callback, which runs for every command.
    """
    global _console
    if _console is None:
        from rich.console import Console

        _console = Console(stderr=True)
    return _console
