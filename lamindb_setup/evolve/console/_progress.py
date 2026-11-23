"""Shared rich-progress helpers for CLI commands.

A single :func:`progress_reporter` context manager renders a stderr progress
display (so stdout / Markdown output stays clean) and yields a
:class:`ProgressReporter` exposing two callbacks reused across commands:

- `report(message)` prints a milestone line above the progress bars.
- `track(sequence, description)` wraps a sequence in a labelled bar that ticks
  once per item.

For orchestration layers that must *not* hold a display open -- because the
per-plan work they call into opens its own -- :func:`report_stage` prints the
same kind of milestone line without a display.

The bars persist after completion (no `transient`) so each phase stays
visible, matching the behaviour the introspect-family commands rely on.
"""

from __future__ import annotations

import contextlib
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from ._console import stderr_console

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Sequence

__all__ = ["ProgressReporter", "progress_reporter", "report_stage"]


@dataclass
class ProgressReporter:
    """Callbacks for reporting progress against a shared rich `Progress`."""

    _progress: Any

    def report(self, message: str) -> None:
        """Print a milestone `message` above the progress bars."""
        self._progress.console.print(message)

    def track(self, sequence: Sequence[Any], description: str) -> Iterable[Any]:
        """Yield from `sequence` while rendering a labelled progress bar."""
        task = self._progress.add_task(description, total=len(sequence))
        for item in sequence:
            yield item
            self._progress.advance(task)


def report_stage(message: str) -> None:
    """Print a milestone `message` to stderr without opening a display.

    The display-free counterpart of :meth:`ProgressReporter.report`, so a command
    that narrates its stages while calling into code that renders its own
    progress bars does not have to nest two displays.
    """
    stderr_console().print(message)


#: The reporter of the display that is currently up, if any.
_active: ProgressReporter | None = None


@contextlib.contextmanager
def progress_reporter() -> Iterator[ProgressReporter]:
    """Yield a :class:`ProgressReporter` writing a progress display to stderr.

    Re-entrant: rich allows only one live display per console and every display
    in this package shares
    :func:`~lamindb_setup.evolve.console._console.stderr_console`, so a nested caller
    joins the display that is already running instead of raising.
    """
    global _active

    if _active is not None:
        yield _active
        return

    from rich.progress import (
        BarColumn,
        MofNCompleteColumn,
        Progress,
        SpinnerColumn,
        TextColumn,
    )

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        console=stderr_console(),
        # stderr is redirected (rich's default) so a plain `echo(err=True)`
        # made while the display is up -- the user announcement, a lock-file
        # warning -- is funneled through the console instead of tearing it.
        # stdout is *not*, because it carries the command's Markdown / JSON
        # result, which must reach the caller's pipe verbatim.
        redirect_stdout=False,
    ) as progress:
        _active = ProgressReporter(progress)
        try:
            yield _active
        finally:
            _active = None
