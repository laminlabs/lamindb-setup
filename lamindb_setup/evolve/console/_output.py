"""How a command renders its summary, and where that document is written.

The format is a command-line option, but the choice reaches much further than
the commands: the apply machinery (see
:mod:`~lamindb_setup.evolve.engine._apply_run`) and the `ci` entry points all branch
on it.  Keeping the format here lets those layers render independently of the CLI.  Validating the `--format`/`--output` pair is the
CLI's own business and stays there.
"""

from __future__ import annotations

import enum
from typing import TYPE_CHECKING

from ._console import echo

if TYPE_CHECKING:
    from pathlib import Path

__all__ = [
    "OutputFormat",
    "join_sections",
    "write_and_echo",
]


class OutputFormat(str, enum.Enum):
    """How a command renders its summary or diff."""

    terminal = "terminal"
    markdown = "markdown"
    json = "json"

    @property
    def is_text(self) -> bool:
        """Whether this format renders to one string the caller may redirect.

        `terminal` is the odd one out: it is written straight to the console
        by `rich`, so it is neither collected across plans nor written to
        `--output`.  The other two are, which is what makes them scriptable.
        """
        return self is not OutputFormat.terminal


def write_and_echo(rendered: str, output: Path | None) -> None:
    """Print `rendered` to stdout and, when `output` is set, write it there."""
    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(rendered, encoding="utf-8")
    echo(rendered)


def join_sections(sections: list[str], output_format: OutputFormat) -> str:
    """Join per-plan `sections` into the one document `output_format` wants.

    Markdown fragments concatenate; JSON documents cannot, so several plans are
    re-serialized as an array.  A single plan still renders as its own object,
    which is what a caller diffing one plan expects to parse.
    """
    if output_format is not OutputFormat.json:
        return "\n\n".join(sections)
    if len(sections) == 1:
        return sections[0]
    import json

    return json.dumps([json.loads(section) for section in sections], indent=2)
