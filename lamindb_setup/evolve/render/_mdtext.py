"""Markdown text helpers shared by the report renderers.

Object names come from a plan file or, for the drift and introspection
reports, straight out of a live instance -- so they are not under the
renderer's control.  A name holding a backtick or a pipe used to break out of
the construct it was interpolated into: the code span closed early, or the
table grew a column.  The HTML renderer has always escaped (`html.escape`);
these are the Markdown equivalents, in one place so the two renderers that
need them cannot drift apart again.
"""

from __future__ import annotations

import re

__all__ = ["append_notes", "code", "table_cell"]

_BACKTICK_RUNS = re.compile(r"`+")


def code(text: str) -> str:
    """Return `text` as an inline code span that survives backticks.

    CommonMark lets a code span be delimited by any number of backticks and
    closes it on the first run of *exactly* that length, so a fence longer than
    the longest run inside the text can never be closed early.  Content that
    itself starts or ends with a backtick additionally needs the padding space
    the spec strips back off when rendering.
    """
    longest = max((len(m.group()) for m in _BACKTICK_RUNS.finditer(text)), default=0)
    fence = "`" * (longest + 1)
    pad = " " if text.startswith("`") or text.endswith("`") else ""
    return f"{fence}{pad}{text}{pad}{fence}"


def table_cell(text: str) -> str:
    """Return `text` safe to place between the pipes of a table row.

    A pipe would start a new column and a newline would end the row, so both
    are neutralized rather than escaped away: the pipe keeps its character
    (escaped), the newline becomes the `<br>` a GitHub table renders as a
    line break.
    """
    return text.replace("\\", "\\\\").replace("|", "\\|").replace("\n", "<br>")


def append_notes(markdown: str, notes: list[str]) -> str:
    """Append advisory `notes` to a rendered Markdown section as a blockquote."""
    if not notes:
        return markdown
    block = "\n".join(f"> {note}" for note in notes)
    return f"{markdown.rstrip()}\n\n{block}\n"
