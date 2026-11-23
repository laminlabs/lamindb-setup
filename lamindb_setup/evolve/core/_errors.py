"""The base class every user-facing error of this package derives from.

The CLI turns these into a one-line `Error: <message>` and exit code 1 rather
than a traceback,
so raising one is a statement that the message *is* the report: it has to say
what is wrong and how to get out of it, because it is all the user will see.

Anything not derived from it keeps its traceback, which is the right outcome for
a bug in this package -- the two are told apart by the class, so a new error type
opts into the clean rendering by subclassing this one.

`RuntimeError` remains a base so that code written against the previous,
unrelated error classes keeps catching them.

:class:`InvalidArgumentError` narrows it to a value the caller passed in -- a plan
target, a plan name.  The CLI reports that one as a bad parameter of the command
it was given to (`Invalid value: <message>`, exit code 2), the way it reports
an option it validates itself.
"""

from __future__ import annotations

__all__ = ["InvalidArgumentError", "LaminSchemasError"]


class LaminSchemasError(RuntimeError):
    """An expected, user-actionable failure of a `lamin evolve` operation."""


class InvalidArgumentError(LaminSchemasError, ValueError):
    """A value passed to an operation is unusable, e.g. a plan target that names no plan."""
