"""LaminDB's trash: recognising a soft-deleted object, and producing one.

LaminDB does not remove a record when it is deleted -- it moves it off the main
branch.  Both halves of that convention live here so they cannot drift apart:
:func:`is_live` decides whether a record still counts, and :func:`delete_object`
is the only way this package deletes one.
"""

from __future__ import annotations

from typing import Any

__all__ = [
    "delete_object",
    "is_live",
]

# The branch lamindb moves a record to when it is soft-deleted.
_TRASH_BRANCH_ID = -1


def is_live(record: Any) -> bool:
    """Whether a live LaminDB `record` is on a branch that still counts.

    LaminDB soft-deletes by moving a record off the main branch: `branch_id`
    `-1` is the instance trash and `0` the archive, while every positive id
    is a live branch.  A soft-deleted record is hidden from ordinary queries --
    lamindb appends a `branch_id__in` filter to every lookup that is *not*
    addressed by `id` / `uid` / `hash` -- and takes no part in validating
    anything, so it must not be diffed as if it were part of the instance.

    Two paths reach one anyway, and both are checked explicitly:

    * a uid-pinned lock entry, because :meth:`~lamindb_setup.evolve.engine._apply.ApplyContext.prefetch`
      resolves locked objects with `uid__in` and so bypasses the branch filter;
      and
    * a relation that carries no branch filter of its own -- a schema's member
      links, and the raw uid list in `Schema._aux` that backs
      `Schema.optionals` -- which keep referencing a member long after it was
      trashed, since trashing detaches nothing.

    A registry without the column (`Space`, `Branch`) has no notion of a
    branch and is always live.
    """
    branch_id = getattr(record, "branch_id", None)
    return branch_id is None or branch_id > 0


def delete_object(record: Any, *, permanent: bool = False) -> None:
    """Delete `record`, saturating at the trash unless `permanent`.

    Deleting is otherwise not idempotent.  lamindb's default
    (`permanent=None`) means "move it to the trash -- unless it is *already*
    there, in which case ask whether to erase it from the database", and that
    question is a literal :func:`input` call.  An object can be in the trash for
    reasons that have nothing to do with this deletion (someone trashed it by
    hand, `repair duplicates` trashed it, a previous run was interrupted
    before its lock was written), so the prompt is reached routinely -- and
    under CI, where stdin is closed, it fails the whole operation with
    `EOFError`, rolling back every unrelated change with it.

    Passing `permanent=False` states the intent instead: trash a live object,
    leave an already-trashed one alone, and never let a soft delete escalate
    into an irreversible one.  The explicit branch check keeps that saturation a
    property of this package rather than of lamindb's branch bookkeeping.  Only
    the trash saturates -- an *archived* object (`branch_id == 0`) has not been
    deleted and is moved to the trash like any other.

    `permanent=True` is unchanged: it erases the record whatever branch it is
    on, which is what makes it the one way to remove something from the trash.

    A registry with no trash at all (lamindb's `BaseSQLRecord`, e.g. `Space`)
    raises `ValueError` on a soft delete; callers that may hold one handle it.
    """
    if not permanent and getattr(record, "branch_id", None) == _TRASH_BRANCH_ID:
        return
    record.delete(permanent=permanent)
