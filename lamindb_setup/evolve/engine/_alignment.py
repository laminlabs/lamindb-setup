"""Keep a plan's objects on the same uid in every instance it is applied to.

A plan is applied to several instances (dev, sandbox, general, ...), and each
used to hand out its own uids, so the same declared object had a different
identity everywhere.  Anything that addresses an object by uid across
instances -- `lamin io sync`, a dtype string such as `cat[Record[<uid>]]`,
a link someone shares -- then sees two unrelated objects.

The rule is that the first locked application of a plan decides the uid.  A
plan keeps one lock file per instance next to it (see :func:`._lock.lock_path`),
so the uids every *other* instance recorded are right there: when an object is
created, it is created with the uid those locks agree on, and an object that
already exists under a different uid is a misalignment that `repair
align-uids` fixes.

Only the plan's *own* locks are consulted.  Two unrelated plans declaring an
object at the same address are not the same object, and reading uids across
plans would quietly merge them.
"""

from __future__ import annotations

import glob
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from ..core import LaminSchemasError
from ._lock import LockFile

if TYPE_CHECKING:
    from pathlib import Path

    from ..authoring import Plan

__all__ = [
    "SiblingUids",
    "UidAlignmentError",
    "fresh_uid",
    "fresh_uids",
    "read_sibling_uids",
    "sibling_lock_paths",
]


class UidAlignmentError(LaminSchemasError):
    """An object's uid differs between the instances a plan is applied to."""


def sibling_lock_paths(
    plan_file: Path, *, exclude: str | None = None
) -> dict[str, Path]:
    """Return `plan_file`'s per-instance lock files, keyed by instance slug.

    The inverse of :func:`._lock.lock_path`: `plan_foo.acme-prod.lock` is the
    lock of `plan_foo.py` against `acme-prod`.  `exclude` drops one slug,
    typically the instance being applied to, whose lock is read separately.
    """
    stem = plan_file.stem
    found: dict[str, Path] = {}
    for path in sorted(plan_file.parent.glob(f"{glob.escape(stem)}.*.lock")):
        slug = path.name[len(stem) + 1 : -len(".lock")]
        if slug and slug != exclude:
            found[slug] = path
    return found


@dataclass(frozen=True)
class SiblingUids:
    """The uids a plan's locks for *other* instances record, per natural key.

    `tracked` covers the lock's managed objects, `seeded` the seed-only ones
    (see :attr:`._lock.LockFile.seeded`).  Each maps a natural key to the uid
    every other instance recorded for it, keyed by that instance's slug.
    """

    tracked: dict[str, dict[str, str]] = field(default_factory=dict)
    seeded: dict[str, dict[str, str]] = field(default_factory=dict)

    @classmethod
    def from_locks(cls, locks: dict[str, LockFile]) -> SiblingUids:
        """Collect the uids recorded by `locks`, keyed by instance slug."""
        tracked: dict[str, dict[str, str]] = {}
        seeded: dict[str, dict[str, str]] = {}
        for slug, lock in sorted(locks.items()):
            for key, entry in lock.objects.items():
                if entry.uid is not None:
                    tracked.setdefault(key, {})[slug] = entry.uid
            for key, uid in lock.seeded.items():
                seeded.setdefault(key, {})[slug] = uid
        return cls(tracked=tracked, seeded=seeded)

    def recorded(self, key: str, *, seeded: bool = False) -> dict[str, str]:
        """The uid each other instance records for `key`, keyed by slug."""
        return dict((self.seeded if seeded else self.tracked).get(key, {}))

    def canonical(self, key: str, *, seeded: bool = False) -> str | None:
        """The uid every other instance agrees on for `key`, else `None`.

        `None` both when no other instance records `key` and when they
        disagree; :meth:`disagree` tells the two apart.
        """
        uids = set(self.recorded(key, seeded=seeded).values())
        return uids.pop() if len(uids) == 1 else None

    def disagree(self, key: str, *, seeded: bool = False) -> bool:
        """Whether the other instances record more than one uid for `key`."""
        return len(set(self.recorded(key, seeded=seeded).values())) > 1


def read_sibling_uids(
    plan_file: Path, plan: Plan[Any], instance_slug: str
) -> SiblingUids:
    """Read the uids `plan_file`'s other per-instance locks record.

    A lock is only counted when it records `plan`: a file whose name merely
    starts with the plan's stem (`plan_foo.v2.py` next to `plan_foo.py`) has
    locks that match the glob but describe another plan.  Each lock is re-keyed
    for the plan's `ctx.moved` declarations first, exactly as this instance's
    own lock is, so an object renamed since another instance last applied is
    still pinned to the uid it has there.  An unreadable lock raises, as it
    does everywhere else: a lock read wrongly would pin a wrong uid.
    """
    # Deferred: `apply` imports this module.
    from ._apply import lock_with_moves

    locks: dict[str, LockFile] = {}
    for slug, path in sibling_lock_paths(plan_file, exclude=instance_slug).items():
        lock = LockFile.read(path)
        if lock is None or (lock.plan and lock.plan != plan.name):
            continue
        moved = lock_with_moves(plan, lock)
        if moved is not None:
            locks[slug] = moved
    return SiblingUids.from_locks(locks)


def fresh_uid(model: Any) -> str:
    """Return a random uid not yet used in `model`'s table, on any branch.

    The length is the one the `uid` column is declared with, so the uid is
    the same shape as one lamindb would have generated.
    """
    return fresh_uids(model, 1)[0]


#: How many uids :func:`fresh_uids` checks against the table in one query.
_FRESH_UID_BATCH = 500


def fresh_uids(model: Any, count: int) -> list[str]:
    """Return `count` distinct random uids not yet used in `model`'s table.

    Like :func:`fresh_uid`, but checked against the table a batch at a time, so
    handing out many costs a query per batch rather than one per uid -- which
    is what a run against a remote instance pays for.
    """
    from lamindb.base.uids import base62

    length = model._meta.get_field("uid").max_length
    found: list[str] = []
    while len(found) < count:
        wanted = min(count - len(found), _FRESH_UID_BATCH)
        candidates = {base62(length) for _ in range(wanted)} - set(found)
        taken = set(
            model.objects.filter(uid__in=list(candidates)).values_list("uid", flat=True)
        )
        found.extend(sorted(candidates - taken))
    return found[:count]
