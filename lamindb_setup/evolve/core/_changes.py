"""What a computed change *is*: the vocabulary the apply pipeline speaks.

A handful of enums and records with no behaviour and no dependencies.  They
used to live in :mod:`lamindb_setup.evolve.engine._apply`, so naming an apply mode or
reading a change item meant importing several thousand lines of engine -- the
CLI, the CI reporter and the renderers all did, only to reach an enum.

Keeping the vocabulary in a leaf lets everything that merely *talks about*
changes depend on this, and leaves `apply` to the code that computes them.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Protocol, Self

__all__ = [
    "PLACEHOLDER_UID",
    "Action",
    "ApplyMode",
    "ChangeItem",
    "ChangeSet",
    "ChangeWarning",
    "MissingPrerequisite",
    "MoveOutcome",
    "Placeholder",
    "PrerequisitePlan",
    "UidMisalignment",
    "WarningCategory",
    "warning_category",
]


PLACEHOLDER_UID = "(known after apply)"


class ApplyMode(enum.Enum):
    """Whether to only inspect the instance (PLAN) or also create objects (APPLY)."""

    PLAN = "plan"
    APPLY = "apply"


class Action(enum.Enum):
    """The outcome computed for a single plan object."""

    CREATE = "create"
    UPDATE = "update"
    NO_CHANGE = "no-change"
    DELETE = "delete"
    TRANSFER = "transfer"
    MOVE = "move"


@dataclass
class Placeholder:
    """Stand-in returned in PLAN mode for an object that would be created."""

    key: str
    kind: str
    uid: str = PLACEHOLDER_UID


@dataclass
class UidMisalignment:
    """An object whose uid here is not the one the plan's other instances record.

    `uid` is the object's uid on this instance (`None` when it does not
    exist here yet), and `others` maps each other instance's slug to the uid
    its lock records for the same natural key.  `reason` says what is wrong in
    a sentence: the uids differ, the other instances disagree among themselves,
    or the uid to create the object with is already taken here.
    """

    uid: str | None
    others: dict[str, str]
    reason: str

    def describe(self) -> str:
        """The reason followed by the uid each instance records."""
        recorded = ", ".join(
            f"{slug}: {uid}" for slug, uid in sorted(self.others.items())
        )
        return f"{self.reason} ({recorded})" if recorded else self.reason


class WarningCategory(enum.Enum):
    """What a :class:`ChangeWarning` is about, which says how to summarise it.

    - `DRIFT`: the instance was changed outside the plan since the last apply.
    - `RENAME`: a create looks like a rename not declared with `ctx.moved`.
    - `PREREQUISITE`: a borrowed object is missing because the prerequisite
      plan owning it has not been applied to the instance.
    - `CAVEAT`: a change the plan makes deserves a reader's attention; nothing
      happened outside the plan.
    """

    DRIFT = "drift"
    RENAME = "rename"
    PREREQUISITE = "prerequisite"
    CAVEAT = "caveat"


class ChangeWarning(str):
    """A warning on a :class:`ChangeItem`: its text, and its :class:`WarningCategory`.

    A `str`, so everything reading a change's warnings as text -- rendering,
    matching, comparing -- keeps doing so; only a renderer summarising them asks
    for the :attr:`category`.  A string operation returns a plain `str`, so
    rewriting a warning's text means building a new one with the same category.
    """

    category: WarningCategory

    def __new__(cls, text: str, category: WarningCategory) -> Self:
        warning = super().__new__(cls, text)
        warning.category = category
        return warning

    def __reduce__(self) -> tuple[type[Self], tuple[str, WarningCategory]]:
        # `str`'s own reduction passes the text alone, losing the category
        # (and failing `__new__`) on a copy or pickle.
        return (type(self), (str(self), self.category))


def warning_category(warning: str) -> WarningCategory:
    """The category of `warning`.

    A plain `str` -- a :class:`ChangeItem` built by hand -- is read as
    `DRIFT`, which is what every warning was read as before they had one.
    """
    if isinstance(warning, ChangeWarning):
        return warning.category
    return WarningCategory.DRIFT


@dataclass
class ChangeItem:
    """A single computed change for diff rendering.

    `warnings` are what the change deserves a reader's attention for, each
    with the :class:`WarningCategory` that decides how a renderer summarises it.
    `uid_misalignment` is set when the object's uid disagrees with the uid the
    plan's lock files for other instances record (see
    :mod:`lamindb_setup.evolve.engine._alignment`).  `suspected_rename_of` is
    set on a create that looks like an undeclared rename: it names the deleted
    object of the same kind and configuration, which `ctx.moved` would have
    renamed in place.  `borrowed` marks an object borrowed from a prerequisite
    plan (see :meth:`~lamindb_setup.evolve.engine._apply.ApplyContext._borrow`):
    creating it is that plan's change, so a borrowed `CREATE` is never counted
    as one of this plan's own.
    """

    obj_id: int
    key: str
    kind: str
    name: str | None
    action: Action
    uid: str
    details: list[str] = field(default_factory=list)
    warnings: list[ChangeWarning] = field(default_factory=list)
    uid_misalignment: UidMisalignment | None = None
    suspected_rename_of: str | None = None
    borrowed: bool = False

    @property
    def is_own_change(self) -> bool:
        """Whether this is a change the plan itself makes: not unchanged, not borrowed."""
        return self.action is not Action.NO_CHANGE and not self.borrowed

    @property
    def is_borrowed_create(self) -> bool:
        """Whether a prerequisite plan has yet to create this borrowed object."""
        return self.borrowed and self.action is Action.CREATE


@dataclass(frozen=True)
class PrerequisitePlan:
    """A plan another plan depends on, as a message names it.

    `target` is the `path/to/plan.py[:plan_variable]` an `apply` takes to
    name it, or `None` when the plan was not loaded from a file.
    """

    name: str
    target: str | None = None


@dataclass(frozen=True)
class MissingPrerequisite:
    """An object borrowed from a prerequisite plan that the instance does not have.

    Recorded by the preview where it warns that the apply will refuse (see
    :meth:`~lamindb_setup.evolve.engine._apply.ApplyContext._borrow`), so the
    apply can refuse before it tracks a run.  `owner` is the prerequisite plan
    declaring the object, `None` when it is not known.
    """

    kind: str
    name: str | None
    key: str
    owner: PrerequisitePlan | None = None

    def describe(self) -> str:
        """The object as a message names it: `FeatureType 'p2_ft'`."""
        return f"{self.kind} {self.name!r}" if self.name is not None else self.key


@dataclass
class MoveOutcome:
    """The result of applying declared `ctx.moved` re-keys to a lock.

    `applied` are (old_key, new_key) pairs that re-keyed a live lock entry;
    `already` were no-ops because the new key was already recorded (a prior
    apply migrated them); `unmatched` found neither the old nor the new key in
    the lock (a typo, or an object that never existed).
    """

    applied: list[tuple[str, str]] = field(default_factory=list)
    already: list[tuple[str, str]] = field(default_factory=list)
    unmatched: list[tuple[str, str]] = field(default_factory=list)


class ChangeSet(Protocol):
    """Anything carrying computed changes, which is all a renderer needs.

    :class:`~lamindb_setup.evolve.engine._apply.ApplyContext` is the implementation.
    Naming the one attribute keeps the renderers off the engine.
    """

    changes: list[ChangeItem]
