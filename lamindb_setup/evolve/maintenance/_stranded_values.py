"""Find values left behind by a feature that was deleted.

Deleting a feature saturates at the trash, and trashing detaches nothing: the
value rows stay keyed to the feature.  Nothing reaches them afterwards -- every
lookup that goes through a live feature misses them, `repair dangling` only
walks structural references, and a feature of the same name declared later is a
different record with a different uid -- yet `get_values()` still reports them
on the rows they annotate.  That is the state this finds.

It is deliberately narrow.  A row carrying a value for a feature its schema does
not declare is *legal* under `minimal_set`, which tolerates undeclared members;
that is exactly what
:func:`~lamindb_setup.evolve.keep_member_values` sanctions.  Only a
value whose *feature itself* is gone is unambiguously orphaned, so only that is
reported.

A deletion that was declared with
:func:`~lamindb_setup.evolve.keep_values` is an accepted retention,
not an accident: it records the acceptance in the feature's `_aux`, which rides
along into the trash.  Those features are listed apart and never repaired.
"""

from __future__ import annotations

import io
from dataclasses import dataclass, field
from typing import Any

from ..engine import values_retained

__all__ = [
    "StrandedFeature",
    "StrandedValuesPlan",
    "compute_stranded_values",
    "execute_stranded_values",
    "print_stranded_values",
    "render_stranded_values",
]


@dataclass
class StrandedFeature:
    """A deleted feature that still owns stored values."""

    uid: str
    name: str | None
    dtype: str | None
    type_name: str | None
    space_uid: str | None
    scalar_values: int = 0
    categorical: bool = False
    retained: bool = False

    @property
    def summary(self) -> str:
        """How much is stored, phrased for one line of the report."""
        parts: list[str] = []
        if self.scalar_values:
            noun = "value" if self.scalar_values == 1 else "values"
            parts.append(f"{self.scalar_values} scalar {noun}")
        if self.categorical:
            parts.append("categorical links")
        return ", ".join(parts) or "values"


@dataclass
class StrandedValuesPlan:
    """The stranded values found, split by whether they were accepted."""

    features: list[StrandedFeature] = field(default_factory=list)
    retained: list[StrandedFeature] = field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        """`True` when there is nothing to repair (accepted ones do not count)."""
        return not self.features


def _describe(record: Any, *, retained: bool) -> StrandedFeature:
    feature_type = getattr(record, "type", None)
    space = getattr(record, "space", None)
    return StrandedFeature(
        uid=str(record.uid),
        name=getattr(record, "name", None),
        dtype=getattr(record, "dtype_as_str", None),
        type_name=getattr(feature_type, "name", None) if feature_type else None,
        space_uid=str(space.uid) if space is not None else None,
        retained=retained,
    )


def compute_stranded_values(
    *,
    space_uids: list[str] | None = None,
    exclude_space_uids: list[str] | None = None,
) -> StrandedValuesPlan:
    """Find every trashed feature that still owns stored values.

    Read-only.  The value probes are the batched ones the drift checks use
    (:func:`~lamindb_setup.evolve.engine._drift_data.prefetch_scalar_values` and
    :func:`~lamindb_setup.evolve.engine._drift_data.prefetch_categorical_presence`), so
    the whole scan costs a handful of queries however many features the instance
    has -- the categorical side is one `UNION` across every link table rather
    than one probe per table per feature.

    `space_uids` / `exclude_space_uids` filter which *features* are
    considered.  The value queries themselves deliberately span every space, as
    everywhere else: a value in an excluded space is just as stranded.
    """
    import lamindb as ln

    from ..engine import (
        prefetch_categorical_presence,
        prefetch_scalar_values,
    )

    # Every lookup goes into one `filter` call: lamindb re-applies its
    # live-branch filter on a chained one, which would drop the trashed features
    # this is looking for.
    lookups: dict[str, Any] = {"branch_id": -1, "is_type": False}
    if space_uids:
        lookups["space__uid__in"] = list(space_uids)
    queryset = ln.Feature.filter(**lookups)
    if exclude_space_uids:
        queryset = queryset.exclude(space__uid__in=list(exclude_space_uids))
    trashed = list(queryset)
    if not trashed:
        return StrandedValuesPlan()

    scalars = prefetch_scalar_values(trashed)
    categorical = prefetch_categorical_presence(trashed)

    plan = StrandedValuesPlan()
    for record in trashed:
        entry = _describe(record, retained=values_retained(record))
        entry.scalar_values = len(scalars.get(record.pk, []))
        entry.categorical = bool(categorical.get(record.pk, False))
        if not entry.scalar_values and not entry.categorical:
            continue
        (plan.retained if entry.retained else plan.features).append(entry)
    plan.features.sort(key=lambda f: (f.name or "", f.uid))
    plan.retained.sort(key=lambda f: (f.name or "", f.uid))
    return plan


def execute_stranded_values(plan: StrandedValuesPlan) -> int:
    """Delete the stranded value rows, returning how many rows were removed.

    Only the features in `plan.features` are touched: an accepted retention is
    reported but never repaired.  The rows go permanently -- a value table has no
    trash state of its own, and the feature they belong to is already deleted.

    Restoring the feature instead is the other way out of this state, and is left
    to the operator: it is a decision about the *schema*, and the plan that
    deleted the feature would otherwise simply delete it again.

    Runs in one transaction.  This is the most destructive of the repairs -- the
    rows go permanently, and a value table has no trash to recover them from --
    so a failure part-way through the feature list must not leave the earlier
    features stripped and the rest untouched, a state that is neither the one
    reported nor one anybody can undo.
    """
    import lamindb as ln
    from django.db import transaction

    from ..engine import delete_stored_values

    removed = 0
    with transaction.atomic():
        for entry in plan.features:
            record = ln.Feature.filter(uid=entry.uid).one_or_none()
            if record is None:
                continue
            removed += delete_stored_values(record)
    return removed


def _build_lines(plan: StrandedValuesPlan, title: str) -> list[str]:
    lines = [title, "=" * len(title), ""]
    if plan.is_empty and not plan.retained:
        lines.append("No stranded values found.")
        return lines
    if plan.features:
        lines.append("Deleted features that still own stored values:")
        lines.append("")
        for entry in plan.features:
            label = entry.name or "(unnamed)"
            where = f" [{entry.type_name}]" if entry.type_name else ""
            lines.append(f"  - {label}{where}  uid {entry.uid}  {entry.summary}")
        lines.append("")
        lines.append(
            f"{len(plan.features)} deleted feature(s) still own values. They are "
            "unreachable: no live feature resolves to them, and a feature of the "
            "same name declared later is a different record."
        )
    if plan.retained:
        lines.append("")
        lines.append("Accepted retentions (declared with keep_values, not repaired):")
        lines.append("")
        for entry in plan.retained:
            label = entry.name or "(unnamed)"
            lines.append(f"  - {label}  uid {entry.uid}  {entry.summary}")
    return lines


def render_stranded_values(plan: StrandedValuesPlan, *, title: str) -> str:
    """Render the scan as plain text."""
    buffer = io.StringIO()
    buffer.write("\n".join(_build_lines(plan, title)))
    buffer.write("\n")
    return buffer.getvalue()


def print_stranded_values(plan: StrandedValuesPlan, *, title: str) -> None:
    """Print the scan to stdout."""
    print(render_stranded_values(plan, title=title), end="")
