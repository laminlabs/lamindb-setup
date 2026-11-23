# Source behavior retained; host-repository lint exceptions.
# ruff: noqa: TC003
"""Apply plans against a LaminDB instance with terraform-like state tracking.

This module drives the read-only existence check and the create step that back
the `lamin evolve` CLI.  An :class:`ApplyContext` walks a finalized plan's
dependency DAG and, for every object, decides whether it already exists (and is
unchanged), would be created, or has drifted from the recorded lock state.
"""

from __future__ import annotations

from typing import Any

from typing_extensions import TypeVar

E = TypeVar("E", bound="PlanExports")

import datetime
import re
from contextlib import contextmanager, nullcontext
from contextvars import ContextVar
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, get_args, get_origin

from ..authoring import (
    CHILDREN_REMOVAL,
    DETACH_MEMBER,
    FLEXIBLE_CHANGE,
    INDEX_CHANGE,
    MAXIMAL_SET,
    MEMBER_ADDED,
    MEMBER_REMOVAL,
    MEMBER_REQUIRED,
    MERGE_SCHEMA,
    MINIMAL_SET,
    NULLABILITY,
    ORDERED_SET,
    RECORD_ASPECTS,
    RECORD_REMOVAL,
    RECORD_UPDATE,
    ROWS_REMOVAL,
    SHEET_SCHEMA,
    SLOT_CHANGE,
    STALE_EXPORTS,
    UNIT_CHANGE,
    VALIDATED_REMOVAL,
    FlexibleChangeContext,
    IndexChangeContext,
    MemberMigrationContext,
    MemberRemovalContext,
    MigrationContext,
    MigrationError,
    MigrationReport,
    RecordRemovalContext,
    RecordTypeRemovalContext,
    RecordUpdateContext,
    SchemaRemovalContext,
    SheetExportContext,
    canonical_dtype_str,
    dtypes_match,
    resolved_dtype_str,
)
from ..core import (
    DEFAULT_MODELS,
    PLACEHOLDER_UID,
    SEED_ONLY_KINDS,
    Action,
    ApplyMode,
    ChangeItem,
    ChangeWarning,
    LaminSchemasError,
    Materializable,
    MissingPrerequisite,
    MoveOutcome,
    Placeholder,
    PrerequisitePlan,
    Promise,
    UidMisalignment,
    WarningCategory,
    provisional_promises,
)
from ._alignment import UidAlignmentError, fresh_uid
from ._drift import Verdict, caveat, classify
from ._loader import plan_target
from ._lock import (
    LOCK_VERSION,
    LockEntry,
    LockFile,
    natural_key,
    registry_for_kind,
    remote_state_hash,
    serialize_config,
    serialize_existing,
)
from ._trash import delete_object, is_live

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence

    from ..authoring import (
        DataMigration,
        Migrated,
        Plan,
        PlanExports,
    )
    from ..core import SpaceUID
    from ._alignment import SiblingUids
    from ._drift_data import MemberOmissions, MemberUsage, SchemaUsage, ValueFailures
    from ._inferred_schemas import InferredMemberRemoval

__all__ = [
    "Action",
    "ApplyContext",
    "ApplyMode",
    "ChangeItem",
    "DriftError",
    "MoveOutcome",
    "Placeholder",
    "PlanChangeRefusedError",
    "SpaceNotFoundError",
    "compute_changes",
    "ExistenceCache",
    "forget_missing",
    "lock_with_moves",
    "plan_natural_keys",
    "prefetch_plans",
    "preview_changes",
    "rehearsal",
    "resolve_existing",
]


# Descriptive metadata that is safe to update in place on an existing object,
# keyed by object kind.  A divergence here is reported and applied as an UPDATE.
_DESCRIPTIVE_FIELDS: dict[str, tuple[str, ...]] = {
    "FeatureType": (),
    "Feature": ("description", "synonyms", "unit"),
    "SchemaType": ("description",),
    "Schema": ("description",),
    "RecordType": ("description",),
    "Sheet": ("description",),
    "Record": ("description", "reference", "reference_type"),
}

# Validation-affecting / structural metadata that cannot be edited safely:
# changing it may invalidate already-validated records, so a divergence raises
# a DriftError rather than being reconciled.
_GUARDED_FIELDS: dict[str, tuple[str, ...]] = {
    "Feature": ("nullable", "default_value", "coerce"),
    "Schema": (
        "coerce",
        "flexible",
        "otype",
        "minimal_set",
        "ordered_set",
        "maximal_set",
    ),
}

# Fields that always carry a concrete value (no "unset" sentinel), so they are
# compared even when the desired value is falsy.
_ALWAYS_SET_FIELDS: frozenset[str] = frozenset(
    {"minimal_set", "ordered_set", "maximal_set"}
)

# Guarded fields a plan may leave unset but that LaminDB nonetheless materializes
# with a concrete default, keyed by `(kind, field)`.
#
# `None` normally means "the plan has no opinion", but that is not true here:
# `ln.Feature.__init__` fills an unset `nullable` with `True` for every
# non-type feature, so a plan that never mentions `nullable` still *creates*
# nullable features.  Treating the unset plan value as "no opinion" during
# reconcile would therefore be inconsistent -- an object the plan adopted (or one
# whose lock baseline is gone) could sit at `nullable=False` forever while the
# plan silently claims otherwise.  Resolving the implicit default makes the plan
# say what it actually means; the resulting `False -> True` edit is a widening
# the drift taxonomy classifies as SAFE, so it is repaired in place rather than
# refused.
_IMPLICIT_DEFAULTS: dict[tuple[str, str], Any] = {("Feature", "nullable"): True}

# The feature fields a member backfill's NULL / DEFAULT resolution reads.  Both
# are guarded, so a plan changing one reaches the backfill as a pending edit that
# PLAN mode has not saved (see :attr:`ApplyContext._pending_feature_fills`).
_FILL_FIELDS: frozenset[str] = frozenset({"nullable", "default_value"})

# Identity metadata: a lock baseline that disagrees with the plan here signals a
# tampered or corrupted lock rather than a stale-but-safe property, so the
# "instance already matches the plan" relaxation in :meth:`_check_plan_edit`
# never applies to these keys.
_IDENTITY_KEYS: frozenset[str] = frozenset({"name", "type"})

# Snapshot keys whose value *references other objects* rather than describing the
# object itself.  :meth:`_check_plan_edit` abstains on them entirely, for two
# reasons that both come down to the plan and the instance speaking different
# languages about a reference.
#
# The two snapshots address references in different namespaces by design --
# :func:`serialize_config` by natural key (a plan has no uids before it is
# applied) and :func:`serialize_existing` by uid (an instance has no
# plan-relative keys) -- so the "instance already matches the plan" relaxation
# cannot even be evaluated: an equality test across the namespaces is never true.
#
# And a lock-vs-plan comparison of these keys cannot reach the right verdict on
# its own: it sees each set independently, so a schema member the plan *drops*
# reads identically to one it makes required again.  Only a diff against the
# instance can tell those apart, which is what :meth:`_schema_feature_diff` and
# :meth:`_schema_optional_diff` do -- and :meth:`_reconcile` vets each verdict
# there, against the live member list rather than against a baseline that may
# predate an out-of-band change.
#
# A schema's `index` is the same kind of key, and is diffed against the
# instance the same way (:meth:`_schema_index_diff`): that is also what lets a
# lock that is merely stale -- the instance already has the plan's index --
# converge without a change, which the namespaces above make impossible here.
# A schema's `otype`, `itype` and `dtype`: what a dataset it validates is,
# and what its members are.  Changed only on a schema that validated nothing
# (see :meth:`ApplyContext._data_safe_reason`).
_SCHEMA_SHAPE_KEYS: frozenset[str] = frozenset({"otype", "itype", "dtype"})

# A schema's set flags.  Turning one off only admits more (see
# :data:`~lamindb_setup.evolve.engine._drift._WIDEN_TRUE_TO_FALSE`); turning one on is
# judged against what the schema validated (see :meth:`ApplyContext._vet_set_flag`
# and :meth:`ApplyContext._vet_minimal_set`).
_SET_FLAGS: frozenset[str] = frozenset({"maximal_set", "minimal_set", "ordered_set"})

_REFERENCE_KEYS: frozenset[str] = frozenset({"features", "optionals", "index", "slots"})

# The configuration keys :meth:`ApplyContext._reconcile` applies to the instance
# -- or diffs against it and refuses -- per kind.  A key outside the set has no
# applier: letting a difference in it through would record the plan's value in
# the lock while the instance kept its own, and nothing would report it again.
# So :meth:`ApplyContext._check_plan_edit` refuses any other key that differs,
# whatever the taxonomy, the data or a declared migration would say.  `name` and
# `type` are absent on purpose: they are applied for a declared `ctx.moved`
# alone, which that check exempts before it gets here.
_APPLIED_KEYS: dict[str, frozenset[str]] = {
    "FeatureType": frozenset({"space_uid"}),
    "Feature": frozenset(
        {
            "space_uid",
            "description",
            "synonyms",
            "unit",
            "nullable",
            "default_value",
            "coerce",
            "dtype",
            "cat_filters",
        }
    ),
    "SchemaType": frozenset({"space_uid", "description"}),
    "Schema": frozenset(
        {
            "space_uid",
            "description",
            "features",
            "optionals",
            "index",
            "coerce",
            "flexible",
            "otype",
            "minimal_set",
            "ordered_set",
            "maximal_set",
            "dtype",
            "itype",
            "formats",
        }
    ),
    "RecordType": frozenset(
        {
            "space_uid",
            "description",
            "single_space_uid",
            "exclude_children_from_plans",
            # Only ever differs for a sheet the plan demotes (see
            # :meth:`ApplyContext._demotes_a_sheet`), which drops it.
            "schema",
        }
    ),
    "Sheet": frozenset(
        {
            "space_uid",
            "description",
            "single_space_uid",
            "exclude_children_from_plans",
            "schema",
            "is_experiment_sheet",
        }
    ),
}

# A feature's dtype as a plan's configuration records it: the bare dtype and the
# categorical filters next to it.  lamindb validates against both together --
# `cat[Record[A]]` and `cat[Record[B]]` are different dtypes -- so the two
# keys are one property to every gate (see :meth:`ApplyContext._dtype_edit_drift`).
_FEATURE_DTYPE_KEYS: tuple[str, ...] = ("dtype", "cat_filters")


# Kinds whose deletion can take more than the object itself: lamindb's trash
# cascades from a type to everything typed by it, and a schema left behind by the
# datasets it validated keeps them pointing at a trashed one (see
# :meth:`ApplyContext._vet_container_removals`).
_VETTED_REMOVAL_KINDS: frozenset[str] = frozenset(
    {"FeatureType", "SchemaType", "RecordType", "Sheet", "Schema"}
)

# What the children of each kind of type are called in a refusal.
_CHILD_NOUNS: dict[str, str] = {
    "FeatureType": "feature",
    "SchemaType": "schema",
    "RecordType": "record type",
    "Sheet": "record type",
}

# Stands in for "the plan declares an index whose feature does not exist yet" in
# :meth:`ApplyContext._check_remote_drift`, so it can never compare equal to the
# uid -- or the `None` -- an instance snapshot records; and for a slot
# component's `dtype` that cannot be known before it exists (see
# :class:`_ComponentShape`).
_UNRESOLVED = object()


def _desired_value(obj: Any, field_name: str) -> Any:
    """Return `obj`'s desired value for `field_name` in instance-comparable form."""
    value = getattr(obj, field_name, None)
    if field_name == "synonyms":
        return "|".join(value) if value else None
    return value


def _descriptive_keys(kind: str) -> frozenset[str]:
    """Return the snapshot keys that are safe to change in place for `kind`.

    Divergence in these keys is reconciled (remote auto-corrected to the plan,
    or a plan edit pushed to the remote).  Every other key is structural and a
    divergence there is a hard :class:`DriftError`.  `space_uid` is included
    because objects can be safely moved between spaces.  The aux-backed record
    type settings (`single_space_uid`, `is_experiment_sheet`) are also safe:
    LaminDB leaves already-validated records untouched when they change.
    """
    keys = frozenset(_DESCRIPTIVE_FIELDS.get(kind, ())) | {"space_uid"}
    if kind in ("RecordType", "Sheet"):
        keys |= {"single_space_uid", "exclude_children_from_plans"}
        keys |= {"is_experiment_sheet"}
    if kind == "Schema":
        # Unhashed by lamindb and only checked on future curation.
        keys |= {"formats"}
    return keys


# Longest rendering of a single offending value in a drift message; anything
# longer is elided so one pathological free-text value cannot swamp the report.
_MAX_VALUE_REPR = 60

# How many objects a refusal names before it elides the rest.
_MAX_LISTED = 5


def _abbreviated_repr(value: Any) -> str:
    """`repr(value)`, shortened to :data:`_MAX_VALUE_REPR` characters."""
    text = repr(value)
    if len(text) <= _MAX_VALUE_REPR:
        return text
    return text[: _MAX_VALUE_REPR - 1] + "…"


def _format_failing_examples(failures: ValueFailures) -> str:
    """Render the offending values of `failures` for a drift message.

    Counts alone ("1176 of 1197 values do not validate") say how much data blocks
    a change but not *what* blocks it, which is what decides between fixing the
    data, declaring a migration, or dropping the change.  So the distinct
    offenders are named: all of them when they fit in the examples
    (`invalid values: 'n/a', 'unknown'`), otherwise the first few behind the
    distinct total (`37 distinct invalid values, e.g. 'n/a', 'unknown', ...`).
    """
    shown = ", ".join(_abbreviated_repr(value) for value in failures.examples)
    if failures.distinct > len(failures.examples):
        return f"{failures.distinct} distinct invalid values, e.g. {shown}"
    noun = "value" if failures.distinct == 1 else "values"
    return f"invalid {noun}: {shown}"


def _optional_safe_reason(omissions: MemberOmissions | None) -> str | None:
    """Why making an optional schema member required is safe for the live rows.

    Two ways it can be, both meaning "no row that omitted the member would be
    invalidated": the schema validated nothing at all, or every row it did
    validate already carries a value.  Returns a short reason for each and
    `None` otherwise -- including when the rows could not be introspected
    (`omissions is None`), which keeps the block in force.
    """
    if omissions is None:
        return None
    if omissions.total == 0:
        return "the schema has validated no records"
    if omissions.omitting == 0:
        if omissions.total == 1:
            return "the one validated record already carries a value"
        return f"all {omissions.total} validated records already carry a value"
    return None


# The row registry of a sheet's rows, scoping a backfill to them (see
# :class:`_DeferredMemberMigration`).
_SHEET_ROWS: frozenset[str] = frozenset({"Record"})


def _added_member_hint(omissions: MemberOmissions | None) -> str:
    """What the rows of a sheet say about a required member the plan adds."""
    if omissions is None:
        return "the rows of the sheets using the schema could not be introspected"
    reason = _optional_safe_reason(omissions)
    if reason is not None:
        # Only reachable without the flag: with it the member was added.
        return (
            "every sheet row already carries a value (could be applied with "
            f"--allow-data-validated-changes: {reason})"
        )
    noun = "row" if omissions.total == 1 else "rows"
    verb = "omits" if omissions.omitting == 1 else "omit"
    shown = ", ".join(repr(label) for label in omissions.examples)
    examples = f", e.g. {shown}" if shown else ""
    return (
        f"{omissions.omitting} of {omissions.total} sheet {noun} {verb} it"
        f"{examples} (declare fill_missing(...) on the schema to backfill them)"
    )


def _nullable_safe_reason(usage: _NullabilityUsage | None) -> str | None:
    """Why turning a feature's `nullable` off invalidates nothing, or `None`.

    Nothing could hold a null for it: no row of a sheet whose schema declares it
    omits it, and no dataset was validated against such a schema.  Its stored
    values say nothing either way -- a null is the absence of one.
    """
    if usage is None or usage.omitting or usage.datasets.total:
        return None
    if not usage.omissions:
        return "no sheet row or dataset validated against a schema declaring it"
    return "every sheet row of a schema declaring it carries a value"


def _omitting_rows_hint(usage: _NullabilityUsage) -> str:
    """The rows null for a feature made non-nullable, for a refusal."""
    examples = [label for _, found in usage.omissions for label in found.examples]
    shown = ", ".join(repr(label) for label in examples[:_MAX_LISTED])
    verb = "omits" if usage.omitting == 1 else "omit"
    return (
        f"{_plural(usage.omitting, 'sheet row')} {verb} it, e.g. {shown}; declare "
        "fill_nulls(...) on the feature to backfill them"
    )


def _datasets_hint(usage: _NullabilityUsage) -> str:
    """The datasets that may hold nulls for a feature made non-nullable."""
    return (
        f"{_describe_usage(usage.datasets)} validated against a schema declaring "
        "it cannot be re-checked for nulls in place; declare "
        "accept_non_null(reason=...) on the feature to vouch for them"
    )


def _nullable_hint(usage: _NullabilityUsage | None) -> str:
    """A parenthetical for a refused `nullable` narrowing the data does not prove."""
    if usage is None:
        return " (what could hold a null for it could not be established)"
    problems = []
    if usage.omitting:
        problems.append(_omitting_rows_hint(usage))
    if usage.datasets.total:
        problems.append(_datasets_hint(usage))
    return f" ({'; '.join(problems)})" if problems else ""


def _removal_safe_reason(usage: MemberUsage | None) -> str | None:
    """Why removing a schema member strands nothing, or `None`.

    The mirror of :func:`_optional_safe_reason` for the opposite edit.  Removing
    a member leaves whatever was stored for it behind, which is harmless in
    exactly two cases: the schema validated nothing at all, or none of the rows
    it validated ever carried the member.  `None` otherwise -- including when
    the rows could not be introspected, which keeps the block in force.
    """
    if usage is None:
        return None
    if usage.total == 0:
        return "the schema has validated no records"
    if usage.carrying == 0:
        if usage.total == 1:
            return "the one validated record carries no value for it"
        return f"none of the {usage.total} validated records carry a value for it"
    return None


def _index_safe_reason(usage: SchemaUsage | None) -> str | None:
    """Why changing a schema's index invalidates nothing, or `None`.

    Unlike a member narrowing there is no per-row test: the index decides how a
    whole dataset is curated, so the change is only provably safe when the schema
    -- directly or through a composite -- has validated nothing at all.  `None`
    otherwise, including when that could not be established.
    """
    if usage is None or usage.total:
        return None
    return "the schema has validated no data"


def _flexible_safe_reason(usage: SchemaUsage | None) -> str | None:
    """Why turning a schema's `flexible` off invalidates nothing, or `None`.

    Only reached for a `maximal_set` schema, where it is refused because a
    dataset that carried a column the flexible match accepted would no longer
    validate.  lamindb consults the flag only when it curates a *dataset* -- never
    for a sheet row -- so the change is provably safe when no artifact or
    collection was validated against the schema, directly or through a
    composite.  `None` otherwise, including when that could not be established.
    """
    if usage is None or usage.artifacts or usage.collections:
        return None
    return "the schema has validated no datasets"


def _datasets_only(usage: SchemaUsage) -> SchemaUsage:
    """`usage` without the sheet rows, which `flexible` has no say over."""
    return usage._replace(rows=0, sheets=[])


def _record_labels(queryset: Any, count: int) -> str:
    """The first few records of `queryset` as a message names them."""
    shown = [
        repr(getattr(record, "name", None) or str(getattr(record, "uid", "?")))
        for record in queryset[:_MAX_LISTED]
    ]
    return ", ".join(shown) + (", ..." if count > len(shown) else "")


def _live_rows(record_type: Any) -> Any:
    """The records typed by `record_type` that are not in the trash, sub-types left out."""
    return (
        type(record_type)
        .objects.filter(type_id=record_type.pk, is_type=False)
        .exclude(branch_id=-1)
    )


def _live_children(record: Any) -> Any:
    """What is typed under the type `record` and not in the trash, its rows left out."""
    children = type(record).objects.filter(type_id=record.pk).exclude(branch_id=-1)
    if type(record).__name__ == "Record":
        children = children.filter(is_type=True)
    return children


def _live_kind(record: Any) -> str:
    """The kind a plan gives the live `record`: `Sheet`, `FeatureType`, ..."""
    model = type(record).__name__
    is_type = bool(getattr(record, "is_type", False))
    if model == "Record" and is_type:
        return (
            "Sheet" if getattr(record, "schema_id", None) is not None else "RecordType"
        )
    if is_type and model in ("Feature", "Schema"):
        return f"{model}Type"
    return model


def _live_key(record: Any) -> str | None:
    """The natural key a plan gives the live `record`; `None` when unnamed."""
    parts: list[str] = []
    node: Any = record
    seen: set[Any] = set()
    while node is not None and node.pk not in seen:
        seen.add(node.pk)
        name = getattr(node, "name", None)
        if not name:
            return None
        parts.insert(0, f"{_live_kind(node)}:{name}")
        node = getattr(node, "type", None)
    return "/".join(parts)


def _live_label(record: Any) -> str:
    """`Sheet 'New'`: the live `record` as a diff names it."""
    name = getattr(record, "name", None) or str(getattr(record, "uid", "?"))
    return f"{_live_kind(record)} {name!r}"


# The kinds each relocation of a deleted record type's rows takes as a target.
_ROWS_TARGET_KINDS: dict[str, frozenset[str]] = {
    "keep_rows": frozenset({"RecordType"}),
    "move_rows": frozenset({"RecordType", "Sheet"}),
}

# The kinds `adopt_children` takes as a target, by the kind of the deleted type.
_CHILD_TARGET_KINDS: dict[str, frozenset[str]] = {
    "FeatureType": frozenset({"FeatureType"}),
    "SchemaType": frozenset({"SchemaType"}),
    "RecordType": frozenset({"RecordType", "Sheet"}),
    "Sheet": frozenset({"RecordType", "Sheet"}),
}

# The references a dataset holds to the schema it was validated against, which
# `revalidate_against` points at another: `(model, foreign key)` by name.
_DATASET_SCHEMA_FKS: frozenset[tuple[str, str]] = frozenset(
    {("Artifact", "schema"), ("Collection", "schema"), ("ArtifactSchema", "schema")}
)


@dataclass
class _Rehoming:
    """Where a removal declaration moves what a deleted type or schema leaves behind.

    Settled while the removal is vetted, in PLAN and APPLY mode alike (see
    :meth:`ApplyContext._container_removal`), and carried out in APPLY mode by
    :meth:`ApplyContext._rehome`, before the deletion.  `target` is the live
    record it moves them onto -- `None` for no type at all -- and `label`
    names it for the diff.  `renames` maps the pk of a moved row to the name
    `index_name_conflict="keep_feature"` gives it, and `kept` holds the pks
    of the sheet exports a revalidation leaves with the deleted schema.
    """

    migration: DataMigration
    target: Any
    label: str
    renames: dict[Any, str] = field(default_factory=dict)
    kept: list[Any] = field(default_factory=list)


def _plural(count: int, noun: str) -> str:
    """`count` and `noun`, with an `s` unless there is exactly one."""
    return f"{count} {noun}" if count == 1 else f"{count} {noun}s"


def _or(parts: Sequence[str]) -> str:
    """`a`, `a or b`, `a, b or c`."""
    return parts[0] if len(parts) == 1 else ", ".join(parts[:-1]) + " or " + parts[-1]


def _conflicting(what: str, migrations: Sequence[DataMigration], choices: str) -> str:
    """The refusal of several declarations answering for the same dependents."""
    return (
        f"{what} take one declaration, not {len(migrations)} ("
        + ", ".join(m.name for m in migrations)
        + f"): declare one of {choices}"
    )


def _omission(name: Any, omissions: MemberOmissions) -> str:
    """How many rows carry no value for the required member `name`."""
    verb = "carries" if omissions.omitting == 1 else "carry"
    examples = ", ".join(repr(label) for label in omissions.examples)
    return (
        f"{omissions.omitting} of them {verb} no value for the required "
        f"{name!r}, e.g. {examples}"
    )


def _record_aspects(migration: DataMigration) -> set[str]:
    """What a :data:`RECORD_UPDATE` declaration writes; all of it when unsaid."""
    return set(migration.updates or RECORD_ASPECTS)


def _record_attributes(aspects: set[str]) -> list[str]:
    """The record attributes the descriptive `aspects` cover."""
    attributes = ["description"] if "description" in aspects else []
    if "reference" in aspects:
        attributes.extend(("reference", "reference_type"))
    return attributes


def _comparable(value: Any, dtype: str = "") -> Any:
    """`value` as `get_values()` reports it for a feature of `dtype`.

    A single value of a `list[...]` feature is a list of one.
    """
    if isinstance(value, (list, tuple, set)):
        return [_comparable_item(item, dtype) for item in value]
    if dtype.startswith("list["):
        return [_comparable_item(value, dtype)]
    return _comparable_item(value, dtype)


def _comparable_item(value: Any, dtype: str) -> Any:
    """One value as `get_values()` reports it for a feature of `dtype`.

    A record by the field its categorical dtype matches labels on -- its name
    field when the dtype names none -- and a date or datetime given as ISO text
    parsed, the way lamindb reads it back from `RecordJson`.
    """
    if getattr(value, "_meta", None) is not None:
        components = DEFAULT_MODELS.parse_dtype(dtype) if "cat[" in dtype else None
        field_name = (components[0].get("field_str") if components else None) or (
            getattr(type(value), "_name_field", None)
        )
        return getattr(value, field_name, value) if field_name else value
    if dtype == "date" and isinstance(value, datetime.datetime):
        return value.date()
    if dtype in ("date", "datetime") and isinstance(value, str):
        try:
            parsed = datetime.datetime.fromisoformat(value)
        except ValueError:
            return value
        return parsed.date() if dtype == "date" else parsed
    return value


def _same_record_value(feature: Any, declared: Any, live: Any) -> bool:
    """Whether `live`, as `get_values()` reports it, is the declared value.

    The labels of a `list[cat[...]]` are link rows, which have no order.
    """
    dtype = getattr(feature, "dtype_as_str", None) or ""
    declared = _comparable(declared, dtype)
    if declared == live:
        return True
    if dtype.startswith("list[cat") and isinstance(live, (list, tuple, set)):
        return isinstance(declared, list) and sorted(map(str, declared)) == sorted(
            map(str, live)
        )
    return False


def _shown_value(live: Any) -> str:
    """A stored value for a change detail; `(unset)` when there is none."""
    return "(unset)" if live is None else repr(live)


def _registry_label(path: str) -> str:
    """A registry's import path as lamindb names it in an `itype`.

    `lamindb.models.feature.Feature` reads `Feature` and
    `bionty.models.Gene` reads `bionty.Gene`, the way
    `SQLRecord.__get_name_with_module__` spells them.  Anything that is not a
    dotted path ending in a class name is returned unchanged.
    """
    segments = path.split(".")
    if len(segments) < 2 or not segments[-1][:1].isupper():
        return path
    if segments[0] == "lamindb":
        return segments[-1]
    return f"{segments[0]}.{segments[-1]}"


def _config_value_label(name: str, value: Any) -> str:
    """A lock or plan configuration value spelled the way the diff spells it.

    The configuration records a dtype as written -- `builtins.str`, a
    registry's import path -- and an `itype` by its import path; the diff
    shows lamindb's strings (`'str'`, `'cat[Record]'`, `'Feature'`).
    """
    if isinstance(value, str):
        if name == "dtype":
            value = canonical_dtype_str(value)
        elif name == "itype":
            value = _registry_label(value)
    return repr(value)


def _refused_edit(name: str, before: Any, after: Any) -> str:
    """One refused configuration change, `dtype 'str' → 'int'`, like a diff detail."""
    return (
        f"{name} {_config_value_label(name, before)} \u2192 "
        f"{_config_value_label(name, after)}"
    )


def _plan_change_refusal(kind: str, name: Any, key: str, lines: list[str]) -> str:
    """The message of a :class:`PlanChangeRefusedError` for the refused `lines`."""
    what = kind if name is None else f"{kind} {name!r}"
    return f"cannot change {what} ({key}) in place:\n  " + "\n  ".join(lines)


# The writer omits these keys when a schema has no slots, optional members,
# or index. Reconciliation handles those additions against the live schema.
_OMITTED_WHEN_EMPTY = frozenset({"slots", "optionals", "index"})

# The plan configuration keys behind a refused property, where they are not the
# property's own name: a feature's dtype is its bare dtype and its filters.
_PLAN_CONFIG_KEYS: dict[str, tuple[str, ...]] = {"dtype": _FEATURE_DTYPE_KEYS}


@dataclass(frozen=True)
class _ChangeOrigin:
    """What a lock entry says about who moved a property plan and instance disagree on.

    `lock_config` and `stored` are the plan's configuration and the
    instance's snapshot, if captured at the last apply;
    `plan_config` and `current` are the plan's and the instance's now (see
    :func:`serialize_config` and :func:`serialize_existing`).  Without a lock
    entry there is no origin at all (`None` where one is taken).
    """

    lock_config: dict[str, Any]
    plan_config: dict[str, Any]
    stored: dict[str, Any] | None
    current: dict[str, Any]

    def is_drift(self, name: str) -> bool:
        """Whether the instance, not the plan, moved `name` since the last apply.

        It did when its snapshot says so, or when the plan still declares what
        the lock recorded for it: the divergence then arose on the instance.
        """
        stored = self.stored
        if (
            stored is not None
            and (name in stored or name not in _OMITTED_WHEN_EMPTY)
            and stored.get(name) != self.current.get(name)
        ):
            return True
        keys = _PLAN_CONFIG_KEYS.get(name, (name,))
        return all(self.lock_config.get(k) == self.plan_config.get(k) for k in keys)


@dataclass(frozen=True)
class _Refusal:
    """A change :meth:`ApplyContext._reconcile` will not make to the instance.

    `field` is the property it concerns, keyed as the lock's remote snapshot
    records it.  Whether the instance or the plan moved decides how it reads: as
    `drift` when the instance changed `field` outside the plan since the
    last apply, otherwise as `plan_change` -- the plan asking for a change the
    engine will not make, in the words :class:`PlanChangeRefusedError` uses.
    """

    field: str
    drift: str
    plan_change: str

    @classmethod
    def of_values(cls, field: str, live: Any, desired: Any, note: str = "") -> _Refusal:
        """Refuse changing `field` from `live` to `desired`; `note` follows.

        Both are the instance's own spelling -- a dtype with its filters
        inlined, a registry as lamindb names it -- so they are shown as they
        are, not canonicalized the way a lock's configuration value is.
        """
        return cls(
            field,
            drift=f"{field}: instance has {live!r}, plan wants {desired!r}{note}",
            plan_change=f"{field} {live!r} \u2192 {desired!r}{note}",
        )

    @classmethod
    def of_text(cls, field: str, text: str) -> _Refusal:
        """A refusal that names no value, reading the same either way."""
        return cls(field, drift=text, plan_change=text)


def _reconcile_refusal(
    kind: str,
    name: Any,
    key: str,
    refusals: Sequence[_Refusal],
    origin: _ChangeOrigin | None,
) -> DriftError:
    """The error refusing `refusals`, telling drift apart from plan changes.

    A refusal of a property the instance moved since the last apply (see
    :meth:`_ChangeOrigin.is_drift`) is drift; every other one -- and every one
    of an object no lock entry records yet (`origin` is `None`) -- is a
    plan change the engine will not make in place, so when there is no drift it
    is a :class:`PlanChangeRefusedError`.  When there is, the plan changes
    refused alongside it follow the drift.
    """

    def drifted(refusal: _Refusal) -> bool:
        return origin is not None and origin.is_drift(refusal.field)

    drifts = [r.drift for r in refusals if drifted(r)]
    refused = [r.plan_change for r in refusals if not drifted(r)]
    if not drifts:
        return PlanChangeRefusedError(_plan_change_refusal(kind, name, key, refused))
    message = (
        f"drift for {key!r}: the instance diverges from the plan in "
        "validation-affecting or structural properties that cannot be "
        "changed safely:\n  " + "\n  ".join(drifts)
    )
    if refused:
        message += "\n" + _plan_change_refusal(kind, name, key, refused)
    return DriftError(message)


def _describe_value_links(
    links: list[tuple[Any, int]], rewritten: dict[Any, int] | None = None
) -> str:
    """Which objects hold a record as their value, for a refusal.

    `rewritten` counts, per feature, the values a record update is yet to
    write as the record (see :meth:`ApplyContext._rewrites_naming`).
    """
    holders: dict[str, tuple[int, set[str]]] = {}
    for link, count in links:
        if not count:
            continue
        owner = link.subject.related_model.__name__ if link.subject else "?"
        features = {
            name for name in link.rows.values_list("feature__name", flat=True) if name
        }
        held, names = holders.get(owner, (0, set()))
        holders[owner] = (held + count, names | features)
    for feature, count in (rewritten or {}).items():
        held, names = holders.get("Record", (0, set()))
        holders["Record"] = (held + count, names | {feature.name})
    parts = []
    for owner, (count, features) in holders.items():
        on = (
            f" via {', '.join(repr(name) for name in sorted(features))}"
            if features
            else ""
        )
        parts.append(f"{owner}: {count}{on}")
    return "; ".join(parts)


def _describe_usage(usage: SchemaUsage) -> str:
    """What a schema validated, for a refusal or an audit note.

    Names the datasets and the sheets, so whoever reads the message knows which
    data the change has to be vouched for against.
    """
    parts: list[str] = []
    if usage.artifacts:
        parts.append(_plural(usage.artifacts, "artifact"))
    if usage.collections:
        parts.append(_plural(usage.collections, "collection"))
    text = " and ".join(parts)
    if usage.examples:
        shown = ", ".join(repr(label) for label in usage.examples)
        more = (
            ", ..." if usage.artifacts + usage.collections > len(usage.examples) else ""
        )
        text += f" ({shown}{more})"
    if usage.rows:
        sheets = ", ".join(repr(name) for name in usage.sheets)
        rows = f"{_plural(usage.rows, 'row')} of sheet {sheets}"
        text = f"{text} and {rows}" if text else rows
    return text


def _index_label(feature: Any) -> str | None:
    """A schema index as a drift message shows it: the feature's name, or `None`."""
    if feature is None:
        return None
    return getattr(feature, "name", None) or str(getattr(feature, "uid", "?"))


@dataclass
class _Detachment:
    """A schema outside the plan that `detach_from_schemas` takes members out of.

    `features` pairs each deleted feature it loses with the declaration
    detaching it.  They leave in a single save, so only the content it ends up
    with has to be free.  `key` is its natural key, `content_hash` the hash
    that save stores (`None` when lamindb cannot compute it), `composites`
    the hash each composite slotting it takes once rehashed to match, by primary
    key, and `obj_id` the deletion change of its first feature, which reports
    what saving it did (`saved`).
    """

    schema: Any
    key: str
    obj_id: int
    features: list[tuple[Any, DataMigration]] = field(default_factory=list)
    content_hash: str | None = None
    composites: dict[Any, tuple[Any, str]] = field(default_factory=dict)
    saved: list[str] = field(default_factory=list)

    def details_for(self, feature: Any) -> list[str]:
        """The details of `feature`'s deletion change, naming this schema."""
        label = getattr(self.schema, "name", None) or str(self.schema.uid)
        details = [
            f"detached from schema {label!r} (uid {self.schema.uid}) via "
            f"{migration.name} ({migration.reason})"
            for member, migration in self.features
            if member.pk == feature.pk
        ]
        if details and self.features[0][0].pk == feature.pk:
            details.extend(self.saved)
        return details


@dataclass
class _SchemaMerge:
    """A live schema a declared :func:`~lamindb_setup.evolve.merge_schema` takes over.

    The merge brackets the plan schema's save: :meth:`trash` before it, so the
    holder no longer holds the content hash the save takes, and :meth:`repoint`
    after it.
    """

    holder: Any
    migration: DataMigration

    def trash(self) -> None:
        """Move the holder to the trash with its own content intact.

        lamindb ignores a trashed schema's hash, so the plan schema can be saved
        onto it.  Its member and slot links stay, so the save that trashes it
        does not rehash it: in the trash it is the schema it was.
        """
        from ._trash import delete_object

        delete_object(type(self.holder).objects.get(pk=self.holder.pk))

    def repoint(self, schema: Any) -> None:
        """Point every reference to the holder at the just-saved `schema`.

        Only what references the holder moves, not its own member and slot
        links.  Moving its sheets after the save keeps them out of lamindb's
        re-keying of a changed index, which reaches every sheet the saved
        schema has: the holder's rows are keyed by the new index already.
        """
        from ._repoint import repoint_references
        from ._schema_collisions import own_content

        model = type(self.holder)
        repoint_references(
            model, self.holder.pk, schema.pk, error=DriftError, keep=own_content(model)
        )

    def detail(self) -> str:
        """The change detail naming the merged schema and the declaration's reason."""
        label = getattr(self.holder, "name", None) or str(self.holder.uid)
        return (
            f"merged schema {label!r} (uid {self.holder.uid}) into it, re-pointing "
            f"what referenced it, via {self.migration.name} ({self.migration.reason})"
        )


def _predicted_composite_hashes(
    schema: Any, content_hash: str
) -> dict[Any, tuple[Any, str]]:
    """The hash each composite slotting `schema` takes once it has `content_hash`.

    The prediction of :meth:`ApplyContext._rehash_composites`, transitively: a
    component's predicted hash stands in for its stored one, which is all
    lamindb hashes a slot by.  Keyed by primary key, holding the composite and
    its hash; a composite whose hash does not change is left out.
    """
    from ._drift_data import _schema_closure
    from ._inferred_schemas import _content_hash, _members

    composites = [c for c in _schema_closure(schema)[1:] if is_live(c)]
    hashes = {schema.pk: content_hash}
    for _ in range(len(composites)):
        changed = False
        for composite in composites:
            slots = {
                slot: SimpleNamespace(hash=hashes[c.pk]) if c.pk in hashes else c
                for slot, c in _live_slots(composite).items()
            }
            value = _content_hash(composite, _members(composite), set(), slots=slots)
            if value is not None and value != hashes.get(composite.pk, composite.hash):
                hashes[composite.pk] = value
                changed = True
        if not changed:
            break
    return {c.pk: (c, hashes[c.pk]) for c in composites if c.pk in hashes}


def _schema_key(schema: Any) -> str | None:
    """The natural key a plan gives the live `schema`; `None` when unnamed."""
    name = getattr(schema, "name", None)
    if not name:
        return None
    parts = [f"Schema:{name}"]
    parent = getattr(schema, "type", None)
    while parent is not None:
        parts.insert(0, f"SchemaType:{parent.name}")
        parent = getattr(parent, "type", None)
    return "/".join(parts)


def _names_schema(target: str | None, schema: Any, *, exactly: bool = False) -> bool:
    """Whether `target` -- a uid or a natural key -- names the live `schema`.

    A bare `Schema:<name>` names it under any schema type too, unless
    `exactly`.
    """
    if not target:
        return False
    if target == str(getattr(schema, "uid", "")):
        return True
    key = _schema_key(schema)
    if key is None:
        return False
    return target == key or (not exactly and target == f"Schema:{schema.name}")


def _composite_collision(composite: Any, component: Any) -> str | None:
    """The refusal of rehashing `composite` after its `component` changed.

    lamindb refused the save, which it does when another live schema already
    holds the hash the composite would take.  That schema is named by looking
    the hash up, recomputed from the composite's live content as the save does.
    `None` when no live schema holds it: the refusal is lamindb's to report.
    """
    from ._schema_collisions import find_collisions

    index = composite.index
    content_hash = DEFAULT_MODELS.schema_content_hash(
        composite,
        features=[m for m in composite.members if index is None or m.pk != index.pk],
        index=index,
        optionals=composite.optionals.get(),
        slots=composite.slots,
        flexible=composite.flexible,
        coerce=composite.coerce,
        minimal_set=composite.minimal_set,
        ordered_set=composite.ordered_set,
        maximal_set=composite.maximal_set,
    )
    if content_hash is None:
        return None
    holders = find_collisions(
        type(composite), {composite.pk: content_hash}, using=composite._state.db
    )[composite.pk]
    if not holders:
        return None
    others = ", ".join(
        f"schema {holder.name or holder.uid!r} (uid {holder.uid})" for holder in holders
    )
    return (
        f"cannot rehash composite {composite.name or composite.uid!r} (uid "
        f"{composite.uid}) after its component {component.name or component.uid!r} "
        f"changed: it would take the same content as {others}, and lamindb keeps "
        "a single schema per content hash.  Resolve the other schema first -- "
        "point what references it at this one and trash it -- or change the plan "
        "so the two differ."
    )


@dataclass
class _SlotChange:
    """The slot edits :meth:`ApplyContext._reconcile` makes to a composite schema."""

    removed: list[str]
    added: list[str]
    swapped: list[str]
    desired: dict[str, Any]
    details: list[str]


def _live_slots(existing: Any) -> dict[str, Any]:
    """A composite schema's `{slot: component}`, read from its link rows.

    Not through `Schema.slots`, which caches the dict on the record for
    lamindb's `save` to write back -- stale once the links are edited.
    """
    through = type(existing).components.through
    links = through.objects.filter(composite_id=existing.pk).select_related("component")
    return {link.slot: link.component for link in links}


def _write_slots(existing: Any, change: _SlotChange) -> None:
    """Edit a composite's slot links; the save that follows rehashes it."""
    through = type(existing).components.through
    gone = change.removed + change.swapped
    if gone:
        through.objects.filter(composite_id=existing.pk, slot__in=gone).delete()
    through.objects.bulk_create(
        [
            through(
                composite_id=existing.pk,
                component_id=change.desired[slot].pk,
                slot=slot,
            )
            for slot in change.added + change.swapped
        ]
    )
    if hasattr(existing, "_slots"):
        delattr(existing, "_slots")


@dataclass(frozen=True)
class _ComponentShape:
    """What a slot component checks of a dataset, as :func:`_component_widens` reads it.

    `members` (in order, the index first), `optionals` and `index` name a
    feature by its live uid -- or, for one the plan has not created yet, by its
    natural key, which no live uid matches.  A planned `dtype` is
    :data:`_UNRESOLVED` when it cannot be known before the component exists.
    """

    members: tuple[str, ...]
    optionals: frozenset[str]
    index: str | None
    maximal_set: bool
    minimal_set: bool
    ordered_set: bool
    flexible: bool
    otype: str | None
    itype: str | None
    dtype: Any


def _component_widens(old: _ComponentShape, new: _ComponentShape) -> bool:
    """Whether every dataset `old` validated also validates against `new`.

    True when `new` keeps every member of `old` -- in their order, when it is
    `ordered_set` -- adds only optional ones, keeps every optional one
    optional, sets no flag `old` leaves off, does not turn `flexible` off
    under `maximal_set` (see :func:`~lamindb_setup.evolve.engine._drift.classify`),
    and has the same index, `otype`, `itype` and `dtype` -- that is, when
    :func:`_shape_shortfalls` finds nothing.
    """
    return not _shape_shortfalls(old, new)


def _shape_shortfalls(
    old: _ComponentShape,
    new: _ComponentShape,
    name_of: Callable[[str], str] = repr,
) -> list[str]:
    """What `new` no longer admits of the datasets `old` validated, said plainly.

    Each failure of the rule :func:`_component_widens` applies, in its order;
    `name_of` names a member by the uid -- or natural key -- a shape holds.
    """
    found: list[str] = []
    old_members = set(old.members)
    new_members = set(new.members)

    def listed(ids: Iterable[str]) -> str:
        return ", ".join(name_of(i) for i in ids)

    if missing := [m for m in old.members if m not in new_members]:
        found.append(f"it does not declare {listed(missing)}")
    if added := [
        m for m in new.members if m not in old_members and m not in new.optionals
    ]:
        found.append(f"it requires {listed(added)}, which the other did not declare")
    if narrowed := sorted(old.optionals - new.optionals):
        found.append(f"it does not leave {listed(narrowed)} optional")
    if new.ordered_set and [m for m in new.members if m in old_members] != list(
        old.members
    ):
        found.append("it orders the members differently")
    for flag in ("maximal_set", "minimal_set", "ordered_set", "flexible"):
        if getattr(new, flag) and not getattr(old, flag):
            found.append(f"it turns {flag} on")
    flexible = classify(
        "Schema",
        "flexible",
        old.flexible,
        new.flexible,
        context={"maximal_set": new.maximal_set},
    )
    if flexible is Verdict.BLOCKED:
        found.append("it turns flexible off under maximal_set")
    if old.index != new.index:
        before = "no index" if old.index is None else name_of(old.index)
        after = "no index" if new.index is None else name_of(new.index)
        found.append(f"its index is {after}, not {before}")
    for attr in ("otype", "itype"):
        if getattr(old, attr) != getattr(new, attr):
            found.append(
                f"its {attr} is {getattr(new, attr)!r}, not {getattr(old, attr)!r}"
            )
    if new.dtype is _UNRESOLVED:
        found.append("its dtype is not known before the apply")
    elif (
        (old.dtype is None or new.dtype is None)
        and old.dtype is not new.dtype
        or (
            old.dtype is not None
            and new.dtype is not None
            and not dtypes_match(old.dtype, new.dtype)
        )
    ):
        found.append(f"its dtype is {new.dtype!r}, not {old.dtype!r}")
    return found


def _live_ancestors(record: Any) -> list[Any]:
    """The types above `record` on the instance, its parent first."""
    ancestors: list[Any] = []
    seen: set[Any] = set()
    parent = getattr(record, "type", None)
    while parent is not None and parent.pk not in seen:
        seen.add(parent.pk)
        ancestors.append(parent)
        parent = getattr(parent, "type", None)
    return ancestors


def _admits_below(feature: Any, registry: Any, staying: set[str]) -> bool:
    """Whether `feature` admits `registry` records below every type in `staying`.

    True when a component of its dtype names the registry unfiltered, or a type
    among `staying` with no further filter -- what a moved record type keeps
    sitting below.  An unreadable dtype admits nothing, so the caller counts.
    """
    for component in DEFAULT_MODELS.parse_dtype(feature.dtype_as_str) or []:
        if component.get("registry") is not registry or component.get("filter_str"):
            continue
        type_uid = component.get("type_uid")
        if not type_uid or str(type_uid) in staying:
            return True
    return False


def _schema_label(schema: Any) -> str | None:
    """A record's schema as a drift message shows it: its name, its uid, or `None`."""
    if schema is None:
        return None
    return getattr(schema, "name", None) or str(getattr(schema, "uid", "?"))


def _declared_members(obj: Any) -> list[Any]:
    """A plan schema's members in the order LaminDB keeps them: the index first.

    LaminDB makes the index a member wherever the plan lists it --
    `Schema.__init__` inserts it at the front -- and the plan may name it only
    as `index=`, so its `features` alone are not the member list.
    """
    declared = list(getattr(obj, "features", []))
    index = getattr(obj, "index", None)
    if index is not None:
        declared = [index, *(f for f in declared if f is not index)]
    return declared


@dataclass
class _IndexChange:
    """A schema index the plan wants different from the instance's.

    `previous` is the live feature the schema is indexed by now (`None` when
    it has no index), `desired` the plan's index feature (`None` to remove it)
    and `live` that feature's record, `None` when this apply has not created
    it yet -- which only a PLAN-mode preview can see.
    """

    previous: Any
    desired: Any
    live: Any

    @property
    def before(self) -> str | None:
        return _index_label(self.previous)

    @property
    def after(self) -> str | None:
        return _index_label(self.desired)

    def describe(self) -> str:
        """`index change (no index → 'barcode')`, for an export's staleness."""
        before = "no index" if self.before is None else repr(self.before)
        after = "no index" if self.after is None else repr(self.after)
        return f"index change ({before} \u2192 {after})"


class DriftError(LaminSchemasError):
    """Raised when the lock state, the plan, and the instance disagree."""


class PlanChangeRefusedError(DriftError):
    """Raised when the plan changes a property that cannot be edited in place.

    Not drift: nothing changed on the instance outside the plan, it is the plan
    asking for a change the engine will not make there.  A :class:`DriftError`
    all the same, so every `except DriftError` -- PLAN mode collecting the
    refusals of a diff among them -- still catches it.
    """


class SpaceNotFoundError(LaminSchemasError):
    """Raised when an object references a space uid that does not exist."""


class MissingPrerequisiteError(LaminSchemasError):
    """Raised when an object a prerequisite plan owns is absent from the instance.

    Applying a plan before the plan it `depends_on` leaves the borrowed objects
    unmaterialized.  Creating them here would be the wrong repair: the owning
    plan may be about to *rename* one, and an object created under its new name
    is precisely the collision that then makes that rename impossible.

    `apply_one` refuses such an apply from its preview already (see
    :attr:`ApplyContext.missing_prerequisites`), before it tracks a run; this is
    the APPLY pass's own guard, which a `plan --preview-migrations` rehearsal
    still reports.
    """


class _MigrationDryRunRollback(Exception):
    """Internal sentinel used to unwind a PLAN-mode migration dry-run savepoint.

    Raised at the end of :meth:`ApplyContext._preview_pending_migrations` so the
    enclosing `transaction.atomic()` rolls back, guaranteeing the dry-run
    persists nothing.  Caught and swallowed by that method; never escapes.
    """


@dataclass
class _PendingMigration:
    """A migration :meth:`ApplyContext._reconcile` resolved but has not yet run.

    Carries everything the migration needs that only the *plan* side knows:
    `desired_dtype` with any `cat_filters` already inlined, plus the live
    registry class and filters resolved from the plan feature's declared
    :class:`~lamindb_setup.evolve.LaminDType` -- the dtype *string* alone
    cannot be resolved back to a registry class.
    """

    migration: DataMigration
    desired_dtype: str
    live_dtype: str | None
    target_registry: Any = None
    cat_filters: dict[str, Any] | None = None


@dataclass
class _DeferredMigration:
    """A feature's resolved migrations, held back until every object exists.

    A migration only ever maps values *onto* records it does not create, so those
    records must already be there when it runs.  Running it inside
    :meth:`ApplyContext._reconcile` forced that ordering onto the *feature* --
    the feature had to be created after every record the migration targets --
    which is both the wrong object to constrain and a genuine cycle for a
    self-referential ontology (a Record annotated with the very feature whose
    values are being migrated onto Records of its own type).

    Deferring execution to :meth:`ApplyContext.run_deferred_migrations`, after the
    whole plan is materialized, removes the constraint entirely: nothing about
    the feature's creation order matters any more.  The dtype flip stays in
    `_reconcile` (it is an ordinary `safe_updates` write), so plan-declared
    records annotated with the feature are written against the *new* dtype and
    the deferred migration only relocates the leftover stale rows.

    `obj_id` locates the feature's :class:`ChangeItem` so the migration report
    still lands in the rendered diff; `pending` carries the field updates the
    PLAN-mode dry-run has to apply itself (APPLY has already saved them).
    """

    obj_id: int
    key: str
    existing: Any
    pending: list[tuple[str, Any]]
    migrations: list[_PendingMigration]


@dataclass
class _DeferredMemberMigration:
    """A backfill that lets a schema make optional members required again.

    Declared with :meth:`ctx.migrate <lamindb_setup.evolve.PlanContext.migrate>`
    on the *schema* rather than on a feature: the change it unblocks is not a dtype
    flip but a narrowing of the schema's optional set, and what has to be fixed up
    is the rows that omitted the member, not a feature's stored values.

    Held back for the same reason a dtype migration is -- the rows it backfills
    may be created by this very plan, and the values it writes may reference
    records the plan declares -- so it runs once everything is materialized.  The
    optional set itself is already narrowed by :meth:`_reconcile`; only the data
    fixup is deferred, and a row still omitting the member afterwards fails the
    apply (see :meth:`run_deferred_migrations`).

    `migrations` is every migration the plan declared for the schema, in
    declaration order.  They are not matched to individual `features`: one may
    cover all of them or each may cover one, and only the post-check decides
    whether they were enough between them.

    `registries` scopes the rows to backfill and re-count to the named row
    registries.  A member the plan *adds* backfills the rows of a sheet alone
    (`"Record"`): the datasets validated without it are vouched for instead
    (see :meth:`ApplyContext._vet_member_adds`).  `None` covers every registry.
    """

    obj_id: int
    key: str
    schema: Any
    features: list[Any]
    migrations: list[DataMigration]
    registries: frozenset[str] | None = None
    # What the backfilled members became, for the failure message.
    purpose: str = "members the plan makes required"


@dataclass
class _NullabilityUsage:
    """What turning a feature's `nullable` off answers to.

    `omissions` pairs every live schema declaring the feature that has sheet
    rows with how many of them omit it -- a row omitting a member is null for it
    -- and `datasets` sums the artifacts and collections validated against those
    schemas, whose cells may have held nulls and cannot be re-checked in place.
    """

    omissions: list[tuple[Any, MemberOmissions]]
    datasets: SchemaUsage

    @property
    def omitting(self) -> int:
        """Rows that omit the feature, across every schema declaring it."""
        return sum(found.omitting for _, found in self.omissions)


# The rows a member backfill declared as deliberately null, keyed by the member's
# primary key and then by the row registry's name -- the shape
# :func:`~lamindb_setup.evolve.engine._drift_data.schema_member_omissions` excludes them
# in.  lamindb has no stored null, so a null-filled row stays un-annotated and
# the post-check has to be told it was answered for rather than skipped.
_AcceptedNulls = dict[Any, dict[str, set[Any]]]


@dataclass
class _DeferredMemberRemoval:
    """The data cleanup that lets a schema drop a member whose values are stored.

    Declared with :meth:`ctx.migrate <lamindb_setup.evolve.PlanContext.migrate>`
    on the *schema*, not on the feature: the feature usually survives the edit --
    only its membership ends -- and it may be dropped from the plan entirely, in
    which case there is no plan object left to attach anything to.  Naming the
    member by string on the schema addresses both.

    Deferred like the others, and for the extra reason that
    :meth:`apply_removals` runs *after* :meth:`run_deferred_migrations`: the
    feature whose values this clears is still live while it does so, even when
    the same apply is about to delete it.

    The member is already unlinked by :meth:`_reconcile`; only the data cleanup
    is deferred, and a row still carrying a value afterwards fails the apply
    unless the migration declared it kept (see
    :meth:`~lamindb_setup.evolve.authoring._migrations.MemberRemovalContext.keep`).
    """

    obj_id: int
    key: str
    schema: Any
    features: list[Any]
    migrations: list[DataMigration]


@dataclass
class _DeferredSheetExports:
    """The sheet exports a change invalidated, and what answers for them.

    A schema change invalidates the exports of the sheets it constrains, and a
    sheet pointed at another schema its own (see :class:`_SheetRepoint`).

    The only deferral that outlives the apply transaction.  An export is a CSV
    already written to storage (see :mod:`lamindb_setup.evolve.engine._sheet_exports`),
    so the new version answering for it cannot be written inside a transaction
    that may still roll back -- the file would outlive the rollback either way.
    It is therefore handed back to the caller, which runs it after the commit and
    onto a dedicated branch, merged only once every one of them succeeded (see
    :attr:`ApplyContext.pending_sheet_exports` and
    :func:`run_sheet_export_migrations`).

    `exports` are the invalidated snapshots, each naming its sheet, the head of
    its version family and why it no longer adheres.  A declaration that only
    *keeps* them (:func:`~lamindb_setup.evolve.keep_stale_exports`)
    still runs, and writes nothing.
    """

    obj_id: int
    key: str
    schema: Any
    exports: list[Any]
    migrations: list[DataMigration]


@dataclass
class _DeferredIndexChange:
    """The declarations that accepted a schema's index change, run once it is made.

    Nothing has to be rewritten -- the index itself was already switched by
    :meth:`ApplyContext._reconcile`, and lamindb re-keyed the sheet rows as it
    saved -- so this only exists to run the declared callables like every other
    narrowing's, with an
    :class:`~lamindb_setup.evolve.authoring._migrations.IndexChangeContext`, after
    the plan is materialized.  APPLY mode only: a preview has no index to hand
    them.
    """

    obj_id: int
    key: str
    schema: Any
    previous: Any
    index: Any
    migrations: list[DataMigration]


@dataclass
class _DeferredRecordUpdate:
    """The updates declared on a seeded record, run once the plan is materialized.

    A value may name a record the plan declares after this one, so the writes
    wait for every object to exist, like the other deferred migrations.  Each
    declaration comes with the :class:`RecordUpdateContext` holding the
    differences it writes.
    """

    obj_id: int
    updates: list[tuple[DataMigration, RecordUpdateContext]]


@dataclass
class _DeferredFlexibleChange:
    """The declarations answering for a schema turning `flexible` off.

    The flag itself is switched by :meth:`ApplyContext._reconcile`; what is left
    is whatever the flexible match annotated while it was on, which the declared
    callables keep (:func:`~lamindb_setup.evolve.accept_flexible_change`) or
    remove (:func:`~lamindb_setup.evolve.drop_nonmember_annotations`) once the
    plan is materialized, each with an
    :class:`~lamindb_setup.evolve.authoring._migrations.FlexibleChangeContext`.
    """

    obj_id: int
    key: str
    schema: Any
    previous: Any
    flexible: Any
    migrations: list[DataMigration]


@dataclass
class _SchemaShape:
    """The shape a reconciled schema has *after* the change, for the export sweep.

    Recorded per reconciled schema so :meth:`ApplyContext.check_sheet_exports`
    can compare an exported file's columns against the schema as the plan leaves
    it -- in PLAN mode, where nothing was written, as much as in APPLY mode.

    `member_ids` and `required_ids` are the ids of the members that survive
    the change and of the subset a validated dataset must carry, `previous_ids`
    the members the schema had *before* it -- the stand-in for what an exported
    file holds when its recorded columns are not independent evidence (see
    :func:`~lamindb_setup.evolve.engine._sheet_exports.export_columns`).
    `added_required` names the required members the plan adds and
    `newly_required` those that stop being optional -- or become required with
    `minimal_set` turned on; neither has an id to compare against.
    `index_change` describes an index this apply adds, changes or removes,
    which re-lays out every export.  `structural` records whether this apply
    touched the member or optional sets or the index at all, or turned a set
    flag on, which is what keeps the sweep from re-judging a schema it did not
    change.
    """

    obj_id: int
    key: str
    schema: Any
    member_ids: set[Any]
    previous_ids: set[Any]
    required_ids: set[Any]
    added_required: list[str]
    newly_required: list[str]
    maximal: bool
    structural: bool
    index_change: str | None = None


@dataclass
class _SheetRepoint:
    """A sheet this apply points at another schema, for the export sweep.

    Its exports were written against `previous` -- `None` for a record type
    promoted to a sheet -- and every one of them is invalidated by pointing it at
    `schema`, the live schema or, in PLAN mode, a placeholder for one the
    apply creates.  The sweep has to be told: the exports name `previous`,
    which this apply did not change, and once re-pointed the sheet no longer
    pairs with them through its schema (see
    :meth:`ApplyContext.check_sheet_exports`).

    Recorded with `repointed` unset for a sheet that is not re-pointed but
    declares what becomes of its exports, so a retry after a failed re-export
    still finds the ones written against an earlier schema -- and for a sheet
    `move_rows` moves `gained` rows into from `source`, which makes every
    export of it miss them.
    """

    obj_id: int
    key: str
    sheet: Any
    previous: Any
    schema: Any
    repointed: bool
    gained: int = 0
    source: str = ""


@dataclass(frozen=True)
class _HashCheck:
    """A schema a PLAN-mode reconcile would rehash, for the collision sweep.

    `content_hash` is the hash the change would store (see
    :meth:`ApplyContext._predicted_schema_hash`).  In APPLY mode the collision is
    handled at save time instead, where an absorbed schema has to be gone before
    the save; PLAN mode saves nothing, so it batches the lookup at the end (see
    :meth:`ApplyContext.check_schema_collisions`).
    """

    obj_id: int
    key: str
    schema: Any
    content_hash: str


# Maximum number of rows fetched per database round-trip during prefetch.  Reads
# are paginated in chunks of this size so a single query never materialises an
# unbounded result set (or builds an unbounded `IN (...)` clause some backends
# reject).  Mirrors the introspect reader's batch size.
_PREFETCH_BATCH_SIZE = 10_000

# Plan object kinds whose backing LaminDB record carries `is_type=True`.
_IS_TYPE_KINDS: frozenset[str] = frozenset(
    {"FeatureType", "SchemaType", "RecordType", "Sheet"}
)

# Plan object kinds whose backing registry is `ln.Record` (which has a
# `schema` foreign key read by Sheet snapshots).
_RECORD_KINDS: frozenset[str] = frozenset({"RecordType", "Sheet", "Record"})


@dataclass
class _CreationPlan:
    """How an object missing from the instance is to be created.

    `uid` is the uid to create it with (`None`: a fresh one), and `holder`
    a trashed record currently holding that uid, to be moved to a fresh uid of
    its own first.  `misalignment` is set when no uid could be pinned; the
    object is then either refused or created with a fresh uid, as
    :attr:`ApplyContext.strict_uids` says.  See
    :meth:`ApplyContext._creation_plan`.
    """

    uid: str | None = None
    holder: Any = None
    misalignment: UidMisalignment | None = None
    details: list[str] = field(default_factory=list)
    warnings: list[ChangeWarning] = field(default_factory=list)


def _describe_record(record: Any) -> str:
    """Name a live record for a message: its kind, name and uid."""
    kind = type(record).__name__
    if getattr(record, "is_type", False):
        kind += " type"
    name = getattr(record, "name", None)
    label = f" {name!r}" if name is not None else ""
    return f"{kind}{label} (uid {record.uid})"


def _chunked(items: Sequence[Any], size: int) -> Iterable[list[Any]]:
    """Yield successive `size`-length chunks of `items`."""
    for start in range(0, len(items), size):
        yield list(items[start : start + size])


def _keyset_pages(qs: Any, size: int) -> Iterable[Any]:
    """Yield rows of `qs` (ordered by `uid`) in keyset-paginated pages.

    Pages with `uid > last_uid` plus a `LIMIT` rather than
    :meth:`QuerySet.iterator`: lamindb wraps every statement to inject an auth
    token, which is incompatible with the server-side (named) cursors that
    `iterator(chunk_size=...)` opens on PostgreSQL.
    """
    last_uid: Any = None
    while True:
        page = qs if last_uid is None else qs.filter(uid__gt=last_uid)
        chunk = list(page[:size])
        if not chunk:
            return
        yield from chunk
        if len(chunk) < size:
            return
        last_uid = chunk[-1].uid


def _schema_member_link(schema: Any) -> tuple[Any, Any, str] | None:
    """Return `(through_model, member_model, member_fk_name)` for `schema`.

    Mirrors how :attr:`lamindb.Schema.members` picks the link table for a
    schema's `itype`: the related manager named by lamindb's
    `_get_related_name` (defaulting to `features`), its `through` model,
    and the one foreign key on that model that is not the schema side.

    Returns `None` for a schema whose shape this cannot resolve -- an `itype`
    with no related manager, or a link table without a second foreign key -- so
    the caller can leave it to lamindb's own property.
    """
    from django.db import models

    try:
        manager = getattr(schema, schema._get_related_name() or "features")
        through = manager.through
        fk_name = next(
            field.name
            for field in through._meta.fields
            if isinstance(field, models.ForeignKey) and field.name != "schema"
        )
    except (AttributeError, StopIteration):
        return None
    return through, manager.model, fk_name


class ExistenceCache:
    """Existence lookups shared across passes and plans over one instance.

    Every "does this object already exist?" answer a run needs goes through
    here, keyed by `id(obj)`.  Sharing it is what lets the apply pass reuse
    the preview's reads, and what lets the plans of one instance share a single
    batched prefetch instead of each running its own.

    The counters make that sharing observable rather than merely intended:
    `queries` counts objects resolved by a per-object `obj.get` round trip,
    `prefilled` those supplied by a batched prefetch, and `hits` those
    served from an answer already held.  A run whose `queries` grows with the
    number of objects has lost the batching this exists for.
    """

    def __init__(self, entries: dict[int, Any] | None = None) -> None:
        self._entries: dict[int, Any] = {} if entries is None else entries
        self.queries = 0
        self.prefilled = 0
        self.hits = 0

    def __contains__(self, key: int) -> bool:
        return key in self._entries

    def __len__(self) -> int:
        return len(self._entries)

    def get(self, key: int, default: Any = None) -> Any:
        """The cached answer for `key`, or `default` when it is not held."""
        return self._entries.get(key, default)

    def values(self) -> Iterable[Any]:
        """Every answer held, resolved records and `None`s alike."""
        return self._entries.values()

    def copy(self) -> ExistenceCache:
        """An independent cache holding the same answers, with fresh counters.

        Lets a caller run a speculative resolution (a read-only "what would this
        plan see?") without the entries it adds leaking back into the shared one.
        """
        return ExistenceCache(dict(self._entries))

    def prefill(self, key: int, existing: Any) -> None:
        """Record an answer a batched prefetch resolved."""
        self._entries[key] = existing
        self.prefilled += 1

    def resolve(self, obj: Materializable, ctx: ApplyContext) -> Any | None:
        """The existing record for `obj`, querying the instance only if needed.

        `obj.get` is a read-only existence query.  Its result is cached so a
        later pass over the same finalized plan (e.g. apply after preview) does
        not re-query.  A `None` result is cached too: an object absent during
        preview must still be created during apply.
        """
        key = id(obj)
        if key in self._entries:
            self.hits += 1
            return self._entries[key]
        existing = obj.get(ctx)
        self._entries[key] = existing
        self.queries += 1
        return existing

    def forget_missing(self) -> None:
        """Drop the "not found" entries from a cache shared across applies.

        :func:`prefetch_plans` records an object the instance does not have as
        `None`, and applying a plan is exactly what can make that answer
        stale: the objects a plan creates are the ones it looked up and did not
        find, and a sibling declaring one of them would otherwise still be told
        it is missing and create it a second time.  Dropping those entries after
        each plan makes the next plan's :meth:`ApplyContext.prefetch` re-read
        them -- one batched query, and none at all once nothing is missing.

        Entries that *did* resolve are left alone: every plan on the instance
        was prefetched together, so they all hold the same record instance, and
        an edit one plan makes is already visible to the rest.
        """
        for key in [key for key, value in self._entries.items() if value is None]:
            del self._entries[key]


class ApplyContext:
    """Resolve plan objects against an instance, tracking changes and lock state."""

    def __init__(
        self,
        *,
        mode: ApplyMode = ApplyMode.APPLY,
        lock: LockFile | None = None,
        default_space_uid: SpaceUID | None = None,
        existing_cache: ExistenceCache | None = None,
        members_cache: dict[int, list[Any]] | None = None,
        exports_cache: dict[str, Any] | None = None,
        plan_name: str = "",
        claimed_keys: set[str] | None = None,
        claimed_by: dict[str, str] | None = None,
        moved_keys: set[str] | None = None,
        borrowed_ids: set[int] | None = None,
        borrowed_keys: set[str] | None = None,
        borrowed_owners: Mapping[int, PrerequisitePlan] | None = None,
        co_applied_keys: set[str] | None = None,
        allow_data_validated_changes: bool = False,
        migrations: dict[str, list[DataMigration]] | None = None,
        preview_migrations: bool = False,
        sibling_uids: SiblingUids | None = None,
        strict_uids: bool = True,
    ) -> None:
        self.mode = mode
        self.lock = lock
        self.default_space_uid = default_space_uid
        # The uids the plan's lock files for *other* instances record (see
        # :mod:`._alignment`).  An object this instance does not have yet is
        # created with the uid they agree on, and is looked up by it before its
        # name; an object this instance has under another uid is a misalignment
        # (see :attr:`ChangeItem.uid_misalignment`).  `None` pins nothing,
        # which is what a lone instance -- or a caller with no plan file -- gets.
        self.sibling_uids = sibling_uids
        # Whether a misalignment refuses the apply (`--strict-uids`, default).
        # Off, an object whose uid cannot be pinned is created with a fresh one
        # and the misalignment is reported as a warning instead.
        self.strict_uids = strict_uids
        # The uid each seed-only object resolved to, written to the lock's
        # identity-only `seeded` section (see :attr:`LockFile.seeded`).
        self.seeded: dict[str, str] = {}
        # The uid :meth:`creation_uid` hands :meth:`Materializable.commit` for the
        # object about to be created, keyed by `id(obj)`.
        self._creation_uids: dict[int, str] = {}
        # Opt-in (`--allow-data-validated-changes`): consult the live instance
        # before refusing a blocked validation-affecting change, allowing it when
        # the feature has no stored values or every value validates against the
        # new dtype (see :meth:`_data_override_reason`).  Off by default so the
        # taxonomy stays a pure, data-independent hard refusal.
        self.allow_data_validated_changes = allow_data_validated_changes
        # Narrowing data migrations declared with `ctx.migrate`, keyed by the
        # natural key of the object they migrate.  When apply reaches a change
        # that the data would otherwise refuse, the registered migrations fix the
        # data up so it is no longer refused (see :meth:`_migrations_for` and
        # :class:`._migrations.DataMigration`).  A list per key: one object may
        # need several, e.g. a schema making two members required at once, where
        # each member gets its own migration.
        self.migrations: dict[str, list[DataMigration]] = migrations or {}
        # Opt-in (`--preview-migrations`): in PLAN mode, actually run each
        # declared migration inside a rolled-back savepoint so the preview can
        # *prove* it relocates every value (rather than only describing intent).
        # Off by default so a plain `plan` stays a pure read-only diff that
        # never executes user migration code.  Ignored in APPLY mode, which runs
        # migrations for real.
        self.preview_migrations = preview_migrations
        # Natural keys produced by sibling plans managed against the same
        # instance.  An object this plan no longer declares is *not* deleted
        # when a sibling still claims its key: ownership simply transfers to the
        # sibling (the key drops out of this plan's rebuilt lock).  Empty for a
        # standalone single-plan apply, which has no notion of siblings.
        self.claimed_keys: set[str] = claimed_keys or set()
        # Optional mapping of a claimed natural key to the sibling plan that now
        # owns it, used only to name the destination in a `TRANSFER` change.
        self.claimed_by: dict[str, str] = claimed_by or {}
        # New natural keys that were reached by re-keying a lock entry from an
        # old address declared via `ctx.moved` (see `_apply_moves_to_lock`).
        # Resolving one of these keys is allowed to change the live object's
        # name and is reported as an `Action.MOVE` rather than delete+create.
        self.moved_keys: set[str] = moved_keys or set()
        # Objects a plan in `depends_on` declares and this plan merely
        # references (see
        # :attr:`~lamindb_setup.evolve.Plan.inherited_dependencies`).  They
        # are resolved but not owned: adopted rather than reconciled, and left
        # out of this plan's lock (see :meth:`_borrow`).  Keyed by `id(obj)`
        # because identity -- not the natural key -- is what makes an object the
        # *same* declaration the prerequisite plan owns.
        self.borrowed_ids: set[int] = borrowed_ids or set()
        # Only the owning plan may pin or remove these keys.
        self.borrowed_keys: set[str] = borrowed_keys or set()
        # The prerequisite plan declaring each borrowed object, keyed like
        # `borrowed_ids`, so a missing one can name the plan to apply first.
        self.borrowed_owners: Mapping[int, PrerequisitePlan] = borrowed_owners or {}
        # Natural keys declared by the prerequisite plans applied in the same run
        # as this one -- listed together, or configured for the same instance.
        # They are applied first, so a borrowed object under one of these keys
        # that the instance lacks is theirs to create: a preview reports it as
        # created by them rather than as missing (see :meth:`_borrow`).
        self.co_applied_keys: set[str] = co_applied_keys or set()
        # Borrowed objects the instance does not have, recorded by a preview
        # where it warns that the apply will refuse (see :meth:`_borrow`), so
        # `apply_one` can refuse before it tracks a run.
        self.missing_prerequisites: list[MissingPrerequisite] = []
        # Set by `compute_changes` to the outcome of applying declared moves
        # to the lock (applied / already-satisfied / unmatched), for reporting.
        self.move_outcome: MoveOutcome | None = None
        self._cache: dict[int, Any] = {}
        self._space_cache: dict[str, Any] = {}
        # Existence lookups (`obj.get`) keyed by `id(obj)`.  Shared between
        # the preview and apply passes so the second pass reuses the first's
        # queries instead of hitting the instance again.
        self._existing_cache: ExistenceCache = (
            existing_cache if existing_cache is not None else ExistenceCache()
        )
        # A live schema's member records, cached by `id(existing)`.
        # `Schema.members` is a computed property that issues two SQL
        # statements per access; resolving one schema touches it up to four
        # times (once per :func:`serialize_existing` call and once in
        # :meth:`_schema_feature_diff`).  Materializing the members once and
        # reusing them collapses those to a single fetch.  The APPLY-mode paths
        # that edit a member list keep the entry accurate in place rather than
        # re-reading it (see :meth:`_reconcile`).  A caller may pass a shared
        # dict to reuse one bulk read across several read-only plans against the
        # same instance (see :func:`prefetch_plans`).
        self._members_cache: dict[int, list[Any]] = (
            members_cache if members_cache is not None else {}
        )
        # A composite schema's live `{slot: component}`, read once per run (see
        # :meth:`_live_slots_of`) and only for a schema that may have some --
        # decided once per natural key in `_slotted` (see
        # :meth:`_may_have_slots`).
        self._slots_cache: dict[Any, dict[str, Any]] = {}
        self._slotted: dict[str, bool] = {}
        # The run's instance-wide reads, made once: where the instance's sheet
        # exports live (see :meth:`_sheet_export_index`) and which schemas
        # validated anything (see :meth:`_schema_validated_anything`).  A caller
        # applying several plans to one instance passes a shared dict so each
        # read happens once for the run rather than once per plan.
        self._exports_cache: dict[str, Any] = (
            exports_cache if exports_cache is not None else {}
        )
        # The plan being applied, which names its re-export branch: an unmerged
        # one says a previous run owes exports (see :meth:`_reexports_owed`).
        self.plan_name = plan_name
        # Feature existence records whose scalar values the data-safety checks may
        # need (set by `compute_changes`).  Their values are prefetched in bulk
        # on first use (`_scalar_values_for`) so the per-feature drift-data
        # queries collapse into a couple of batched reads, and their
        # categorical-link presence likewise (`_categorical_presence_for`).
        self._feature_records: list[Any] = []
        # Lazily-built map of feature `pk` -> prefetched scalar values (see
        # :func:`lamindb_setup.evolve.engine._drift_data.prefetch_scalar_values`).
        # `None` until first accessed; a missing key means "query per-object".
        self._feature_value_cache: dict[int, list[Any]] | None = None
        # Lazily-built map of feature `pk` -> whether it owns any stored
        # categorical link row (see
        # :func:`lamindb_setup.evolve.engine._drift_data.prefetch_categorical_presence`).
        # `None` until first accessed; a missing key means "query per-object".
        self._categorical_presence_cache: dict[Any, bool] | None = None
        # Memoizes categorical label lookups across features for the data-safety
        # checks (see `_drift_data._cat_labels_for`): keyed by
        # `(id(registry), field)` so a value validated once is not re-queried.
        self._cat_label_cache: dict[Any, Any] = {}
        self.entries: dict[str, LockEntry] = {}
        self.changes: list[ChangeItem] = []
        self._change_ids: set[int] = set()
        # Declared migrations resolved by `_reconcile` but not yet executed:
        # they run once every plan object has been materialized, so a migration's
        # targets never constrain when the *feature* is created (see
        # :class:`_DeferredMigration` and :meth:`run_deferred_migrations`).
        self._deferred_migrations: list[_DeferredMigration] = []
        # The same, for a schema whose optional set the plan narrows: the
        # migration backfills the rows that omitted the newly-required members
        # (see :class:`_DeferredMemberMigration`).
        self._deferred_member_migrations: list[_DeferredMemberMigration] = []
        self._deferred_member_removals: list[_DeferredMemberRemoval] = []
        # Index changes a declaration accepted, whose callables run with the
        # other deferred migrations (see :class:`_DeferredIndexChange`).
        self._deferred_index_changes: list[_DeferredIndexChange] = []
        # Schemas turning `flexible` off whose declarations keep or remove what
        # the flexible match annotated (see :class:`_DeferredFlexibleChange`).
        self._deferred_flexible_changes: list[_DeferredFlexibleChange] = []
        # Seeded records an `update_record()` brings in line with the plan
        # (see :class:`_DeferredRecordUpdate`).
        self._deferred_record_updates: list[_DeferredRecordUpdate] = []
        # PLAN mode only: what APPLY clears before :meth:`drop_records` runs, so
        # the preview judges a drop against what will be left (see
        # :meth:`_holding_links`).  Each value an `update_record()` rewrites,
        # keyed by `(record pk, feature pk)` with the feature and the declared
        # value, and the features and record types :meth:`apply_removals`
        # deletes -- the former with their values.
        self._rewritten_values: dict[tuple[Any, Any], tuple[Any, Any]] = {}
        self._cleared_feature_pks: set[Any] = set()
        self._removed_type_pks: set[Any] = set()
        # PLAN mode only: the refusals of every object that could not be
        # reconciled, so a diff reports all of them at once instead of stopping
        # at the first (see :meth:`materialize_object`).
        self.drift_errors: list[DriftError] = []
        # What each live schema validated, keyed by primary key: asked both by
        # the remote-drift check and by the reconcile of an index change.
        self._schema_usage_cache: dict[Any, SchemaUsage | None] = {}
        # In-place edits this apply makes to a live feature's *fill* semantics
        # (`nullable` / `default_value`), keyed by primary key.  A member
        # backfill resolves NULL / DEFAULT against those two fields, so it has to
        # see the values the plan is writing rather than the ones on disk; in
        # PLAN mode nothing is saved, so the dry-run has to overlay them itself
        # to report what the apply would actually do (see :meth:`_member_fills`).
        self._pending_feature_fills: dict[Any, dict[str, Any]] = {}
        # Schema changes whose sheet exports (CSV snapshots already written to
        # storage) no longer adhere.  Unlike every other deferral these are not
        # executed here: a file write cannot be rolled back with the transaction,
        # so they are handed to the caller to run after the commit (see
        # :class:`_DeferredSheetExports`).
        self.pending_sheet_exports: list[_DeferredSheetExports] = []
        # Live features this apply renamed or retyped, keyed by primary key and
        # valued by a short description.  A sheet export's columns are the
        # *feature records* it was written from, so identity alone cannot tell
        # that one of them now means something else; this is what does.
        self._changed_features: dict[Any, str] = {}
        # Every schema this apply reconciled, as the shape it has afterwards.
        # Collected during resolution so the stale-export sweep can be a single
        # pass at the end, which is what lets it see a member renamed *after* its
        # schema was reconciled (see :class:`_SchemaShape`).
        self._schema_shapes: list[_SchemaShape] = []
        # Sheets this apply points at another schema, whose exports the sweep
        # cannot reach through a schema (see :class:`_SheetRepoint`).
        self._sheet_repoints: list[_SheetRepoint] = []
        # What the removal declarations move instead of letting the trash take
        # it, by the natural key of the deleted object (see :class:`_Rehoming`).
        self._rehomings: dict[str, list[_Rehoming]] = {}
        # Schemas a PLAN-mode reconcile would rehash, checked together for a
        # content collision at the end (see :meth:`check_schema_collisions`).
        self._hash_checks: list[_HashCheck] = []
        # The columns an inferred schema recorded before this apply re-pointed
        # or rewrote it, keyed by the artifact's primary key: the link now names
        # another schema, or one without a deleted member the file still has, so
        # the export sweep reads the file's shape from here instead.
        self._absorbed_columns: dict[Any, set[Any]] = {}

    @classmethod
    def standalone(cls) -> ApplyContext:
        """Return an APPLY-mode context with no lock for ad-hoc materialization."""
        return cls(mode=ApplyMode.APPLY, lock=None)

    def _schema_members(self, existing: Any) -> list[Any]:
        """Return `existing`'s schema members, materialized once and cached.

        `Schema.members` is a computed property (two SQL statements per
        access), so the list is cached by `id(existing)` and reused by every
        snapshot and the feature diff within one resolution.  The APPLY-mode
        paths that edit a member list write the new list back here themselves
        (:meth:`_reconcile`, :meth:`_reorder_schema_members`), so a post-edit
        read is served from memory rather than re-querying.
        """
        cache_key = id(existing)
        cached = self._members_cache.get(cache_key)
        if cached is not None:
            return cached
        members = list(existing.members.all())
        self._members_cache[cache_key] = members
        return members

    def prefetch_schema_members(self, schemas: Sequence[Any]) -> None:
        """Bulk-load the members of live `schemas` into the members cache.

        :attr:`lamindb.Schema.members` costs two statements per schema -- one
        over the link table, then one over the member registry ordered by a
        `CASE` expression carrying a branch per member -- and every resolution
        of a schema needs that list.  A plan of `S` schemas therefore spends
        `2 * S` round-trips on a read whose rows can be fetched together:
        reading the link rows for every schema at once and grouping them in
        Python collapses it to two statements per link table, whatever `S` is.

        The per-schema order lamindb preserves (link rows by `id`) is
        preserved here too.  A schema this cannot mirror exactly -- a type or
        composite schema, whose members are empty by definition, or an `itype`
        whose link table is not resolvable -- is simply left out of the cache, so
        :meth:`_schema_members` falls back to `Schema.members` for it.
        """
        groups: dict[tuple[int, str | None], dict[str, Any]] = {}
        for schema in schemas:
            if id(schema) in self._members_cache:
                continue
            if schema.is_type or schema.itype == "Composite":
                continue
            link = _schema_member_link(schema)
            if link is None:
                continue
            through, member_model, fk_name = link
            using = schema._state.db
            group = groups.setdefault(
                (id(through), using),
                {
                    "through": through,
                    "model": member_model,
                    "fk": fk_name,
                    "using": using,
                    "schemas": {},
                },
            )
            group["schemas"][schema.pk] = schema

        for group in groups.values():
            using = group["using"]
            ids_by_schema: dict[Any, list[Any]] = {pk: [] for pk in group["schemas"]}
            member_ids: set[Any] = set()
            for chunk in _chunked(list(ids_by_schema), _PREFETCH_BATCH_SIZE):
                rows = (
                    group["through"]
                    .objects.using(using)
                    .filter(schema_id__in=chunk)
                    .order_by("id")
                    .values_list("schema_id", f"{group['fk']}_id")
                )
                for schema_pk, member_pk in rows:
                    ids_by_schema[schema_pk].append(member_pk)
                    member_ids.add(member_pk)
            loaded: dict[Any, Any] = {}
            for chunk in _chunked(sorted(member_ids), _PREFETCH_BATCH_SIZE):
                for member in group["model"].objects.using(using).filter(id__in=chunk):
                    loaded[member.pk] = member
            for schema_pk, schema in group["schemas"].items():
                self._members_cache[id(schema)] = [
                    loaded[member_pk]
                    for member_pk in ids_by_schema[schema_pk]
                    if member_pk in loaded
                ]

    def _snapshot(self, existing: Any, kind: str) -> dict[str, Any]:
        """Serialize `existing` for the lock, reusing cached schema members."""
        members = self._schema_members(existing) if kind == "Schema" else None
        return serialize_existing(existing, kind, members=members)

    def _scalar_values_for(self, existing: Any) -> list[Any] | None:
        """Return `existing`'s prefetched scalar values, or `None` to query.

        Builds the feature-value cache in bulk on first use (one batched query
        per value table across every plan feature, see
        :func:`lamindb_setup.evolve.engine._drift_data.prefetch_scalar_values`), so the
        data-safety checks no longer emit a fetch / `exists` / `count` per
        feature.  `None` when the feature was not prefetched (a categorical
        source, one not among the plan's features, or a batch that failed) so the
        drift-data helper falls back to its conservative per-object query.
        """
        if self._feature_value_cache is None:
            from ._drift_data import prefetch_scalar_values

            self._feature_value_cache = prefetch_scalar_values(self._feature_records)
        return self._feature_value_cache.get(existing.pk)

    def _categorical_presence_for(self, existing: Any) -> bool | None:
        """Whether `existing` owns categorical link rows, from the bulk prefetch.

        Builds the presence cache in bulk on first use -- a single union across
        every categorical link table for all of the plan's features (see
        :func:`lamindb_setup.evolve.engine._drift_data.prefetch_categorical_presence`) --
        so the relocation check no longer walks lamindb's twenty-odd link tables
        with an `exists` apiece for every feature that asks.

        Built lazily because only a dtype change *to* `str` asks the question at
        all: a plan with no such change never pays for the prefetch.  `None` when
        the feature was not prefetched (not among the plan's features, or a batch
        that failed) so the drift-data helper falls back to its conservative
        per-object walk.
        """
        if self._categorical_presence_cache is None:
            from ._drift_data import prefetch_categorical_presence

            self._categorical_presence_cache = prefetch_categorical_presence(
                self._feature_records
            )
        return self._categorical_presence_cache.get(existing.pk)

    def owns(self, obj: Materializable) -> bool:
        """Whether this plan owns `obj` rather than borrowing it from a prerequisite."""
        return id(obj) not in self.borrowed_ids

    def locked_uid(self, key: str) -> str | None:
        """Return the uid recorded in the lock for `key` if any.

        The owning plan pins a borrowed object.
        """
        if self.lock is None or key in self.borrowed_keys:
            return None
        return self.lock.uid_for(key)

    def locked_uid_of(self, obj: Materializable) -> str | None:
        """Return the uid recorded in the lock for `obj` if any.

        Computing the natural key is the lock format's business, so a declared
        object asks the context about itself rather than importing the key
        function.
        """
        return self.locked_uid(natural_key(obj))

    def pinned_uid(self, key: str, *, seeded: bool = False) -> str | None:
        """Return the uid `key` should have here when this lock does not pin it.

        For a tracked object that is the uid the plan's locks for other
        instances agree on (see :class:`._alignment.SiblingUids`).  A seed-only
        object is never pinned by the lock, but the lock remembers the uid it
        resolved to last time (:attr:`LockFile.seeded`), which is tried first.
        A borrowed object is pinned by its owning plan's locks, not this one's.
        """
        if key in self.borrowed_keys:
            return None
        if seeded and self.lock is not None:
            remembered = self.lock.seeded.get(key)
            if remembered is not None:
                return remembered
        if self.sibling_uids is None:
            return None
        return self.sibling_uids.canonical(key, seeded=seeded)

    def pinned_uid_of(self, obj: Materializable) -> str | None:
        """Return :meth:`pinned_uid` for `obj`, which may be looked up by it."""
        if not self.owns(obj):
            return None
        return self.pinned_uid(
            natural_key(obj), seeded=type(obj).__name__ in SEED_ONLY_KINDS
        )

    def creation_uid(self, obj: Materializable) -> str | None:
        """Return the uid :meth:`_resolve` decided to create `obj` with, once."""
        return self._creation_uids.pop(id(obj), None)

    @staticmethod
    def _pinned_match(obj: Materializable, record: Any, parent: Any) -> bool:
        """Whether `record`, found by `obj`'s pinned uid, is that object.

        The uid alone is not enough: it has to be a live record of the same
        kind under the same parent, else it is some other object that happens
        to hold the uid (see :meth:`_creation_plan`).  The name is deliberately
        not compared -- an object renamed on the instance is still the object,
        and is renamed back to the plan's name.
        """
        if record is None or not is_live(record):
            return False
        kind = type(obj).__name__
        if bool(getattr(record, "is_type", False)) != (kind in _IS_TYPE_KINDS):
            return False
        if getattr(obj, "type", None) is not None and parent is None:
            return False
        want = parent.pk if parent is not None else None
        return getattr(record, "type_id", None) == want

    def effective_space_uid(self, obj: Materializable) -> SpaceUID | None:
        """Return `obj`'s space uid, falling back to the plan-wide default."""
        uid = getattr(obj, "space_uid", None)
        if uid is None:
            uid = self.default_space_uid
        return uid

    def resolve_space(self, obj: Materializable) -> Any | None:
        """Look up the existing LaminDB `Space` for `obj`, or `None`.

        The space is resolved from `obj`'s `space_uid` or the plan-wide
        default. The space must already exist in the instance; a missing space
        raises :class:`SpaceNotFoundError`.
        """
        uid = self.effective_space_uid(obj)
        if uid is None:
            return None
        return self.resolve_space_uid(uid)

    def resolve_space_uid(self, uid: SpaceUID) -> Any:
        """Look up the existing LaminDB `Space` for an explicit `uid`.

        The space must already exist in the instance; a missing space raises
        :class:`SpaceNotFoundError`.
        """
        cached = self._space_cache.get(uid)
        if cached is not None:
            return cached
        import lamindb as ln

        registry: Any = ln.Space
        space = registry.filter(uid=uid).one_or_none()
        if space is None:
            raise SpaceNotFoundError(
                f"space with uid {uid!r} does not exist in the instance; "
                "create it before applying the plan."
            )
        self._space_cache[uid] = space
        return space

    def find(self, registry: Any, key: str, **filters: Any) -> Any | None:
        """Read-only lookup: by recorded uid when locked, else by natural filters.

        `registry` is a LaminDB model class (e.g. `ln.Feature`).  Returns the
        matching record or `None`; never writes to the instance.
        """
        uid = self.locked_uid(key)
        if uid is not None:
            return registry.filter(uid=uid).one_or_none()
        return registry.filter(**filters).one_or_none()

    def find_typed(
        self, registry: Any, obj: Materializable, **filters: Any
    ) -> Any | None:
        """Read-only lookup of `obj` that also constrains by its parent type.

        Two objects with the same name but different parent `type` are distinct
        in the plan model (the natural key encodes the full type chain), so the
        live query must match on the parent too.  The parent is resolved
        read-only; when `obj` declares a parent type that does not yet exist in
        the instance, no matching object can exist and `None` is returned.
        Objects without a parent are matched only against untyped records.

        When a uid is recorded in the lock, that takes precedence and the parent
        is not resolved.  Otherwise a uid the plan's other instances pin
        (:meth:`pinned_uid_of`) is tried before the name, so an object that was
        renamed here -- or synced here under its uid -- is still found.
        """
        key = natural_key(obj)
        if self.locked_uid(key) is None:
            type_decl = getattr(obj, "type", None)
            parent: Any = None
            if type_decl is not None:
                parent = type_decl.get(self)
                if parent is None:
                    return None
                filters["type"] = parent
            else:
                filters["type"] = None
            pinned = self.pinned_uid_of(obj)
            if pinned is not None:
                record = registry.filter(uid=pinned).one_or_none()
                if self._pinned_match(obj, record, parent):
                    return record
            if "name" in filters and filters["name"] is None:
                return None
        return self.find(registry, key, **filters)

    def materialize_object(self, obj: Materializable) -> Any:
        """Resolve `obj`: reuse, create, or return a placeholder (PLAN mode).

        In PLAN mode an object that cannot be reconciled does not end the diff:
        its :class:`DriftError` is collected on :attr:`drift_errors` and the object
        stands in as it is on the instance (see :meth:`_drift_stand_in`), so the
        objects after it are still judged.  :meth:`raise_collected_drift` then
        reports them all at once.  APPLY mode still stops at the first one --
        :func:`apply_one` previews first, so it only gets there when the
        instance changed in between.
        """
        cache_key = id(obj)
        if cache_key in self._cache:
            return self._cache[cache_key]
        try:
            result = self._resolve(obj)
        except DriftError as exc:
            if self.mode is not ApplyMode.PLAN:
                raise
            self.drift_errors.append(exc)
            result = self._drift_stand_in(obj)
        self._cache[cache_key] = result
        return result

    def _drift_stand_in(self, obj: Materializable) -> Any:
        """What an object that could not be reconciled resolves to for the rest of a diff.

        Its live record when it has one -- the dependents are then diffed against
        the instance as it is -- and otherwise a :class:`Placeholder`, which is
        what PLAN mode resolves any object it would create to.
        """
        existing = self._existing(obj)
        if existing is not None and is_live(existing):
            return existing
        return Placeholder(key=natural_key(obj), kind=type(obj).__name__)

    def raise_collected_drift(self) -> None:
        """Raise the refusals :meth:`materialize_object` collected, if any.

        A single one is re-raised as it was.  Several are combined into one
        :class:`DriftError` listing each, so a pull request sees every object it
        has to change at once rather than one per round trip -- a
        :class:`PlanChangeRefusedError` when each of them is one.
        """
        if not self.drift_errors:
            return
        if len(self.drift_errors) == 1:
            raise self.drift_errors[0]
        combined = (
            PlanChangeRefusedError
            if all(isinstance(e, PlanChangeRefusedError) for e in self.drift_errors)
            else DriftError
        )
        raise combined(
            f"{len(self.drift_errors)} objects cannot be reconciled with the plan:\n\n"
            + "\n\n".join(str(error) for error in self.drift_errors)
        )

    def _existing(self, obj: Materializable) -> Any:
        """Return `obj`'s existing record, reusing a shared cached lookup.

        `obj.get` is a read-only existence query against the instance.  Its
        result is cached by `id(obj)` so a later pass over the same finalized
        plan (e.g. apply after preview) does not re-query.  A `None` result is
        cached too: an object absent during preview must still be created during
        apply.
        """
        return self._existing_cache.resolve(obj, self)

    def _live_record(self, obj: Materializable) -> Any | None:
        """`obj`'s live record, including one this very apply just created.

        :meth:`_existing` caches the answer from *before* the object was resolved,
        so an object the apply created reads as absent there; its resolution
        holds the record instead.  `None` while it does not exist -- in PLAN
        mode its resolution is only a placeholder.
        """
        resolved = self._cache.get(id(obj))
        if resolved is not None and not isinstance(resolved, Placeholder):
            return resolved
        return self._existing(obj)

    def _plan_index_uid(self, obj: Materializable) -> Any:
        """The uid of the index `obj` declares, as an instance snapshot records it.

        `None` when the plan declares no index, and :data:`_UNRESOLVED` when it
        declares one whose feature does not exist yet, which no snapshot can match.
        """
        index = getattr(obj, "index", None)
        if index is None:
            return None
        live = self._live_record(index)
        return str(live.uid) if live is not None else _UNRESOLVED

    def _schema_usage(self, existing: Any) -> SchemaUsage | None:
        """What `existing` has validated, queried once per schema (see `SchemaUsage`)."""
        from ._drift_data import schema_validated_usage

        if existing.pk not in self._schema_usage_cache:
            self._schema_usage_cache[existing.pk] = schema_validated_usage(existing)
        return self._schema_usage_cache[existing.pk]

    def _schema_validated_anything(self, existing: Any) -> bool | None:
        """Whether `existing` validated anything, from one bulk read for the run.

        Read on first use for every live schema the existence cache holds --
        every plan's, when a run shares the cache across the plans of an
        instance -- (see
        :func:`~lamindb_setup.evolve.engine._drift_data.prefetch_schema_usage_presence`)
        and kept in the run's shared instance-wide cache, so a run editing many
        schemas asks the instance once rather than once per schema or plan.
        `None` when the read failed, so the caller asks :meth:`_schema_usage`.
        """
        presence: dict[Any, bool] = self._exports_cache.setdefault(
            "schema_presence", {}
        )
        if existing.pk not in presence:
            from ._drift_data import prefetch_schema_usage_presence

            pending = {
                record.pk: record
                for record in (*self._existing_cache.values(), existing)
                if type(record).__name__ == "Schema"
                and getattr(record, "pk", None) is not None
                and record.pk not in presence
            }
            presence.update(prefetch_schema_usage_presence(list(pending.values())))
        return presence.get(existing.pk)

    def prefetch(
        self,
        objs: Sequence[Materializable],
        *,
        locked_uids: Mapping[int, str] | None = None,
        pinned_uids: Mapping[int, str] | None = None,
    ) -> None:
        """Bulk-load existence lookups for `objs` into the shared cache.

        Replaces the per-object existence query (`obj.get` →
        `filter(...).one_or_none()`) with a handful of batched reads: a
        `uid__in` query per backing registry for the objects the lock already
        pins, a `name__in` query for the rest, eagerly joining the foreign keys
        that snapshots read.  The results are indexed and each object's existing
        record is resolved in dependency order (so a typed object's parent is
        already resolved) and stored in `_existing_cache` keyed by `id(obj)`.
        The subsequent materialize loop then issues no existence queries at all.

        Objects already present in the cache (e.g. a second pass sharing the
        cache) are skipped, so calling this once per finalized plan is enough.

        `locked_uids` maps `id(obj)` to the uid the lock records for it,
        overriding this context's own lock.  It exists so one prefetch can span
        several plans that each carry a different lock (see
        :func:`prefetch_plans`); when omitted the context's lock is consulted.
        `pinned_uids` likewise overrides :meth:`pinned_uid_of`: the uid an
        object without a lock entry is looked up by before its name.
        """
        pending = [obj for obj in objs if id(obj) not in self._existing_cache]
        if not pending:
            return

        from ._lock import registry_for_kind

        # Each object's lock-recorded uid, resolved once here and reused by the
        # lookup pass below (which would otherwise recompute every natural key).
        uids: dict[int, str | None]
        if locked_uids is not None:
            uids = {id(obj): locked_uids.get(id(obj)) for obj in pending}
        else:
            uids = {id(obj): self.locked_uid(natural_key(obj)) for obj in pending}
        pinned: dict[int, str | None]
        if pinned_uids is not None:
            pinned = {id(obj): pinned_uids.get(id(obj)) for obj in pending}
        else:
            pinned = {
                id(obj): self.pinned_uid_of(obj) if uids[id(obj)] is None else None
                for obj in pending
            }

        # Group by backing registry, collecting the names and lock-recorded uids
        # to look up and whether the registry is `ln.Record` (needs `schema`).
        # A locked object is resolved by uid alone -- :meth:`_lookup_prefetched`
        # never consults the name index for it, and a uid the instance does not
        # have is drift, not a cue to fall back to the name -- so its name is
        # deliberately left out.  In steady state that is every object, which
        # halves the reads.
        groups: dict[int, dict[str, Any]] = {}
        for obj in pending:
            kind = type(obj).__name__
            registry = registry_for_kind(kind)
            group = groups.setdefault(
                id(registry),
                {"registry": registry, "names": set(), "uids": set(), "record": False},
            )
            uid = uids[id(obj)]
            if uid is not None:
                group["uids"].add(uid)
            else:
                name = getattr(obj, "name", None)
                if name is not None:
                    group["names"].add(name)
                # A pinned uid is only a first guess -- the record holding it may
                # be some other object -- so the name is loaded alongside it.
                pinned_uid = pinned[id(obj)]
                if pinned_uid is not None:
                    group["uids"].add(pinned_uid)
            if kind in _RECORD_KINDS:
                group["record"] = True

        # Bulk-load each registry and build the existence indices.
        by_uid: dict[tuple[int, str], Any] = {}
        by_typed: dict[tuple[int, bool, str, str | None], Any] = {}
        for group in groups.values():
            registry = group["registry"]
            rid = id(registry)
            related = (
                ["type", "space", "schema"] if group["record"] else ["type", "space"]
            )
            loaded: dict[Any, Any] = {}
            for chunk in _chunked(sorted(group["names"]), _PREFETCH_BATCH_SIZE):
                qs = (
                    registry.filter(name__in=chunk)
                    .select_related(*related)
                    .order_by("uid")
                )
                for rec in _keyset_pages(qs, _PREFETCH_BATCH_SIZE):
                    loaded[rec.pk] = rec
            for chunk in _chunked(sorted(group["uids"]), _PREFETCH_BATCH_SIZE):
                qs = (
                    registry.filter(uid__in=chunk)
                    .select_related(*related)
                    .order_by("uid")
                )
                for rec in _keyset_pages(qs, _PREFETCH_BATCH_SIZE):
                    loaded[rec.pk] = rec
            for rec in loaded.values():
                by_uid[(rid, str(rec.uid))] = rec
                name = getattr(rec, "name", None)
                if name is None:
                    continue
                if not is_live(rec):
                    # Only the `uid__in` pass can load a soft-deleted record:
                    # lamindb exempts uid lookups from its branch filter, while
                    # the `name__in` pass above is filtered (see
                    # :func:`~lamindb_setup.evolve.engine._trash.is_live`).  Such a
                    # record stays addressable by uid
                    # -- :meth:`_resolve` reports it as trashed rather than
                    # pretending the lock still manages something -- but it must
                    # stay out of the name index, where it would either shadow
                    # the live record of the same name or, worse, be reported as
                    # a duplicate of it, sending the operator to `repair
                    # duplicates` for a duplicate that repair has already made.
                    continue
                is_type = bool(getattr(rec, "is_type", False))
                parent = getattr(rec, "type", None)
                parent_uid = str(parent.uid) if parent is not None else None
                index_key = (rid, is_type, name, parent_uid)
                existing = by_typed.get(index_key)
                if existing is not None and existing.pk != rec.pk:
                    raise DriftError(
                        f"multiple records match {name!r} (is_type={is_type}, "
                        f"parent={parent_uid!r}); resolve the duplicates (e.g. with "
                        "repair duplicates) before applying."
                    )
                by_typed[index_key] = rec

        # Resolve each object's existing record in dependency order.
        for obj in pending:
            self._existing_cache.prefill(
                id(obj),
                self._lookup_prefetched(
                    obj, by_uid, by_typed, uids[id(obj)], pinned=pinned[id(obj)]
                ),
            )

    def _lookup_prefetched(
        self,
        obj: Materializable,
        by_uid: dict[tuple[int, str], Any],
        by_typed: dict[tuple[int, bool, str, str | None], Any],
        uid: str | None,
        *,
        pinned: str | None = None,
    ) -> Any:
        """Resolve `obj`'s existing record from the prefetched indices.

        Mirrors the read-only `get` / :meth:`find_typed` semantics: a
        lock-recorded `uid` wins; otherwise a `pinned` uid is tried, then
        the object is matched by name, `is_type` and parent uid.  Unnamed
        schemas and records without a lock uid have no resolvable identity and
        return `None`, matching their per-object `get`.
        """
        from ._lock import registry_for_kind

        kind = type(obj).__name__
        rid = id(registry_for_kind(kind))
        if uid is not None:
            return by_uid.get((rid, str(uid)))

        type_decl = getattr(obj, "type", None)
        parent_existing = (
            self._existing_cache.get(id(type_decl)) if type_decl is not None else None
        )
        if pinned is not None:
            record = by_uid.get((rid, str(pinned)))
            if self._pinned_match(obj, record, parent_existing):
                return record

        name = getattr(obj, "name", None)
        if kind in ("Schema", "Record") and name is None:
            return None

        if type_decl is not None:
            if parent_existing is None:
                return None
            parent_uid: str | None = str(parent_existing.uid)
        else:
            parent_uid = None
        return by_typed.get((rid, kind in _IS_TYPE_KINDS, name, parent_uid))

    def _resolve(self, obj: Materializable) -> Any:
        key = natural_key(obj)
        kind = type(obj).__name__
        config = serialize_config(obj)
        space_uid = self.effective_space_uid(obj)
        if space_uid is not None:
            config["space_uid"] = str(space_uid)
        if kind in SEED_ONLY_KINDS:
            return self._seed(obj, key, kind, config)
        if not self.owns(obj):
            return self._borrow(obj, key, kind, config)
        previous = self.lock.entry_for(key) if self.lock is not None else None

        existing = self._existing(obj)
        if existing is not None and not is_live(existing):
            # A soft-deleted object stays "managed" forever if this is not said
            # out loud: the lock keeps pinning its uid, every lookup that would
            # notice its absence is a uid lookup that does not, and the drift
            # checks below then diff the plan against a record that validates
            # nothing.  Neither answer is the plan's to guess, so say what is
            # wrong and name both ways out.
            raise DriftError(
                f"drift for {key!r}: the instance record uid "
                f"{str(existing.uid)!r} is soft-deleted (branch_id="
                f"{getattr(existing, 'branch_id', None)!r}; -1 is the trash, 0 "
                "the archive). It is reachable here only because the lock pins "
                "its uid -- every other lookup hides it -- and it validates "
                "nothing, so the plan cannot be reconciled against it. Restore "
                "it in the instance to keep managing it, or drop the object "
                "from the plan so the next apply deletes it and forgets the "
                "lock entry."
            )
        if existing is not None:
            warnings: list[ChangeWarning] = []
            allow_rename = key in self.moved_keys
            if (
                previous is None
                and kind == "RecordType"
                and getattr(existing, "schema_id", None) is not None
            ):
                # Found by name, a sheet's record would be adopted as a plain
                # record type -- and the sheet's own lock entry then deleted by
                # uid, trashing the very record with every row under it.
                raise DriftError(
                    f"drift for {key!r}: the instance record type uid "
                    f"{str(existing.uid)!r} validates its records against schema "
                    f"{_schema_label(existing.schema)!r} -- it is a sheet -- but "
                    "the plan declares a plain record type there. Declare it as "
                    "a Sheet with that schema, or give the record type another name."
                )
            misalignment = self._existing_misalignment(key, existing)
            # Found by the uid the other instances pin while carrying another
            # name here (renamed, or synced in under its uid): it is this object,
            # so it is renamed back to the plan's name like a declared move.
            adopted_by_uid = (
                previous is None
                and (pinned := self.pinned_uid(key)) is not None
                and str(existing.uid) == pinned
                and getattr(existing, "name", None) != getattr(obj, "name", None)
            )
            if adopted_by_uid:
                allow_rename = True
            # Serialize the live record once and share it with both drift checks
            # (which each used to recompute it) and, when nothing changes, the
            # recorded snapshot below.  For a schema this also fixes the member
            # list once via the shared cache.
            members = self._schema_members(existing) if kind == "Schema" else None
            snapshot = serialize_existing(
                existing,
                kind,
                members=members,
                slots=self._slot_uids(obj, key, existing) if kind == "Schema" else None,
            )
            desired_dtype = self._desired_dtype(obj) if kind == "Feature" else None
            if previous is not None:
                self._check_remote_drift(
                    existing,
                    key,
                    kind,
                    previous,
                    warnings,
                    current=snapshot,
                    baseline=previous.config,
                    desired=config,
                    allow_rename=allow_rename,
                    desired_index=(
                        self._plan_index_uid(obj) if kind == "Schema" else _UNRESOLVED
                    ),
                )
                self._check_plan_edit(
                    existing,
                    key,
                    kind,
                    previous.config,
                    config,
                    current=snapshot,
                    allow_rename=allow_rename,
                    desired_dtype=desired_dtype,
                )
            action, details = self._reconcile(
                obj,
                existing,
                key,
                allow_rename=allow_rename,
                members=members,
                warnings=warnings,
                desired_dtype=desired_dtype,
                origin=(
                    None
                    if previous is None
                    else _ChangeOrigin(
                        lock_config=previous.config,
                        plan_config=config,
                        stored=previous.remote_snapshot,
                        current=snapshot,
                    )
                ),
            )
            if warnings and action is Action.NO_CHANGE:
                action = Action.UPDATE
            if self.mode is ApplyMode.APPLY and action is not Action.NO_CHANGE:
                # A reconcile in APPLY mode may have written to `existing`
                # (setattr + save, or new schema members); re-serialize so the
                # lock records the post-apply state.  PLAN mode never mutates,
                # so the pre-reconcile snapshot already matches.  Only the two
                # member-editing paths change a schema's members and both update
                # the cache in place, so this reads them rather than re-querying.
                if kind == "Schema":
                    members = self._schema_members(existing)
                snapshot = serialize_existing(
                    existing,
                    kind,
                    members=members,
                    slots=(
                        self._slot_uids(obj, key, existing)
                        if kind == "Schema"
                        else None
                    ),
                )
            uid = str(existing.uid)
            if adopted_by_uid:
                details.insert(
                    0,
                    f"adopted by uid {uid}, the uid the plan's other instances "
                    "record for it",
                )
            self._record(
                obj,
                key,
                kind,
                uid,
                config,
                action,
                details=details,
                warnings=warnings,
                snapshot=snapshot,
                misalignment=misalignment,
            )
            return existing

        # Not found in the instance.
        if previous is not None and previous.uid is not None:
            raise DriftError(
                f"drift for {key!r}: the lock records uid {previous.uid!r} but no "
                f"matching object exists in the instance."
            )

        creation = self._creation_plan(obj, key)
        if self.mode is ApplyMode.PLAN:
            placeholder = Placeholder(key=key, kind=kind)
            self._record(
                obj,
                key,
                kind,
                PLACEHOLDER_UID,
                config,
                Action.CREATE,
                details=creation.details,
                warnings=creation.warnings,
                misalignment=creation.misalignment,
            )
            return placeholder

        record = self._commit(obj, key, creation)
        uid = str(record.uid)
        snapshot = serialize_existing(
            record,
            kind,
            slots=self._slot_uids(obj, key, record) if kind == "Schema" else None,
        )
        self._record(
            obj,
            key,
            kind,
            uid,
            config,
            Action.CREATE,
            details=creation.details,
            warnings=creation.warnings,
            snapshot=snapshot,
            misalignment=creation.misalignment
            or self._created_misalignment(key, record),
        )
        return record

    def _existing_misalignment(
        self, key: str, existing: Any, *, seeded: bool = False
    ) -> UidMisalignment | None:
        """Describe how `existing`'s uid disagrees with the other instances, if it does."""
        if self.sibling_uids is None:
            return None
        others = self.sibling_uids.recorded(key, seeded=seeded)
        if not others:
            return None
        uid = str(existing.uid)
        agreed = set(others.values())
        if agreed == {uid}:
            return None
        if len(agreed) > 1:
            reason = "the instances this plan is applied to record different uids"
        else:
            reason = f"uid {uid} here, {next(iter(agreed))} on the other instances"
        return UidMisalignment(uid=uid, others=others, reason=reason)

    def _created_misalignment(self, key: str, record: Any) -> UidMisalignment | None:
        """A just-created `record` that did not get the pinned uid, if any.

        lamindb resolves a schema by its content hash, so creating one whose
        content is already stored under another uid hands that record back
        rather than a new one; its uid then differs from the other instances'.
        """
        if self.sibling_uids is None:
            return None
        pinned = self.sibling_uids.canonical(key)
        if pinned is None or str(record.uid) == pinned:
            return None
        return self._existing_misalignment(key, record)

    def _creation_plan(self, obj: Materializable, key: str) -> _CreationPlan:
        """Decide which uid an object missing from this instance is created with.

        The uid the plan's other instances agree on, so the object has the same
        identity everywhere.  Three things can stand in the way:

        - the other instances disagree, so there is no one uid to choose;
        - a live record that is *not* this object already holds the uid (the
          lookup would have adopted it otherwise); or
        - a trashed (or archived) record holds it -- a copy left behind by a soft
          `destroy`.  That one is moved to a fresh uid of its own, untouched
          otherwise, so the uid is free and the new object behaves as if the
          trashed one had never existed.

        The first two are misalignments: refused under `--strict-uids`, and
        otherwise created with a fresh uid and a warning.
        """
        kind = type(obj).__name__
        seeded = kind in SEED_ONLY_KINDS
        if self.sibling_uids is None or not self.owns(obj):
            return _CreationPlan()
        others = self.sibling_uids.recorded(key, seeded=seeded)
        if not others:
            return _CreationPlan()
        agreed = set(others.values())
        fallback = ChangeWarning(
            "created with a fresh uid instead (--no-strict-uids); align the "
            "instances with 'lamin evolve repair align-uids'",
            WarningCategory.CAVEAT,
        )
        if len(agreed) > 1:
            misalignment = UidMisalignment(
                uid=None,
                others=others,
                reason=(
                    "the instances this plan is applied to record different uids, "
                    "so there is no single uid to create it with"
                ),
            )
            return _CreationPlan(
                misalignment=misalignment,
                warnings=[] if self.strict_uids else [fallback],
            )
        uid = next(iter(agreed))
        # `objects` rather than `filter`: the holder may sit on any branch.
        holder = registry_for_kind(kind).objects.filter(uid=uid).first()
        slugs = ", ".join(sorted(others))
        if holder is None:
            return _CreationPlan(uid=uid, details=[f"uid {uid} as on {slugs}"])
        if not is_live(holder):
            return _CreationPlan(
                uid=uid,
                holder=holder,
                details=[
                    f"uid {uid} as on {slugs}; frees it from a trashed copy, which "
                    "gets a fresh uid"
                ],
            )
        misalignment = UidMisalignment(
            uid=None,
            others=others,
            reason=(
                f"uid {uid} is already held here by {_describe_record(holder)}, "
                "which is not this object"
            ),
        )
        return _CreationPlan(
            misalignment=misalignment,
            warnings=[] if self.strict_uids else [fallback],
        )

    def _commit(self, obj: Materializable, key: str, creation: _CreationPlan) -> Any:
        """Create `obj` (APPLY mode) with the uid :meth:`_creation_plan` chose."""
        if creation.misalignment is not None and self.strict_uids:
            raise UidAlignmentError(
                f"refusing to create {key!r}: {creation.misalignment.describe()}. "
                "Align the instances with 'lamin evolve repair align-uids', or "
                "pass --no-strict-uids to create it with a fresh uid."
            )
        if creation.holder is not None:
            model = type(creation.holder)
            model.objects.filter(pk=creation.holder.pk).update(uid=fresh_uid(model))
        if creation.uid is not None:
            self._creation_uids[id(obj)] = creation.uid
        try:
            return obj.commit(self)
        finally:
            self._creation_uids.pop(id(obj), None)

    def _seed(
        self, obj: Materializable, key: str, kind: str, config: dict[str, Any]
    ) -> Any:
        """Resolve a seed-only object: create it when missing, adopt it when present.

        Seed-only kinds (:data:`~lamindb_setup.evolve.core.SEED_ONLY_KINDS`)
        take no part in the state machine.  They are still materialized, so a
        plan seeds the vocabulary values it declares and they remain available
        as migration targets, but they are never drift-checked against the
        instance, never reconciled towards the plan, and never deleted when a
        plan stops declaring them.  Adoption is by the uid the lock remembers
        (or the plan's other instances pin), else by `(name, type)`, which is
        why an unnamed seed-only object is rejected when the plan is finalized:
        without a name it has no identity to find the first time, and every
        apply would create another copy.

        Their changes are still reported, so `plan` shows what a first apply
        will seed -- as `CREATE` when the instance has no match, otherwise as
        `NO_CHANGE`.

        The uid each one resolves to *is* recorded, in the lock's identity-only
        `seeded` section: it is what a value is created with on the plan's
        other instances, and what `repair align-uids` aligns them by.  A seeded
        object borrowed from a prerequisite plan is that plan's to record -- and,
        when the instance lacks it and that plan is applied in the same run, its
        to create (see :meth:`_created_by_prerequisite`).
        """
        if not self.owns(obj) and self._created_by_prerequisite(obj, key):
            return self._adopt_created_by_prerequisite(obj, key, kind, config)
        return self._adopt(obj, key, kind, config, seeded=self.owns(obj))

    def _created_by_prerequisite(self, obj: Materializable, key: str) -> bool:
        """Whether a prerequisite plan applied in this run creates borrowed `obj`.

        True in a preview when the instance lacks `obj` and a prerequisite
        plan of the same run declares `key` (:attr:`co_applied_keys`): that
        plan is applied first and creates it, so it is neither missing nor this
        plan's to create.  Never in APPLY mode, where the prerequisite has
        already run, so an object still missing is missing.
        """
        return (
            self.mode is ApplyMode.PLAN
            and key in self.co_applied_keys
            and self._existing(obj) is None
        )

    def _adopt_created_by_prerequisite(
        self, obj: Materializable, key: str, kind: str, config: dict[str, Any]
    ) -> Any:
        """Report a borrowed object a prerequisite plan of this run creates."""
        return self._adopt(
            obj,
            key,
            kind,
            config,
            details=[_created_in_run_detail(self.borrowed_owners.get(id(obj)))],
            borrowed=True,
        )

    def _borrow(
        self, obj: Materializable, key: str, kind: str, config: dict[str, Any]
    ) -> Any:
        """Resolve an object this plan references but a prerequisite plan owns.

        An object inherited from a plan in `depends_on` (see
        :attr:`~lamindb_setup.evolve.Plan.inherited_dependencies`) has to be
        resolved -- a schema cannot link a borrowed feature without it -- but it
        is not this plan's to manage.  It is therefore adopted rather than
        owned: nothing is written to this plan's lock, so it is never
        drift-checked against a stale copy of the owning plan's configuration,
        never reconciled towards this plan, and never deleted when the owning
        plan stops declaring it.

        This is what makes a change to a shared object a *single* event.  Before,
        every downstream plan kept its own copy of the object's configuration, so
        the owner editing it -- widening a dtype, running a `ctx.migrate`
        narrowing, renaming it -- surfaced again as drift in each downstream
        plan, and a rename could even make a downstream plan delete the object
        the owner had just renamed in place.

        Because a borrowed object is only ever *adopted*, never reconciled, this
        plan can never push an edit to it -- which is what makes it safe not to
        inherit the owner's `ctx.migrate` declarations: the narrowing they
        unblock is only ever performed by the plan that owns the feature.

        An apply that cannot find the object refuses rather than creating it: it
        means the owning plan has not been applied to this instance yet, and the
        object created here under the plan's *current* declaration is what would
        then block the owner's own rename (see :class:`MissingPrerequisiteError`).
        A preview still reports the `CREATE`, so `plan` against a fresh
        instance keeps working, and records the object on
        :attr:`missing_prerequisites`, from which `apply_one` refuses before
        it tracks a run -- unless the owning plan is applied in the same run,
        which creates it first (see :meth:`_created_by_prerequisite`).  Either
        way the change is marked :attr:`~ChangeItem.borrowed`: it is never
        counted as this plan's create.
        """
        if self._created_by_prerequisite(obj, key):
            return self._adopt_created_by_prerequisite(obj, key, kind, config)
        if self._existing(obj) is None:
            if self.mode is ApplyMode.APPLY:
                raise MissingPrerequisiteError(
                    f"{key!r} is owned by a plan this one depends on, but it does "
                    "not exist in this instance. Apply that plan first: creating "
                    "the object here would make it impossible for its owner to "
                    "rename or migrate it later."
                )
            self.missing_prerequisites.append(
                MissingPrerequisite(
                    kind=kind,
                    name=getattr(obj, "name", None),
                    key=key,
                    owner=self.borrowed_owners.get(id(obj)),
                )
            )
        return self._adopt(
            obj,
            key,
            kind,
            config,
            details=["managed by a prerequisite plan"],
            missing_warning=ChangeWarning(
                "owned by a prerequisite plan that has not been applied to this "
                "instance; apply that plan first, or this apply will refuse.",
                WarningCategory.PREREQUISITE,
            ),
            borrowed=True,
        )

    def _adopt(
        self,
        obj: Materializable,
        key: str,
        kind: str,
        config: dict[str, Any],
        *,
        details: list[str] | None = None,
        missing_warning: ChangeWarning | None = None,
        seeded: bool = False,
        borrowed: bool = False,
    ) -> Any:
        """Resolve `obj` without taking ownership: adopt it, else create it.

        Shared by :meth:`_seed` and :meth:`_borrow`, the two ways an object can
        be part of a plan without being managed by it.  Nothing is written to the
        lock (`track=False`), which is what keeps the object out of the drift
        checks, the reconciliation and the removal pass.  The change is still
        recorded so it appears in the diff.

        `missing_warning` is attached to the `CREATE` reported when the
        instance has no match, to flag that the plan is creating something it
        does not manage.

        `seeded` marks a seed-only object this plan owns: its uid is pinned
        and checked against the plan's other instances like a tracked object's,
        and remembered in :attr:`seeded` for the lock's identity-only section.

        `borrowed` marks the recorded change as a prerequisite plan's rather
        than this plan's (see :attr:`~lamindb_setup.evolve.core.ChangeItem.borrowed`).
        """
        existing = self._existing(obj)
        if existing is not None:
            misalignment = None
            updates: list[str] = []
            if seeded:
                self.seeded[key] = str(existing.uid)
                misalignment = self._existing_misalignment(key, existing, seeded=True)
                if migrations := self._migrations_for(key, narrowing=RECORD_UPDATE):
                    updates = self._vet_record_update(obj, existing, migrations)
            self._record(
                obj,
                key,
                kind,
                str(existing.uid),
                config,
                Action.UPDATE if updates else Action.NO_CHANGE,
                details=[*(details or []), *updates],
                track=False,
                misalignment=misalignment,
                borrowed=borrowed,
            )
            return existing
        warnings = [missing_warning] if missing_warning is not None else []
        creation = self._creation_plan(obj, key) if seeded else _CreationPlan()
        if self.mode is ApplyMode.PLAN:
            self._record(
                obj,
                key,
                kind,
                PLACEHOLDER_UID,
                config,
                Action.CREATE,
                details=[*(details or []), *creation.details],
                warnings=[*warnings, *creation.warnings],
                track=False,
                misalignment=creation.misalignment,
                borrowed=borrowed,
            )
            return Placeholder(key=key, kind=kind)
        record = self._commit(obj, key, creation)
        if seeded:
            self.seeded[key] = str(record.uid)
        self._record(
            obj,
            key,
            kind,
            str(record.uid),
            config,
            Action.CREATE,
            details=[*(details or []), *creation.details],
            warnings=[*warnings, *creation.warnings],
            track=False,
            misalignment=creation.misalignment,
        )
        return record

    def _vet_record_update(
        self, obj: Materializable, existing: Any, migrations: list[DataMigration]
    ) -> list[str]:
        """Diff a seeded record against the plan for the updates declared on it.

        Only what the plan declares is compared (see :class:`RecordUpdateContext`),
        and only the aspects some declaration updates.  Returns the change details,
        empty when the record already matches; in APPLY mode the writes are queued
        for after the plan is materialized (see :class:`_DeferredRecordUpdate`),
        in PLAN mode the values they rewrite are remembered for
        :meth:`drop_records` (see :meth:`_holding_links`).
        """
        wanted = set().union(*(_record_aspects(m) for m in migrations))
        fields = {
            attribute: desired
            for attribute in _record_attributes(wanted)
            if (desired := getattr(obj, attribute, None)) is not None
            and desired != getattr(existing, attribute, None)
        }
        values = self._record_value_changes(obj, existing) if "values" in wanted else {}
        if not fields and not values:
            return []
        details = [
            f"{attribute}: {getattr(existing, attribute, None)!r} → {desired!r}"
            for attribute, desired in fields.items()
        ]
        details.extend(
            f"{name}: {_shown_value(live)} → {desired!r}"
            for name, (_feature, live, desired) in values.items()
        )
        details.append(
            "updated via " + ", ".join(f"{m.name} ({m.reason})" for m in migrations)
        )
        if self.mode is ApplyMode.APPLY:
            updates = []
            for migration in migrations:
                aspects = _record_aspects(migration)
                attributes = _record_attributes(aspects)
                context = RecordUpdateContext(
                    record=existing,
                    values={
                        feature: desired
                        for feature, _live, desired in values.values()
                        if "values" in aspects
                    },
                    fields={a: v for a, v in fields.items() if a in attributes},
                )
                updates.append((migration, context))
            self._deferred_record_updates.append(
                _DeferredRecordUpdate(obj_id=id(obj), updates=updates)
            )
        else:
            for feature, _live, desired in values.values():
                if getattr(feature, "pk", None) is not None:
                    self._rewritten_values[(existing.pk, feature.pk)] = (
                        feature,
                        desired,
                    )
        return details

    def _record_value_changes(
        self, obj: Materializable, existing: Any
    ) -> dict[str, tuple[Any, Any, Any]]:
        """The feature values `obj` declares that `existing` does not store.

        Keyed by feature name, each with the feature (live, or a placeholder for
        one the plan has yet to create), the stored value as `get_values()`
        reports it and the declared one.  A sheet's index feature is skipped: its
        value is the record's name, which is how the plan found the record.
        """
        declared = getattr(obj, "features", None) or {}
        if not declared:
            return {}
        live_values = existing.features.get_values()
        index = (
            DEFAULT_MODELS.type_schema_index(existing.type)
            if existing.type_id is not None
            else None
        )
        changes: dict[str, tuple[Any, Any, Any]] = {}
        for declared_key, desired in declared.items():
            if desired is None:
                continue
            feature, name = self._declared_feature(declared_key, natural_key(obj))
            if index is not None and getattr(feature, "uid", None) == index.uid:
                continue
            live = live_values.get(name)
            if not _same_record_value(feature, desired, live):
                changes[name] = (feature, live, desired)
        return changes

    def _declared_feature(self, declared: Any, key: str) -> tuple[Any, str]:
        """The feature a record's `features` key names, with its name.

        A plan `Feature` is resolved like any plan object -- a placeholder when
        PLAN mode has yet to create it.  A name has to match exactly one live
        feature, as it does for lamindb writing the record in the first place.
        """
        if isinstance(declared, Materializable):
            return self.materialize_object(declared), str(declared.name)  # type: ignore[attr-defined]
        import lamindb as ln

        matches = list(ln.Feature.filter(name=declared, is_type=False)[:2])
        if len(matches) != 1:
            problem = "no feature" if not matches else "more than one feature"
            raise DriftError(
                f"cannot update {key!r}: {problem} is named {declared!r}; declare "
                "the value with the plan's Feature object instead of its name."
            )
        return matches[0], str(declared)

    def _data_override_reason(
        self,
        kind: str,
        field: str,
        new: object,
        existing: Any,
    ) -> str | None:
        """Why a taxonomy-`BLOCKED` change is safe for `existing`'s data, or `None`.

        Only consulted after :func:`~lamindb_setup.evolve.engine._drift.classify` has
        already returned `BLOCKED` and only when the operator opted in with
        `allow_data_validated_changes`.  Returns a short human-readable reason
        when the block may be lifted -- either the feature holds no values to
        invalidate, or (for a dtype change) every stored value validates against
        the new dtype -- otherwise `None` to keep the block in force.

        Delegates the actual data question to :meth:`_data_safe_reason`; this
        wrapper only adds the opt-in gate, so the block stays a hard refusal
        unless the operator explicitly allowed data-validated changes.
        """
        if not self.allow_data_validated_changes:
            return None
        return self._data_safe_reason(kind, field, new, existing)

    def _migrations_for(self, key: str, *, narrowing: str = "") -> list[DataMigration]:
        """The narrowing data migrations declared for `key`, in declaration order.

        Looked up by natural key (see :attr:`ApplyContext.migrations`): a match
        unblocks a change the live data would otherwise refuse, letting
        :meth:`_reconcile` make it and the migrations fix the data up.  Several
        may be declared for one object -- a schema making two members required at
        once takes one migration per member -- and they run in the order the plan
        declares them.

        `narrowing` keeps a declaration from answering for a block it does
        nothing about.  A schema takes declarations for two unrelated narrowings
        at once -- members it makes required, members it removes -- and the key
        alone cannot tell them apart, so a `drop_member_values` would otherwise
        read as a backfill and satisfy a block it never touches.  A migration
        that does not name a narrowing (a hand-written one) answers whichever it
        is declared for.
        """
        declared = self.migrations.get(key, [])
        if not narrowing:
            return declared
        return [m for m in declared if m.narrowing in ("", narrowing)]

    def _vet_schema_shape(
        self,
        existing: Any,
        field: str,
        live: Any,
        desired: Any,
        override_notes: list[str],
    ) -> _Refusal | None:
        """Judge changing a schema's `dtype` or `itype` against what it validated.

        Allowed with `allow_data_validated_changes` when the schema validated
        nothing, directly or through a composite; refused otherwise, recommending
        a new schema.
        """
        reason = self._data_override_reason("Schema", field, desired, existing)
        if reason is not None:
            override_notes.append(f"{field}: allowed despite block ({reason})")
            return None
        return _Refusal.of_values(
            field,
            live,
            desired,
            self._override_hint("Schema", field, desired, existing),
        )

    def _vet_set_flag(
        self,
        existing: Any,
        key: str,
        flag: str,
        override_notes: list[str],
    ) -> _Refusal | None:
        """Judge turning a schema's `maximal_set` or `ordered_set` on.

        Each constrains a dataset more when on, so it is refused unless the data
        proves it safe -- with `allow_data_validated_changes` -- or a
        declaration answers it:

        * `maximal_set`: a dataset or a sheet row carrying a column the schema
          does not declare would no longer validate -- proven when there can be
          none (see :meth:`_maximal_set_reason`), vouched for by
          :func:`~lamindb_setup.evolve.accept_maximal_set`;
        * `ordered_set`: the column order of a validated dataset is not
          reliably recorded, so it is proven only when the schema validated
          nothing, and vouched for by :func:`~lamindb_setup.evolve.accept_ordered_set`.

        `minimal_set` is judged against the members the plan leaves, once they
        are known (see :meth:`_vet_minimal_set`).
        """
        narrowing = MAXIMAL_SET if flag == "maximal_set" else ORDERED_SET
        declared = self._migrations_for(key, narrowing=narrowing)
        if declared:
            override_notes.append(
                f"{flag}: turned on, vouched for by "
                + ", ".join(f"{m.name} ({m.reason})" for m in declared)
            )
            return None
        if flag == "ordered_set":
            reason = _index_safe_reason(self._schema_usage(existing))
            unproven = "the column order of the data it validated is not recorded"
        else:
            reason, unproven = self._maximal_set_reason(existing)
        if reason is not None and self.allow_data_validated_changes:
            override_notes.append(f"{flag}: allowed despite block ({reason})")
            return None
        vouch = f"accept_{flag}(reason=...)"
        other = self._other_narrowing_hint(key, narrowing)
        if reason is not None:
            return _Refusal.of_values(
                flag,
                False,
                True,
                f" (could be applied with --allow-data-validated-changes: "
                f"{reason}; or declare a ctx.migrate with {vouch})" + other,
            )
        return _Refusal.of_values(
            flag,
            False,
            True,
            f" ({unproven}; declare a ctx.migrate with {vouch})" + other,
        )

    def _maximal_set_reason(self, existing: Any) -> tuple[str | None, str]:
        """Why turning `maximal_set` on invalidates nothing, or what it may.

        A sheet row's values are all stored, so the rows are checked for one held
        for a feature the schema does not declare.  A dataset's columns are not:
        lamindb records only the columns a schema that is not `flexible`
        declares, so a column it does not declare leaves no trace and the proof
        needs the schema to have validated no dataset.  A sheet's exports are
        judged as such (see :meth:`check_sheet_exports`).
        """
        from ._drift_data import schema_nonmember_rows, schema_validated_usage

        usage = schema_validated_usage(existing, exclude_sheet_exports=True)
        rows = schema_nonmember_rows(existing)
        if usage is None or rows is None:
            return None, "what it validated could not be read"
        datasets = _datasets_only(usage)
        if not datasets.total and not rows.rows:
            return "nothing it validated carries a column it does not declare", ""
        found = []
        if datasets.total:
            found.append(
                f"{_describe_usage(datasets)} validated against it may carry a "
                "column it does not declare, which is not recorded and cannot be "
                "re-checked in place"
            )
        if rows.rows:
            verb = "holds" if rows.rows == 1 else "hold"
            found.append(
                f"{_plural(rows.rows, 'sheet row')} {verb} a value for a feature it "
                "does not declare (" + ", ".join(rows.examples) + ")"
            )
        return None, "; ".join(found)

    def _minimal_set_members(
        self,
        obj: Materializable,
        existing: Any,
        *,
        members: list[Any] | None,
        feature_removals: list[Any],
    ) -> list[Any]:
        """The live members turning `minimal_set` on makes required.

        Read off the shape the plan leaves: a member it removes is no member, and
        one it marks optional stays omissible -- as does one the instance marks
        optional, which :meth:`_vet_optional_drops` judges when the plan stops
        marking it.  The index is left out, since a sheet row keeps it on
        `Record.name` rather than in an annotation, and so are the members the
        plan adds, which :meth:`_vet_member_adds` judges.
        """
        live_members = (
            members if members is not None else self._schema_members(existing)
        )
        optionals = getattr(existing, "optionals", None)
        omissible = set(optionals.get_uids()) if optionals is not None else set()
        omissible |= {str(m.uid) for m in feature_removals}
        for feature in getattr(obj, "optionals", None) or ():
            live = self._existing(feature)
            if live is not None:
                omissible.add(str(live.uid))
        index = self._plan_index_uid(obj)
        return [
            m
            for m in live_members
            if is_live(m) and str(m.uid) not in omissible and str(m.uid) != index
        ]

    def _vet_minimal_set(
        self,
        existing: Any,
        key: str,
        obj_id: int,
        required: list[Any],
        override_notes: list[str],
    ) -> _Refusal | None:
        """Judge turning `minimal_set` on; the refusal, or `None` to apply it.

        `required` are the members it makes required (see
        :meth:`_minimal_set_members`), each the optional-to-required narrowing,
        judged like a required member the plan adds (see :meth:`_vet_member_adds`):

        * the rows of a sheet using the schema: proven when none omits it (with
          `allow_data_validated_changes`), or backfilled by a declared
          :func:`~lamindb_setup.evolve.fill_missing` (see
          :meth:`_vet_optional_drops`) -- which a schema that validated nothing
          at all takes the flag for;
        * datasets (artifacts, collections): a column leaves no annotation to
          count and a backfill cannot add one to a file, so the plan vouches for
          them with :func:`~lamindb_setup.evolve.accept_minimal_set`, which the
          flag does not replace.

        A sheet's exports are judged as such (see :meth:`check_sheet_exports`).
        """
        if not required:
            return None
        from ._drift_data import schema_validated_usage

        usage = schema_validated_usage(existing, exclude_sheet_exports=True)
        if usage is None:
            return _Refusal.of_values(
                "minimal_set",
                False,
                True,
                " (what the schema validated could not be established)",
            )
        blocked: list[str] = []
        datasets = _datasets_only(usage)
        if datasets.total:
            vouches = self._migrations_for(key, narrowing=MINIMAL_SET)
            if vouches:
                override_notes.append(
                    f"minimal_set: {_describe_usage(datasets)} validated while its "
                    "members were not required, vouched for by "
                    + ", ".join(f"{m.name} ({m.reason})" for m in vouches)
                )
            else:
                blocked.append(
                    f"minimal_set: {_describe_usage(datasets)} validated while its "
                    "members were not required cannot be re-checked in place "
                    "(declare accept_minimal_set(reason=...) on the schema to vouch "
                    "for them)" + self._other_narrowing_hint(key, MINIMAL_SET)
                )
        if usage.rows or not datasets.total:
            drops = [
                (member, f"minimal_set: member {member.name!r} becomes required")
                for member in required
            ]
            _, omitted = self._vet_optional_drops(
                existing,
                key,
                obj_id,
                drops,
                override_notes,
                label="minimal_set: member",
                registries=_SHEET_ROWS,
            )
            blocked.extend(omitted)
        if not blocked:
            return None
        return _Refusal.of_text("minimal_set", "\n  ".join(blocked))

    def _vet_slot_changes(
        self,
        obj: Materializable,
        existing: Any,
        key: str,
        override_notes: list[str],
    ) -> tuple[_SlotChange | None, list[str]]:
        """Judge the plan's slots for a composite schema against the instance's.

        Dropping a slot stops checking that part of a dataset, which only admits
        more, and so does swapping a component for one that admits more (see
        :meth:`_swap_widens`).  Adding a slot, or swapping in a component that
        admits less, judges the datasets the composite already validated by
        something they were never checked against: allowed when it validated
        nothing and `allow_data_validated_changes` is set, or when a declared
        :func:`~lamindb_setup.evolve.accept_slot_change` vouches for them.

        Returns the change to make (`None` when the slots match) and the
        refusals.
        """
        if not self._may_have_slots(obj, key, existing):
            return None, []
        live = self._live_slots_of(existing)
        components = getattr(obj, "slots", None) or {}
        desired = {
            slot: self.materialize_object(component)
            for slot, component in components.items()
        }
        labels = {
            slot: natural_key(component) for slot, component in components.items()
        }
        removed = sorted(slot for slot in live if slot not in desired)
        added = sorted(slot for slot in desired if slot not in live)
        swapped = sorted(
            slot
            for slot in desired
            if slot in live and getattr(desired[slot], "pk", None) != live[slot].pk
        )
        if not (removed or added or swapped):
            return None, []
        details = [f"slot {slot!r} removed" for slot in removed]
        details += [f"slot {slot!r} \u2192 {labels[slot]!r}" for slot in added]
        details += [
            f"slot {slot!r} {_schema_label(live[slot])!r} \u2192 {labels[slot]!r}"
            for slot in swapped
        ]
        change = _SlotChange(
            removed=removed,
            added=added,
            swapped=swapped,
            desired=desired,
            details=details,
        )
        narrowing = added + [
            slot
            for slot in swapped
            if not self._swap_widens(live[slot], components[slot], desired[slot])
        ]
        if not narrowing:
            return change, []
        declared = self._migrations_for(key, narrowing=SLOT_CHANGE)
        named = ", ".join(repr(slot) for slot in narrowing)
        if declared:
            override_notes.append(
                f"slots: {named} accepted by "
                + ", ".join(f"{m.name} ({m.reason})" for m in declared)
            )
            return change, []
        reason = _index_safe_reason(self._schema_usage(existing))
        if reason is not None and self.allow_data_validated_changes:
            override_notes.append(f"slots: allowed despite block ({reason})")
            return change, []
        hint = (
            f"could be applied with --allow-data-validated-changes: {reason}"
            if reason is not None
            else "the datasets it validated were never checked against them; "
            "declare a ctx.migrate with accept_slot_change(reason=...) to vouch "
            "for them"
        )
        return None, [f"slots: slot {named} would judge validated data ({hint})"]

    def _swap_widens(self, old: Any, component: Materializable, resolved: Any) -> bool:
        """Whether swapping the live `old` for the plan's `component` admits more.

        `component` is judged as this apply leaves it (see
        :meth:`_planned_component_shape`), so a PLAN-mode preview -- holding only
        a placeholder for a component the plan creates, and the unedited record
        of one it edits -- reaches the verdict APPLY mode does.  APPLY mode also
        judges `resolved`, the record as it now is on the instance.
        """
        before = self._live_component_shape(old)
        if not _component_widens(before, self._planned_component_shape(component)):
            return False
        if self.mode is not ApplyMode.APPLY:
            return True
        return _component_widens(before, self._live_component_shape(resolved))

    def _live_component_shape(self, schema: Any) -> _ComponentShape:
        """A live schema's :class:`_ComponentShape`."""
        return _ComponentShape(
            members=tuple(str(m.uid) for m in self._schema_members(schema)),
            optionals=frozenset(str(uid) for uid in schema.optionals.get_uids()),
            index=getattr(schema, "_index_feature_uid", None),
            maximal_set=bool(schema.maximal_set),
            minimal_set=bool(schema.minimal_set),
            ordered_set=bool(schema.ordered_set),
            flexible=bool(schema.flexible),
            otype=schema.otype,
            itype=schema.itype,
            dtype=schema.dtype,
        )

    def _planned_component_shape(self, component: Materializable) -> _ComponentShape:
        """A plan schema's :class:`_ComponentShape` once this apply is done with it.

        The reconcile drives a schema's members, optionals, index and set flags
        to the plan's or refuses (as :meth:`_predicted_schema_hash` relies on),
        so those are read off the plan.  `flexible`, `otype`, `itype` and
        `dtype` are the plan's where it states them, and otherwise the record's
        from before this run -- or, for a schema the run creates, lamindb's
        default: a schema of features is not flexible, has the `Feature`
        itype and no dtype.  A schema a prerequisite plan owns is never
        reconciled here, so it is read off the instance.
        """
        before = self._existing(component)
        if before is not None and not self.owns(component):
            return self._live_component_shape(before)
        declared = _declared_members(component)
        index = getattr(component, "index", None)
        flexible = getattr(component, "flexible", None)
        if flexible is None:
            flexible = before.flexible if before is not None else not declared
        otype = getattr(component, "otype", None)
        if otype is None and before is not None:
            otype = before.otype
        itype = getattr(component, "itype", None)
        if itype is not None:
            itype = DEFAULT_MODELS.itype_str(itype)
        elif before is not None:
            itype = before.itype
        elif declared:
            itype = "Feature"
        dtype = serialize_config(component).get("dtype")
        if dtype is None:
            if before is not None:
                dtype = before.dtype
            elif not (itype or "").startswith("Feature"):
                dtype = _UNRESOLVED
        return _ComponentShape(
            members=tuple(self._member_key(f) for f in declared),
            optionals=frozenset(
                self._member_key(f) for f in getattr(component, "optionals", None) or ()
            ),
            index=self._member_key(index) if index is not None else None,
            maximal_set=bool(getattr(component, "maximal_set", False)),
            minimal_set=bool(getattr(component, "minimal_set", False)),
            ordered_set=bool(getattr(component, "ordered_set", False)),
            flexible=bool(flexible),
            otype=otype,
            itype=itype,
            dtype=dtype,
        )

    def _member_key(self, feature: Materializable) -> str:
        """How a :class:`_ComponentShape` names a plan feature: its uid, or its key."""
        live = self._live_record(feature)
        return str(live.uid) if live is not None else natural_key(feature)

    def _may_have_slots(self, obj: Materializable, key: str, existing: Any) -> bool:
        """Whether `existing` may be a composite, by the plan, the lock or its hash.

        Reading a schema's slots costs a query, so only a schema the plan gives
        slots -- or whose lock entry recorded some, which the plan may be
        dropping -- is asked, and one whose content hash moved since the lock
        recorded it, as a slot added to it outside the plan moves it; every
        other schema has none to compare.  Decided once per run, before the
        apply rehashes the schema itself.
        """
        if getattr(obj, "slots", None):
            return True
        if key not in self._slotted:
            previous = self.lock.entry_for(key) if self.lock is not None else None
            stored = (previous.remote_snapshot or {}) if previous is not None else {}
            live_hash = getattr(existing, "hash", None)
            self._slotted[key] = previous is not None and (
                bool(previous.config.get("slots"))
                or ("hash" in stored and stored["hash"] != live_hash)
            )
        return self._slotted[key]

    def _live_slots_of(self, existing: Any) -> dict[str, Any]:
        """`existing`'s live `{slot: component}`, read once per run."""
        if existing.pk not in self._slots_cache:
            self._slots_cache[existing.pk] = _live_slots(existing)
        return self._slots_cache[existing.pk]

    def _slot_uids(
        self, obj: Materializable, key: str, existing: Any
    ) -> dict[str, str | None]:
        """`existing`'s slots as the lock snapshot records them (see :func:`serialize_existing`)."""
        if not self._may_have_slots(obj, key, existing):
            return {}
        return {
            slot: str(component.uid)
            for slot, component in self._live_slots_of(existing).items()
        }

    def _value_migrations_for(self, key: str) -> list[DataMigration]:
        """The migrations answering for a deleted feature's stored values.

        A :func:`~lamindb_setup.evolve.detach_from_schemas` declared on the same
        key answers for the schemas listing the feature, not for its values.
        """
        return [m for m in self._migrations_for(key) if m.narrowing != DETACH_MEMBER]

    def _dtype_migrations_for(self, key: str) -> list[DataMigration]:
        """The migrations declared for `key` that may answer a dtype change.

        A feature also takes declarations for its `nullable` and its `unit`,
        which do nothing about its dtype; only an untagged migration -- a dtype
        helper, or a hand-written one -- answers it.
        """
        return [m for m in self._migrations_for(key) if not m.narrowing]

    def _vet_unit_change(
        self,
        obj: Materializable,
        existing: Any,
        key: str,
        desired_dtype: str | None,
        override_notes: list[str],
        pending_migrations: list[_PendingMigration],
    ) -> _Refusal | None:
        """Refuse a feature's new unit while values are stored, unless answered.

        A stored number is a quantity in the feature's unit, so a new unit
        silently reinterprets every one -- `1500` under `mg` reads as
        `1500 g` under `g`.  With no value stored the unit changes in place.
        Otherwise a declared :func:`~lamindb_setup.evolve.rescale` rewrites the
        values for the new unit -- deferred like a dtype migration, with its
        post-check -- or :func:`~lamindb_setup.evolve.accept_unit_change` vouches
        that they already are in it.  No proof replaces either: the data cannot
        say which unit it was meant in.
        """
        from ._drift_data import feature_has_any_stored_value

        desired = _desired_value(obj, "unit")
        live = getattr(existing, "unit", None)
        if desired is None or desired == live:
            return None
        presence = self._categorical_presence_for(existing)
        if not feature_has_any_stored_value(existing, has_links=presence):
            return None
        migrations = self._migrations_for(key, narrowing=UNIT_CHANGE)
        if not migrations:
            return _Refusal.of_values(
                "unit",
                live,
                desired,
                " (its stored values would silently read in the new unit. If the "
                "unit was only mislabelled or misspelled, declare a ctx.migrate "
                "with accept_unit_change(reason=...); to convert the values, "
                "declare rescale(factor))",
            )
        rescales = [m for m in migrations if not m.keeps_values]
        vouches = [m for m in migrations if m.keeps_values]
        if vouches:
            override_notes.append(
                "unit: stored values kept as they are, vouched for by "
                + ", ".join(f"{m.name} ({m.reason})" for m in vouches)
            )
        if rescales:
            live_dtype = getattr(existing, "dtype_as_str", None)
            override_notes.append(
                "unit: stored values rescaled by "
                + ", ".join(f"{m.name} ({m.reason})" for m in rescales)
            )
            pending_migrations.extend(
                _PendingMigration(
                    migration=m,
                    desired_dtype=desired_dtype or live_dtype or "",
                    live_dtype=live_dtype,
                    target_registry=None,
                    cat_filters=None,
                )
                for m in rescales
            )
        return None

    def _other_narrowing_hint(self, key: str, narrowing: str) -> str:
        """Name the declarations on `key` that answer a *different* narrowing.

        Without it a plan that declared a migration on the schema is told to
        declare one, which reads as the engine not having seen it.
        """
        others = [
            m
            for m in self.migrations.get(key, [])
            if m.narrowing not in ("", narrowing)
        ]
        if not others:
            return ""
        names = ", ".join(repr(m.name) for m in others)
        verb = "is" if len(others) == 1 else "are"
        return f" ({names} {verb} declared here, but answers another narrowing)"

    def _dtype_change_needs_relocation(
        self, desired_dtype: object, existing: Any
    ) -> bool:
        """Whether a dtype change must move its values to another store first.

        A categorical dtype reads its values from foreign-key links in
        per-registry tables, a scalar one from the `JsonValue` / `RecordJson`
        rows, and an in-place `_dtype_str` flip moves nothing.  So two changes
        hold the data in the wrong store afterwards, whatever the data proves:

        * a `cat[...] -> str` widening, *revalidation*-safe (every label renders
          as its name) and so :attr:`~lamindb_setup.evolve.engine._drift.Verdict.SAFE`
          to the pure taxonomy, whose links a scalar dtype no longer reads; and
        * a `<scalar> -> cat[...]` change, whose stored strings lamindb then
          refuses to read at all -- even when each is a label of the new dtype.

        A change of shape within the scalar store -- `str -> list[str]`,
        `list[int] -> int` -- leaves every value in a shape lamindb's dtype
        check refuses, so it has to rewrite them just the same.

        Returns `True` (the change must be held back for a migration that
        relocates the values) only when the feature holds values in the store or
        shape the old dtype uses.  Every other change keeps its values where the
        new dtype reads them, and returns `False`.

        The dtype tests come first deliberately: they are free, and they keep the
        (batched, but still non-trivial) presence reads off every plan with no
        such change at all.
        """
        from ._drift_data import (
            _is_categorical_feature,
            _list_element_dtype,
            _target_is_categorical,
            feature_has_any_categorical_value,
            feature_has_any_value,
        )

        desired = desired_dtype if isinstance(desired_dtype, str) else None
        if canonical_dtype_str(desired) == "str" and feature_has_any_categorical_value(
            existing, has_values=self._categorical_presence_for(existing)
        ):
            return True
        if desired is None or _is_categorical_feature(existing):
            return False
        if not _target_is_categorical(desired):
            live = getattr(existing, "dtype_as_str", None)
            was_list = _list_element_dtype(live) is not None
            if was_list == (_list_element_dtype(desired) is not None):
                return False
        return feature_has_any_value(existing, values=self._scalar_values_for(existing))

    def _data_safe_reason(
        self,
        kind: str,
        field: str,
        new: object,
        existing: Any,
    ) -> str | None:
        """Why a taxonomy-`BLOCKED` change is safe for `existing`'s live data.

        The flag-independent core of :meth:`_data_override_reason`: it answers the
        data question directly, ignoring `allow_data_validated_changes`.  So it
        can both *lift* a block (via :meth:`_data_override_reason`, when the
        operator opted in) and *explain* one that is still in force (a `plan` /
        `apply` without the flag reports that the flag would unblock the change;
        see :meth:`_override_hint`).

        Returns a short reason when the block is data-safe -- the feature holds no
        values to invalidate, or (for a dtype change) every stored value validates
        against the new dtype -- otherwise `None`.

        Scoped to features -- only a `Feature` carries per-record values that a
        change could invalidate -- with two exceptions: a schema's `index`, which
        is safe exactly when the schema has validated nothing (see
        :func:`_index_safe_reason`), and turning its `flexible` off, which is
        safe when it has validated no dataset (see :func:`_flexible_safe_reason`).
        Categorical values are not introspected (see
        :mod:`lamindb_setup.evolve.engine._drift_data`), so a categorical feature is
        always treated conservatively and stays blocked.

        Identity (:data:`_IDENTITY_KEYS`) is never data-safe, whatever the
        values: a rename is answered by a declared `ctx.moved` and nothing
        applies a parent change, so lifting either would record the plan's
        address in the lock while the instance kept its own.
        """
        if field in _IDENTITY_KEYS:
            return None
        if kind == "Feature" and field == "nullable":
            return _nullable_safe_reason(self._nullability_usage(existing))
        if kind == "Schema" and field == "index":
            return _index_safe_reason(self._schema_usage(existing))
        if kind == "Schema" and field == "flexible":
            return _flexible_safe_reason(self._schema_usage(existing))
        if kind == "Schema" and field in _SCHEMA_SHAPE_KEYS:
            # What a dataset is and what its members are: nothing is re-checked
            # against another one in place, so only a schema that validated
            # nothing takes it.
            return _index_safe_reason(self._schema_usage(existing))
        if kind != "Feature":
            return None
        from ._drift_data import (
            _is_categorical_feature,
            feature_has_any_stored_value,
            feature_has_any_value,
            feature_value_count,
            feature_values_all_validate,
        )

        values = self._scalar_values_for(existing)
        categorical = _is_categorical_feature(existing)
        stored = (
            # Looked at, not presumed: the links say whether it holds any value.
            feature_has_any_stored_value(
                existing, has_links=self._categorical_presence_for(existing)
            )
            if categorical
            else feature_has_any_value(existing, values=values)
        )
        if not stored:
            return "feature has no stored values"
        if field != "dtype" or not isinstance(new, str):
            return None
        if categorical:
            return self._categorical_safe_reason(existing, new)
        if feature_values_all_validate(
            existing, new, values=values, cat_cache=self._cat_label_cache
        ):
            count = feature_value_count(existing, values=values)
            plural = "value" if count == 1 else "values"
            return f"all {count} stored {plural} validate against the new dtype"
        return None

    def _categorical_safe_reason(self, existing: Any, new: str) -> str | None:
        """Why a categorical feature's stored labels all fit `new`, or `None`.

        A categorical value is a link to a label record, so it validates against
        another categorical dtype when the dtype admits that record -- its
        registry, and the type (subtypes included) and filters it names (see
        :func:`~lamindb_setup.evolve.engine._drift_data.feature_inadmissible_link_count`).
        That covers dropping a union member no link uses, and moving a filter to
        a type every label sits under.  Turning a list into a scalar is left out:
        admitting each label says nothing about how many a record links.
        """
        from ._drift_data import (
            _raw_scalar_value_count,
            categorical_value_count,
            feature_inadmissible_link_count,
        )

        live = getattr(existing, "dtype_as_str", None) or ""
        if live.startswith("list[") and not new.startswith("list["):
            return None
        if _raw_scalar_value_count(existing) != 0:
            return None
        if feature_inadmissible_link_count(existing, new) != 0:
            return None
        count = categorical_value_count(existing)
        if not count:
            return None
        plural = "value is a label" if count == 1 else "values are labels"
        return f"all {count} stored categorical {plural} the new dtype admits"

    def _override_hint(
        self,
        kind: str,
        field: str,
        new: object,
        existing: Any,
    ) -> str:
        """A parenthetical explaining a blocked change against the live data, else `""`.

        Two mutually exclusive notes, appended to a refused drift message:

        * when the operator has *not* set `allow_data_validated_changes` and the
          live data proves the change safe (see :meth:`_data_safe_reason`), that
          re-running with `--allow-data-validated-changes` would apply it in
          place; and
        * otherwise, for a dtype change the data makes genuinely unsafe, how many
          stored values block it *and what they look like* (e.g. `3 of 5 stored
          values do not validate against the new dtype; 2 distinct invalid values,
          e.g. 'n/a', 'unknown'`) -- reported regardless of the flag, since a
          change that stays blocked with the flag on is exactly this case.

        Empty when neither applies: the flag is off but the data does not prove the
        change safe *and* the failing count cannot be introspected (a categorical
        source feature, a non-dtype field, or an unsupported target dtype).
        """
        if (
            kind == "Feature"
            and field == "dtype"
            and self._dtype_change_needs_relocation(new, existing)
        ):
            return self._relocation_hint(new, existing)
        if not self.allow_data_validated_changes:
            reason = self._data_safe_reason(kind, field, new, existing)
            if reason is not None:
                return (
                    f" (could be applied with --allow-data-validated-changes: {reason})"
                )
        if kind == "Feature" and field == "nullable":
            return _nullable_hint(self._nullability_usage(existing))
        if kind == "Schema" and field in _SCHEMA_SHAPE_KEYS:
            return (
                " (the schema has validated data, which cannot be re-checked against "
                f"another {field} in place; declare a new schema and point the "
                "sheets and artifacts at it instead)"
            )
        if kind == "Feature" and field == "dtype" and isinstance(new, str):
            from ._drift_data import feature_value_failures

            failures = feature_value_failures(
                existing,
                new,
                values=self._scalar_values_for(existing),
                cat_cache=self._cat_label_cache,
            )
            if failures is not None and failures.failing:
                noun = "value" if failures.total == 1 else "values"
                verb = "does not" if failures.failing == 1 else "do not"
                return (
                    f" ({failures.failing} of {failures.total} stored {noun} "
                    f"{verb} validate against the new dtype; "
                    f"{_format_failing_examples(failures)})"
                )
            return self._inadmissible_links_hint(new, existing)
        return ""

    def _inadmissible_links_hint(self, new: str, existing: Any) -> str:
        """How many of a categorical feature's labels a `cat -> cat` change strands."""
        from ._drift_data import (
            _is_categorical_feature,
            _target_is_categorical,
            feature_inadmissible_link_count,
        )

        if not (_is_categorical_feature(existing) and _target_is_categorical(new)):
            return ""
        count = feature_inadmissible_link_count(existing, new)
        if not count:
            return ""
        if count == 1:
            stranded, them = "1 stored categorical value is a label", "it"
        else:
            stranded, them = f"{count} stored categorical values are labels", "them"
        return (
            f" ({stranded} the new dtype does not admit; declare a ctx.migrate "
            f"(e.g. remap_records()) to relocate {them})"
        )

    def _vet_record_type_move(
        self, obj: Materializable, existing: Any, key: str
    ) -> None:
        """Refuse moving a record type out from under a type whose features hold it.

        A `cat[Record[<type>]]` feature admits the type's whole subtree, as
        lamindb validates it, so a record type moved out from under an ancestor
        takes its subtree -- itself, its subtypes and their records -- out of
        every such feature's dtype.  The move goes ahead when none of them
        stores a value there: nothing it admitted is lost.  A feature whose
        dtype also names a type the subtree stays below, or the registry
        unfiltered, keeps admitting it and is not asked.
        """
        from ._drift_data import type_subtree_link_count
        from ._lock import registry_for_kind

        staying = self._planned_ancestor_uids(obj)
        lost = [t for t in _live_ancestors(existing) if str(t.uid) not in staying]
        if not lost:
            return
        registry = type(existing)
        features = registry_for_kind("Feature").objects.exclude(branch_id=-1)
        seen: set[Any] = set()
        held: list[str] = []
        for ancestor in lost:
            for feature in features.filter(_dtype_str__contains=str(ancestor.uid)):
                if feature.pk in seen or _admits_below(feature, registry, staying):
                    continue
                seen.add(feature.pk)
                count = type_subtree_link_count(feature, existing)
                where = f"feature {feature.name!r} ({feature.dtype_as_str})"
                if count is None:
                    held.append(f"the values of {where}, which cannot be read")
                elif count:
                    noun = "value" if count == 1 else "values"
                    held.append(f"{count} stored {noun} of {where}")
        if held:
            names = ", ".join(repr(t.name) for t in lost)
            kind = type(obj).__name__
            raise PlanChangeRefusedError(
                f"cannot move {kind} {getattr(obj, 'name', None)!r} ({key}) out from "
                f"under {names}: that takes it and everything below it out of what "
                f"a feature filtered on {names} admits, yet values point there: "
                f"{'; '.join(held)}. Relocate or remove them first, or keep the "
                "record type where it is."
            )

    def _planned_ancestor_uids(self, obj: Materializable) -> set[str]:
        """The uids of the existing types the plan puts `obj` below.

        Walks the plan's parent chain rather than the instance's, since the plan
        may move an ancestor too; a parent the plan has yet to create has no
        records below it and is skipped.
        """
        uids: set[str] = set()
        seen: set[int] = set()
        parent = getattr(obj, "type", None)
        while parent is not None and id(parent) not in seen:
            seen.add(id(parent))
            live = self.materialize_object(parent)
            if not isinstance(live, Placeholder) and getattr(live, "uid", None):
                uids.add(str(live.uid))
            parent = getattr(parent, "type", None)
        return uids

    def _vet_sheet_schema(
        self,
        existing: Any,
        key: str,
        live: Any,
        desired: Any,
        planned: Materializable,
        override_notes: list[str],
    ) -> _Refusal | None:
        """Judge pointing a sheet at another schema against the rows it holds.

        The schema decides what every row must carry -- a record type promoted to
        a sheet (`ctx.moved(Sheet(...), "RecordType:X")`) gains one the same
        way.  A sheet with no row takes the new schema in place: nothing was
        validated, so nothing breaks.  With rows -- every one not in the trash
        (see :func:`~lamindb_setup.evolve.engine._drift_data.sheet_rows`) -- each
        must validate against the new schema, which
        `allow_data_validated_changes` or a declared
        :func:`~lamindb_setup.evolve.revalidate_rows` asks for:

        * `planned`, the plan's schema, declares the live schema's index: a
          new one would re-key every row's name, which is not done here;
        * every row carries a value for each member `planned` requires (see
          :meth:`_sheet_rows_omitting`); and
        * lamindb's curator accepts the rows (see
          :func:`~lamindb_setup.evolve.engine._drift_data.sheet_rows_validate`).

        The first two are read off the plan, so a dry run answers them as the
        apply does.  The last needs `desired` as the plan leaves it, so a
        schema this plan has yet to create -- or edits, which a dry run has not
        done -- is curated against by the apply only.
        """
        from ._drift_data import sheet_rows, sheet_rows_validate

        desired_label = natural_key(planned)

        def refuse(note: str) -> _Refusal:
            return _Refusal.of_values(
                "schema", _schema_label(live), desired_label, note
            )

        rows = sheet_rows(existing).count()
        if not rows:
            return None
        counted = _plural(rows, "row")
        live_index = getattr(getattr(live, "index", None), "uid", None)
        if self._plan_index_uid(planned) != (
            None if live_index is None else str(live_index)
        ):
            return refuse(
                f" (the {counted} of the sheet are keyed by the old schema's index "
                "and would have to be re-keyed by the new one, which is not done in "
                "place)"
            )
        omitted = self._sheet_rows_omitting(existing, planned)
        if omitted is None:
            return refuse(
                f" (whether the {counted} of the sheet carry the members it "
                "requires could not be established)"
            )
        if omitted:
            return refuse(
                f" (the {counted} of the sheet do not all validate against it: "
                + "; ".join(omitted)
                + ")"
            )
        declared = self._migrations_for(key, narrowing=SHEET_SCHEMA)
        unmade = (
            "creates"
            if isinstance(desired, Placeholder)
            else "edits"
            if self._unwritten_edit(planned)
            else None
        )
        if unmade is not None:
            if declared or self.allow_data_validated_changes:
                override_notes.append(
                    f"schema: the {counted} of the sheet are validated against "
                    f"{desired_label!r} once the apply {unmade} it"
                )
                return None
            return refuse(
                f" (the sheet holds {counted}; declare a ctx.migrate with "
                "revalidate_rows() to have the apply check them against the new "
                "schema)"
            )
        check = sheet_rows_validate(existing, desired)
        if check is None or check.rows != rows:
            return refuse(
                f" (whether the {counted} of the sheet validate against it could "
                "not be established)"
            )
        if check.failure is not None:
            return refuse(
                f" (the {counted} of the sheet do not all validate against it: "
                f"{check.failure})"
            )
        fits = (
            "the sheet's row validates against it"
            if rows == 1
            else f"all {counted} of the sheet validate against it"
        )
        if declared:
            override_notes.append(
                f"schema: {fits}, checked by "
                + ", ".join(f"{m.name} ({m.reason})" for m in declared)
            )
            return None
        if self.allow_data_validated_changes:
            override_notes.append(f"schema: allowed despite block ({fits})")
            return None
        return refuse(
            f" (could be applied with --allow-data-validated-changes: {fits}; or "
            "declare a ctx.migrate with revalidate_rows())"
        )

    def _sheet_rows_omitting(
        self, existing: Any, planned: Materializable
    ) -> list[str] | None:
        """What the rows of sheet `existing` omit of the members `planned` requires.

        lamindb's curator sees a column, which the export fills with blanks for a
        member a row has no value for, so a required member the rows lack passes
        it.  Each is counted per row instead, as making a member required in
        place is (see
        :func:`~lamindb_setup.evolve.engine._drift_data.sheet_member_omissions`).
        Read off the plan: a member it has yet to create is carried by no row.

        One description per member some row omits; `None` when the rows could
        not be queried.
        """
        from ._drift_data import sheet_member_omissions

        if not getattr(planned, "minimal_set", True):
            return []
        index = getattr(planned, "index", None)
        optional = {id(f) for f in getattr(planned, "optionals", None) or ()}
        found: list[str] = []
        for member in _declared_members(planned):
            if member is index or id(member) in optional:
                continue
            omissions = sheet_member_omissions(existing, self._live_record(member))
            if omissions is None:
                return None
            if omissions.omitting:
                found.append(_omission(getattr(member, "name", None), omissions))
        return found

    def _unwritten_edit(self, obj: Materializable) -> bool:
        """Whether a dry run reads `obj` as it stands before the plan's edits.

        PLAN mode writes nothing, so a live object the run updates still has its
        old state; APPLY mode has made the edits by the time a dependent reads it.
        """
        return self.mode is ApplyMode.PLAN and any(
            change.obj_id == id(obj) and change.action is not Action.NO_CHANGE
            for change in self.changes
        )

    def _demotes_a_sheet(self, kind: str, key: str) -> bool:
        """Whether the plan turns the sheet at `key` into a plain record type.

        Declared with `ctx.moved(RecordType("X"), "Sheet:X")`: the move re-keys
        the sheet's lock entry to the record type, so the entry it hands over
        was recorded as a sheet.  Once applied the lock records the record type
        and the move is a no-op.
        """
        if kind != "RecordType" or key not in self.moved_keys or self.lock is None:
            return False
        previous = self.lock.entry_for(key)
        return previous is not None and previous.kind == "Sheet"

    def _relocation_hint(self, new: object, existing: Any) -> str:
        """Why a dtype change waits for a migration moving its values, and to where.

        A change to a categorical dtype says how the stored scalar values fit it
        too: whether a migration relocating them (`map_to_records`) would find a
        label for each, or which ones it would have to map (see
        :func:`~lamindb_setup.evolve.engine._drift_data.feature_value_failures`).
        """
        from ._drift_data import (
            _is_categorical_feature,
            _target_is_categorical,
            feature_value_failures,
        )

        scalar_source = not _is_categorical_feature(existing)
        if isinstance(new, str) and scalar_source and not _target_is_categorical(new):
            return self._reshape_hint(new, existing)
        if not (isinstance(new, str) and _target_is_categorical(new)):
            return (
                " (categorical values must be relocated to the scalar store; "
                "declare a ctx.migrate to do so)"
            )
        failures = feature_value_failures(
            existing,
            new,
            values=self._scalar_values_for(existing),
            cat_cache=self._cat_label_cache,
        )
        relocate = (
            "scalar values must be relocated to the categorical store; declare a "
            "ctx.migrate (e.g. map_to_records({})) to do so"
        )
        if failures is None or not failures.total:
            return f" ({relocate.format('')})"
        if not failures.failing:
            fit = (
                "the stored value is a label"
                if failures.total == 1
                else f"all {failures.total} stored values are labels"
            )
            return f" ({fit} the new dtype admits, but {relocate.format('')})"
        noun = "value" if failures.total == 1 else "values"
        verb = "is not a label" if failures.failing == 1 else "are not labels"
        return (
            f" ({failures.failing} of {failures.total} stored {noun} {verb} the "
            f"new dtype admits, {_format_failing_examples(failures)}; "
            f"{relocate.format('mapping=...')})"
        )

    def _reshape_hint(self, new: str, existing: Any) -> str:
        """Why a change between a single value and a list waits for a migration.

        Turning values into lists says whether each would be a valid element;
        turning lists into single values says how many hold more than one.
        """
        from ._drift_data import (
            _iter_scalar_values,
            _list_element_dtype,
            feature_value_failures,
        )

        values = self._scalar_values_for(existing)
        element = _list_element_dtype(new)
        if element is None:
            stored = list(_iter_scalar_values(existing)) if values is None else values
            longer = sum(1 for v in stored if isinstance(v, list) and len(v) > 1)
            unwrap = (
                "each stored list must be unwrapped to a single value; declare a "
                "ctx.migrate (e.g. unwrap_list({})) to do so"
            )
            if not longer:
                return f" ({unwrap.format('')})"
            held = "list holds" if longer == 1 else "lists hold"
            pick = unwrap.format("pick='first'")
            return f" ({longer} stored {held} more than one element; {pick})"
        wrap = (
            "each stored value must be wrapped in a list; declare a ctx.migrate "
            "(e.g. wrap_in_list({})) to do so"
        )
        failures = feature_value_failures(
            existing, element, values=values, cat_cache=self._cat_label_cache
        )
        if failures is None or not failures.total:
            return f" ({wrap.format('')})"
        if not failures.failing:
            fit = (
                "the stored value is a valid element"
                if failures.total == 1
                else f"all {failures.total} stored values are valid elements"
            )
            return f" ({fit} of the new dtype, but {wrap.format('')})"
        noun = "value" if failures.total == 1 else "values"
        verb = (
            "is not a valid element"
            if failures.failing == 1
            else "are not valid elements"
        )
        return (
            f" ({failures.failing} of {failures.total} stored {noun} {verb} of the "
            f"new dtype, {_format_failing_examples(failures)}; "
            f"{wrap.format('mapping=...')})"
        )

    def _vet_optional_drops(
        self,
        existing: Any,
        key: str,
        obj_id: int,
        drops: list[tuple[Any, str]],
        override_notes: list[str],
        *,
        label: str = "optional",
        registries: frozenset[str] | None = None,
    ) -> tuple[list[Any], list[str]]:
        """Split optional members the plan makes required into allowed and blocked.

        Dropping a member from a schema's optional set is the one narrowing the
        pure taxonomy cannot judge from the plan alone: whether it invalidates
        anything is a question about the rows the schema validated.  Each drop is
        vetted separately and lands in one of three outcomes:

        * the live rows prove it safe -- none exist, or all already carry a value
          -- *and* the operator opted in with `allow_data_validated_changes`: the
          member is unmarked in place and the reason is recorded in
          `override_notes`;
        * a :meth:`ctx.migrate <lamindb_setup.evolve.PlanContext.migrate>` is
          declared for the schema: the member is unmarked and the migrations are
          deferred to backfill the omitting rows, with a post-check that refuses
          the apply if any is left (see :meth:`run_deferred_migrations`).  The
          declarations are not matched to individual members -- one may cover
          several, or one member each -- so it is the post-check, not this split,
          that decides whether they were enough; or
        * neither: it is returned as a blocked drift, described together with what
          the live rows say about it (see :meth:`_optional_hint`).

        `label` opens each note about a member (`optional 'x': ...`), and
        `registries` scopes the rows judged and backfilled (see
        :class:`_DeferredMemberMigration`).

        Returns `(allowed_members, blocked_descriptions)`.
        """
        from ._drift_data import schema_member_omissions

        migrations = self._migrations_for(key, narrowing=MEMBER_REQUIRED)
        allowed: list[Any] = []
        blocked: list[str] = []
        migrated: list[Any] = []
        for member, description in drops:
            omissions = schema_member_omissions(existing, member, registries=registries)
            reason = _optional_safe_reason(omissions)
            name = getattr(member, "name", None)
            if reason is not None and self.allow_data_validated_changes:
                allowed.append(member)
                override_notes.append(
                    f"{label} {name!r}: allowed despite block ({reason})"
                )
            elif migrations:
                allowed.append(member)
                migrated.append(member)
                override_notes.append(
                    f"{label} {name!r}: made required, omitting rows backfilled by "
                    + ", ".join(f"{m.name} ({m.reason})" for m in migrations)
                )
            else:
                blocked.append(
                    description
                    + self._optional_hint(omissions)
                    + self._other_narrowing_hint(key, MEMBER_REQUIRED)
                )
        if migrated:
            # Deferred for the same reason a dtype migration is: the rows it
            # backfills may be created by this very plan, so it can only run once
            # everything is materialized (see :class:`_DeferredMemberMigration`).
            self._deferred_member_migrations.append(
                _DeferredMemberMigration(
                    obj_id=obj_id,
                    key=key,
                    schema=existing,
                    features=migrated,
                    migrations=migrations,
                    registries=registries,
                )
            )
        return allowed, blocked

    def _vet_member_adds(
        self,
        obj: Materializable,
        existing: Any,
        key: str,
        obj_id: int,
        adds: list[Any],
        override_notes: list[str],
    ) -> list[str]:
        """Judge the *required* members the plan adds against what the schema validated.

        A member that is not optional must be present in everything the schema
        validates, so adding one narrows a schema that validated anything -- the
        same narrowing as making an optional member required (see
        :meth:`_vet_optional_drops`), reached from the other side.  An optional
        member needs nothing, and neither does a new index (vetted as an index
        change, see :meth:`_vet_index_change`).

        * The schema validated nothing: the member is added, as before.
        * Rows of a sheet using the schema must each carry a value.  With
          `allow_data_validated_changes` the member is added when every row
          already does -- the feature annotated them before it became a member --
          and a declared :func:`~lamindb_setup.evolve.fill_missing` backfills the
          rows that omit it, under the usual post-check (see
          :meth:`run_deferred_migrations`).
        * Datasets (artifacts, collections) were validated without the member and
          cannot be re-curated in place, so the plan vouches for them with
          :func:`~lamindb_setup.evolve.accept_added_member`; the flag does not
          lift this, as with an index change.

        Returns the blocked descriptions.
        """
        index = getattr(obj, "index", None)
        optional = {id(f) for f in getattr(obj, "optionals", None) or ()}
        required = [f for f in adds if id(f) not in optional and f is not index]
        if not required or self._schema_validated_anything(existing) is False:
            return []
        usage = self._schema_usage(existing)
        if usage is not None and usage.total == 0:
            return []
        from ._drift_data import (
            MemberOmissions,
            schema_member_omissions,
            schema_validated_usage,
        )

        # A sheet's exports are answered for by the sheet-export narrowing
        # (see :meth:`check_sheet_exports`), not vouched for here.
        validated = (
            None
            if usage is None
            else schema_validated_usage(existing, exclude_sheet_exports=True)
        )
        backfills = self._migrations_for(key, narrowing=MEMBER_REQUIRED)
        vouches = self._migrations_for(key, narrowing=MEMBER_ADDED)
        blocked: list[str] = []
        backfilled: list[Any] = []
        for member in required:
            name = getattr(member, "name", None)
            if usage is None or validated is None:
                blocked.append(
                    f"features: required member {name!r} added, but what the "
                    "schema validated could not be established"
                )
                continue
            reasons: list[str] = []
            datasets = _datasets_only(validated)
            if datasets.total and vouches:
                override_notes.append(
                    f"required {name!r}: {_describe_usage(datasets)} validated "
                    "without it, vouched for by "
                    + ", ".join(f"{m.name} ({m.reason})" for m in vouches)
                )
            elif datasets.total:
                reasons.append(
                    f"{_describe_usage(datasets)} validated without it cannot be "
                    "re-checked in place (declare accept_added_member(reason=...) "
                    "on the schema to vouch for them)"
                )
            if usage.rows:
                live = member.materialize(self)
                omissions = (
                    # Not created yet (PLAN mode): no row can carry a value for it.
                    MemberOmissions(total=usage.rows, omitting=usage.rows, examples=[])
                    if isinstance(live, Placeholder)
                    else schema_member_omissions(existing, live, registries=_SHEET_ROWS)
                )
                safe = _optional_safe_reason(omissions)
                if safe is not None and self.allow_data_validated_changes:
                    override_notes.append(
                        f"required {name!r}: added despite block ({safe})"
                    )
                elif backfills:
                    override_notes.append(
                        f"required {name!r}: added, omitting rows backfilled by "
                        + ", ".join(f"{m.name} ({m.reason})" for m in backfills)
                    )
                    if isinstance(live, Placeholder):
                        override_notes.append(
                            f"dry-run: backfilling {name!r} not previewed, the "
                            "member does not exist yet"
                        )
                    else:
                        backfilled.append(live)
                else:
                    reasons.append(_added_member_hint(omissions))
            if reasons:
                blocked.append(
                    f"features: required member {name!r} added to a schema in use: "
                    + "; ".join(reasons)
                )
        if backfilled:
            # Deferred like an optional drop's backfill, but scoped to the rows of
            # a sheet: the datasets are what the vouch above answers for.
            self._deferred_member_migrations.append(
                _DeferredMemberMigration(
                    obj_id=obj_id,
                    key=key,
                    schema=existing,
                    features=backfilled,
                    migrations=backfills,
                    registries=_SHEET_ROWS,
                )
            )
        return blocked

    def _nullability_usage(self, existing: Any) -> _NullabilityUsage | None:
        """What turning `existing`'s `nullable` off answers to, or `None`.

        A null is the absence of a value, so the stored values cannot tell: a
        row of a sheet that omits the feature is null for it, and a dataset whose
        column held a null left no trace.  This counts both, over every live
        schema declaring the feature (see :class:`_NullabilityUsage`).  `None`
        when that could not be established, which keeps the change refused.
        """
        from ._drift_data import SchemaUsage, schema_member_omissions
        from ._inferred_schemas import live_member_holders

        try:
            schemas = live_member_holders(existing)
        except Exception:
            return None
        omissions: list[tuple[Any, MemberOmissions]] = []
        artifacts = collections = 0
        examples: list[str] = []
        for schema in schemas:
            usage = self._schema_usage(schema)
            if usage is None:
                return None
            artifacts += usage.artifacts
            collections += usage.collections
            examples.extend(usage.examples)
            if usage.rows:
                found = schema_member_omissions(
                    schema, existing, registries=_SHEET_ROWS
                )
                if found is None:
                    return None
                omissions.append((schema, found))
        datasets = SchemaUsage(
            artifacts=artifacts,
            collections=collections,
            rows=0,
            examples=examples[:_MAX_LISTED],
            sheets=[],
        )
        return _NullabilityUsage(omissions=omissions, datasets=datasets)

    def _vet_nullability(
        self,
        existing: Any,
        key: str,
        obj_id: int,
        live: Any,
        desired: Any,
        override_notes: list[str],
    ) -> _Refusal | None:
        """Judge turning a feature's `nullable` off; the refusal, or `None` to apply it.

        The stored values cannot prove it safe -- a null is the *absence* of one --
        so it is judged against what could hold a null for the feature (see
        :meth:`_nullability_usage`):

        * rows of a sheet whose schema declares it and that omit it: proven
          absent with `allow_data_validated_changes`, or backfilled by a declared
          :func:`~lamindb_setup.evolve.fill_nulls`, deferred like a member
          backfill with the same post-check;
        * datasets validated against such a schema: their cells cannot be
          re-checked in place, so the plan vouches for them with
          :func:`~lamindb_setup.evolve.accept_non_null`, which the flag does not
          replace.
        """
        usage = self._nullability_usage(existing)
        reason = _nullable_safe_reason(usage)
        if reason is not None and self.allow_data_validated_changes:
            override_notes.append(f"nullable: allowed despite block ({reason})")
            return None
        if usage is None:
            return _Refusal.of_values(
                "nullable",
                live,
                desired,
                " (what could hold a null for it could not be established)",
            )
        migrations = self._migrations_for(key, narrowing=NULLABILITY)
        if not migrations:
            # Even data that proves it safe needs the flag to say so.
            return _Refusal.of_values(
                "nullable",
                live,
                desired,
                self._override_hint("Feature", "nullable", desired, existing),
            )
        fills = [m for m in migrations if not m.keeps_values]
        vouches = [m for m in migrations if m.keeps_values]
        problems: list[str] = []
        if usage.omitting and fills:
            override_notes.append(
                f"nullable: {_plural(usage.omitting, 'row')} null for it backfilled by "
                + ", ".join(f"{m.name} ({m.reason})" for m in fills)
            )
            for schema, found in usage.omissions:
                if found.omitting:
                    self._deferred_member_migrations.append(
                        _DeferredMemberMigration(
                            obj_id=obj_id,
                            key=key,
                            schema=schema,
                            features=[existing],
                            migrations=fills,
                            registries=_SHEET_ROWS,
                            purpose="a feature the plan makes non-nullable",
                        )
                    )
        elif usage.omitting:
            problems.append(_omitting_rows_hint(usage))
        if usage.datasets.total and vouches:
            override_notes.append(
                f"nullable: {_describe_usage(usage.datasets)} vouched for by "
                + ", ".join(f"{m.name} ({m.reason})" for m in vouches)
            )
        elif usage.datasets.total:
            problems.append(_datasets_hint(usage))
        if problems:
            return _Refusal.of_values(
                "nullable", live, desired, " (" + "; ".join(problems) + ")"
            )
        return None

    def _vet_member_removals(
        self,
        obj: Materializable,
        existing: Any,
        key: str,
        obj_id: int,
        removals: list[Any],
        override_notes: list[str],
    ) -> list[str]:
        """Judge the members the plan no longer declares against the live rows.

        Removing a member is not the relaxation it looks like.  The schema stops
        declaring the feature, but every value already stored for it stays exactly
        where it was -- now annotating rows with something the schema no longer
        describes, invisible to `prune` and to every other check here.  So each
        removal is vetted against the rows that actually carry it:

        * no row carries it (or the schema validated nothing): the removal
          strands nothing and is allowed with no declaration;
        * a :meth:`ctx.migrate <lamindb_setup.evolve.PlanContext.migrate>` is
          declared on the schema: allowed, and the cleanup is deferred with a
          post-check that refuses the apply if a value is left behind (see
          :meth:`run_deferred_migrations`); or
        * neither: blocked, quantified and illustrated by
          :meth:`_removal_hint`.

        A `maximal_set` schema is judged on its configuration instead: it
        forbids members it does not declare, so a removal there is refused unless
        a *destructive* migration is declared, whatever the values say.

        `--allow-data-validated-changes` deliberately does *not* lift this.  The
        flag means "the live data is already safe", which is the first case and
        needs no flag; when values do exist the question is what should happen to
        them, and only a declaration can answer that.

        Returns the blocked descriptions; the removals themselves proceed either
        way, since a blocked one has already raised by the time they are applied.
        """
        from ._drift_data import schema_member_conflicts, schema_member_usage

        migrations = self._migrations_for(key, narrowing=MEMBER_REMOVAL)
        maximal = bool(getattr(obj, "maximal_set", False))
        destructive = [m for m in migrations if not m.keeps_values]
        blocked: list[str] = []
        migrated: list[Any] = []
        for member in removals:
            name = getattr(member, "name", None)
            usage = schema_member_usage(existing, member)
            if maximal and not destructive:
                # `maximal_set` forbids members the schema does not declare, so
                # dropping one turns every dataset still carrying it into a
                # dataset with an undeclared member.  Judged on the schema's
                # configuration rather than on the stored values: what such a
                # schema constrains is the *shape* of a dataset, and a column can
                # outlive the feature values mirroring it, so a count of zero
                # here would not prove the drop safe.  Keeping the values is
                # therefore the one resolution it cannot accept either.
                blocked.append(
                    f"features: the plan removes {name!r} from a maximal_set "
                    "schema (datasets that still carry it would have members the "
                    "schema no longer allows)"
                    + (
                        "; keep_member_values cannot apply here, declare "
                        "drop_member_values to delete them"
                        if migrations
                        else "; declare a ctx.migrate on the schema with "
                        "drop_member_values to delete them"
                    )
                )
                continue
            if not maximal and _removal_safe_reason(usage) is not None:
                continue
            if not migrations:
                blocked.append(
                    f"features: the plan removes {name!r}"
                    + self._removal_hint(usage)
                    + self._other_narrowing_hint(key, MEMBER_REMOVAL)
                )
                continue
            conflicts = schema_member_conflicts(existing, member)
            if conflicts is None:
                blocked.append(
                    f"features: the plan removes {name!r}, but whether another "
                    "schema still needs its values could not be established"
                )
                continue
            if conflicts:
                blocked.append(
                    f"features: the plan removes {name!r}, but "
                    + ", ".join(repr(other) for other in conflicts)
                    + " still declares it and validates rows carrying it "
                    "(clearing them here would invalidate that schema)"
                )
                continue
            migrated.append(member)
            override_notes.append(
                f"member {name!r}: removed, stored values handled by "
                + ", ".join(f"{m.name} ({m.reason})" for m in migrations)
            )
        if migrated:
            self._deferred_member_removals.append(
                _DeferredMemberRemoval(
                    obj_id=obj_id,
                    key=key,
                    schema=existing,
                    features=migrated,
                    migrations=migrations,
                )
            )
        return blocked

    def _schema_index_diff(
        self, obj: Materializable, existing: Any, *, members: list[Any] | None
    ) -> _IndexChange | None:
        """The index change `obj` asks of `existing`, or `None` when they agree.

        Diffed against the *instance* rather than the lock, like the member and
        optional sets (see :data:`_REFERENCE_KEYS`): the plan names its index by
        natural key and the instance by uid, and only this comparison can tell a
        plan edit from a lock that is merely behind an instance already matching
        the plan.

        `index=None` is a definite desired state, not "no opinion": a plan
        removing `index=` removes the index.  lamindb only ever sets one when
        asked to, so a schema the plan created without one never has one unless it
        was set outside the plan -- which is then exactly the divergence to report.

        Costs no query unless they disagree: the live index uid is read from the
        already-loaded `_aux`, and the plan feature from the resolution caches.
        """
        desired = getattr(obj, "index", None)
        live_uid = getattr(existing, "_index_feature_uid", None)
        live = self._live_record(desired) if desired is not None else None
        if desired is None and live_uid is None:
            return None
        if live is not None and live_uid is not None and str(live.uid) == live_uid:
            return None
        previous: Any = None
        if live_uid is not None:
            in_hand = members if members is not None else self._schema_members(existing)
            previous = next((m for m in in_hand if str(m.uid) == live_uid), None)
            if previous is None:
                previous = existing.index
        return _IndexChange(previous=previous, desired=desired, live=live)

    def _vet_index_change(
        self,
        existing: Any,
        key: str,
        obj_id: int,
        change: _IndexChange,
        override_notes: list[str],
    ) -> tuple[list[_Refusal], str | None]:
        """Judge an index change against what the schema has validated.

        The index decides how a whole dataset is curated -- which column or
        `obs_names` identify its rows -- so changing it changes the verdict on
        every dataset already validated, and that cannot be re-checked in place.
        Three outcomes, like the member narrowings:

        * the schema validated nothing -- no artifact or collection, directly or
          through a composite that slots it, and no row in a sheet that uses it --
          *and* the operator opted in with `allow_data_validated_changes`: the
          change is made and the reason recorded in `override_notes`;
        * a :meth:`ctx.migrate <lamindb_setup.evolve.PlanContext.migrate>`
          answering :data:`~lamindb_setup.evolve.authoring._migrations.INDEX_CHANGE`
          is declared on the schema: the plan author vouches for the data, so the
          change is made and the declarations deferred (see
          :class:`_DeferredIndexChange`); or
        * neither: blocked, naming the data that would have to conform.

        An allowed change then has to survive lamindb re-keying the rows of the
        sheets that use the schema (see :meth:`_vet_sheet_index`).

        Returns the blocked descriptions and how lamindb should resolve a sheet
        row whose name and new index value differ (`None` when no declaration
        chose).
        """
        migrations = self._migrations_for(key, narrowing=INDEX_CHANGE)
        usage = self._schema_usage(existing)
        reason = _index_safe_reason(usage)
        if reason is not None and self.allow_data_validated_changes:
            override_notes.append(f"index: allowed despite block ({reason})")
        elif migrations:
            validated = (
                f", although the schema has validated {_describe_usage(usage)}"
                if usage is not None and usage.total
                else ""
            )
            override_notes.append(
                f"index: {change.before!r} \u2192 {change.after!r} accepted by "
                + ", ".join(f"{m.name} ({m.reason})" for m in migrations)
                + validated
            )
            if self.mode is ApplyMode.APPLY:
                self._deferred_index_changes.append(
                    _DeferredIndexChange(
                        obj_id=obj_id,
                        key=key,
                        schema=existing,
                        previous=change.previous,
                        index=change.live,
                        migrations=migrations,
                    )
                )
        else:
            return [
                _Refusal.of_values(
                    "index",
                    change.before,
                    change.after,
                    self._index_hint(usage)
                    + self._other_narrowing_hint(key, INDEX_CHANGE),
                )
            ], None
        blocked, resolution = self._vet_sheet_index(
            existing, change, migrations, override_notes
        )
        return [_Refusal.of_text("index", line) for line in blocked], resolution

    def _vet_sheet_index(
        self,
        existing: Any,
        change: _IndexChange,
        migrations: list[DataMigration],
        override_notes: list[str],
    ) -> tuple[list[str], str | None]:
        """Make sure lamindb can re-key the schema's sheet rows without asking.

        A sheet stores each row's index value on `Record.name`, so saving a
        schema whose index changed moves values between the two (lamindb's
        `migrate_record_sheet_index_on_schema_save`).  Two things there would
        stop the apply in a way no transaction cleanly undoes:

        * the new index is not a `str` feature, which `Record.name` cannot
          hold -- lamindb raises; and
        * a row already has a `Record.name` *and* a different value for the new
          index -- lamindb prompts on stdin, which hangs or fails in CI.

        Both are found up front.  The first is refused; the second is settled by
        the `index_name_conflict` a declaration chose, and refused with the rows
        named when none did.  Only *adding* an index can conflict: replacing one
        first moves every name back onto the old index feature.

        Returns the blocked descriptions and the resolution to save with.
        """
        from ._drift_data import sheet_index_name_conflicts

        feature = change.live
        if change.desired is None or feature is None:
            # Removing the index only moves names onto the old feature, and a
            # feature this preview has not created holds no values to re-key.
            return [], None
        found = sheet_index_name_conflicts(
            existing, feature, replaces_index=change.previous is not None
        )
        if found is None:
            return [
                f"index: whether the rows of the sheets using it can be re-keyed by "
                f"{change.after!r} could not be established"
            ], None
        if not found.sheets:
            return [], None
        sheets = ", ".join(repr(name) for name in found.sheets)
        dtype = getattr(feature, "dtype_as_str", None)
        if dtype != "str":
            return [
                f"index: {change.after!r} cannot index sheet {sheets}: a sheet keeps "
                f"its index on Record.name, which needs dtype 'str', not {dtype!r}"
            ], None
        chosen = sorted(
            {m.index_name_conflict for m in migrations if m.index_name_conflict}
        )
        if len(chosen) > 1:
            return [
                "index: the declarations disagree on index_name_conflict ("
                + ", ".join(repr(choice) for choice in chosen)
                + ")"
            ], None
        resolution = chosen[0] if chosen else None
        if not found.conflicts:
            return [], resolution
        rows = _plural(found.conflicts, "row")
        has, its = ("has", "its") if found.conflicts == 1 else ("have", "their")
        if resolution is None:
            return [
                f"index: {rows} of sheet {sheets} already {has} a Record.name "
                f"that differs from {its} {change.after!r} value ("
                + "; ".join(found.examples)
                + (", ..." if found.conflicts > len(found.examples) else "")
                + "), and lamindb would have to ask which to keep; declare "
                "accept_index_change(index_name_conflict='keep_name') to keep the "
                "names or index_name_conflict='keep_feature' to take the values"
            ], None
        kept = (
            "kept Record.name"
            if resolution == "keep_name"
            else f"took the {change.after!r} value"
        )
        override_notes.append(
            f"index: {rows} of sheet {sheets} whose name differed from {its} "
            f"{change.after!r} value {kept} (index_name_conflict={resolution!r})"
        )
        return [], resolution

    def _index_hint(self, usage: SchemaUsage | None) -> str:
        """A parenthetical explaining a refused index change, else `""`.

        The counterpart of :meth:`_removal_hint`: either the schema validated
        nothing and only the opt-in flag is missing, or it names the data that
        would have to conform and the declaration that vouches for it.
        """
        if usage is None:
            return (
                ", but whether the schema has validated any data could not be "
                "established; declare a ctx.migrate on the schema with "
                "accept_index_change once the data is known to conform"
            )
        if (reason := _index_safe_reason(usage)) is not None:
            return f" (could be applied with --allow-data-validated-changes: {reason})"
        return (
            f" while the schema has validated {_describe_usage(usage)}, which "
            "cannot be re-checked against the new index in place; declare a "
            "ctx.migrate on the schema with accept_index_change(reason=...) once "
            "that data is known to conform"
        )

    def _vet_flexible_change(
        self,
        obj: Materializable,
        existing: Any,
        key: str,
        live: Any,
        desired: Any,
        *,
        override_notes: list[str],
        flexible_notes: list[str],
        warnings: list[ChangeWarning],
    ) -> _Refusal | None:
        """Judge switching `existing`'s `flexible` flag: the refusal, else `None`.

        The taxonomy decides by `maximal_set` as the plan leaves it (see
        :mod:`._drift`).  Turning the flag on is applied -- with a warning under
        `maximal_set=False`, where the extra columns start being validated -- and
        so is turning it off under `maximal_set=False`.  Turning it off on a
        `maximal_set` schema is refused unless the schema validated no dataset
        (with `allow_data_validated_changes`) or a declaration answers for the
        datasets (see :func:`~lamindb_setup.evolve.accept_flexible_change`).

        A flag turned off is reported with what the flexible match annotated while
        it was on, and the declarations are deferred to keep or remove that (see
        :class:`_DeferredFlexibleChange`).
        """
        maximal = _desired_value(obj, "maximal_set")
        after = {
            "maximal_set": (
                getattr(existing, "maximal_set", None) if maximal is None else maximal
            )
        }
        turning_off = bool(live) and not bool(desired)
        migrations = (
            self._migrations_for(key, narrowing=FLEXIBLE_CHANGE) if turning_off else []
        )
        if classify("Schema", "flexible", live, desired, context=after) is (
            Verdict.SAFE
        ):
            note = caveat("Schema", "flexible", live, desired, context=after)
            if note is not None:
                warnings.append(
                    ChangeWarning(
                        f"flexible {live!r} \u2192 {desired!r}: {note}",
                        WarningCategory.CAVEAT,
                    )
                )
        elif (
            reason := self._data_override_reason(
                "Schema", "flexible", desired, existing
            )
        ) is not None:
            override_notes.append(f"flexible: allowed despite block ({reason})")
        elif migrations:
            usage = self._schema_usage(existing)
            validated = (
                ", although the schema has validated "
                + _describe_usage(_datasets_only(usage))
                if usage is not None and (usage.artifacts or usage.collections)
                else ""
            )
            override_notes.append(
                f"flexible: {live!r} \u2192 {desired!r} accepted by "
                + ", ".join(f"{m.name} ({m.reason})" for m in migrations)
                + validated
            )
        else:
            return _Refusal.of_values(
                "flexible", live, desired, self._flexible_hint(existing, key)
            )
        if turning_off:
            flexible_notes.extend(self._nonmember_notes(existing, migrations))
            if migrations:
                self._deferred_flexible_changes.append(
                    _DeferredFlexibleChange(
                        obj_id=id(obj),
                        key=key,
                        schema=existing,
                        previous=live,
                        flexible=desired,
                        migrations=migrations,
                    )
                )
        return None

    def _flexible_hint(self, existing: Any, key: str) -> str:
        """A parenthetical explaining a refused `flexible` change, else `""`.

        Only turning the flag off on a `maximal_set` schema is refused.  The
        counterpart of :meth:`_index_hint`: either the schema validated no dataset
        and only the opt-in flag is missing, or it names the datasets and the
        declarations that answer for them.
        """
        usage = self._schema_usage(existing)
        other = self._other_narrowing_hint(key, FLEXIBLE_CHANGE)
        if usage is None:
            return (
                ", but whether the schema has validated any dataset could not be "
                "established; declare a ctx.migrate on the schema with "
                "accept_flexible_change once its datasets are known to conform" + other
            )
        if (reason := _flexible_safe_reason(usage)) is not None:
            return f" (could be applied with --allow-data-validated-changes: {reason})"
        return (
            " while the schema is maximal_set and has validated "
            f"{_describe_usage(_datasets_only(usage))}: one that carried a column "
            "the flexible match accepted would no longer validate, which cannot be "
            "re-checked in place; declare a ctx.migrate on the schema with "
            "accept_flexible_change(reason=...) once those datasets are known to "
            "conform, or drop_nonmember_annotations() to also remove what the "
            "flexible match annotated" + other
        )

    def _nonmember_notes(
        self, existing: Any, migrations: list[DataMigration]
    ) -> list[str]:
        """What turning `flexible` off leaves on the artifacts the schema curated.

        While the flag was on, curation also recorded -- and, for a categorical
        one, labelled -- every column that matched a registered feature.  That
        stays unless a declaration removes it.  Empty when nothing is left over or
        it cannot be established: the change itself is not in question here.
        """
        from ._drift_data import schema_nonmember_usage

        usage = schema_nonmember_usage(existing)
        if usage is None or not usage.artifacts:
            return []
        shown = ", ".join(repr(label) for label in usage.examples)
        more = ", ..." if usage.artifacts > len(usage.examples) else ""
        artifacts = f"{_plural(usage.artifacts, 'artifact')} ({shown}{more})"
        columns = ", ".join(repr(name) for name in usage.features)
        removing = [m.name for m in migrations if not m.keeps_values]
        if removing:
            return [
                f"flexible: {', '.join(removing)} removes what the flexible match "
                f"annotated for non-member columns {columns} on {artifacts}"
            ]
        verb = "keeps" if usage.artifacts == 1 else "keep"
        return [
            f"flexible: {artifacts} {verb} what the flexible match annotated for "
            f"non-member columns {columns}; declare drop_nonmember_annotations() to "
            "remove it"
        ]

    def _record_schema_shape(
        self,
        obj: Materializable,
        existing: Any,
        key: str,
        *,
        members: list[Any] | None,
        feature_adds: list[Any],
        feature_removals: list[Any],
        optional_adds: list[Any],
        optional_removals: list[Any],
        index_change: _IndexChange | None = None,
        flags_on: Sequence[str] = (),
        made_required: Sequence[Any] = (),
    ) -> None:
        """Remember the shape `existing` has *after* this reconcile.

        Collected here rather than read back later because this is the only place
        that knows both states: in PLAN mode nothing is written, so the live
        schema still has its old members, and in APPLY mode the edits are already
        folded in.  Expressing the result as "the ids that survive, the ids that
        are required, and the names that are brand new" makes both modes describe
        the same post-change shape, which is what the stale-export sweep compares
        an exported file against (see :meth:`check_sheet_exports`).

        Every reconciled schema is recorded, changed or not: a member renamed
        *after* its schema was reconciled invalidates that schema's exports just
        as much, and only the sweep at the end can see it.

        `flags_on` are the set flags the plan turns on, which change what an
        export has to satisfy as much as a member edit does; `made_required`
        the members `minimal_set` makes required (see
        :meth:`_minimal_set_members`), each written while it could be omitted.
        """
        live_members = (
            members if members is not None else self._schema_members(existing)
        )
        removed_uids = {str(m.uid) for m in feature_removals}
        surviving = [m for m in live_members if str(m.uid) not in removed_uids]
        # Read *before* the APPLY-mode edits below, so both modes see the same
        # pre-edit state and the post-change shape is derived the same way.
        optionals = getattr(existing, "optionals", None)
        optional_uids = set(optionals.get_uids()) if optionals is not None else set()
        optional_uids -= {str(m.uid) for m in optional_removals}
        added_names = {getattr(f, "name", None) for f in feature_adds}
        # `optional_adds` are *plan* features, whose uid is still a promise, so
        # a member the plan newly marks optional is excluded by name -- otherwise
        # it would keep counting as required here.
        optional_add_names = {getattr(f, "name", None) for f in optional_adds}
        newly_required = [
            getattr(m, "name", None) for m in (*optional_removals, *made_required)
        ]
        self._schema_shapes.append(
            _SchemaShape(
                obj_id=id(obj),
                key=key,
                schema=existing,
                member_ids={m.pk for m in surviving},
                previous_ids={m.pk for m in live_members},
                required_ids={
                    m.pk
                    for m in surviving
                    if str(m.uid) not in optional_uids
                    and getattr(m, "name", None) not in optional_add_names
                },
                # A member the plan adds has no exported column by construction,
                # so only the required ones matter -- and they are named rather
                # than keyed, since in PLAN mode they may not exist yet.
                added_required=sorted(
                    str(name)
                    for name in added_names - optional_add_names
                    if name is not None
                ),
                # A member that stops being optional does have a column, but the
                # file was written while it was allowed to be blank.
                newly_required=sorted(
                    str(name) for name in newly_required if name is not None
                ),
                maximal=bool(getattr(obj, "maximal_set", False)),
                structural=bool(
                    feature_adds
                    or feature_removals
                    or optional_adds
                    or optional_removals
                    or index_change is not None
                    or flags_on
                ),
                index_change=(
                    index_change.describe() if index_change is not None else None
                ),
            )
        )

    def check_sheet_exports(self) -> None:
        """Vet the sheet exports this apply's schema changes invalidate.

        A sheet export is a CSV snapshot already written to storage (see
        :mod:`lamindb_setup.evolve.engine._sheet_exports`).  It is the one thing a schema
        change cannot reconcile in place: renaming a member changes a column
        header, retyping one changes how its values read, making one required
        adds a column the file does not have.  The artifact keeps pointing at the
        changed schema through its own `schema` foreign key, so it goes on
        claiming to validate against a shape it no longer has.

        Judged against the exports that actually exist, exactly as a member
        removal is judged against the rows that actually carry it:

        * no sheet of the schema was ever exported, or the exports still fit the
          new shape: nothing to declare;
        * a :meth:`ctx.migrate <lamindb_setup.evolve.PlanContext.migrate>` is
          declared on the schema: allowed, and the re-export is handed to the
          caller to run after the transaction commits (see
          :class:`_DeferredSheetExports`); or
        * neither: blocked, naming each stale export and why.

        Only a schema this apply actually changed is examined -- unless a previous
        run of this plan left a re-export branch unmerged, which says its exports
        are still owed and makes every schema a candidate again, so a retry
        converges instead of silently doing nothing.  An export left stale by
        some *other* change is real, but it is not this apply's doing, and
        refusing every later apply over it would make an unrelated change
        impossible; `repair` territory rather than a block here.

        A sheet this apply points at another schema invalidates its exports the
        same way, and is answered by a declaration on the sheet (see
        :meth:`_repointed_sheet_exports`).

        `--allow-data-validated-changes` deliberately does not lift this, for
        the same reason it does not lift a member removal: the flag says the live
        data is already safe, which is the first case above and needs no flag.
        Once an export is stale the question is what should become of it, and only
        a declaration answers that.
        """
        from ._sheet_exports import SheetExport, schema_sheet_exports, stale_exports

        if not self._schema_shapes and not self._sheet_repoints:
            return
        # `owed` can only ever re-drive a declaration the plan already made, so
        # a plan that declares none cannot use it -- and must not pay for the
        # branch read either.  Checked in memory, before anything is queried.
        owed = (
            any(
                self._migrations_for(entry.key, narrowing=STALE_EXPORTS)  # type: ignore[attr-defined]
                for entry in [*self._schema_shapes, *self._sheet_repoints]
            )
            and self._reexports_owed()
        )
        blocked = self._repointed_sheet_exports(owed)
        candidates = [
            shape
            for shape in self._schema_shapes
            if owed
            or shape.structural
            or (shape.member_ids & self._changed_features.keys())
        ]
        if not candidates and not blocked:
            return
        index = self._sheet_export_index() if candidates else {}
        if index is None:
            raise DriftError(
                "the instance holds exported sheet snapshots this change may "
                "invalidate:\n  whether any exported snapshot of "
                + ", ".join(sorted(repr(shape.key) for shape in candidates))
                + " is invalidated could not be established"
            )
        # A sheet this apply re-points answers for its exports itself, and a dry
        # run still finds it paired with its old schema.
        moved = {entry.sheet.pk for entry in self._sheet_repoints if entry.repointed}
        found = {
            schema_pk: [pair for pair in pairs if pair[0].pk not in moved]
            for schema_pk, pairs in schema_sheet_exports(
                [shape.schema for shape in candidates], index
            ).items()
        }
        for shape in candidates:
            migrations = self._migrations_for(shape.key, narrowing=STALE_EXPORTS)
            exports = stale_exports(
                shape.schema,
                found.get(shape.schema.pk, []),
                member_ids=shape.member_ids,
                previous_ids=shape.previous_ids,
                required_ids=shape.required_ids,
                added_required=shape.added_required,
                newly_required=shape.newly_required,
                maximal=shape.maximal,
                changed_features=self._changed_features,
                index_change=shape.index_change,
                recorded_columns=self._absorbed_columns,
            )
            if not exports and owed and migrations:
                # A retry after a failed re-export: the schema already matches the
                # plan, so nothing reads as stale any more -- yet the exports the
                # failed run owed are still owed.  Only a schema whose plan
                # already declared what to do is re-driven, so an unmerged branch
                # can never demand a *new* declaration somewhere else.
                exports = [
                    SheetExport(
                        sheet=sheet,
                        artifact=artifact,
                        reasons=("a previous run's re-export did not complete",),
                    )
                    for sheet, artifact in found.get(shape.schema.pk, [])
                ]
            if not exports:
                continue
            if not migrations:
                blocked.append(
                    f"sheet exports: {len(exports)} exported snapshot(s) "
                    f"of {shape.key!r} no longer adhere to it \u2014 "
                    + "; ".join(export.describe() for export in exports)
                    + "; declare a ctx.migrate on the schema with "
                    "reexport_sheets to write a new version of each, or "
                    "keep_stale_exports to leave them as they are"
                    + self._other_narrowing_hint(shape.key, STALE_EXPORTS)
                )
                continue
            self.pending_sheet_exports.append(
                _DeferredSheetExports(
                    obj_id=shape.obj_id,
                    key=shape.key,
                    schema=shape.schema,
                    exports=exports,
                    migrations=migrations,
                )
            )
            self._change_details_for(shape.obj_id).append(
                f"sheet exports: {len(exports)} invalidated, handled by "
                + ", ".join(f"{m.name} ({m.reason})" for m in migrations)
            )
        if blocked:
            raise DriftError(
                "the instance holds exported sheet snapshots this change "
                "invalidates:\n  " + "\n  ".join(blocked)
            )

    def _repointed_sheet_exports(self, owed: bool) -> list[str]:
        """Vet the exports of the sheets this apply points at another schema.

        An export written against the schema a sheet is pointed away from holds
        the rows as that schema shaped them, while the sheet's rows are now judged
        by the new one -- so a re-point invalidates every such export.  The old
        schema did not change, so the schema sweep cannot see it, and once
        re-pointed the sheet no longer pairs with the export through its schema;
        the exports are found from the sheet's side instead (see
        :func:`~lamindb_setup.evolve.engine._sheet_exports.sheet_exports`).  A
        declaration on the *sheet* answers for them, as one on a schema does for
        its exports: :func:`~lamindb_setup.evolve.reexport_sheets` writes a new
        version against the new schema, and
        :func:`~lamindb_setup.evolve.keep_stale_exports` leaves them as they are.

        `owed` says a previous run's re-export did not complete: a sheet that
        declares what becomes of its exports then re-drives the ones still
        written against another schema than its own.

        Returns the blocked descriptions.
        """
        from ._sheet_exports import SheetExport, sheet_exports

        entries = [
            entry
            for entry in self._sheet_repoints
            if entry.repointed
            or entry.gained
            or (owed and self._migrations_for(entry.key, narrowing=STALE_EXPORTS))
        ]
        if not entries:
            return []
        found = sheet_exports([entry.sheet for entry in entries])
        if found is None:
            raise DriftError(
                "the instance holds exported sheet snapshots this change may "
                "invalidate:\n  whether any exported snapshot of "
                + ", ".join(sorted(repr(entry.key) for entry in entries))
                + " is invalidated could not be established"
            )
        blocked: list[str] = []
        for entry in entries:
            pairs = found.get(entry.sheet.pk, [])
            if entry.gained:
                # Every export of it misses the rows moved in, whatever schema
                # it was written against.
                reason = (
                    f"written before {_plural(entry.gained, 'row')} moved into "
                    f"it from {entry.source}"
                )
                exports = [
                    SheetExport(sheet=sheet, artifact=artifact, reasons=(reason,))
                    for sheet, artifact in pairs
                ]
            elif entry.repointed:
                previous = getattr(entry.previous, "pk", None)
                reason = (
                    f"written against {_schema_label(entry.previous)!r}, which the "
                    "sheet is pointed away from"
                )
                exports = [
                    SheetExport(sheet=sheet, artifact=artifact, reasons=(reason,))
                    for sheet, artifact in pairs
                    if previous is not None and artifact.schema_id == previous
                ]
            else:
                current = getattr(entry.schema, "pk", None)
                exports = [
                    SheetExport(
                        sheet=sheet,
                        artifact=artifact,
                        reasons=("a previous run's re-export did not complete",),
                    )
                    for sheet, artifact in pairs
                    if artifact.schema_id != current
                ]
            if not exports:
                continue
            migrations = self._migrations_for(entry.key, narrowing=STALE_EXPORTS)
            if not migrations:
                rewrite = (
                    "with the rows moved into it"
                    if entry.gained
                    else "against its new schema"
                )
                blocked.append(
                    f"sheet exports: {len(exports)} exported snapshot(s) of "
                    f"{entry.key!r} no longer adhere to it \u2014 "
                    + "; ".join(export.describe() for export in exports)
                    + "; declare a ctx.migrate on the sheet with reexport_sheets "
                    f"to write a new version of each {rewrite}, or "
                    "keep_stale_exports to leave them as they are"
                    + self._other_narrowing_hint(entry.key, STALE_EXPORTS)
                )
                continue
            self.pending_sheet_exports.append(
                _DeferredSheetExports(
                    obj_id=entry.obj_id,
                    key=entry.key,
                    schema=entry.schema,
                    exports=exports,
                    migrations=migrations,
                )
            )
            self._change_details_for(entry.obj_id).append(
                f"sheet exports: {len(exports)} invalidated, handled by "
                + ", ".join(f"{m.name} ({m.reason})" for m in migrations)
            )
        return blocked

    def _sheet_export_index(self) -> dict[Any, list[Any]] | None:
        """Where the instance's sheet exports live, read once and shared.

        A run applying several plans to one instance sweeps each of them, and
        the index does not depend on the plan -- so the caller hands the same
        cache to every plan and the instance-wide read happens once.
        """
        from ._sheet_exports import sheet_export_index

        if "index" not in self._exports_cache:
            self._exports_cache["index"] = sheet_export_index()
        return self._exports_cache["index"]

    def _reexports_owed(self) -> bool:
        """Whether a previous run of this plan failed before its exports went live.

        A failed re-export leaves its versions parked on the branch that run
        opened, unmerged.  Nothing else in the instance records that they are
        owed, so without this the schema would already match the plan on the next
        apply, the sweep would find nothing to do, and the command would report
        success while the stale export stayed live and the corrected one stayed
        invisible.  An unmerged branch under the plan's prefix makes every schema
        a candidate again, so the retry redoes the export from the current rows;
        merging its own branch then closes the abandoned one, which clears this.

        One query per instance, cached alongside the export index.
        """
        from ._sheet_exports import branch_prefix_for, unmerged_reexport_branches

        if not self.plan_name:
            return False
        # Keyed by prefix: the cache is shared by every plan on the instance, and
        # the answer is per plan.
        key = f"branches:{branch_prefix_for(self.plan_name)}"
        if key not in self._exports_cache:
            self._exports_cache[key] = unmerged_reexport_branches(
                branch_prefix_for(self.plan_name)
            )
        return bool(self._exports_cache[key])

    def _removal_hint(self, usage: MemberUsage | None) -> str:
        """A parenthetical explaining a refused member removal, else `""`.

        The counterpart of :meth:`_optional_hint`: it quantifies and illustrates
        what the removal would strand, and names both ways out -- which are
        genuinely different decisions, not one with a flag, so both are offered.

        Empty when the rows could not be introspected, so a failed query stays
        silent rather than reporting a misleading count.
        """
        if usage is None:
            return (
                ", but whether any record carries a value for it could not be "
                "established"
            )
        noun = "row" if usage.total == 1 else "rows"
        verb = "carries" if usage.carrying == 1 else "carry"
        examples = ", ".join(repr(label) for label in usage.examples)
        return (
            f" while {usage.carrying} of {usage.total} validated {noun} "
            f"{verb} a value for it, e.g. {examples}; declare a ctx.migrate on "
            "the schema with drop_member_values to delete them or "
            "keep_member_values to keep them as extra data"
        )

    def _optional_hint(self, omissions: MemberOmissions | None) -> str:
        """A parenthetical explaining a refused optional-to-required drop, else `""`.

        Mirrors :meth:`_override_hint` for the one narrowing that is judged
        against *rows* rather than a feature's values: either the rows prove the
        drop safe and only the opt-in flag is missing, or they quantify and
        illustrate what blocks it and name the way out.

        Empty when the rows could not be introspected, so a failed query stays
        silent rather than reporting a misleading count.
        """
        if omissions is None:
            return ""
        if _optional_safe_reason(omissions) is not None:
            # Only reachable without the flag: with it the drop was allowed.
            return (
                " (could be applied with --allow-data-validated-changes: "
                f"{_optional_safe_reason(omissions)})"
            )
        noun = "row" if omissions.total == 1 else "rows"
        verb = "omits" if omissions.omitting == 1 else "omit"
        examples = ", ".join(repr(label) for label in omissions.examples)
        return (
            f" ({omissions.omitting} of {omissions.total} validated {noun} "
            f"{verb} it, e.g. {examples}; declare a ctx.migrate on the schema to "
            "backfill them)"
        )

    def _check_remote_drift(
        self,
        existing: Any,
        key: str,
        kind: str,
        previous: LockEntry,
        warnings: list[ChangeWarning],
        *,
        current: dict[str, Any] | None = None,
        baseline: dict[str, Any] | None = None,
        desired: dict[str, Any] | None = None,
        allow_rename: bool = False,
        desired_index: Any = _UNRESOLVED,
    ) -> None:
        """Detect changes made to the instance outside the plan since the last apply.

        Compares the current remote snapshot against the one recorded in the
        lock (a *remote-to-remote* comparison).  Divergence in structural /
        validation-affecting properties raises :class:`DriftError`; divergence in
        descriptive properties is recorded as a loud `DRIFT` warning and left
        for :meth:`_reconcile` to auto-correct back to the plan.

        `current` may supply `existing`'s already-computed snapshot (see
        :func:`serialize_existing`) so it is not re-serialized here; when
        `None` it is computed lazily.  `baseline` and `desired` are the
        lock's and the plan's *configurations* (natural-key addressed), consulted
        by :meth:`_schema_member_loss_is_recoverable` and by the `name`
        relaxation below.

        `allow_rename` says the caller reached this entry by re-keying the lock
        for a declared `ctx.moved`, so the recorded snapshot still describes the
        object under its *previous* name (:func:`_rekey_prefix` deliberately
        leaves it alone).  A `name` divergence is then the declared rename
        showing up on the remote side rather than drift -- see
        :meth:`_rename_drift_warning`.

        `desired_index` is the uid of the index the plan declares for a schema
        (see :meth:`_plan_index_uid`).  An index changed on the instance to
        exactly that one is the plan and the instance agreeing while the lock
        lags behind, so it is reported and the lock brought up to date rather
        than refused.
        """
        stored = previous.remote_snapshot
        if stored is None:
            return
        if current is None:
            current = self._snapshot(existing, kind)
        if (
            previous.remote_hash is not None
            and remote_state_hash(current) == previous.remote_hash
        ):
            return

        def drifted(text: str) -> None:
            warnings.append(ChangeWarning(text, WarningCategory.DRIFT))

        descriptive = _descriptive_keys(kind)
        structural_diffs: list[str] = []
        for name in sorted(set(stored) | set(current)):
            if name == "kind" or stored.get(name) == current.get(name):
                continue
            if kind == "Schema" and name == "hash":
                # Derived from the granular fields and the member list, both of
                # which are compared explicitly; ignore the opaque digest.
                continue
            if name in _OMITTED_WHEN_EMPTY and name not in stored:
                continue
            if kind == "Schema" and name == "features":
                stored_f = set(stored.get(name) or [])
                current_f = set(current.get(name) or [])
                removed = sorted(stored_f - current_f)
                added = sorted(current_f - stored_f)
                if removed:
                    if self._schema_member_loss_is_recoverable(baseline, desired):
                        drifted(
                            "the instance dropped schema members the plan still "
                            "declares and they will be re-linked from the plan: "
                            f"{removed!r}"
                        )
                    else:
                        structural_diffs.append(
                            f"features: instance removed {removed!r} outside the plan"
                        )
                if added:
                    drifted(
                        "remote added schema features outside the plan and they "
                        f"will be kept: {added!r}"
                    )
                continue
            message = (
                f"{name}: instance now {current.get(name)!r}, "
                f"lock recorded {stored.get(name)!r}"
            )
            if kind == "Schema" and name == "index":
                if current.get(name) == desired_index:
                    drifted(
                        "the index was already changed outside the plan to the "
                        "one the plan declares; the lock will be brought up to "
                        f"date: {message}"
                    )
                    continue
            if name == "name" and allow_rename:
                drifted(
                    self._rename_drift_warning(
                        current.get(name),
                        (desired or {}).get(name),
                        message,
                    )
                )
                continue
            if name in descriptive:
                drifted(
                    "remote changed outside the plan and will be auto-corrected "
                    f"to the plan: {message}"
                )
            elif classify(
                kind, name, stored.get(name), current.get(name), context=current
            ) is (Verdict.SAFE):
                drifted(
                    self._safe_remote_change_warning(
                        kind, name, current, desired, message
                    )
                )
            elif (
                reason := self._data_override_reason(
                    kind, name, current.get(name), existing
                )
            ) is not None:
                drifted(
                    "remote changed outside the plan in a way that is data-safe "
                    f"({reason}) and will be kept: {message}"
                )
            else:
                structural_diffs.append(
                    message
                    + self._override_hint(kind, name, current.get(name), existing)
                )
        if structural_diffs:
            raise DriftError(
                f"remote drift for {key!r}: the instance was changed outside the "
                "plan in structural or validation-affecting properties that cannot "
                "be reconciled safely:\n  " + "\n  ".join(structural_diffs)
            )

    @staticmethod
    def _safe_remote_change_warning(
        kind: str,
        name: str,
        current: dict[str, Any],
        desired: dict[str, Any] | None,
        message: str,
    ) -> str:
        """Say what becomes of a safe change made to the instance outside the plan.

        It is kept unless the plan states a value of its own for the field and
        going back to it is safe too: :meth:`_reconcile` then restores the plan's
        value, so calling the change "kept" would contradict the diff below it.
        """
        planned = (desired or {}).get(name)
        restored = (
            name in _GUARDED_FIELDS.get(kind, ())
            and planned is not None
            and planned != current.get(name)
            and classify(kind, name, current.get(name), planned, context=desired)
            is Verdict.SAFE
        )
        outcome = "will be changed back to the plan" if restored else "will be kept"
        return (
            "remote changed outside the plan in a safe, non-invalidating way and "
            f"{outcome}: {message}"
        )

    @staticmethod
    def _rename_drift_warning(live_name: Any, desired_name: Any, message: str) -> str:
        """Explain a `name` divergence covered by a declared `ctx.moved`.

        The recorded snapshot describes the object under its previous name, so a
        declared move makes a `name` difference *expected* rather than drift --
        :meth:`_check_plan_edit` exempts the same key, and both sides have to
        agree or a rename could never be applied.

        Which of the two things happened is worth saying, though.  Usually the
        object is still on the instance under its old name and
        :meth:`_reconcile` is about to rename it.  But the rename may equally
        have been done on the instance first and the plan edited afterwards to
        match, which is what a lock recording the old name and an instance
        already carrying the plan's new one means: the plan and the instance
        agree, there is nothing to write, and only the lock is behind.
        """
        if desired_name is not None and live_name == desired_name:
            return (
                "the declared rename was already applied to the instance outside "
                f"the plan; the lock will be brought up to date: {message}"
            )
        return (
            "remote changed outside the plan but the plan declares a rename for "
            f"it, so it will be renamed to {desired_name!r}: {message}"
        )

    @staticmethod
    def _schema_member_loss_is_recoverable(
        baseline: dict[str, Any] | None, desired: dict[str, Any] | None
    ) -> bool:
        """Whether a schema member the instance dropped will be re-linked from the plan.

        The remote snapshot addresses a schema's members by `uid`, so a member
        that was *replaced* -- destroyed and re-created, which is the documented
        workflow for a dtype narrowing no migration can express -- reads as a
        removal even though the plan still declares a feature at the same
        address.  The plan's own configuration addresses members by natural key
        and is unaffected by the new uid, so comparing *those* tells the two
        apart: when the plan still declares every member it declared at the last
        apply, nothing was silently dropped and :meth:`_reconcile` re-links the
        replacement.  A member the plan has *also* stopped declaring is real
        structural drift and still refuses.

        A recorded member list is required to establish which members the plan owned.
        """
        if baseline is None or desired is None or "features" not in baseline:
            return False
        return set(baseline.get("features") or []) <= set(desired.get("features") or [])

    def _check_plan_edit(
        self,
        existing: Any,
        key: str,
        kind: str,
        baseline: dict[str, Any],
        config: dict[str, Any],
        *,
        current: dict[str, Any] | None = None,
        allow_rename: bool = False,
        desired_dtype: str | None = None,
    ) -> None:
        """Allow descriptive plan edits but reject structural ones.

        Compares the plan's current configuration against the lock baseline.
        Differences in descriptive properties are intentional edits and are
        pushed to the remote by :meth:`_reconcile`.  Differences in structural /
        validation-affecting properties raise :class:`PlanChangeRefusedError`
        because changing them in place may invalidate already-validated records.
        That is a refusal of the plan's edit, not drift: each refused key reads
        as the diff would show it (`dtype 'str' → 'int'`), never in the lock's
        own encoding (`builtins.str`, a registry's import path).

        A structural difference is *not* an in-place edit, however, when the
        live instance already holds the plan's desired value: the lock baseline
        is merely stale (e.g. the value was fixed out of band).  In that case applying the plan
        changes nothing on the instance -- no already-validated record is
        affected -- so the divergence is accepted and the lock is refreshed by
        :meth:`_record` instead of raising.  Identity metadata
        (:data:`_IDENTITY_KEYS`) is excluded: a mismatch there points at a
        tampered lock and is always surfaced.

        Reference-valued keys (:data:`_REFERENCE_KEYS`) -- a schema's members, the
        optional subset of them and its index -- are skipped altogether: the plan
        addresses them by natural key and the instance by uid, and the verdict on
        them belongs to the diff against the instance that :meth:`_reconcile` runs.

        A key :meth:`_reconcile` does not apply (see :data:`_APPLIED_KEYS`) is
        refused whenever it differs, before the taxonomy, the data or a declared
        migration is asked: no verdict can make a change happen that nothing
        writes.

        `allow_rename` relaxes the identity keys -- `name` and `type` --
        because the caller reached this key by re-keying the lock for a declared
        `ctx.moved`: the name and the parent are *expected* to differ and are
        applied by :meth:`_reconcile`.  Every other property must still match, so
        a move bundled with a structural change is still rejected.

        `current` may supply `existing`'s already-computed snapshot (see
        :func:`serialize_existing`) so it is not re-serialized here; when
        `None` it is computed lazily.

        A feature's `dtype` and `cat_filters` are judged together, as the one
        dtype lamindb validates against (see :meth:`_dtype_edit_drift`);
        `desired_dtype` is that dtype in full, as :meth:`_desired_dtype` spells
        it.
        """
        descriptive = _descriptive_keys(kind)
        if current is None:
            current = self._snapshot(existing, kind)
        structural_diffs: list[str] = []
        if kind == "Feature":
            structural_diffs.extend(
                self._dtype_edit_drift(
                    existing, key, baseline, config, current, desired_dtype
                )
            )
        for name in sorted(set(baseline) | set(config)):
            if name in descriptive or baseline.get(name) == config.get(name):
                continue
            if kind == "Feature" and name in _FEATURE_DTYPE_KEYS:
                continue
            if allow_rename and name in _IDENTITY_KEYS:
                # A declared move renames and re-parents the object, which
                # :meth:`_reconcile` applies -- vetting a record type's new place.
                continue
            if name in _REFERENCE_KEYS:
                continue
            if kind == "Sheet" and name == "schema":
                # Judged against the instance -- rows and all -- by
                # :meth:`_reconcile` (see :meth:`_vet_sheet_schema`).
                continue
            if (
                name not in _IDENTITY_KEYS
                and name in current
                and current.get(name) == config.get(name)
            ):
                # The instance already matches the plan; the lock is stale.
                continue
            if name not in _APPLIED_KEYS.get(kind, frozenset()):
                structural_diffs.append(
                    _refused_edit(name, baseline.get(name), config.get(name))
                    + " (nothing changes it in place)"
                )
                continue
            if (
                kind == "Schema"
                and name in _SCHEMA_SHAPE_KEYS
                and config.get(name) is None
            ):
                # :meth:`_reconcile` writes only a declared otype, itype or dtype:
                # lifting the removal would record its absence in the lock while
                # the instance kept the value.
                structural_diffs.append(
                    _refused_edit(name, baseline.get(name), None)
                    + " (nothing unsets it in place; keep it, or declare a new schema)"
                )
                continue
            if classify(
                kind, name, baseline.get(name), config.get(name), context=config
            ) is (Verdict.SAFE):
                # A widening / future-only edit: safe to push in place, applied
                # by :meth:`_reconcile`.
                continue
            if (
                self._data_override_reason(kind, name, config.get(name), existing)
                is not None
            ):
                # Blocked by the taxonomy, but the live data proves the edit is
                # safe; :meth:`_reconcile` pushes it in place.
                continue
            if (
                kind == "Feature"
                and name == "nullable"
                and self._migrations_for(key, narrowing=NULLABILITY)
            ):
                # Declared backfills and vouches answer for the rows and datasets
                # that may hold a null; :meth:`_vet_nullability` judges them.
                continue
            if kind == "Schema" and name in _SET_FLAGS:
                # Judged against what the schema validated by :meth:`_reconcile`
                # (see :meth:`_vet_set_flag`).
                continue
            if (
                kind == "Schema"
                and name == "flexible"
                and self._migrations_for(key, narrowing=FLEXIBLE_CHANGE)
            ):
                # The plan author vouched for the data the schema validated (see
                # :meth:`_vet_flexible_change`), which :meth:`_reconcile` reports.
                continue
            hint = (
                self._flexible_hint(existing, key)
                if kind == "Schema" and name == "flexible"
                else self._override_hint(kind, name, config.get(name), existing)
            )
            structural_diffs.append(
                _refused_edit(name, baseline.get(name), config.get(name)) + hint
            )
        if structural_diffs:
            raise PlanChangeRefusedError(
                _plan_change_refusal(kind, config.get("name"), key, structural_diffs)
            )

    def _dtype_edit_drift(
        self,
        existing: Any,
        key: str,
        baseline: dict[str, Any],
        config: dict[str, Any],
        current: dict[str, Any],
        desired_dtype: str | None,
    ) -> list[str]:
        """Judge a plan edit to a feature's dtype and `cat_filters` as one change.

        The configuration records a categorical's filters next to its bare dtype,
        but lamindb validates against both together, so an edit to either half is
        a dtype change: it is classified, proven against the data, or answered by
        a declared migration as a whole -- never let through on the strength of
        one half while the other is refused, or vice versa.  Whatever passes here
        is applied (or refused) by :meth:`_reconcile`, which diffs the same full
        dtype against the instance.

        The edit needs nothing when the instance already holds the plan's dtype,
        filters included (`desired_dtype`, see :meth:`_desired_dtype`): the lock
        is merely stale.

        Returns the refusal line -- one for both keys, the dtype the instance
        holds and the one the plan asks for in full, as the diff shows a dtype
        change -- or nothing when the edit may proceed.
        """
        changed = [
            name
            for name in _FEATURE_DTYPE_KEYS
            if baseline.get(name) != config.get(name)
        ]
        if not changed:
            return []
        if (
            desired_dtype is not None
            and "dtype" in current
            and dtypes_match(current.get("dtype"), desired_dtype)
        ):
            return []
        new_dtype = desired_dtype if desired_dtype is not None else config.get("dtype")
        needs_reloc = self._dtype_change_needs_relocation(new_dtype, existing)
        if not needs_reloc and all(
            classify("Feature", name, baseline.get(name), config.get(name))
            is Verdict.SAFE
            for name in changed
        ):
            return []
        if (
            not needs_reloc
            and self._data_override_reason("Feature", "dtype", new_dtype, existing)
            is not None
        ):
            return []
        if self._dtype_migrations_for(key):
            # A declared `ctx.migrate` flips the dtype, filters included, and
            # rewrites the stored values in :meth:`_reconcile`.
            return []
        # Both halves in one line, in full, as the diff spells the change: the
        # instance's dtype -- which the remote drift check holds to the lock's --
        # and the one the plan asks for, filters inlined.
        before = (
            repr(current["dtype"])
            if "dtype" in current
            else _config_value_label("dtype", baseline.get("dtype"))
        )
        after = (
            repr(desired_dtype)
            if desired_dtype is not None
            else _config_value_label("dtype", config.get("dtype"))
        )
        return [
            f"dtype {before} \u2192 {after}"
            + self._override_hint("Feature", "dtype", new_dtype, existing)
        ]

    def _reconcile(
        self,
        obj: Materializable,
        existing: Any,
        key: str,
        *,
        allow_rename: bool = False,
        members: list[Any] | None = None,
        warnings: list[ChangeWarning] | None = None,
        desired_dtype: str | None = None,
        origin: _ChangeOrigin | None = None,
    ) -> tuple[Action, list[str]]:
        """Reconcile `existing` against `obj`'s desired configuration.

        Three kinds of properties are handled:

        - **Space** and **descriptive metadata** (`description`, `synonyms`,
          `unit`, `reference`, `reference_type`) are safe to change in
          place: a divergence is reported as an `UPDATE` and applied
          (`setattr` + `save`) in APPLY mode.
        - **Validation-affecting and structural metadata** are classified by the
          drift taxonomy (:func:`lamindb_setup.evolve.engine._drift.classify`).  A
          *safe* change -- a widening (`nullable` `False->True`, a widening
          `dtype` such as `int->float` or `<any>->str`) or a future-only
          field (`coerce`, `default_value`) -- is applied in place like
          descriptive metadata.  A change the taxonomy marks as `BLOCKED` (a
          narrowing `nullable`/`dtype`, `otype`, or the set-semantics
          flags) may invalidate records already validated against the object and
          raises :class:`DriftError`.  A schema's `flexible` is judged against
          its `maximal_set` instead -- see :meth:`_vet_flexible_change`.
        - A **schema's member list** is reconciled towards the plan in place
          (recomputing the content hash on `save`): features the plan added
          are linked, and features it no longer declares are unlinked.  A drop
          only invalidates already-validated datasets when the schema is
          `maximal_set` -- then, and only then, the members it does not
          declare are forbidden rather than merely tolerated -- so that case
          raises :class:`DriftError` instead (the same rule
          :meth:`lamindb.Schema.remove` enforces).
        - A **schema's index** is diffed against the instance too, and changed
          in place only when the schema has validated nothing (with
          `allow_data_validated_changes`) or a declaration vouches for the data
          it did validate -- see :meth:`_vet_index_change`.  A save that rehashes
          a schema also re-saves the composites slotting it
          (:meth:`_rehash_composites`).

        Only properties the plan explicitly sets are considered (a `None`
        desired value means "no opinion"), except the schema set-semantics flags
        which always carry a concrete value and `Feature.nullable`, which
        LaminDB materializes with a concrete default the plan therefore also
        owns when it says nothing (see :data:`_IMPLICIT_DEFAULTS`).

        `allow_rename` marks a declared `ctx.moved`: the live object's
        `name` is set to the plan's name (after checking the new name is not
        already taken by a different object) and the change is reported as an
        `Action.MOVE`.

        `members` optionally supplies a schema's already-materialized member
        records so the feature diff reuses them instead of re-querying
        `existing.members`.

        `warnings` collects changes that are applied but deserve a reader's
        attention (see :func:`~lamindb_setup.evolve.engine._drift.caveat`); they
        land on the object's :class:`ChangeItem` with the drift warnings, as
        `CAVEAT` warnings since they are the plan's own changes.

        `desired_dtype` is a feature's dtype in full, its filters inlined (see
        :meth:`_desired_dtype`); computed here when not given.  It is what the
        live `dtype_as_str` is diffed against, so a filter change --
        `cat[Record[A]] -> cat[Record[B]]` -- is a dtype change like any other.

        `origin` is what the object's lock entry says about who moved a
        property since the last apply (`None` without one).  A change refused
        for a property the instance moved is drift; every other refusal is the
        plan asking for a change the engine will not make in place, raised as
        :class:`PlanChangeRefusedError` in the words :meth:`_check_plan_edit`
        uses (`cannot change Feature 'x' (…) in place: nullable True → False`),
        since nothing changed on the instance (see :func:`_reconcile_refusal`).

        Returns the computed :class:`Action` and human-readable change details.
        """
        kind_name = type(obj).__name__
        if warnings is None:
            warnings = []
        refusals: list[_Refusal] = []
        safe_updates: list[tuple[str, Any]] = []
        # Notes for blocked changes the live data proved safe (see
        # :meth:`_data_override_reason`), surfaced in the change details for audit.
        override_notes: list[str] = []
        # What turning a schema's `flexible` off leaves behind, reported right
        # after the flag change itself (see :meth:`_nonmember_notes`).
        flexible_notes: list[str] = []
        # Narrowing data migrations resolved here but *executed* later, once the
        # whole plan is materialized (see :meth:`run_deferred_migrations`): each
        # rewrites the feature's stored values so they validate against the new
        # dtype, which this reconcile flips in place via `_dtype_str`.
        pending_migrations: list[_PendingMigration] = []
        # The set flags the plan turns on, for the export sweep (see
        # :meth:`_record_schema_shape`).
        flags_on: list[str] = []
        for field_name in _GUARDED_FIELDS.get(kind_name, ()):
            desired = _desired_value(obj, field_name)
            if desired is None:
                desired = _IMPLICIT_DEFAULTS.get((kind_name, field_name))
            if desired is None and field_name not in _ALWAYS_SET_FIELDS:
                continue
            live = getattr(existing, field_name, None)
            if desired == live:
                continue
            if (
                kind_name == "Feature"
                and field_name == "nullable"
                and classify(kind_name, field_name, live, desired) is Verdict.BLOCKED
            ):
                refused = self._vet_nullability(
                    existing, key, id(obj), live, desired, override_notes
                )
                if refused is None:
                    safe_updates.append((field_name, desired))
                else:
                    refusals.append(refused)
                continue
            if (
                kind_name == "Schema"
                and field_name in _SET_FLAGS
                and classify(kind_name, field_name, live, desired) is Verdict.BLOCKED
            ):
                flags_on.append(field_name)
                if field_name == "minimal_set":
                    # Judged once the member and optional diffs below say which
                    # members it makes required (see :meth:`_vet_minimal_set`).
                    continue
                refused = self._vet_set_flag(existing, key, field_name, override_notes)
                if refused is None:
                    safe_updates.append((field_name, desired))
                else:
                    refusals.append(refused)
                continue
            if kind_name == "Schema" and field_name == "flexible":
                refused = self._vet_flexible_change(
                    obj,
                    existing,
                    key,
                    live,
                    desired,
                    override_notes=override_notes,
                    flexible_notes=flexible_notes,
                    warnings=warnings,
                )
                if refused is None:
                    safe_updates.append((field_name, desired))
                else:
                    refusals.append(refused)
                continue
            if classify(kind_name, field_name, live, desired) is Verdict.SAFE:
                # A widening (e.g. nullable False->True) or future-only change:
                # safe to push in place rather than refuse as drift.
                safe_updates.append((field_name, desired))
                continue
            reason = self._data_override_reason(
                kind_name, field_name, desired, existing
            )
            if reason is not None:
                safe_updates.append((field_name, desired))
                override_notes.append(f"{field_name}: allowed despite block ({reason})")
                continue
            refusals.append(
                _Refusal.of_values(
                    field_name,
                    live,
                    desired,
                    self._override_hint(kind_name, field_name, desired, existing),
                )
            )

        # A schema's `dtype` is changed by nothing in place, so a plan dtype the
        # instance does not have is refused here too -- not only when the lock
        # records the edit (see :meth:`_check_plan_edit`): a schema adopted
        # without one would otherwise record the plan's dtype in the lock while
        # the instance kept its own.
        if kind_name == "Schema":
            planned_dtype = serialize_config(obj).get("dtype")
            live_schema_dtype = getattr(existing, "dtype", None)
            if planned_dtype is not None and not (
                isinstance(live_schema_dtype, str)
                and dtypes_match(planned_dtype, live_schema_dtype)
            ):
                wanted = canonical_dtype_str(planned_dtype) or planned_dtype
                refused = self._vet_schema_shape(
                    existing, "dtype", live_schema_dtype, wanted, override_notes
                )
                if refused is None:
                    safe_updates.append(("dtype", wanted))
                else:
                    refusals.append(refused)
            planned_itype = getattr(obj, "itype", None)
            wanted_itype = (
                None
                if planned_itype is None
                else DEFAULT_MODELS.itype_str(planned_itype)
            )
            live_itype = getattr(existing, "itype", None)
            if wanted_itype is not None and wanted_itype != live_itype:
                refused = self._vet_schema_shape(
                    existing, "itype", live_itype, wanted_itype, override_notes
                )
                if refused is None and self._schema_members(existing):
                    refused = _Refusal.of_values(
                        "itype",
                        live_itype,
                        wanted_itype,
                        " (its members are linked through the old itype's table; "
                        "declare a new schema instead)",
                    )
                if refused is None:
                    safe_updates.append(("itype", wanted_itype))
                else:
                    refusals.append(refused)

        # A sheet's schema decides what its rows must carry, so pointing it at
        # another one -- or giving a record type one -- is vetted against those
        # rows (see :meth:`_vet_sheet_schema`).  It is diffed against the
        # instance because the lock cannot see the one case that matters most:
        # a sheet that found a same-named record type by name and adopted it --
        # schema and all, or none at all.
        sheet_schema = getattr(obj, "schema", None)
        repointed: Any = None
        if kind_name == "Sheet" and isinstance(sheet_schema, Materializable):
            desired_schema = self.materialize_object(sheet_schema)
            live_schema = getattr(existing, "schema", None)
            if isinstance(desired_schema, Placeholder) or getattr(
                desired_schema, "pk", None
            ) != getattr(live_schema, "pk", None):
                refused = self._vet_sheet_schema(
                    existing,
                    key,
                    live_schema,
                    desired_schema,
                    sheet_schema,
                    override_notes,
                )
                if refused is None:
                    repointed = desired_schema
                else:
                    refusals.append(refused)

        # A feature's dtype is captured in the config snapshot rather than as a
        # guarded scalar, but a widening dtype edit (int -> float / num, or
        # <any> -> str, or dropping a categorical filter) keeps existing values
        # valid and is applied in place via the writable `_dtype_str` field; a
        # narrowing one is refused.  The plan's dtype is compared in full, its
        # `cat_filters` inlined, so a filter change is a dtype change too.
        if kind_name == "Feature":
            if desired_dtype is None:
                desired_dtype = self._desired_dtype(obj)
            live_dtype = getattr(existing, "dtype_as_str", None)
            if desired_dtype is not None and not dtypes_match(
                desired_dtype, live_dtype
            ):
                # An in-place flip moves no value between the categorical link
                # tables and the scalar store: a `cat[...] -> str` widening or a
                # `str -> cat[...]` change waits for a migration relocating them
                # whenever such a value is stored.
                needs_reloc = self._dtype_change_needs_relocation(
                    desired_dtype, existing
                )
                if (
                    classify("Feature", "dtype", live_dtype, desired_dtype)
                    is Verdict.SAFE
                    and not needs_reloc
                ):
                    safe_updates.append(("_dtype_str", desired_dtype))
                elif (
                    not needs_reloc
                    and (
                        dtype_reason := self._data_override_reason(
                            "Feature", "dtype", desired_dtype, existing
                        )
                    )
                    is not None
                ):
                    safe_updates.append(("_dtype_str", desired_dtype))
                    override_notes.append(
                        f"dtype: allowed despite block ({dtype_reason})"
                    )
                elif declared_migrations := self._dtype_migrations_for(key):
                    # The declared migrations own the value fixup: the engine
                    # flips the dtype here (via `_dtype_str`) and runs them
                    # after every plan object exists (see
                    # :meth:`run_deferred_migrations`).  The full dtype is set
                    # purely, from the bare string and the resolved filters:
                    # staging an `ln.Feature` would re-load this very feature
                    # by name, and lamindb refuses to hand back an existing
                    # feature under a dtype it does not already have.
                    declared_dtype = getattr(obj, "dtype", None)
                    declared_filters = getattr(declared_dtype, "cat_filters", None)
                    # In PLAN mode a filter may name an object this very plan
                    # creates, whose uid is still a placeholder (see
                    # :meth:`_desired_dtype`): the dry-run is skipped (PLAN mode
                    # never creates it, so deferring would not help).  Anything
                    # else that fails here is a real error and surfaces.
                    unresolved = self._placeholder_filters(declared_filters)
                    cat_filters = (
                        None
                        if unresolved is not None
                        else self._resolve_cat_filters(declared_filters)
                    )
                    resolved_dtype = desired_dtype
                    safe_updates.append(("_dtype_str", resolved_dtype))
                    for migration in declared_migrations:
                        if unresolved is None:
                            pending_migrations.append(
                                _PendingMigration(
                                    migration=migration,
                                    desired_dtype=resolved_dtype,
                                    live_dtype=live_dtype,
                                    # Resolve the declared dtype to its *live*
                                    # registry class here: the plan holds it, the
                                    # dtype string does not, and there is no
                                    # name-to-class resolver.
                                    target_registry=self._migration_target_registry(
                                        declared_dtype
                                    ),
                                    cat_filters=cat_filters,
                                )
                            )
                        else:
                            override_notes.append(
                                f"dry-run: {migration.name} not previewed, its "
                                f"target does not exist yet ({unresolved})"
                            )
                        override_notes.append(
                            f"dtype: migrated {live_dtype!r} \u2192 "
                            f"{resolved_dtype!r} via {migration.name} "
                            f"({migration.reason})"
                        )
                else:
                    refusals.append(
                        _Refusal.of_values(
                            "dtype",
                            live_dtype,
                            desired_dtype,
                            self._override_hint(
                                "Feature", "dtype", desired_dtype, existing
                            ),
                        )
                    )
        if kind_name == "Feature":
            refused = self._vet_unit_change(
                obj, existing, key, desired_dtype, override_notes, pending_migrations
            )
            if refused is not None:
                refusals.append(refused)

        # A schema's member list is reconciled separately from its scalar
        # fields: both linking a feature the plan added and unlinking one it no
        # longer declares recompute the content hash in place.  The *optional*
        # subset of those members is tracked separately again, since LaminDB
        # keeps it in `Schema._aux` rather than on the member links.
        feature_adds: list[Any] = []
        feature_removals: list[Any] = []
        optional_adds: list[Any] = []
        optional_removals: list[Any] = []
        feature_details: list[str] = []
        reorder_features = False
        # The index, kept in `Schema._aux` like the optional set and diffed
        # against the instance like it; `index_conflict` is how lamindb should
        # settle a sheet row whose name and new index value differ.
        index_change: _IndexChange | None = None
        index_conflict: str | None = None
        slot_change: _SlotChange | None = None
        made_required: list[Any] = []
        if type(obj).__name__ == "Schema":
            feature_adds, feature_removals, reorder_features = (
                self._schema_feature_diff(obj, existing, members=members)
            )
            feature_details = [
                f"+feature {getattr(f, 'name', None)!r}" for f in feature_adds
            ]
            feature_details.extend(
                f"-feature {getattr(m, 'name', None)!r}" for m in feature_removals
            )
            if reorder_features:
                feature_details.append("reorder features to match plan order")
            if feature_adds:
                refusals.extend(
                    _Refusal.of_text("features", refused)
                    for refused in self._vet_member_adds(
                        obj, existing, key, id(obj), feature_adds, override_notes
                    )
                )
            if feature_removals:
                refusals.extend(
                    _Refusal.of_text("features", refused)
                    for refused in self._vet_member_removals(
                        obj, existing, key, id(obj), feature_removals, override_notes
                    )
                )
            # A feature the plan drops is no longer a member, so it cannot be an
            # optional member either; diff the optional set against the members
            # that survive rather than reporting the drop twice.
            dropped_uids = {str(m.uid) for m in feature_removals}
            optional_adds, optional_drops = self._schema_optional_diff(
                obj,
                existing,
                members=[
                    m
                    for m in (
                        members
                        if members is not None
                        else self._schema_members(existing)
                    )
                    if str(m.uid) not in dropped_uids
                ],
            )
            feature_details.extend(
                f"+optional {getattr(f, 'name', None)!r}" for f in optional_adds
            )
            if optional_drops:
                optional_removals, blocked_optionals = self._vet_optional_drops(
                    existing, key, id(obj), optional_drops, override_notes
                )
                feature_details.extend(
                    f"-optional {getattr(m, 'name', None)!r}" for m in optional_removals
                )
                if blocked_optionals:
                    listed = ", ".join(blocked_optionals)
                    omitted = "records that omitted them would no longer validate"
                    refusals.append(
                        _Refusal(
                            "optionals",
                            drift=f"optionals: instance has {listed} optional, the "
                            f"plan makes them required ({omitted})",
                            plan_change=f"optionals: {listed} optional \u2192 "
                            f"required ({omitted})",
                        )
                    )
            if "minimal_set" in flags_on:
                made_required = self._minimal_set_members(
                    obj, existing, members=members, feature_removals=feature_removals
                )
                refused = self._vet_minimal_set(
                    existing, key, id(obj), made_required, override_notes
                )
                if refused is None:
                    safe_updates.append(("minimal_set", True))
                else:
                    refusals.append(refused)
            index_change = self._schema_index_diff(obj, existing, members=members)
            if index_change is not None:
                blocked_index, index_conflict = self._vet_index_change(
                    existing, key, id(obj), index_change, override_notes
                )
                refusals.extend(blocked_index)
                feature_details.append(
                    f"index {index_change.before!r} \u2192 {index_change.after!r}"
                )
            slot_change, blocked_slots = self._vet_slot_changes(
                obj, existing, key, override_notes
            )
            refusals.extend(_Refusal.of_text("slots", r) for r in blocked_slots)
            if slot_change is not None:
                feature_details.extend(slot_change.details)
            self._record_schema_shape(
                obj,
                existing,
                key,
                members=members,
                feature_adds=feature_adds,
                feature_removals=feature_removals,
                optional_adds=optional_adds,
                optional_removals=optional_removals,
                index_change=index_change,
                flags_on=flags_on,
                made_required=made_required,
            )

        if refusals:
            raise _reconcile_refusal(
                kind_name,
                getattr(obj, "name", None),
                key,
                refusals,
                origin,
            )

        # Collect the safe, in-place updates: space first, then descriptive
        # metadata.  Applied together with a single save in APPLY mode.
        pending: list[tuple[str, Any]] = []
        details: list[str] = []
        # Record any data-safe overrides of blocked changes for audit visibility.
        details.extend(override_notes)

        # Widening / future-only structural edits vetted by the drift taxonomy.
        for field_name, value in safe_updates:
            live = getattr(existing, field_name, None)
            label = "dtype" if field_name == "_dtype_str" else field_name
            details.append(f"{label} {live!r} \u2192 {value!r}")
            pending.append((field_name, value))
        details.extend(flexible_notes)

        if repointed is not None:
            live_label = _schema_label(getattr(existing, "schema", None))
            details.append(
                f"schema {live_label!r} \u2192 {natural_key(sheet_schema)!r}"
            )
            pending.append(("schema", repointed))
        if kind_name == "Sheet" and (
            repointed is not None or self._migrations_for(key, narrowing=STALE_EXPORTS)
        ):
            live_schema = getattr(existing, "schema", None)
            self._sheet_repoints.append(
                _SheetRepoint(
                    obj_id=id(obj),
                    key=key,
                    sheet=existing,
                    previous=live_schema,
                    schema=live_schema if repointed is None else repointed,
                    repointed=repointed is not None,
                )
            )

        # A sheet the plan demotes to a plain record type stops validating its
        # rows: its schema is dropped, which only admits more.  The rows keep
        # their values, and an export keeps the schema it was written with.  No
        # longer a sheet, it is no experiment sheet either.
        clear_experiment_sheet = False
        if self._demotes_a_sheet(kind_name, key):
            live_schema = getattr(existing, "schema", None)
            if live_schema is not None:
                details.append(
                    f"schema {_schema_label(live_schema)!r} \u2192 None (a sheet "
                    "demoted to a record type)"
                )
                pending.append(("schema", None))
            if (getattr(existing, "_aux", None) or {}).get("isExperimentSheet"):
                details.append("is_experiment_sheet True \u2192 False")
                clear_experiment_sheet = True

        # A declared move (`ctx.moved`) renames and re-parents the live object
        # in place.  Its new address must not already be taken by a different
        # object, otherwise the save would create a duplicate or clash.  The
        # parent of a feature, a schema or a feature / schema type only groups
        # it; a record type's decides which features admit its records, so its
        # new place is vetted first.
        name_changed = False
        parent_changed = False
        if allow_rename:
            desired_name = getattr(obj, "name", None)
            live_name = getattr(existing, "name", None)
            name_changed = desired_name is not None and desired_name != live_name
            type_decl = getattr(obj, "type", None)
            desired_parent = (
                None if type_decl is None else self.materialize_object(type_decl)
            )
            live_parent = getattr(existing, "type", None)
            parent_changed = getattr(desired_parent, "pk", None) != getattr(
                live_parent, "pk", None
            )
            if name_changed or parent_changed:
                conflict = self._name_taken_by_other(obj, existing)
                if conflict is not None:
                    verb = f"rename {key!r} to {desired_name!r}"
                    if not name_changed:
                        verb = f"move {key!r}"
                    raise DriftError(
                        f"cannot {verb}: another object already uses that name "
                        f"there (uid {conflict.uid!r})."
                    )
            if name_changed:
                details.append(f"was {live_name!r}")
                pending.append(("name", desired_name))
            if parent_changed:
                if kind_name in ("RecordType", "Sheet"):
                    self._vet_record_type_move(obj, existing, key)
                live_label = None if live_parent is None else live_parent.name
                desired_label = None if type_decl is None else natural_key(type_decl)
                details.append(f"type {live_label!r} \u2192 {desired_label!r}")
                pending.append(("type", desired_parent))

        desired_space = self.effective_space_uid(obj)
        if desired_space is not None:
            current = getattr(existing, "space", None)
            current_uid = str(current.uid) if current is not None else None
            if current_uid != str(desired_space):
                details.append(f"space {current_uid!r} → {str(desired_space)!r}")
                pending.append(("space", self.resolve_space(obj)))

        for field_name in _DESCRIPTIVE_FIELDS.get(type(obj).__name__, ()):
            desired = _desired_value(obj, field_name)
            if desired is None:
                continue
            live = getattr(existing, field_name, None)
            if desired != live:
                details.append(f"{field_name} {live!r} → {desired!r}")
                pending.append((field_name, desired))

        details.extend(feature_details)

        # RecordType/Sheet settings stored outside direct columns: single_space
        # (in `_aux["ss"]`) and isExperimentSheet (in `_aux`).  Presence in
        # the plan means "enforce"; absence means "no opinion" (left untouched).
        resolved_single_space: Any | None = None
        desired_single_space = getattr(obj, "single_space_uid", None)
        if desired_single_space is not None:
            current_ss = self._current_single_space_uid(existing)
            if current_ss != str(desired_single_space):
                details.append(
                    f"single_space {current_ss!r} → {str(desired_single_space)!r}"
                )
                resolved_single_space = self.resolve_space_uid(desired_single_space)

        set_experiment_sheet = False
        if getattr(obj, "is_experiment_sheet", False):
            current_exp = bool(
                (getattr(existing, "_aux", None) or {}).get("isExperimentSheet")
            )
            if not current_exp:
                details.append("is_experiment_sheet False → True")
                set_experiment_sheet = True

        # exclude_children_from_plans (aux `_excludeChildrenFromPlans`) is a
        # definite desired state, not "no opinion": `True` enforces the key and
        # `False` removes it, so the aux is always driven to match the plan.
        exclude_children: bool | None = None
        desired_exclude = bool(getattr(obj, "exclude_children_from_plans", False))
        current_exclude = bool(
            (getattr(existing, "_aux", None) or {}).get("_excludeChildrenFromPlans")
        )
        if desired_exclude != current_exclude:
            details.append(
                f"exclude_children_from_plans {current_exclude} → {desired_exclude}"
            )
            exclude_children = desired_exclude

        # A schema's format constraints (`Schema._aux["zarr"]`), driven to the
        # plan's like the flag above.
        zarr_changed = False
        desired_zarr = (getattr(obj, "formats", None) or {}).get("zarr")
        if kind_name == "Schema":
            live_zarr = (getattr(existing, "_aux", None) or {}).get("zarr")
            if desired_zarr != live_zarr:
                details.append(f"formats.zarr {live_zarr!r} → {desired_zarr!r}")
                zarr_changed = True

        if kind_name == "Feature":
            # Remember what a sheet export written from this feature can no longer
            # be read as.  A column is exported under the feature's *name* and
            # spelled by its *dtype*, and neither survives a rename or a value
            # rewrite -- while the export still lists the same feature record, so
            # the columns alone cannot tell (see :mod:`._sheet_exports`).  A
            # widening dtype edit is deliberately absent: the stored spellings
            # keep validating, so the file does too.
            if name_changed:
                self._changed_features[existing.pk] = (
                    f"column {getattr(existing, 'name', None)!r} renamed to "
                    f"{getattr(obj, 'name', None)!r}"
                )
            elif pending_migrations:
                self._changed_features[existing.pk] = (
                    f"column {getattr(obj, 'name', None)!r} retyped, its values "
                    "were rewritten"
                )

        if (
            not pending
            and not feature_adds
            and not feature_removals
            and not optional_adds
            and not optional_removals
            and not reorder_features
            and resolved_single_space is None
            and not set_experiment_sheet
            and not clear_experiment_sheet
            and exclude_children is None
            and not zarr_changed
            and not pending_migrations
            and index_change is None
            and slot_change is None
        ):
            return Action.NO_CHANGE, []
        if pending_migrations:
            # Only the dtype flip happens here; the value rewrite is deferred
            # until every plan object has been materialized, so a migration's
            # lookup targets never dictate when this feature is created (see
            # :class:`_DeferredMigration`).  `pending` is copied because the
            # PLAN-mode dry-run applies the field updates itself, whereas the
            # APPLY branch below has already saved them.
            self._deferred_migrations.append(
                _DeferredMigration(
                    obj_id=id(obj),
                    key=key,
                    existing=existing,
                    pending=list(pending),
                    migrations=pending_migrations,
                )
            )
        if type(obj).__name__ == "Feature" and getattr(existing, "pk", None):
            # Remembered for the member backfill, which resolves NULL / DEFAULT
            # against these two fields and runs in a later phase -- by which time
            # PLAN mode has saved nothing (see :meth:`_member_fills`).
            fills = {
                field_name: value
                for field_name, value in pending
                if field_name in _FILL_FIELDS
            }
            if fills:
                self._pending_feature_fills[existing.pk] = fills
        content_hash = (
            self._predicted_schema_hash(obj, existing, pending)
            if kind_name == "Schema"
            else None
        )
        if content_hash is not None and self.mode is not ApplyMode.APPLY:
            self._hash_checks.append(
                _HashCheck(
                    obj_id=id(obj), key=key, schema=existing, content_hash=content_hash
                )
            )
        if self.mode is ApplyMode.APPLY:
            for field_name, value in pending:
                setattr(existing, field_name, value)
            if feature_removals:
                # An unlinked feature cannot stay in the optional set: that set
                # is a bare uid list in `_aux` with no referential integrity,
                # and `Schema.save` reads it back to recompute the content
                # hash, so a uid left behind would be hashed as a member the
                # schema no longer has.  Done for the reorder path too, which
                # rebuilds the links from the plan and drops them just the same.
                self._forget_optional_members(existing, feature_removals)
            if reorder_features:
                # Rebuilding the links in plan order also (re-)creates any added
                # feature in its declared slot and omits the removed ones, so
                # the plain append/unlink below is skipped.
                self._reorder_schema_members(existing, obj)
            elif feature_adds or feature_removals:
                # Read the current members *before* editing the links: the list
                # in hand, minus what is about to be unlinked and plus what is
                # about to be appended, is exactly the post-edit state, so the
                # cache can be corrected without re-reading it
                # (`features.add` only ever appends).
                current = (
                    members if members is not None else self._schema_members(existing)
                )
                if feature_removals:
                    existing.features.remove(*feature_removals)
                    dropped = {str(m.uid) for m in feature_removals}
                    current = [m for m in current if str(m.uid) not in dropped]
                if feature_adds:
                    added = [f.materialize(self) for f in feature_adds]
                    existing.features.add(*added)
                    current = [*current, *added]
                self._members_cache[id(existing)] = current
            if optional_adds:
                # Marking a member optional only touches `Schema._aux`; the
                # save below persists it (and recomputes the content hash).
                existing.optionals.add([f.materialize(self) for f in optional_adds])
            if optional_removals:
                # The vetted narrowing: a member the plan makes required again,
                # allowed only because the live rows prove it safe or a declared
                # migration backfills them (see :meth:`_vet_optional_drops`).
                existing.optionals.remove(optional_removals)
            if resolved_single_space is not None:
                existing.settings.single_space = resolved_single_space
            if set_experiment_sheet:
                existing._aux = existing._aux or {}
                existing._aux["isExperimentSheet"] = True
            if clear_experiment_sheet:
                existing._aux.pop("isExperimentSheet", None)
            if exclude_children is not None:
                existing._aux = existing._aux or {}
                if exclude_children:
                    existing._aux["_excludeChildrenFromPlans"] = 1
                else:
                    existing._aux.pop("_excludeChildrenFromPlans", None)
            if zarr_changed:
                existing.formats.zarr = desired_zarr
            save_kwargs: dict[str, Any] = {}
            if index_change is not None:
                # Set after the member edits above, so a feature the plan adds
                # as the new index is already linked when lamindb rehashes.
                desired_index = index_change.desired
                existing.index = (
                    desired_index.materialize(self)
                    if desired_index is not None
                    else None
                )
                save_kwargs = {
                    # Vetted and reported by :meth:`_vet_index_change`, which
                    # names the data concerned; lamindb's own warning would only
                    # repeat it as a raw table.
                    "print_hash_mutation_warning": False,
                    # Never `None`, which makes lamindb prompt on stdin.  The
                    # rows that would conflict were counted up front and refused
                    # without a declared choice, so with none declared there is
                    # nothing for the fallback to decide.
                    "index_name_conflict": index_conflict or "keep_name",
                }
            if slot_change is not None:
                _write_slots(existing, slot_change)
                self._slots_cache[existing.pk] = dict(slot_change.desired)
            previous_hash = getattr(existing, "hash", None)
            if kind_name == "Schema":
                details.extend(
                    self._save_schema(existing, key, save_kwargs, content_hash)
                )
            else:
                existing.save(**save_kwargs)
            if kind_name == "Schema" and existing.hash != previous_hash:
                details.extend(self._rehash_composites(existing))
        moved = name_changed or parent_changed
        return (Action.MOVE if moved else Action.UPDATE), details

    def _predicted_schema_hash(
        self, obj: Materializable, existing: Any, pending: list[tuple[str, Any]]
    ) -> str | None:
        """The content hash `existing` takes once this reconcile is saved.

        `None` when it keeps its hash, or when the change cannot collide: a
        member, optional or slot that does not exist yet (only a PLAN-mode
        preview sees one) is part of no live schema's content.  The post-change
        content is the plan's own -- the reconcile drives members, optionals and
        index to it or refuses -- and its flags, `itype` and `dtype` are the
        pending edits over the live ones.  Members are in plan order: an
        `ordered_set` schema is rebuilt into it, and any other hashes them
        sorted.
        """
        index = getattr(obj, "index", None)
        members = [self._live_record(f) for f in _declared_members(obj)]
        optionals = [
            self._live_record(f) for f in getattr(obj, "optionals", None) or []
        ]
        slots = {
            slot: self._live_record(component)
            for slot, component in (getattr(obj, "slots", None) or {}).items()
        }
        if any(r is None for r in (*members, *optionals, *slots.values())):
            return None
        edits = dict(pending)

        def after(field_name: str) -> Any:
            return edits.get(field_name, getattr(existing, field_name, None))

        content_hash = DEFAULT_MODELS.schema_content_hash(
            existing,
            features=members[1:] if index is not None else members,
            index=members[0] if index is not None else None,
            optionals=optionals,
            slots=slots,
            flexible=after("flexible"),
            coerce=after("coerce"),
            minimal_set=after("minimal_set"),
            ordered_set=after("ordered_set"),
            maximal_set=after("maximal_set"),
            itype=after("itype"),
            dtype=after("dtype"),
        )
        if content_hash is None or content_hash == getattr(existing, "hash", None):
            return None
        return content_hash

    def _save_schema(
        self,
        existing: Any,
        key: str,
        save_kwargs: dict[str, Any],
        content_hash: str | None,
    ) -> list[str]:
        """Save `existing`, absorbing or merging a schema that holds its content.

        lamindb refuses the save -- before writing anything -- when another live
        schema already holds the new hash.  Only then is the holder looked up, so
        an ordinary save costs nothing extra: an inferred schema is absorbed, a
        holder a declared `merge_schema` names is trashed, and the save
        retried, anything else refused with the reasons (see
        :mod:`._schema_collisions`).  A merged holder's references are moved
        only once `existing` is saved (see :class:`_SchemaMerge`).  A refusal
        not explained by a holder of the predicted hash is lamindb's to report,
        so it is re-raised unchanged.
        """
        from lamindb.errors import ValidationError

        from ._schema_collisions import absorb, find_collisions

        try:
            existing.save(**save_kwargs)
            return []
        except ValidationError:
            if content_hash is None:
                raise
            holders = find_collisions(
                type(existing),
                {existing.pk: content_hash},
                using=existing._state.db,
            )[existing.pk]
            if not holders:
                raise
        collisions, merges = self._classify_collisions(key, holders)
        for collision in collisions:
            self._absorbed_columns.update(absorb(existing, collision))
        for merge in merges:
            merge.trash()
        existing.save(**save_kwargs)
        for merge in merges:
            merge.repoint(existing)
        return [c.detail() for c in collisions] + [m.detail() for m in merges]

    def _classify_collisions(
        self, key: str, holders: list[Any]
    ) -> tuple[list[Any], list[_SchemaMerge]]:
        """Sort `holders` of `key`'s new content into absorbed and merged ones.

        An inferred schema nothing names is absorbed (see :mod:`._schema_collisions`).
        Any other is merged when the plan schema declares
        :func:`~lamindb_setup.evolve.merge_schema` naming it -- by uid or natural
        key -- and is not managed by a plan, and refused otherwise.  A plan
        manages it when this one resolved it or locked it, or when a sibling
        plan applied alongside declares its natural key (`claimed_keys`).
        """
        from ._schema_collisions import classify, refusal

        managed_pks, managed_uids = self._managed_schemas()
        declared = [
            m for m in self._migrations_for(key, narrowing=MERGE_SCHEMA) if m.merges
        ]
        collisions: list[Any] = []
        merges: list[_SchemaMerge] = []
        blocked: list[str] = []
        for holder in holders:
            managed = (
                holder.pk in managed_pks
                or str(holder.uid) in managed_uids
                or _schema_key(holder) in self.claimed_keys
            )
            merge = next((m for m in declared if _names_schema(m.merges, holder)), None)
            if merge is not None and not managed:
                merges.append(_SchemaMerge(holder=holder, migration=merge))
                continue
            collision = classify(holder, managed=managed)
            if collision.absorbable:
                collisions.append(collision)
            else:
                blocked.append(f"{collision.label}: {'; '.join(collision.blockers)}")
        if blocked:
            raise PlanChangeRefusedError(refusal(key, blocked))
        return collisions, merges

    def _managed_schemas(self) -> tuple[set[Any], set[str]]:
        """The live schemas a plan manages: resolved this run, or in the lock."""
        records = [*self._existing_cache.values(), *self._cache.values()]
        pks = {
            record.pk
            for record in records
            if type(record).__name__ == "Schema" and getattr(record, "pk", None)
        }
        uids = (
            {str(e.uid) for e in self.lock.objects.values() if e.uid is not None}
            if self.lock is not None
            else set()
        )
        return pks, uids

    def check_schema_collisions(self) -> None:
        """Preview the content collisions of the schemas PLAN mode would rehash.

        The PLAN-mode counterpart of :meth:`_save_schema`, judged identically but
        batched into one lookup at the end, since nothing is saved: an absorbable
        holder is reported on the change, anything else refused.  Two plan
        schemas changed onto the same content collide with each other too -- in
        APPLY mode the second save finds the first.
        """
        from ._schema_collisions import find_collisions, refusal

        if not self._hash_checks:
            return
        first = self._hash_checks[0].schema
        found = find_collisions(
            type(first),
            {check.schema.pk: check.content_hash for check in self._hash_checks},
            using=first._state.db,
        )
        refusals: list[str] = []
        for check in self._hash_checks:
            siblings = [
                other.key
                for other in self._hash_checks
                if other is not check and other.content_hash == check.content_hash
            ]
            if siblings:
                refusals.append(
                    refusal(
                        check.key,
                        [f"the plan gives {', '.join(map(repr, siblings))} it too"],
                    )
                )
                continue
            holders = found.get(check.schema.pk) or []
            if not holders:
                continue
            try:
                collisions, merges = self._classify_collisions(check.key, holders)
            except DriftError as exc:
                refusals.append(str(exc))
                continue
            details = self._change_details_for(check.obj_id)
            details.extend(collision.detail() for collision in collisions)
            details.extend(merge.detail() for merge in merges)
        if refusals:
            raise PlanChangeRefusedError("\n".join(refusals))

    def _rehash_composites(
        self, schema: Any, predicted: Mapping[Any, str] | None = None
    ) -> list[str]:
        """Re-save every composite embedding `schema`, which was just rehashed.

        lamindb derives a composite's hash from its components' hashes, but only
        when the composite itself is saved -- rehashing a component in place
        leaves every composite slotting it hashed against the component's *old*
        hash, which breaks lamindb's hash-based get-or-create for the composite.
        So each composite is re-saved, transitively and until none changes (a
        composite may slot another one that was itself just rehashed).

        The composites need not belong to this plan: the hash is derived data
        every save of them would recompute anyway.  The cached copies this run
        holds are corrected in place, so a composite the plan reconciles later
        does not write its stale hash back.  A composite the save would give the
        content of another live schema -- e.g. a duplicate a `merge_schema`
        left slotting `schema` too -- is refused with a :class:`DriftError`
        naming it (see :func:`_composite_collision`); any other refusal is
        lamindb's to report.

        `predicted` maps a composite's primary key to the hash it is expected
        to take (see :func:`_predicted_composite_hashes`); such a composite is
        saved through :meth:`_save_schema`, so landing on another live schema's
        content is settled as the PLAN-mode sweep previewed it.

        Returns a change detail naming the composites that were rehashed, and
        what saving them settled.
        """
        from lamindb.errors import ValidationError

        from ._drift_data import _schema_closure

        model = type(schema)
        composites = [c for c in _schema_closure(schema)[1:] if is_live(c)]
        rehashed: list[str] = []
        settled: list[str] = []
        for _ in range(len(composites)):
            changed = False
            for composite in composites:
                fresh = model.objects.get(pk=composite.pk)
                before = fresh.hash
                content_hash = (predicted or {}).get(fresh.pk)
                if content_hash is None:
                    try:
                        fresh.save(print_hash_mutation_warning=False)
                    except ValidationError as exc:
                        collision = _composite_collision(fresh, schema)
                        if collision is None:
                            raise
                        raise PlanChangeRefusedError(collision) from exc
                else:
                    settled.extend(
                        self._save_schema(
                            fresh,
                            _schema_key(fresh) or str(fresh.uid),
                            {"print_hash_mutation_warning": False},
                            content_hash,
                        )
                    )
                if fresh.hash == before:
                    continue
                changed = True
                label = fresh.name or str(fresh.uid)
                if label not in rehashed:
                    rehashed.append(label)
                self._refresh_cached_hash(fresh)
            if not changed:
                break
        if not rehashed:
            return settled
        return [
            "rehashed composite "
            + ", ".join(repr(name) for name in rehashed)
            + " to match",
            *settled,
        ]

    def _refresh_cached_hash(self, schema: Any) -> None:
        """Carry `schema`'s new hash onto every copy of it this run holds.

        A composite this plan manages is resolved after its components, from the
        copy cached before they were rehashed, and that copy is what its lock
        snapshot is serialized from -- so without this the lock would record the
        stale hash.  Its cached `slots` go too: they hold the components as they
        were, and a save of the composite recomputes its hash from them.
        """
        model = type(schema)
        for record in [*self._existing_cache.values(), *self._cache.values()]:
            if type(record) is model and record.pk == schema.pk:
                record.hash = schema.hash
                record.__dict__.pop("_slots", None)

    def run_deferred_migrations(self) -> None:
        """Execute every migration `_reconcile` deferred, plan now materialized.

        Runs as its own phase after the last object has been resolved so a
        migration's lookup targets need only be *declared* somewhere in the plan,
        not created before the feature (see :class:`_DeferredMigration`).  The
        dtype flip is already persisted by then, which is what the ordering
        actually requires: lamindb rejects a categorical value while the feature
        is still scalar, and the stale scalar rows survive the flip so the
        migration can still read the old values.

        In APPLY mode the migrations run for real inside the enclosing
        `transaction.atomic()` (see the CLI), so a stranded value -- or any
        exception -- rolls the flip and the partial rewrite back together.  In
        PLAN mode nothing runs unless `preview_migrations` opted in, and then
        only inside a savepoint that is always rolled back.  Either way the
        outcome is appended to the feature's :class:`ChangeItem` so it reaches
        the rendered diff.
        """
        for deferred in self._deferred_migrations:
            # Objects materialized after this feature was reconciled may have
            # annotated it, so re-read before handing it to the migration.
            deferred.existing.refresh_from_db()
            details = self._change_details_for(deferred.obj_id)
            if self.mode is ApplyMode.APPLY:
                for migration, stranded, report in self._run_pending_migrations(
                    deferred.existing, deferred.migrations
                ):
                    if stranded:
                        raise MigrationError(
                            f"migration {migration.name!r} for {deferred.key!r} "
                            f"left {stranded} value(s) stranded after relocating: "
                            "the feature's dtype was flipped but some values were "
                            "left in a store it does not read, or link labels "
                            "outside the registries or filters it admits, so they "
                            "no longer validate. Fix the migration to relocate "
                            "every value."
                        )
                    if (summary := report.summary()) is not None:
                        details.append(f"{migration.name}: {summary}")
            elif self.preview_migrations:
                # Prove (rather than only describe) the declared migration by
                # running it in a rolled-back savepoint and reporting whether it
                # relocates every value (see :meth:`_preview_pending_migrations`).
                note = self._preview_pending_migrations(
                    deferred.existing, deferred.pending, deferred.migrations
                )
                if note is not None:
                    details.append(note)
        for member_migration in self._deferred_member_migrations:
            self._run_member_migration(member_migration)
        for member_removal in self._deferred_member_removals:
            self._run_member_removal(member_removal)
        for index_change in self._deferred_index_changes:
            self._run_index_change(index_change)
        for flexible_change in self._deferred_flexible_changes:
            self._run_flexible_change(flexible_change)
        # Last: a value is written against the feature's dtype as the migrations
        # above leave it.
        for record_update in self._deferred_record_updates:
            details = self._change_details_for(record_update.obj_id)
            for migration, context in record_update.updates:
                migration.run(context)
                if (summary := context.report.summary()) is not None:
                    details.append(f"{migration.name}: {summary}")

    def _run_flexible_change(self, deferred: _DeferredFlexibleChange) -> None:
        """Run the declarations answering for a schema turning `flexible` off.

        The flag is already switched by :meth:`_reconcile`; the declarations keep
        or remove what the flexible match annotated while it was on.  Unless one
        of them keeps it (`keeps_values`), the result is then *proven*: an
        artifact still recording a non-member column fails the apply, and the
        enclosing `transaction.atomic()` rolls the flag back with it.

        In PLAN mode they run only when `preview_migrations` opted in, and then
        inside a savepoint that is always rolled back.
        """
        details = self._change_details_for(deferred.obj_id)
        if self.mode is ApplyMode.APPLY:
            reports = self._answer_flexible_change(deferred)
            remaining = self._nonmembers_left(deferred)
            if remaining:
                raise MigrationError(
                    f"{self._migration_names(deferred)} for {deferred.key!r} left "
                    f"{_plural(remaining, 'artifact')} recording columns the schema "
                    "does not declare. Fix the migration to remove them, or declare "
                    "accept_flexible_change to keep them."
                )
            for migration, report in reports:
                if (summary := report.summary()) is not None:
                    details.append(f"{migration.name}: {summary}")
        elif self.preview_migrations:
            details.append(self._preview_flexible_change(deferred))

    def _answer_flexible_change(
        self, deferred: _DeferredFlexibleChange
    ) -> list[tuple[DataMigration, MigrationReport]]:
        """Run each declared migration once, each with its own report."""
        from ._drift_data import schema_nonmember_columns

        deferred.schema.refresh_from_db()
        reports: list[tuple[DataMigration, MigrationReport]] = []
        for migration in deferred.migrations:
            report = MigrationReport()
            migration.run(
                FlexibleChangeContext(
                    schema=deferred.schema,
                    previous=deferred.previous,
                    flexible=deferred.flexible,
                    report=report,
                    recorded_nonmembers=schema_nonmember_columns,
                )
            )
            reports.append((migration, report))
        return reports

    @staticmethod
    def _nonmembers_left(deferred: _DeferredFlexibleChange) -> int:
        """How many artifacts still record a non-member column, unless that is kept.

        A count that cannot be established reads as one leftover: the migration
        has to *prove* it removed everything.
        """
        from ._drift_data import schema_nonmember_usage

        if any(m.keeps_values for m in deferred.migrations):
            return 0
        usage = schema_nonmember_usage(deferred.schema)
        return 1 if usage is None else usage.artifacts

    def _preview_flexible_change(self, deferred: _DeferredFlexibleChange) -> str:
        """Dry-run the declarations in a rolled-back savepoint (PLAN mode).

        Mirrors :meth:`_preview_member_removal`: nothing they write survives, but
        the preview reports whether they would leave a non-member column behind.
        """
        from django.db import transaction

        remaining = 0
        reports: list[tuple[DataMigration, MigrationReport]] = []
        error: str | None = None
        try:
            with transaction.atomic():
                reports = self._answer_flexible_change(deferred)
                remaining = self._nonmembers_left(deferred)
                raise _MigrationDryRunRollback
        except _MigrationDryRunRollback:
            pass
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
        finally:
            try:
                deferred.schema.refresh_from_db()
            except Exception:
                pass
        if error is not None:
            return f"dry-run: migration would fail ({error})"
        names = ", ".join(m.name for m, _ in reports)
        if remaining:
            return (
                f"dry-run: {names} would leave {_plural(remaining, 'artifact')} "
                "recording columns the schema does not declare"
            )
        summaries = [
            summary
            for _, report in reports
            if (summary := report.summary()) is not None
        ]
        detail = f" ({'; '.join(summaries)})" if summaries else ""
        return f"dry-run: {names} would succeed{detail}"

    def _run_index_change(self, deferred: _DeferredIndexChange) -> None:
        """Run the declarations that accepted an index change (APPLY mode only).

        The index is already switched and the sheet rows re-keyed by
        :meth:`_reconcile`, so there is nothing to verify afterwards: each
        callable gets an
        :class:`~lamindb_setup.evolve.authoring._migrations.IndexChangeContext` and
        whatever it reports lands in the diff.  Raising rolls the change back with
        the rest of the apply.
        """
        details = self._change_details_for(deferred.obj_id)
        deferred.schema.refresh_from_db()
        for migration in deferred.migrations:
            report = MigrationReport()
            migration.run(
                IndexChangeContext(
                    schema=deferred.schema,
                    previous=deferred.previous,
                    index=deferred.index,
                    report=report,
                )
            )
            if (summary := report.summary()) is not None:
                details.append(f"{migration.name}: {summary}")

    def _run_member_removal(self, deferred: _DeferredMemberRemoval) -> None:
        """Clear the values of members the schema no longer declares.

        The members were already unlinked by :meth:`_reconcile`; this runs the
        declared migrations that decide what happens to what they leave behind,
        then *proves* they did it: a value still stored for a member the migration
        did not declare kept would sit on a row annotating something the schema no
        longer describes, which is the leak the block exists to prevent, so it
        fails the apply and the enclosing `transaction.atomic()` rolls the
        removal back with it.

        A member the migration declared kept (see
        :meth:`~lamindb_setup.evolve.authoring._migrations.MemberRemovalContext.keep`) is
        exempt: its values are meant to stay, as undeclared extra data.

        Coverage is checked once, after all of them have run, so it does not
        matter whether one migration clears every member or each has its own.

        In PLAN mode they run only when `preview_migrations` opted in, and then
        inside a savepoint that is always rolled back.
        """
        details = self._change_details_for(deferred.obj_id)
        if self.mode is ApplyMode.APPLY:
            reports, kept = self._clear_members(deferred)
            remaining = self._members_still_carrying(deferred, kept)
            if remaining:
                raise MigrationError(
                    f"{self._migration_names(deferred)} for "
                    f"{deferred.key!r} left {remaining} stored value(s) for "
                    "members the plan removes: they would stay on records the "
                    "schema no longer describes. Fix the migration to clear "
                    "every value, or declare keep_member_values to keep them."
                )
            for migration, report in reports:
                if (summary := report.summary()) is not None:
                    details.append(f"{migration.name}: {summary}")
        elif self.preview_migrations:
            details.append(self._preview_member_removal(deferred))

    def _clear_members(
        self, deferred: _DeferredMemberRemoval
    ) -> tuple[list[tuple[DataMigration, MigrationReport]], list[Any]]:
        """Run each declared migration against the removed members.

        Every migration sees *all* of them, as with a backfill: which ones a given
        migration is responsible for is its own business (see
        :func:`~lamindb_setup.evolve.drop_member_values`'s `feature`).
        Returns the per-migration reports and the members declared kept between
        them, which the post-check exempts.
        """
        deferred.schema.refresh_from_db()
        reports: list[tuple[DataMigration, MigrationReport]] = []
        kept: list[Any] = []
        from ._drift_data import schema_rows_carrying_member

        for migration in deferred.migrations:
            report = MigrationReport()
            context = MemberRemovalContext(
                schema=deferred.schema,
                features=deferred.features,
                report=report,
                rows_carrying=schema_rows_carrying_member,
            )
            migration.run(context)
            kept.extend(context.kept)
            reports.append((migration, report))
        return reports, kept

    def _members_still_carrying(
        self, deferred: _DeferredMemberRemoval, kept: list[Any]
    ) -> int:
        """How many rows still carry a value for a removed member.

        Counted per member and summed, skipping the members the migration
        declared kept.  A count that cannot be established reads as one leftover,
        keeping the apply conservative -- the migration has to *prove* it
        succeeded, and an unprovable cleanup is a failed one.
        """
        from ._drift_data import schema_member_usage

        exempt = {member.pk for member in kept}
        remaining = 0
        for feature in deferred.features:
            if feature.pk in exempt:
                continue
            usage = schema_member_usage(deferred.schema, feature)
            remaining += 1 if usage is None else usage.carrying
        return remaining

    def _preview_member_removal(self, deferred: _DeferredMemberRemoval) -> str:
        """Dry-run a member cleanup in a rolled-back savepoint (PLAN mode).

        Mirrors :meth:`_preview_member_migration`: the migrations run for real
        inside a `transaction.atomic()` that is always rolled back, so a plain
        `plan` stays read-only while still reporting whether they would clear
        every value *between them*.
        """
        from django.db import transaction

        remaining: int | None = None
        reports: list[tuple[DataMigration, MigrationReport]] = []
        error: str | None = None
        try:
            with transaction.atomic():
                reports, kept = self._clear_members(deferred)
                remaining = self._members_still_carrying(deferred, kept)
                raise _MigrationDryRunRollback
        except _MigrationDryRunRollback:
            pass
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
        finally:
            try:
                deferred.schema.refresh_from_db()
            except Exception:
                pass
        if error is not None:
            return f"dry-run: migration would fail ({error})"
        names = ", ".join(m.name for m, _ in reports)
        if remaining:
            return (
                f"dry-run: {names} would leave {remaining} stored value(s) on "
                "records the schema no longer describes"
            )
        summaries = [
            summary
            for _, report in reports
            if (summary := report.summary()) is not None
        ]
        detail = f" ({'; '.join(summaries)})" if summaries else ""
        return f"dry-run: {names} accounted for every stored value{detail}"

    def _run_member_migration(self, deferred: _DeferredMemberMigration) -> None:
        """Backfill the rows that omitted a schema's newly-required members.

        The optional set was already narrowed by :meth:`_reconcile`; this runs the
        declared migrations that have to make the data match, then *proves* they
        did: any row still omitting one of the members would no longer validate
        against the schema, which is exactly the invalidation the block exists to
        prevent, so it fails the apply and the enclosing `transaction.atomic()`
        rolls the narrowing back with it.

        Coverage is checked once, after all of them have run, so it does not
        matter whether one migration backfills every member or each member has
        its own -- only that between them nothing is left behind.

        In PLAN mode they run only when `preview_migrations` opted in, and then
        inside a savepoint that is always rolled back, so a dry-run reports what
        the backfill *would* leave behind without persisting it.
        """
        details = self._change_details_for(deferred.obj_id)
        if self.mode is ApplyMode.APPLY:
            reports, accepted_nulls = self._backfill_members(deferred)
            remaining = self._members_still_omitted(deferred, accepted_nulls)
            if remaining:
                raise MigrationError(
                    f"{self._migration_names(deferred)} for {deferred.key!r} left "
                    f"{remaining} record(s) without a value for {deferred.purpose}: "
                    "they would no longer validate against the schema. Fix the "
                    "migration to backfill every record."
                )
            for migration, report in reports:
                if (summary := report.summary()) is not None:
                    details.append(f"{migration.name}: {summary}")
        elif self.preview_migrations:
            details.append(self._preview_member_migration(deferred))

    @staticmethod
    def _migration_names(
        deferred: _DeferredMemberMigration
        | _DeferredMemberRemoval
        | _DeferredFlexibleChange,
    ) -> str:
        """The declared migrations named for a message, e.g. `migrations 'a', 'b'`."""
        names = ", ".join(repr(m.name) for m in deferred.migrations)
        noun = "migration" if len(deferred.migrations) == 1 else "migrations"
        return f"{noun} {names}"

    def _backfill_members(
        self, deferred: _DeferredMemberMigration
    ) -> tuple[list[tuple[DataMigration, MigrationReport]], _AcceptedNulls]:
        """Run each declared backfill against the schema's newly-required members.

        Every migration sees *all* the members, not a slice of them: which ones a
        given migration is responsible for is its own business (see
        :func:`~lamindb_setup.evolve.fill_missing`'s `feature`), and
        the engine only judges the result.  Each gets its own report so the diff
        can attribute what was written.

        Returns the reports together with the rows the migrations declared as
        deliberately null (see
        :meth:`~lamindb_setup.evolve.authoring._migrations.MemberMigrationContext.accept_null`),
        merged across them: lamindb has no stored null, so a null-filled row
        stays un-annotated and the post-check has to be told it was answered for.
        """
        deferred.schema.refresh_from_db()
        self._member_fills(deferred)
        reports: list[tuple[DataMigration, MigrationReport]] = []
        accepted_nulls: _AcceptedNulls = {}
        from ._drift_data import schema_rows_omitting_member

        def rows_omitting(schema: Any, feature: Any) -> list[Any]:
            return schema_rows_omitting_member(
                schema, feature, registries=deferred.registries
            )

        for migration in deferred.migrations:
            context = MemberMigrationContext(
                schema=deferred.schema,
                features=deferred.features,
                report=MigrationReport(),
                rows_omitting=rows_omitting,
            )
            migration.run(context)
            reports.append((migration, context.report))
            for feature_pk, by_model in context.accepted_nulls.items():
                merged = accepted_nulls.setdefault(feature_pk, {})
                for model_name, pks in by_model.items():
                    merged.setdefault(model_name, set()).update(pks)
        return reports, accepted_nulls

    def _member_fills(self, deferred: _DeferredMemberMigration) -> None:
        """Give each member the `nullable` / `default_value` the plan wants.

        A backfill resolves `NULL` / `DEFAULT` against these two fields, so
        both modes have to see the values the plan is writing rather than the
        ones the members were loaded with -- otherwise a plan that makes a member
        nullable, or gives it a default, previews an outcome (or a failure) the
        apply would not produce.

        The members are re-read first: they were captured while the *schema* was
        reconciled, and APPLY has saved their own edits since.  PLAN has not, so
        the pending edits recorded during their reconcile are overlaid on top
        (see :attr:`_pending_feature_fills`); in APPLY that overlay is what the
        refresh already returned, which is exactly why the two agree.
        """
        for member in deferred.features:
            member.refresh_from_db()
            for field_name, value in self._pending_feature_fills.get(
                member.pk, {}
            ).items():
                setattr(member, field_name, value)

    def _members_still_omitted(
        self, deferred: _DeferredMemberMigration, accepted_nulls: _AcceptedNulls
    ) -> int:
        """How many rows still omit one of the newly-required members.

        Counted per member and summed, so a row omitting two of them counts
        twice: the number is only ever compared against zero and reported, and
        summing keeps it a plain per-member total rather than implying a distinct
        row count.  An omission count that cannot be established reads as one
        omission, keeping the apply conservative -- the migration has to *prove*
        it succeeded, and an unprovable backfill is a failed one.

        Rows a migration declared as deliberately null do not count: an
        un-annotated row *is* the null, so nothing distinguishes it from a
        skipped one except the migration having said which it is.
        """
        from ._drift_data import schema_member_omissions

        remaining = 0
        for feature in deferred.features:
            omissions = schema_member_omissions(
                deferred.schema,
                feature,
                exempt=accepted_nulls.get(feature.pk),
                registries=deferred.registries,
            )
            remaining += 1 if omissions is None else omissions.omitting
        return remaining

    def _preview_member_migration(self, deferred: _DeferredMemberMigration) -> str:
        """Dry-run a member backfill in a rolled-back savepoint (PLAN mode).

        Mirrors :meth:`_preview_pending_migrations`: the migrations run for real
        inside a `transaction.atomic()` that is always rolled back, so a plain
        `plan` stays read-only while still reporting whether they would actually
        cover every row *between them*.
        """
        from django.db import transaction

        remaining: int | None = None
        reports: list[tuple[DataMigration, MigrationReport]] = []
        error: str | None = None
        try:
            with transaction.atomic():
                reports, accepted_nulls = self._backfill_members(deferred)
                remaining = self._members_still_omitted(deferred, accepted_nulls)
                raise _MigrationDryRunRollback
        except _MigrationDryRunRollback:
            pass
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
        finally:
            try:
                deferred.schema.refresh_from_db()
            except Exception:
                pass
        if error is not None:
            return f"dry-run: migration would fail ({error})"
        names = self._migration_names(deferred)
        if remaining:
            return (
                f"dry-run: {names} would leave {remaining} record(s) without a "
                "value for a required member"
            )
        summaries = [
            f"{migration.name}: {summary}"
            for migration, report in reports
            if (summary := report.summary()) is not None
        ]
        verb = "backfills" if len(deferred.migrations) == 1 else "backfill"
        return f"dry-run: {names} {verb} every record" + (
            f" ({'; '.join(summaries)})" if summaries else ""
        )

    def _change_details_for(self, obj_id: int) -> list[str]:
        """The mutable `details` of the change recorded for `obj_id`.

        A phase running after :meth:`_record` still needs to contribute to the
        rendered diff; :class:`ChangeItem` carries the plan object's `id` for
        exactly that.  A `NO_CHANGE` reconcile records an item too, which is
        what lets :meth:`check_sheet_exports` annotate a schema whose own
        configuration did not change but whose members were renamed elsewhere in
        the plan.  A throwaway list is returned when no change was recorded at
        all, so a late annotation can never fail the apply.
        """
        for change in self.changes:
            if change.obj_id == obj_id:
                return change.details
        return []

    def _run_pending_migrations(
        self,
        existing: Any,
        pending_migrations: list[_PendingMigration],
    ) -> list[tuple[DataMigration, int | None, MigrationReport]]:
        """Run each pending migration against the dtype-flipped `existing`.

        Assumes the dtype flip has already been persisted (a categorical value
        cannot be written while the feature is still scalar).  Runs each
        migration, refreshes the feature, and reports the number of values it
        left *stranded* in the store the old dtype used (see
        :func:`lamindb_setup.evolve.engine._drift_data.feature_stranded_value_count`;
        `None` when that count cannot be established) together with the
        migration's :class:`~lamindb_setup.evolve.authoring._migrations.MigrationReport`.
        Shared by the APPLY path (which raises :class:`MigrationError` on a
        non-zero count) and the PLAN dry-run (which reports it).
        """
        from ._drift_data import feature_stranded_value_count

        outcomes: list[tuple[DataMigration, int | None, MigrationReport]] = []
        for pending in pending_migrations:
            report = MigrationReport()
            pending.migration.run(
                MigrationContext(
                    feature=existing,
                    desired_dtype=pending.desired_dtype,
                    live_dtype=pending.live_dtype,
                    target_registry=pending.target_registry,
                    cat_filters=pending.cat_filters,
                    report=report,
                )
            )
            existing.refresh_from_db()
            outcomes.append(
                (
                    pending.migration,
                    feature_stranded_value_count(existing, pending.desired_dtype),
                    report,
                )
            )
        return outcomes

    def _migration_target_registry(self, declared_dtype: Any) -> Any:
        """The live registry class a plan feature's declared dtype points at.

        A migration that maps values *into* a registry needs the class itself,
        but the engine only carries the dtype as a *string* and no
        registry-name-to-class resolver exists.  The plan's
        :class:`~lamindb_setup.evolve.LaminDType` does hold it: its
        `dtype` property re-imports the class against the currently connected
        instance, so it is never a stale object from a previous connection.

        Returns `None` for a scalar target (a plain Python type or a dtype
        sentinel string is not a registry) or when the declared dtype cannot be
        resolved.
        """
        try:
            resolved = getattr(declared_dtype, "dtype", None)
        except Exception:
            # A dtype the current instance cannot resolve (e.g. a registry from
            # a module this instance does not load) is simply not available to
            # the migration; it may still map values by other means.
            return None
        # A `list[cat[...]]` target links into the same registry as its
        # scalar `cat[...]` counterpart.
        if get_origin(resolved) is list:
            (resolved,) = get_args(resolved)
        # A registry class is a Django model; a scalar target resolves to a
        # builtin type or a string sentinel ("num" / "path" / "url").
        return resolved if hasattr(resolved, "_meta") else None

    def _desired_dtype(self, obj: Materializable) -> str | None:
        """The full dtype a plan feature asks for: its bare dtype with its filters.

        :attr:`~lamindb_setup.evolve.LaminDType.dtype_str` is bare
        (`cat[Record]`); the feature's `cat_filters` complete it
        (`cat[Record[<type uid>]]`) and are as much a part of what lamindb
        validates against, so this inlines them (see :func:`resolved_dtype_str`).
        In PLAN mode a filter naming an object this plan has not created yet
        cannot be resolved (see :meth:`_placeholder_filters`) and is spelled
        :data:`~lamindb_setup.evolve.core.PLACEHOLDER_UID`, which no live dtype
        carries, so the result still compares as the change it is.

        `None` when the plan object carries no dtype.  A filter lamindb would
        reject raises `ValueError`, as creating the feature would.
        """
        declared = getattr(obj, "dtype", None)
        bare = getattr(declared, "dtype_str", None)
        if bare is None:
            return None
        declared_filters = getattr(declared, "cat_filters", None)
        if not declared_filters:
            return bare
        cat_filters = self._resolve_cat_filters(
            {
                key: PLACEHOLDER_UID if self._is_placeholder_promise(value) else value
                for key, value in declared_filters.items()
            }
        )
        return resolved_dtype_str(bare, cat_filters)

    def _resolve_cat_filters(self, cat_filters: Any) -> dict[str, Any] | None:
        """A plan dtype's `cat_filters` with every :class:`Promise` resolved.

        A declared filter may reference another plan object's uid (e.g.
        `type__uid=record_type.uid`), which is a lazy promise until that object
        is materialized.  A migration uses these as *query* filters, so they must
        be concrete values; this mirrors what
        :meth:`~lamindb_setup.evolve.authoring._features.Feature.stage` does when it inlines
        them into the dtype string.  Returns `None` when there are no filters.
        """
        if not cat_filters:
            return None
        return {
            key: (value.resolve(self) if isinstance(value, Promise) else value)
            for key, value in cat_filters.items()
        }

    def _placeholder_filters(self, cat_filters: Any) -> str | None:
        """Name the `cat_filters` that point at an object not created yet.

        In PLAN mode an object the plan would create materializes to a
        :class:`~lamindb_setup.evolve.core.Placeholder` with no real uid, so a
        filter promising that uid cannot be resolved, let alone inlined into a
        dtype string.  Returns a note naming each such filter (by its promise
        label), or `None` when every filter can resolve.
        """
        if not cat_filters:
            return None
        pending = [
            value.label or key
            for key, value in cat_filters.items()
            if self._is_placeholder_promise(value)
        ]
        if not pending:
            return None
        return f"known only after apply: {', '.join(pending)}"

    def _is_placeholder_promise(self, value: Any) -> bool:
        """Whether `value` promises the uid of an object PLAN mode has not created."""
        return (
            isinstance(value, Promise)
            and isinstance(target := value.get_arg(), Materializable)
            and isinstance(self.materialize_object(target), Placeholder)
        )

    def _preview_pending_migrations(
        self,
        existing: Any,
        pending: list[tuple[str, Any]],
        pending_migrations: list[_PendingMigration],
    ) -> str | None:
        """Dry-run the declared migrations in a rolled-back savepoint (PLAN mode).

        Applies the dtype flip and runs each migration inside a
        `transaction.atomic()` that is always rolled back, so nothing persists
        and a plain `plan` stays read-only.  Returns a human-readable note for
        the change details: whether the migration relocated every value, left
        some stranded, or itself failed (lamindb rejects an invalid categorical
        value at write time, so an unrepresentable target surfaces here).
        """
        from django.db import transaction

        outcomes: list[tuple[DataMigration, int | None, MigrationReport]] = []
        error: str | None = None
        try:
            with transaction.atomic():
                for field_name, value in pending:
                    setattr(existing, field_name, value)
                existing.save()
                outcomes = self._run_pending_migrations(existing, pending_migrations)
                # Force a rollback so the dry-run persists nothing; the sentinel
                # unwinds the atomic block without committing.
                raise _MigrationDryRunRollback
        except _MigrationDryRunRollback:
            pass
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
        finally:
            # Discard the in-memory mutations from the rolled-back savepoint so
            # `existing` again reflects the (unchanged) committed state.
            try:
                existing.refresh_from_db()
            except Exception:
                pass
        if error is not None:
            return f"dry-run: migration would fail ({error})"
        parts: list[str] = []
        for migration, stranded, report in outcomes:
            if stranded:
                parts.append(
                    f"dry-run: {migration.name} would leave {stranded} value(s) "
                    "stranded (they would not validate)"
                )
            elif stranded is None:
                parts.append(
                    f"dry-run: {migration.name} ran but relocation could not be "
                    "verified"
                )
            else:
                parts.append(
                    f"dry-run: {migration.name} relocated all values, none stranded"
                )
            if (summary := report.summary()) is not None:
                parts.append(f"dry-run: {migration.name} would apply {summary}")
        return "; ".join(parts) if parts else None

    def _name_taken_by_other(self, obj: Materializable, existing: Any) -> Any | None:
        """Return a live record other than `existing` already using `obj`'s name.

        Used before a declared rename to refuse renaming onto a name already
        occupied by a *different* object of the same kind and parent type.  The
        lookup mirrors the read-only identity match (name + `is_type` +
        parent) and excludes `existing` itself; `None` means the name is
        free.
        """
        name = getattr(obj, "name", None)
        if name is None:
            return None
        kind = type(obj).__name__
        registry = registry_for_kind(kind)
        filters: dict[str, Any] = {"name": name, "is_type": kind in _IS_TYPE_KINDS}
        type_decl = getattr(obj, "type", None)
        if type_decl is not None:
            parent = type_decl.get(self)
            if parent is None:
                return None
            filters["type"] = parent
        else:
            filters["type"] = None
        for rec in registry.filter(**filters):
            if str(rec.uid) != str(existing.uid):
                return rec
        return None

    def _current_single_space_uid(self, existing: Any) -> str | None:
        """Return the space uid currently enforced by `existing`'s `single_space`.

        `False` (disabled) yields `None`; `True` (enforce the dynamic
        registry's own space) resolves to the record's own `space` uid so the
        comparison is idempotent against a plan that names that exact space.
        """
        settings = getattr(existing, "settings", None)
        if settings is None:
            return None
        value = settings.single_space
        if value is False:
            return None
        if value is True:
            space = getattr(existing, "space", None)
            return str(space.uid) if space is not None else None
        return str(value.uid)

    def _schema_feature_diff(
        self, obj: Materializable, existing: Any, *, members: list[Any] | None = None
    ) -> tuple[list[Any], list[Any], bool]:
        """Diff a schema's desired members against the instance's members.

        Returns `(added, removed, reorder)` where `added` are the plan
        features not yet linked to `existing` (matched by the live feature's
        uid, so a feature that already exists standalone but is not a member
        counts as an addition), `removed` are the live members the plan no
        longer declares -- which :meth:`_reconcile` unlinks -- and `reorder` is
        `True` when the schema is `ordered_set` and the plan's declared
        member order differs from the order the instance would end up in by
        merely appending `added` to the surviving members.  Plan features are
        resolved through the shared existence cache (:meth:`_existing`), so this
        adds no per-feature queries on top of the prefetched lookups and is safe
        in PLAN mode.

        `members` optionally supplies `existing`'s already-materialized
        members so this reuses them instead of re-querying `existing.members`
        (the same list the snapshot uses); when `None` they are fetched
        through the shared cache (:meth:`_schema_members`).

        The plan's `index` feature counts as a declared member wherever the
        plan lists it, because LaminDB makes it one: `Schema.__init__` inserts
        it at the front of the member list, and :meth:`Schema.stage
        <lamindb_setup.evolve.authoring._schemas.Schema.stage>` drops it from `features`
        so it is not linked twice.  Reading the plan's `features` alone would
        therefore report an index the plan declares only as `index=` as a
        member to remove -- unlinking it on the next apply and leaving
        `Schema.index` pointing at a feature the schema no longer has.
        Normalizing to LaminDB's own order also settles the `ordered_set`
        comparison below, which would otherwise ask for a reorder forever when
        the plan lists the index anywhere but first.

        Members that were soft-deleted in the instance are dropped first (see
        :func:`~lamindb_setup.evolve.engine._trash.is_live`): trashing a feature does
        not unlink it, so it would
        otherwise read as a member the plan removes, while the live feature the
        plan does declare -- a different uid under the same name -- would read as
        an addition that can never converge.
        """
        if members is None:
            members = self._schema_members(existing)
        members = [member for member in members if is_live(member)]
        member_uids = {str(m.uid) for m in members}
        declared = _declared_members(obj)
        desired = [(f, self._existing(f)) for f in declared]
        desired_uids = {str(live.uid) for _, live in desired if live is not None}
        added = [
            f for f, live in desired if live is None or str(live.uid) not in member_uids
        ]
        removed = [m for m in members if str(m.uid) not in desired_uids]

        # For an `ordered_set` schema the member order is significant, but
        # `existing.features.add` only ever appends (LaminDB orders members by
        # their through-row id, see `Schema.members`).  Inserting a feature in
        # the middle therefore lands it at the end unless the links are rebuilt.
        # Detect that case: compare the plan's declared order against the order
        # appending `added` to the surviving members would produce, matching
        # each feature by its live uid (or, for a not-yet-created feature, its
        # plan object identity, which is stable across both sequences).
        # `removed` members are already excluded from that comparison, so a
        # drop that leaves the rest in plan order needs no rebuild.
        reorder = False
        if getattr(obj, "ordered_set", False):

            def _ident(feature: Any, live: Any) -> Any:
                return str(live.uid) if live is not None else id(feature)

            added_ids = {id(f) for f in added}
            desired_seq = [_ident(f, live) for f, live in desired]
            append_seq = [str(m.uid) for m in members if str(m.uid) in desired_uids] + [
                _ident(f, live) for f, live in desired if id(f) in added_ids
            ]
            reorder = desired_seq != append_seq
        return added, removed, reorder

    def _schema_optional_diff(
        self, obj: Materializable, existing: Any, *, members: list[Any] | None = None
    ) -> tuple[list[Any], list[tuple[Any, str]]]:
        """Diff a schema's desired optional members against the instance's.

        Returns `(added, dropped)` where `added` are the plan features the
        plan marks optional and the instance does not -- a widening, since an
        optional member may be omitted, so marking one more member optional only
        admits more records -- and `dropped` pairs each live optional member the
        plan no longer marks optional with a description of it.  Dropping one
        makes it required again, which invalidates every record that omitted it,
        so the caller has to vet each against the live data (or a declared
        migration) before it may be pushed; the member record itself is returned
        for exactly that, and to unmark it when it may.

        LaminDB keeps the optional set in `Schema._aux` rather than on the
        member links, so it is invisible to :meth:`_schema_feature_diff`; reading
        it through `optionals.get_uids()` uses the already-loaded aux and costs
        no query.  Plan features resolve through the shared existence cache
        (:meth:`_existing`), so one that does not exist yet is necessarily an
        addition.

        Being a bare uid list, that aux entry has neither a branch filter nor
        referential integrity of its own; both are recovered from the member
        records, which carry the branch.  An optional uid that is not a *live*
        member -- soft-deleted (see
        :func:`~lamindb_setup.evolve.engine._trash.is_live`), or dangling because the
        record is gone -- cannot make any record validate differently, so it is
        not part of the schema's effective optional set and is ignored.  Without
        that filter a trashed member is an unfixable drift: the plan addresses
        features by natural key, which can only ever resolve to the live record
        of that name, so no plan edit can put the trashed uid back.
        """
        if members is None:
            members = self._schema_members(existing)
        live_members = {str(m.uid): m for m in members if is_live(m)}
        optionals = getattr(existing, "optionals", None)
        live = {
            uid
            for uid in (optionals.get_uids() if optionals is not None else ())
            if uid in live_members
        }
        desired_uids: set[str] = set()
        # The uid the plan's own optional of a given *name* resolves to, so a
        # dropped optional can say when the plan simply addresses a different
        # object (see :meth:`_describe_dropped_optional`).
        desired_by_name: dict[str, str] = {}
        added: list[Any] = []
        for feature in getattr(obj, "optionals", None) or []:
            live_feature = self._existing(feature)
            uid = str(live_feature.uid) if live_feature is not None else None
            if uid is not None:
                desired_uids.add(uid)
                name = getattr(feature, "name", None)
                if name is not None:
                    desired_by_name[str(name)] = uid
            if uid is None or uid not in live:
                added.append(feature)
        dropped_uids = live - desired_uids
        return added, [
            (
                live_members[uid],
                self._describe_dropped_optional(
                    uid, live_members[uid], desired_by_name
                ),
            )
            for uid in sorted(dropped_uids)
        ]

    @staticmethod
    def _describe_dropped_optional(
        uid: str, member: Any, desired_by_name: dict[str, str]
    ) -> str:
        """Describe an optional member the plan would make required again.

        The name alone is ambiguous: several features can carry it (they are
        unique per feature type, not per instance), and the plan addresses them
        by natural key, so "the plan makes 'x' required" reads as a plan edit
        even when the plan does declare an optional called `x` -- one that
        merely resolves to a *different* record.  Naming the uid on both sides
        says which of the two situations it is.
        """
        name = getattr(member, "name", None)
        if name is None:
            return f"uid {uid!r}"
        declared = desired_by_name.get(str(name))
        if declared is not None and declared != uid:
            return (
                f"{name!r} (uid {uid!r}; the plan's {name!r} is a different "
                f"object, uid {declared!r})"
            )
        return f"{name!r} (uid {uid!r})"

    @staticmethod
    def _forget_optional_members(existing: Any, removed: Sequence[Any]) -> None:
        """Drop `removed` from `existing`'s optional set, if listed there.

        Optionality is a property of *membership*, so a feature the schema no
        longer has cannot remain in it.  The set is a bare uid list in
        `Schema._aux` (see :meth:`_schema_optional_diff`), and
        :meth:`lamindb.Schema.save` reads it back through `optionals.get()`
        when it recomputes the content hash -- which resolves a uid by registry
        lookup, membership or not -- so an entry left behind would be hashed as
        a member the schema does not have.

        Only the uids actually listed are passed on: LaminDB's
        `optionals.remove` removes them from the list by value and raises on
        one that is not there.  The caller need not know which members were
        optional.
        """
        optionals = getattr(existing, "optionals", None)
        if optionals is None:
            return
        listed = set(optionals.get_uids())
        stale = [member for member in removed if str(member.uid) in listed]
        if stale:
            optionals.remove(stale)

    def _reorder_schema_members(self, existing: Any, obj: Materializable) -> None:
        """Rebuild `existing`'s member links to match the plan's declared order.

        LaminDB orders schema members by their through-row id, so an ordered
        schema can only be reordered by dropping and re-creating the links in the
        desired sequence (the same approach LaminDB uses internally when
        transferring schema members).  Every plan feature is materialized first
        (creating any not yet present), then the through rows are recreated in
        plan order so the auto-incrementing ids -- and thus `Schema.members` --
        follow it.  The order is the one :meth:`_schema_feature_diff` compared
        against, index first: a plan that declares its index only as `index=`
        must not lose that link here.

        The through rows are the one deletion here that does *not* go through
        :func:`~lamindb_setup.evolve.engine._trash.delete_object`: a link row is a
        `BaseSQLRecord` with no `branch` field, so it has no trash to
        saturate at and its queryset delete is already an unconditional row
        removal.  It must stay one -- `Schema.members` resolves the links by id
        with no branch filter, so a link that could be soft-deleted would come
        back as a duplicate member of the row that replaced it.
        """
        ordered = [f.materialize(self) for f in _declared_members(obj)]
        through = existing.features.through
        fk_name = next(
            field.name
            for field in through._meta.get_fields()
            if getattr(field, "many_to_one", False) and field.name != "schema"
        )
        through.objects.filter(schema_id=existing.id).delete()
        through.objects.bulk_create(
            [
                through(**{"schema_id": existing.id, f"{fk_name}_id": feature.id})
                for feature in ordered
            ]
        )
        # The links were just written in this order, so it *is* the new member
        # list; recording it keeps the cache accurate without a re-read.
        self._members_cache[id(existing)] = ordered

    def _record(
        self,
        obj: Materializable,
        key: str,
        kind: str,
        uid: str,
        config: dict[str, Any],
        action: Action,
        *,
        details: list[str] | None = None,
        warnings: list[ChangeWarning] | None = None,
        snapshot: dict[str, Any] | None = None,
        track: bool = True,
        misalignment: UidMisalignment | None = None,
        borrowed: bool = False,
    ) -> None:
        if track:
            stored_uid = None if uid == PLACEHOLDER_UID else uid
            self.entries[key] = LockEntry(
                key=key,
                kind=kind,
                name=getattr(obj, "name", None),
                uid=stored_uid,
                config=config,
                remote_snapshot=snapshot,
                remote_hash=(
                    remote_state_hash(snapshot) if snapshot is not None else None
                ),
            )
        if id(obj) in self._change_ids:
            return
        self._change_ids.add(id(obj))
        self.changes.append(
            ChangeItem(
                obj_id=id(obj),
                key=key,
                kind=kind,
                name=getattr(obj, "name", None),
                action=action,
                uid=uid,
                details=details or [],
                warnings=warnings or [],
                uid_misalignment=misalignment,
                borrowed=borrowed,
            )
        )

    def apply_removals(self) -> None:
        """Report (and in APPLY mode delete) objects dropped from the plan.

        Any object recorded in the lock whose natural key the plan no longer
        produces is a removal: it is rendered as a `DELETE` change and, in
        APPLY mode, deleted from the instance.  Deletions run deepest-key-first
        so children are removed before the parents they reference.  Removed
        entries are not carried into the new lock (`build_lock` only records
        resolved objects).

        A removed key that a sibling plan still claims (`claimed_keys`) is not
        deleted -- the live object survives -- but is reported as a `TRANSFER`
        so the handoff of ownership between plans managed against the same
        instance is visible rather than silent.

        Seed-only and borrowed objects are outside this plan's deletion scope.

        The deletion itself saturates at the trash (see
        :func:`~lamindb_setup.evolve.engine._trash.delete_object`).  Dropping the object
        from the plan is exactly what :meth:`_resolve` tells the operator to do
        about an object trashed out-of-band, so the record this deletes is
        routinely in the trash already; it is then left there, and the lock entry
        is forgotten either way.

        A feature is only deleted once no schema outside the plan lists it any
        more.  An inferred artifact schema has it removed, after the deletions
        and reported on the feature's change; any other holder refuses the
        deletion (see :meth:`_settle_member_holders`).
        """
        if self.lock is None:
            return
        removed: list[LockEntry] = []
        transferred: list[LockEntry] = []
        for key, entry in self.lock.objects.items():
            if (
                key in self.entries
                or entry.kind in SEED_ONLY_KINDS
                or key in self.borrowed_keys
            ):
                continue
            (transferred if key in self.claimed_keys else removed).append(entry)
        self._refuse_deleting_claimed_uids(removed)
        for entry in transferred:
            owner = self.claimed_by.get(entry.key)
            detail = (
                f"now tracked by {owner}"
                if owner is not None
                else "now tracked by another plan"
            )
            self.changes.append(
                ChangeItem(
                    obj_id=id(entry),
                    key=entry.key,
                    kind=entry.kind,
                    name=entry.name,
                    action=Action.TRANSFER,
                    uid=entry.uid or "(unknown)",
                    details=[detail],
                )
            )
        self._warn_unmoved_renames(removed)
        removed.sort(key=lambda entry: entry.key.count("/"), reverse=True)
        records = {
            entry.key: registry_for_kind(entry.kind).filter(uid=entry.uid).one_or_none()
            for entry in removed
            if entry.uid is not None
        }
        inferred, detached = self._settle_member_holders(removed, records)
        container_details = self._vet_container_removals(removed, records)
        if self.mode is ApplyMode.APPLY:
            # Before any deletion, while every feature is still live.
            self._detach_members(detached)
        for entry in removed:
            record = records.get(entry.key)
            if record is not None:
                details = self._vet_feature_deletion(entry, record)
                if entry.kind == "Feature":
                    details.extend(inferred.details_for(record))
                    for detachment in detached:
                        details.extend(detachment.details_for(record))
                details.extend(container_details.get(entry.key, ()))
                if self.mode is ApplyMode.APPLY:
                    details.extend(self._run_removal_migrations(entry, record))
                elif entry.kind in ("RecordType", "Sheet") and not any(
                    rehoming.migration.relocation in _ROWS_TARGET_KINDS
                    for rehoming in self._rehomings.get(entry.key, [])
                ):
                    # What APPLY clears before the drops (see :meth:`_holding_links`),
                    # unless keep_rows or move_rows keeps the rows instead.
                    self._removed_type_pks.add(record.pk)
                elif entry.kind == "Feature" and not any(
                    m.keeps_values for m in self._value_migrations_for(entry.key)
                ):
                    self._cleared_feature_pks.add(record.pk)
            else:
                details = []
            self.changes.append(
                ChangeItem(
                    obj_id=id(entry),
                    key=entry.key,
                    kind=entry.kind,
                    name=entry.name,
                    action=Action.DELETE,
                    uid=entry.uid or "(unknown)",
                    details=details,
                )
            )
            if self.mode is ApplyMode.APPLY and record is not None:
                delete_object(record)
        if self.mode is ApplyMode.APPLY and inferred:
            # After the deletions, so a schema this pass trashes no longer holds
            # its content when an inferred one is rehashed onto it.  Trashing a
            # feature detaches nothing, so its member links are still there.
            self._absorbed_columns.update(inferred.apply())

    def drop_records(self) -> None:
        """Trash the seeded records the plan declares dropped with `drop_record()`.

        A plan never deletes a record it stops declaring (see
        :data:`~lamindb_setup.evolve.core.SEED_ONLY_KINDS`), so a removal is only
        ever what a :data:`RECORD_REMOVAL` declaration asks for, by natural key.
        It is refused while the plan -- or a sibling plan -- still declares the
        record, while objects still hold it as a categorical value, unless the
        declaration relocates them onto another record that every annotating
        feature admits, and while records still name it as their parent.  A
        record already gone is skipped, so the declaration can stay in place.

        The drops are judged together, whatever order they are declared in:
        every record and relocation target is resolved before the first one is
        trashed, a record dropped as well holds nothing, and nothing is relocated
        onto one.
        """
        drops = {
            key: removals
            for key, declared in self.migrations.items()
            if (removals := [m for m in declared if m.narrowing == RECORD_REMOVAL])
        }
        doomed = {
            key: record
            for key, removals in drops.items()
            if (record := self._record_to_drop(key, removals)) is not None
        }
        targets = {
            key: self._relocation_target(key, record, drops[key], doomed)
            for key, record in doomed.items()
        }
        for key, record in doomed.items():
            self._drop_record(key, record, drops[key], targets[key], doomed)

    def _record_to_drop(self, key: str, removals: list[DataMigration]) -> Any | None:
        """The live record the `removals` declared for `key` drop, or `None`."""
        names = ", ".join(f"{m.name} ({m.reason})" for m in removals)
        if key.rpartition("/")[2].partition(":")[0] not in SEED_ONLY_KINDS:
            raise DriftError(
                f"{names} is declared for {key!r}, which is not a record's natural "
                "key ('RecordType:<type>/Record:<name>'). Any other object is "
                "deleted by no longer declaring it."
            )
        if key in self.seeded or key in self.borrowed_keys or key in self.claimed_keys:
            raise DriftError(
                f"cannot drop {key!r}: a plan still declares it and would seed it "
                "again. Stop declaring it to drop it."
            )
        return self._seeded_record_for_key(key)

    def _drop_record(
        self,
        key: str,
        record: Any,
        removals: list[DataMigration],
        target: Any | None,
        doomed: dict[str, Any],
    ) -> None:
        """Run the `removals` declared for `key` and trash the record."""
        from ._drift_data import record_value_links

        names = ", ".join(f"{m.name} ({m.reason})" for m in removals)
        links = [
            (link, link.rows.count()) for link in self._holding_links(record, doomed)
        ]
        rewritten = self._rewrites_naming(record)
        held = sum(count for _link, count in links) + sum(rewritten.values())
        if held and target is None:
            raise DriftError(
                f"cannot drop {key!r}: {_plural(held, 'annotation')} still "
                f"hold it as their value ({_describe_value_links(links, rewritten)})."
                " Declare drop_record(relocate_to=...) to move them onto another "
                "record first, or remove them."
            )
        if held and target is not None and not isinstance(target, Placeholder):
            feature_ids = {feature.pk for feature in rewritten}
            for link, count in links:
                if count:
                    feature_ids.update(link.rows.values_list("feature_id", flat=True))
            self._vet_relocation(key, target, feature_ids)
        self._refuse_dropping_a_parent(key, record, doomed)
        details = [f"dropped via {names}"]
        if self.mode is ApplyMode.APPLY:
            for migration in removals:
                context = RecordRemovalContext(
                    record=record,
                    relocate_to=target,
                    value_links=record_value_links,
                )
                migration.run(context)
                if (summary := context.report.summary()) is not None:
                    details.append(f"{migration.name}: {summary}")
            remaining = sum(
                link.rows.count() for link in self._holding_links(record, doomed)
            )
            if remaining:
                raise MigrationError(
                    f"{key!r}: {_plural(remaining, 'annotation')} still hold it as "
                    f"their value after {names} ran, and would point into the "
                    "trash. Relocate or remove them in the migration."
                )
            delete_object(record)
        elif held and target is not None:
            onto = (
                PLACEHOLDER_UID
                if isinstance(target, Placeholder)
                else repr(getattr(target, "name", None) or str(target.uid))
            )
            details.append(f"{_plural(held, 'annotation')} relocated onto {onto}")
        self.changes.append(
            ChangeItem(
                obj_id=id(record),
                key=key,
                kind="Record",
                name=record.name,
                action=Action.DELETE,
                uid=str(record.uid),
                details=details,
            )
        )

    def _holding_links(self, record: Any, doomed: dict[str, Any]) -> list[Any]:
        """The link rows holding `record` as their value when it is dropped.

        What :func:`~lamindb_setup.evolve.engine._drift_data.record_value_links`
        finds, less the annotations of a record `doomed` alongside it, which
        go to the trash in the same apply.  APPLY has also run the record updates
        and the removals by then; a PLAN-mode preview has not, so it leaves out
        the rows they clear: a value an `update_record()` rewrites (what it
        writes instead is :meth:`_rewrites_naming`'s), the values of a feature
        deleted with them, and the annotations of a deleted record type's
        records.
        """
        from django.db.models import Q

        from ._drift_data import record_value_links

        dropped = [other.pk for other in doomed.values() if other.pk != record.pk]
        preview = self.mode is ApplyMode.PLAN
        links = []
        for link in record_value_links(record):
            subject = link.subject
            on_records = subject is not None and subject.related_model is type(record)
            cleared = Q()
            if on_records and dropped:
                cleared |= Q(**{f"{subject.attname}__in": dropped})
            if preview and self._cleared_feature_pks:
                cleared |= Q(feature_id__in=self._cleared_feature_pks)
            if preview and on_records and self._removed_type_pks:
                cleared |= Q(**{f"{subject.name}__type_id__in": self._removed_type_pks})
            if preview and on_records:
                for record_pk, feature_pk in self._rewritten_values:
                    cleared |= Q(
                        **{subject.attname: record_pk, "feature_id": feature_pk}
                    )
            links.append(link._replace(rows=link.rows.exclude(cleared)))
        return links

    def _rewrites_naming(self, record: Any) -> dict[Any, int]:
        """Per feature, how many values a PLAN-mode record update writes as `record`.

        APPLY has written them by the time `record` is dropped (see
        :meth:`_holding_links`), so the preview counts them as the link rows they
        become: a declared record by identity, a name as the feature resolves
        it.
        """
        from ._migrations import _is_categorical, _label_for

        counts: dict[Any, int] = {}
        for feature, desired in self._rewritten_values.values():
            if not _is_categorical(feature):
                continue
            values = desired if isinstance(desired, (list, tuple, set)) else [desired]
            for value in values:
                if getattr(value, "_meta", None) is None:
                    try:
                        value = _label_for(feature, value)
                    except MigrationError:
                        continue
                if type(value) is type(record) and value.pk == record.pk:
                    counts[feature] = counts.get(feature, 0) + 1
        return counts

    def _refuse_dropping_a_parent(
        self, key: str, record: Any, doomed: dict[str, Any]
    ) -> None:
        """Refuse dropping `record` while a record it keeps names it as a parent.

        A child keeps its link to a trashed parent, so a hierarchy would hang off
        the trash.  A child dropped alongside it goes too, and in a PLAN-mode
        preview so does one of a record type :meth:`apply_removals` deletes.
        """
        children = getattr(record, "children", None)
        if children is None:
            return
        left = children.exclude(branch_id=-1).exclude(
            pk__in=[other.pk for other in doomed.values()]
        )
        if self.mode is ApplyMode.PLAN and self._removed_type_pks:
            left = left.exclude(type_id__in=self._removed_type_pks)
        count = left.count()
        if not count:
            return
        shown = ", ".join(
            repr(name) for name in left.values_list("name", flat=True)[:5]
        )
        more = ", ..." if count > 5 else ""
        raise DriftError(
            f"cannot drop {key!r}: {_plural(count, 'record')} still "
            f"{'names' if count == 1 else 'name'} it as a parent ({shown}{more}), "
            "and would hang off the trash. Move them under another parent, or "
            "drop them as well."
        )

    def _seeded_record_for_key(self, key: str) -> Any | None:
        """The live record `key` names, or `None`.

        By the uid this plan's lock remembers for it (see
        :attr:`LockFile.seeded`), else by walking the name chain: record types
        down to the record.  A name shared by several records at one level is
        refused rather than guessed.
        """
        import lamindb as ln

        uid = self.lock.seeded.get(key) if self.lock is not None else None
        if uid is not None:
            record = ln.Record.filter(uid=uid).one_or_none()
            if record is not None and is_live(record):
                return record
        segments = key.split("/")
        parent = None
        for depth, segment in enumerate(segments):
            kind, _sep, name = segment.partition(":")
            is_type = depth < len(segments) - 1
            if kind not in (("RecordType", "Sheet") if is_type else SEED_ONLY_KINDS):
                return None
            matches = list(
                ln.Record.filter(name=name, is_type=is_type, type=parent)[:2]
            )
            if len(matches) > 1:
                raise DriftError(
                    f"{key!r} names more than one record ({segment!r} is not "
                    "unique under its parent); rename the duplicates first."
                )
            if not matches:
                return None
            parent = matches[0]
        return parent

    def _relocation_target(
        self,
        key: str,
        record: Any,
        removals: list[DataMigration],
        doomed: dict[str, Any],
    ) -> Any | None:
        """The record `removals` move `record`'s annotations onto, if any.

        A plan `Record` is resolved like any plan object (a placeholder in PLAN
        mode while it is yet to be created), a string as a natural key, else as a
        uid.  A record `doomed` as well is refused: the annotations would go to
        the trash with it.
        """
        import lamindb as ln

        specs = [m.relocates_to for m in removals if m.relocates_to is not None]
        if not specs:
            return None
        if len(specs) > 1:
            raise DriftError(
                f"cannot drop {key!r}: more than one declaration names a record "
                "to relocate its annotations onto."
            )
        (spec,) = specs
        if isinstance(spec, Materializable):
            target = self.materialize_object(spec)
        elif isinstance(spec, str) and ":" in spec:
            target = self._seeded_record_for_key(spec)
        elif isinstance(spec, str):
            target = ln.Record.filter(uid=spec).one_or_none()
        else:
            target = spec
        if isinstance(target, Placeholder):
            return target
        if target is None or not is_live(target):
            raise DriftError(
                f"cannot drop {key!r}: the record to relocate its annotations "
                f"onto ({spec!r}) does not exist."
            )
        if target.pk == record.pk:
            raise DriftError(
                f"cannot drop {key!r}: it cannot be relocated onto itself."
            )
        for other, dropped in doomed.items():
            if type(dropped) is type(target) and dropped.pk == target.pk:
                raise DriftError(
                    f"cannot drop {key!r} onto {other!r}: the plan drops that "
                    "record too. Relocate onto a record the plan keeps."
                )
        return target

    def _vet_relocation(self, key: str, target: Any, feature_ids: set[Any]) -> None:
        """Refuse relocating onto `target` when an annotating feature rejects it.

        `feature_ids` are the features the annotations are values of; `None`
        stands for a label attached without one, which any record may be.
        """
        import lamindb as ln

        from ._drift_data import admitted_label_keys

        rejecting = [
            feature
            for feature in ln.Feature.objects.filter(pk__in=feature_ids - {None})
            if (admitted := admitted_label_keys([target], feature.dtype_as_str))
            is not None
            and (type(target), target.pk) not in admitted
        ]
        if rejecting:
            raise DriftError(
                f"cannot drop {key!r} onto {target.name!r}: "
                + ", ".join(
                    f"feature {f.name!r} ({f.dtype_as_str}) does not admit it"
                    for f in rejecting
                )
                + ". Relocate onto a record each of them admits."
            )

    def _vet_container_removals(
        self, removed: list[LockEntry], records: dict[str, Any]
    ) -> dict[str, list[str]]:
        """Refuse deleting a type or schema that would take more than itself.

        Deleting moves the object to the trash, and lamindb's trash cascades: a
        `FeatureType`, `SchemaType`, `RecordType` or `Sheet` takes every
        object typed by it along, and a `Schema` leaves the datasets it
        validated pointing at a trashed one.  Objects the plan deletes in the same
        apply are expected to go; anything else is refused, naming it, unless the
        plan said what should become of it, by the deleted object's natural key:

        * a record type's (or sheet's) *records* take exactly one of
          `drop_rows()`, which trashes them deliberately, `keep_rows()` and
          `move_rows()`, which re-type them (see :meth:`_rows_removal`);
        * a schema's validated *datasets* take one of
          `keep_validated(reason=...)`, which vouches for leaving them pointing
          at the trashed schema, and `revalidate_against()`, which points them
          at another (see :meth:`_datasets_removal`);
        * a sub-type, a feature or a schema typed under a deleted type takes
          `adopt_children()`, which re-parents it (see :meth:`_children_removal`);
        * a sheet validating against a deleted schema, or a composite slotting
          it: nothing the plan can declare -- move or delete it first.

        Every refusal is raised at once, before anything is deleted, in PLAN and
        APPLY mode alike.  Returns the notes for the deletions a declaration
        answered, keyed by natural key.
        """
        removed_pks: dict[Any, set[Any]] = {}
        for record in records.values():
            if record is not None:
                removed_pks.setdefault(type(record), set()).add(record.pk)
        self._rehomings = {}
        refusals: list[str] = []
        notes: dict[str, list[str]] = {}
        for entry in removed:
            record = records.get(entry.key)
            if (
                record is None
                or entry.kind not in _VETTED_REMOVAL_KINDS
                or not is_live(record)
            ):
                continue
            problems, notes[entry.key] = self._container_removal(
                entry, record, removed_pks
            )
            if problems:
                refusals.append(
                    f"{entry.kind} {entry.name!r} (uid {entry.uid!r}): "
                    + "; ".join(problems)
                )
        if refusals:
            raise DriftError(
                "cannot delete objects the plan no longer declares: the trash "
                "would take what still depends on them along:\n  "
                + "\n  ".join(refusals)
            )
        return notes

    def _container_removal(
        self, entry: LockEntry, record: Any, removed_pks: dict[Any, set[Any]]
    ) -> tuple[list[str], list[str]]:
        """What deleting one type or schema would take along: `(refusals, notes)`."""
        if entry.kind == "Schema":
            return self._schema_removal(entry, record, removed_pks)
        problems: list[str] = []
        notes: list[str] = []
        gone = removed_pks.get(type(record), set())
        if entry.kind in ("RecordType", "Sheet"):
            rows = _live_rows(record).exclude(pk__in=gone)
            if count := rows.count():
                found, noted = self._rows_removal(
                    entry, record, rows, count, removed_pks
                )
                problems.extend(found)
                notes.extend(noted)
        children = _live_children(record).exclude(pk__in=gone)
        if count := children.count():
            found, noted = self._children_removal(
                entry, record, children, count, removed_pks
            )
            problems.extend(found)
            notes.extend(noted)
        return problems, notes

    def _schema_removal(
        self, entry: LockEntry, record: Any, removed_pks: dict[Any, set[Any]]
    ) -> tuple[list[str], list[str]]:
        """What deleting a schema would leave pointing at it: `(refusals, notes)`."""
        from ._drift_data import schema_validated_usage

        problems: list[str] = []
        notes: list[str] = []
        records_model = registry_for_kind("Sheet")
        sheets = (
            records_model.objects.filter(is_type=True, schema_id=record.pk)
            .exclude(branch_id=-1)
            .exclude(pk__in=removed_pks.get(records_model, set()))
        )
        if count := sheets.count():
            problems.append(
                f"{_plural(count, 'sheet')} the plan does not delete "
                f"({_record_labels(sheets, count)}) "
                f"{'validates' if count == 1 else 'validate'} against it"
            )
        composites = record.composites.exclude(branch_id=-1).exclude(
            pk__in=removed_pks.get(type(record), set())
        )
        if count := composites.count():
            problems.append(
                f"{_plural(count, 'composite schema')} the plan does not delete "
                f"({_record_labels(composites, count)}) "
                f"{'slots' if count == 1 else 'slot'} it"
            )
        usage = schema_validated_usage(record)
        if usage is None:
            problems.append("what it validated could not be established")
            return problems, notes
        datasets = _datasets_only(usage)
        if not datasets.total:
            return problems, notes
        found, noted = self._datasets_removal(entry, record, datasets, removed_pks)
        problems.extend(found)
        notes.extend(noted)
        return problems, notes

    # --- what a removal declaration re-homes -----------------------------

    def _rows_removal(
        self,
        entry: LockEntry,
        record: Any,
        rows: Any,
        count: int,
        removed_pks: dict[Any, set[Any]],
    ) -> tuple[list[str], list[str]]:
        """What becomes of a deleted record type's records: `(refusals, notes)`.

        They take exactly one declaration: `drop_rows()` (or a hand-written
        migration) trashes them with it, `keep_rows()` and `move_rows()`
        re-type them instead (see :meth:`_vet_rows_rehoming`).
        """
        migrations = self._migrations_for(entry.key, narrowing=ROWS_REMOVAL)
        if not migrations:
            return [
                f"{_plural(count, 'record')} ({_record_labels(rows, count)}) "
                "would go to the trash with it; declare "
                f"ctx.migrate({entry.key!r}, drop_rows()) to trash them "
                "deliberately, or keep_rows() or move_rows(to=...) to keep them"
            ], []
        relocating = [m for m in migrations if m.relocation]
        if relocating and len(migrations) > 1:
            return [
                _conflicting(
                    "its records", migrations, "drop_rows, keep_rows or move_rows"
                )
            ], []
        if not relocating:
            return [], [
                f"{_plural(count, 'record')} trashed with it by "
                + ", ".join(f"{m.name} ({m.reason})" for m in migrations)
            ]
        return self._vet_rows_rehoming(
            entry, record, rows, count, removed_pks, relocating[0]
        )

    def _vet_rows_rehoming(
        self,
        entry: LockEntry,
        record: Any,
        rows: Any,
        count: int,
        removed_pks: dict[Any, set[Any]],
        migration: DataMigration,
    ) -> tuple[list[str], list[str]]:
        """Judge `keep_rows()` or `move_rows()` re-typing the records.

        Refused: a target of the wrong kind (a sheet for `keep_rows`, see
        :meth:`_rehoming_target`), a name already taken there, and a feature
        filtering on the deleted type that holds the records as values -- re-typed,
        they would no longer satisfy it (see :meth:`_held_by_filters`).  Rows
        moving into a sheet are vetted against its schema as well (see
        :meth:`_vet_rows_into_sheet`).
        """
        moving = migration.relocation == "move_rows"
        target, problem = self._rehoming_target(
            record,
            migration,
            _ROWS_TARGET_KINDS[migration.relocation],
            removed_pks,
        )
        if problem is not None:
            return [problem], []
        label = self._rehoming_label(migration, target, none="no record type")
        rehoming = _Rehoming(migration=migration, target=target, label=label)
        problems = [
            f"{migration.name}: {held}"
            for held in self._held_by_filters(record, removed_pks)
        ]
        validated = ""
        if moving and self._rehoming_kind(migration, target) == "Sheet":
            found, validated = self._vet_rows_into_sheet(
                record, rows, count, target, migration, rehoming
            )
            problems.extend(f"{migration.name}: {p}" for p in found)
        if not isinstance(target, Placeholder):
            names = dict(rows.values_list("pk", "name"))
            names.update(rehoming.renames)
            if taken := self._names_taken(type(record), names.values(), target):
                where = (
                    "among the untyped records" if target is None else f"under {label}"
                )
                problems.append(
                    f"{migration.name}: {len(taken)} of its records "
                    f"({', '.join(repr(n) for n in taken[:_MAX_LISTED])}"
                    f"{', ...' if len(taken) > _MAX_LISTED else ''}) would take a "
                    f"name already used {where}"
                )
        if problems:
            return problems, []
        self._rehomings.setdefault(entry.key, []).append(rehoming)
        if moving and target is not None and not isinstance(target, Placeholder):
            if _live_kind(target) == "Sheet":
                self._mark_sheet_gaining(entry, migration, target, count)
        if not moving:
            where = "untyped" if target is None else f"under {label}"
            done = f"kept {where}"
        else:
            done = f"moved to {label}{validated}"
        return [], [
            f"{_plural(count, 'record')} {done} by {migration.name} "
            f"({migration.reason})"
        ]

    def _vet_rows_into_sheet(
        self,
        record: Any,
        rows: Any,
        count: int,
        target: Any,
        migration: DataMigration,
        rehoming: _Rehoming,
    ) -> tuple[list[str], str]:
        """Vet rows `move_rows()` moves into a sheet: `(refusals, how validated)`.

        As for a sheet pointed at another schema (see :meth:`_vet_sheet_schema`),
        exactly the moved rows are judged against the target's schema: they keep
        their names, so a schema keyed by another index than the source's would
        have to re-key every one, which is not done in place; each must carry
        every member it requires, which the curator does not check; and
        lamindb's curator must accept them.  A row whose name differs from its
        value for the shared index is settled by `index_name_conflict`.  The
        first two are read off the plan for a plan sheet, so a dry run answers
        them as the apply does; the curation needs the schema as the apply
        leaves it, so one the plan creates or edits is curated by the apply only.
        """
        from ._drift_data import rows_index_name_conflicts, sheet_rows_validate

        spec = migration.relocates_to
        planned = (
            getattr(spec, "schema", None) if isinstance(spec, Materializable) else None
        )
        if planned is not None:
            schema = self.materialize_object(planned)
            index = self._plan_index_uid(planned)
            schema_label = f"Schema {getattr(planned, 'name', None)!r}"
        else:
            schema = getattr(target, "schema", None)
            index = getattr(schema, "_index_feature_uid", None)
            schema_label = f"Schema {_schema_label(schema)!r}"
        source = getattr(record, "schema", None)
        source_index = getattr(source, "_index_feature_uid", None)
        if index is not None and index != source_index:
            return [
                f"{rehoming.label} is keyed by its schema's index, which its "
                "records are not; they would have to be re-keyed by it, which is "
                "not done in place"
            ], ""
        problems: list[str] = []
        feature = (
            None
            if index is None
            else self._live_record(planned.index)
            if planned is not None
            else getattr(schema, "index", None)
        )
        if feature is not None:
            conflicts = rows_index_name_conflicts(rows, feature)
            if conflicts is None:
                problems.append(
                    f"whether its records' names agree with their "
                    f"{feature.name!r} values could not be established"
                )
            elif conflicts and migration.index_name_conflict is None:
                shown = "; ".join(
                    f"row {name!r}: {feature.name}={value!r}"
                    for name, value in list(conflicts.values())[:_MAX_LISTED]
                )
                problems.append(
                    f"{len(conflicts)} of its records "
                    f"{'has' if len(conflicts) == 1 else 'have'} a name that "
                    f"differs from {'its' if len(conflicts) == 1 else 'their'} "
                    f"{feature.name!r} value ({shown}); declare "
                    "index_name_conflict='keep_name' to keep the names or "
                    "'keep_feature' to take the values"
                )
            elif conflicts and migration.index_name_conflict == "keep_feature":
                rehoming.renames = {
                    pk: value for pk, (_name, value) in conflicts.items()
                }
        omitted = (
            self._sheet_rows_omitting(record, planned)
            if planned is not None
            else self._live_rows_omitting(record, schema)
        )
        if omitted is None:
            problems.append(
                f"whether they carry the members {schema_label} requires could "
                "not be established"
            )
        elif omitted:
            problems.append(
                f"they do not all carry the members {schema_label} requires: "
                + "; ".join(omitted)
            )
        unmade = (
            "creates"
            if isinstance(schema, Placeholder) or isinstance(target, Placeholder)
            else "edits"
            if (planned is not None and self._unwritten_edit(planned))
            or (isinstance(spec, Materializable) and self._unwritten_edit(spec))
            else None
        )
        if unmade is not None:
            return (
                problems,
                f", validated against {schema_label} once the apply {unmade} it",
            )
        check = sheet_rows_validate(record, schema)
        if check is None or check.rows != count:
            problems.append(
                f"whether they validate against {schema_label} could not be established"
            )
        elif check.failure is not None:
            problems.append(
                f"they do not all validate against {schema_label}: {check.failure}"
            )
        return problems, f", validated against {schema_label}"

    def _mark_sheet_gaining(
        self, entry: LockEntry, migration: DataMigration, target: Any, count: int
    ) -> None:
        """Hand the export sweep a sheet `move_rows()` moves rows into.

        Every export of it misses them, so it is judged like a re-pointed sheet
        (see :meth:`_repointed_sheet_exports`), answered by a declaration on the
        target -- by its plan object, or by its natural key.
        """
        spec = migration.relocates_to
        planned = isinstance(spec, Materializable)
        self._sheet_repoints.append(
            _SheetRepoint(
                obj_id=id(spec) if planned else id(entry),
                key=natural_key(spec)
                if planned
                else (_live_key(target) or str(target.uid)),
                sheet=target,
                previous=getattr(target, "schema", None),
                schema=getattr(target, "schema", None),
                repointed=False,
                gained=count,
                source=f"{entry.kind} {entry.name!r}",
            )
        )

    def _live_rows_omitting(self, existing: Any, schema: Any) -> list[str] | None:
        """What the rows of `existing` omit of the members the live `schema` requires.

        The counterpart of :meth:`_sheet_rows_omitting` for a schema no plan
        object describes.
        """
        from ._drift_data import sheet_member_omissions

        if schema is None or not schema.minimal_set:
            return []
        index = getattr(schema, "_index_feature_uid", None)
        optional = {str(uid) for uid in schema.optionals.get_uids()}
        found: list[str] = []
        for member in self._schema_members(schema):
            if str(member.uid) == index or str(member.uid) in optional:
                continue
            omissions = sheet_member_omissions(existing, member)
            if omissions is None:
                return None
            if omissions.omitting:
                found.append(_omission(member.name, omissions))
        return found

    def _held_by_filters(
        self, record: Any, removed_pks: dict[Any, set[Any]]
    ) -> list[str]:
        """The features filtering on `record` that hold its records as values.

        `cat[Record[<record>]]` admits the records below it; re-typed away,
        they no longer satisfy the filter, so each such feature's values have to
        be relocated first.  A feature the plan deletes in the same apply is not
        asked.
        """
        from ._drift_data import type_subtree_link_count

        features_model = registry_for_kind("Feature")
        features = (
            features_model.objects.filter(
                is_type=False, _dtype_str__contains=str(record.uid)
            )
            .exclude(branch_id=-1)
            .exclude(pk__in=removed_pks.get(features_model, set()))
        )
        held: list[str] = []
        for feature in features:
            count = type_subtree_link_count(feature, record)
            where = f"feature {feature.name!r} ({feature.dtype_as_str})"
            if count is None:
                held.append(
                    f"whether {where}, which filters on it, holds its records as "
                    "values could not be established"
                )
            elif count:
                held.append(
                    f"{where} filters on it and holds {_plural(count, 'value')} "
                    "among its records, which would no longer satisfy it; "
                    "relocate them first with a ctx.migrate on the feature "
                    "(remap_records() or map_to_records())"
                )
        return held

    @staticmethod
    def _names_taken(model: Any, names: Iterable[Any], target: Any) -> list[str]:
        """The `names` a live record already uses under `target` (`None`: untyped)."""
        wanted = {name for name in names if name}
        if not wanted:
            return []
        taken = (
            model.objects.filter(
                type_id=None if target is None else target.pk,
                is_type=False,
                name__in=wanted,
            )
            .exclude(branch_id=-1)
            .values_list("name", flat=True)
        )
        return sorted(set(taken))

    def _children_removal(
        self,
        entry: LockEntry,
        record: Any,
        children: Any,
        count: int,
        removed_pks: dict[Any, set[Any]],
    ) -> tuple[list[str], list[str]]:
        """What becomes of the children typed under a deleted type: `(refusals, notes)`.

        Without `adopt_children()` the deletion is refused.  With it each
        child moves under its target -- or to the root -- keeping its uid and its
        own children, as a `ctx.moved` re-parenting does: a name already taken
        there is refused, and so is a child another plan manages, since moving it
        would leave that plan's lock on a stale key.
        """
        noun = _CHILD_NOUNS[entry.kind]
        listed = f"{_plural(count, noun)} the plan does not delete ({_record_labels(children, count)})"
        migrations = self._migrations_for(entry.key, narrowing=CHILDREN_REMOVAL)
        if not migrations:
            return [
                f"{listed} {'is' if count == 1 else 'are'} typed under it and "
                "would go to the trash too; move or delete "
                f"{'it' if count == 1 else 'them'} first, keep declaring it, or "
                f"declare ctx.migrate({entry.key!r}, adopt_children(to=...)) to "
                "re-parent them"
            ], []
        if len(migrations) > 1:
            return [_conflicting("its children", migrations, "adopt_children")], []
        (migration,) = migrations
        target, problem = self._rehoming_target(
            record, migration, _CHILD_TARGET_KINDS[entry.kind], removed_pks
        )
        if problem is not None:
            return [problem], []
        label = self._rehoming_label(migration, target, none="the root")
        problems: list[str] = []
        owned = sorted(
            key
            for child in children
            if (key := _live_key(child)) is not None and key in self.claimed_keys
        )
        if owned:
            owners = sorted({self.claimed_by.get(key, "another plan") for key in owned})
            problems.append(
                f"{migration.name}: {', '.join(repr(k) for k in owned)} "
                f"{'is' if len(owned) == 1 else 'are'} managed by "
                f"{', '.join(owners)}; moving {'it' if len(owned) == 1 else 'them'} "
                "would leave that plan's lock on a stale key -- move "
                f"{'it' if len(owned) == 1 else 'them'} there"
            )
        if not isinstance(target, Placeholder):
            taken = self._children_taken(record, children, target)
            if taken:
                problems.append(
                    f"{migration.name}: {', '.join(repr(n) for n in taken)} "
                    f"would take a name already used under {label}"
                )
        if problems:
            return problems, []
        self._rehomings.setdefault(entry.key, []).append(
            _Rehoming(migration=migration, target=target, label=label)
        )
        return [], [
            f"{_plural(count, noun)} moved to {label} by {migration.name} "
            f"({migration.reason})"
        ]

    @staticmethod
    def _children_taken(record: Any, children: Any, target: Any) -> list[str]:
        """The children's names a live sibling of the same kind holds under `target`."""
        model = type(record)
        siblings = model.objects.filter(
            type_id=None if target is None else target.pk
        ).exclude(branch_id=-1)
        taken: list[str] = []
        for child in children:
            clash = siblings.filter(name=child.name, is_type=child.is_type).exclude(
                pk=child.pk
            )
            if any(_live_kind(other) == _live_kind(child) for other in clash):
                taken.append(child.name)
        return sorted(taken)

    def _datasets_removal(
        self,
        entry: LockEntry,
        record: Any,
        datasets: SchemaUsage,
        removed_pks: dict[Any, set[Any]],
    ) -> tuple[list[str], list[str]]:
        """What becomes of a deleted schema's datasets: `(refusals, notes)`.

        They take one declaration: `keep_validated()` vouches for leaving them
        pointing at the trashed schema, `revalidate_against()` points them at
        another one admitting everything the deleted one did (see
        :meth:`_vet_revalidation`).
        """
        migrations = self._migrations_for(entry.key, narrowing=VALIDATED_REMOVAL)
        if not migrations:
            return [
                f"{_describe_usage(datasets)} validated against it would point at "
                f"a trashed schema; declare ctx.migrate({entry.key!r}, "
                "keep_validated(reason=...)) to vouch for that, or "
                "revalidate_against(schema=...) to point them at another"
            ], []
        relocating = [m for m in migrations if m.relocation]
        if relocating and len(migrations) > 1:
            return [
                _conflicting(
                    "its datasets", migrations, "keep_validated or revalidate_against"
                )
            ], []
        if not relocating:
            return [], [
                f"{_describe_usage(datasets)} keep pointing at it, vouched for by "
                + ", ".join(f"{m.name} ({m.reason})" for m in migrations)
            ]
        return self._vet_revalidation(entry, record, removed_pks, relocating[0])

    def _vet_revalidation(
        self,
        entry: LockEntry,
        record: Any,
        removed_pks: dict[Any, set[Any]],
        migration: DataMigration,
    ) -> tuple[list[str], list[str]]:
        """Judge `revalidate_against()` pointing the deleted schema's datasets elsewhere.

        Nothing re-curates a dataset in place, so the target has to admit every
        one the deleted schema did, by the rule a widening slot swap follows (see
        :func:`_shape_shortfalls`): judged as this apply leaves a plan schema, so
        a dry run reaches the apply's verdict, and for a composite slot by slot.
        `narrower=True` vouches for a target admitting less.  The sheet exports
        stay with the deleted schema: the CSV was written against it.
        """
        from ._drift_data import schema_validated_usage

        target, problem = self._rehoming_target(
            record, migration, frozenset({"Schema"}), removed_pks
        )
        if problem is not None:
            return [problem], []
        label = self._rehoming_label(migration, target, none="no schema")
        shortfalls = self._schema_shortfalls(record, migration.relocates_to, target)
        if shortfalls and not migration.narrower:
            return [
                f"{migration.name}: {label} does not admit everything it did ("
                + "; ".join(shortfalls)
                + "); declare narrower=True with a reason to vouch for the "
                "datasets anyway"
            ], []
        moved = schema_validated_usage(record, exclude_sheet_exports=True)
        if moved is None:
            return [f"{migration.name}: what it validated could not be established"], []
        moved = _datasets_only(moved)
        kept = self._sheet_export_pks(record)
        self._rehomings.setdefault(entry.key, []).append(
            _Rehoming(migration=migration, target=target, label=label, kept=kept)
        )
        admits = f", which admits less ({'; '.join(shortfalls)})" if shortfalls else ""
        notes = (
            [
                f"{_describe_usage(moved)} re-pointed to {label}{admits} by "
                f"{migration.name} ({migration.reason})"
            ]
            if moved.total
            else []
        )
        if kept:
            notes.append(
                f"{_plural(len(kept), 'sheet export')} keep pointing at it, as "
                "written against it"
            )
        return [], notes

    def _schema_shortfalls(self, record: Any, spec: Any, target: Any) -> list[str]:
        """What `target` -- or the plan schema `spec` -- no longer admits of `record`.

        A plan schema is judged as this apply leaves it (see
        :meth:`_planned_component_shape`) and, in APPLY mode, as it now is.  A
        composite needs the same slots, each component judged alike.
        """
        planned = isinstance(spec, Materializable)
        before = self._live_component_shape(record)
        shapes = [self._planned_component_shape(spec)] if planned else []
        if not isinstance(target, Placeholder) and (
            not planned or self.mode is ApplyMode.APPLY
        ):
            shapes.append(self._live_component_shape(target))
        ids = set(before.members) | set(before.optionals) | {before.index}
        for shape in shapes:
            ids |= set(shape.members) | set(shape.optionals) | {shape.index}
        name_of = self._member_names(i for i in ids if i)
        found: list[str] = []
        for shape in shapes:
            found.extend(
                s for s in _shape_shortfalls(before, shape, name_of) if s not in found
            )
        old_slots = self._live_slots_of(record)
        if not old_slots:
            return found
        new_slots: dict[str, Any] = (
            dict(getattr(spec, "slots", None) or {})
            if planned
            else self._live_slots_of(target)
        )
        if missing := sorted(set(old_slots) - set(new_slots)):
            found.append(f"it has no slot {', '.join(repr(s) for s in missing)}")
        if extra := sorted(set(new_slots) - set(old_slots)):
            found.append(f"it adds slot {', '.join(repr(s) for s in extra)}")
        for slot in sorted(set(old_slots) & set(new_slots)):
            component = new_slots[slot]
            if isinstance(component, Materializable):
                resolved = self.materialize_object(component)
                widens = (
                    self._swap_widens(old_slots[slot], component, resolved)
                    if not isinstance(resolved, Placeholder)
                    else _component_widens(
                        self._live_component_shape(old_slots[slot]),
                        self._planned_component_shape(component),
                    )
                )
            else:
                widens = _component_widens(
                    self._live_component_shape(old_slots[slot]),
                    self._live_component_shape(component),
                )
            if not widens:
                found.append(f"its {slot!r} component admits less")
        return found

    def _member_names(self, ids: Iterable[str]) -> Callable[[str], str]:
        """Name a member a :class:`_ComponentShape` holds by uid, or natural key."""
        ids = list(ids)
        names = dict(
            registry_for_kind("Feature")
            .objects.filter(uid__in=[i for i in ids if ":" not in i])
            .values_list("uid", "name")
        )

        def name_of(member: str) -> str:
            return repr(names.get(member) or member.rsplit(":", 1)[-1])

        return name_of

    @staticmethod
    def _sheet_export_pks(record: Any) -> list[Any]:
        """The sheet exports validated against the schema `record`."""
        from ._repoint import referencing_fields
        from ._sheet_exports import export_marker

        for related, fk in referencing_fields(type(record)):
            if (related.__name__, fk.name) == ("Artifact", "schema"):
                return list(
                    related.objects.filter(**{fk.attname: record.pk})
                    .filter(export_marker())
                    .values_list("pk", flat=True)
                )
        return []

    def _rehoming_target(
        self,
        record: Any,
        migration: DataMigration,
        kinds: frozenset[str],
        removed_pks: dict[Any, set[Any]],
    ) -> tuple[Any, str | None]:
        """What `migration` re-homes what `record` leaves behind onto.

        `relocates_to` is a plan object -- a placeholder in PLAN mode while
        the apply has yet to create it -- a natural key or a uid; `None` stands
        for no type at all.  Returns the target, or the refusal of one that does
        not exist, is not one of `kinds`, is named by a bare `Kind:name`
        several objects carry, is `record` itself, lies below it, or is
        deleted by the plan as well.
        """
        spec = migration.relocates_to
        if spec is None:
            return None, None
        wanted = _or(sorted(kinds))
        if isinstance(spec, Materializable):
            target = self.materialize_object(spec)
            if isinstance(target, Placeholder):
                return target, None
            named = f"{type(spec).__name__} {getattr(spec, 'name', None)!r}"
        else:
            named = repr(spec)
            matches = self._live_references(str(spec), type(record))
            if len(matches) > 1:
                listed = ", ".join(
                    f"{_live_key(m) or m.uid!r} (uid {m.uid})"
                    for m in matches[:_MAX_LISTED]
                )
                return None, (
                    f"{migration.name}: {named} names {len(matches)} objects "
                    f"({listed}); name it by uid or full natural key"
                )
            if not matches:
                return None, f"{migration.name}: {named} names no live {wanted}"
            (target,) = matches
        if target is None or not is_live(target):
            return None, f"{migration.name}: {named} does not exist"
        kind = _live_kind(target)
        if kind not in kinds:
            hint = (
                "; rows that land in a sheet must validate against its schema, "
                "which move_rows(to=...) checks"
                if migration.relocation == "keep_rows" and kind == "Sheet"
                else ""
            )
            return (
                None,
                f"{migration.name}: {_live_label(target)} is a {kind}, not a {wanted}{hint}",
            )
        if target.pk == record.pk:
            return None, f"{migration.name}: it cannot be re-homed onto itself"
        if target.pk in removed_pks.get(type(target), set()):
            return None, (
                f"{migration.name}: the plan deletes {_live_label(target)} too; "
                "name one it keeps"
            )
        if migration.relocation == "adopt_children" and any(
            ancestor.pk == record.pk for ancestor in _live_ancestors(target)
        ):
            return None, (
                f"{migration.name}: {_live_label(target)} is typed under it, so "
                "it would have to adopt itself"
            )
        return target, None

    @staticmethod
    def _live_references(spec: str, model: Any) -> list[Any]:
        """The live records of `model` that `spec` -- a uid or a natural key -- names.

        A full natural key names one; a bare `Kind:name` names one of that name
        under any parent, so it may name several, as for
        :func:`~lamindb_setup.evolve.detach_from_schemas`.
        """
        from ..authoring import parse_natural_key

        live = model.objects.exclude(branch_id=-1)
        if ":" not in spec:
            return list(live.filter(uid=spec))
        try:
            segments = parse_natural_key(spec)
        except ValueError:
            return []
        kind, name = segments[-1]
        candidates = [r for r in live.filter(name=name) if _live_kind(r) == kind]
        exact = [r for r in candidates if _live_key(r) == spec]
        if exact or len(segments) > 1:
            return exact
        return candidates

    @staticmethod
    def _rehoming_label(migration: DataMigration, target: Any, *, none: str) -> str:
        """The target of `migration` as the diff names it; `none` for no target."""
        if target is None:
            return none
        if isinstance(target, Placeholder):
            spec = migration.relocates_to
            return f"{type(spec).__name__} {getattr(spec, 'name', None)!r}"
        return _live_label(target)

    @staticmethod
    def _rehoming_kind(migration: DataMigration, target: Any) -> str | None:
        """The kind of `migration`'s target, a placeholder's read off the plan."""
        if target is None:
            return None
        if isinstance(target, Placeholder):
            return type(migration.relocates_to).__name__
        return _live_kind(target)

    def _rehome(self, entry: LockEntry, record: Any, rehoming: _Rehoming) -> str:
        """Carry out `rehoming` before `record` is deleted (APPLY); its detail.

        Each is checked afterwards: a row, dataset or child still pointing at
        the deleted object would go to the trash with it -- or be left on a
        trashed one -- which is exactly what the declaration had to prevent.
        """
        migration = rehoming.migration
        model = type(record)
        target_pk = None if rehoming.target is None else rehoming.target.pk
        if migration.relocation == "revalidate_against":
            moved = self._revalidate(record, rehoming)
            return f"{migration.name}: {_plural(moved, 'reference')} re-pointed to {rehoming.label}"
        if migration.relocation == "adopt_children":
            moving = _live_children(record)
            noun = "child"
        else:
            moving = _live_rows(record)
            noun = "record"
        pks = list(moving.values_list("pk", flat=True))
        for pk, name in rehoming.renames.items():
            model.objects.filter(pk=pk).update(name=name)
        model.objects.filter(pk__in=pks).update(type_id=target_pk)
        left = moving.count()
        astray = (
            model.objects.filter(pk__in=pks)
            .exclude(
                **(
                    {"type_id": target_pk}
                    if target_pk is not None
                    else {"type_id__isnull": True}
                )
            )
            .count()
        )
        if left or astray:
            raise MigrationError(
                f"{migration.name} for {entry.key!r} left "
                f"{_plural(left or astray, noun)} typed by a type the plan "
                "deletes or away from its target: the trash would take them along."
            )
        verb = (
            "kept untyped"
            if migration.relocation == "keep_rows" and target_pk is None
            else f"moved to {rehoming.label}"
        )
        plural = "children" if noun == "child" and len(pks) != 1 else None
        counted = f"{len(pks)} {plural}" if plural else _plural(len(pks), noun)
        return f"{migration.name}: {counted} {verb}"

    def _revalidate(self, record: Any, rehoming: _Rehoming) -> int:
        """Point the deleted schema's datasets at the target; how many references moved.

        Only the dataset references move -- an artifact's or a collection's
        schema and an artifact's slot links -- and not those of the sheet
        exports `rehoming` keeps.  A dataset still on the deleted schema
        afterwards fails the apply.
        """
        from ._repoint import referencing_fields, repoint_references

        model = type(record)
        fks = [
            (related, fk)
            for related, fk in referencing_fields(model)
            if (related.__name__, fk.name) in _DATASET_SCHEMA_FKS
        ]
        kept = set(rehoming.kept)
        exclude: dict[Any, list[Any]] = {}
        for related, fk in fks:
            if related.__name__ == "Artifact":
                exclude[related] = list(kept)
            elif related.__name__ == "ArtifactSchema" and kept:
                exclude[related] = list(
                    related.objects.filter(
                        **{fk.attname: record.pk}, artifact_id__in=kept
                    ).values_list("pk", flat=True)
                )
        moved, dropped = repoint_references(
            model,
            record.pk,
            rehoming.target.pk,
            error=MigrationError,
            only=[fk for _related, fk in fks],
            exclude=exclude,
        )
        left = 0
        for related, fk in fks:
            if related.__name__ in ("Artifact", "Collection"):
                left += (
                    related.objects.filter(**{fk.attname: record.pk})
                    .exclude(branch_id=-1)
                    .exclude(pk__in=exclude.get(related, []))
                    .count()
                )
        if left:
            raise MigrationError(
                f"{rehoming.migration.name} left {_plural(left, 'dataset')} "
                "validated against a schema the plan deletes."
            )
        return moved + dropped

    def _run_removal_migrations(self, entry: LockEntry, record: Any) -> list[str]:
        """Run what the plan declared for a type or schema it deletes (APPLY).

        Runs before the deletion, against the still-live object: first what the
        removal declarations re-home (see :meth:`_rehome`), then the migrations
        that answer for it in place.  A record type's records are re-counted
        afterwards: one still there would go to the trash with it, which is
        exactly what the declaration had to answer for.
        """
        if entry.kind not in _VETTED_REMOVAL_KINDS:
            return []
        details = [
            self._rehome(entry, record, rehoming)
            for rehoming in self._rehomings.get(entry.key, [])
        ]
        if entry.kind not in ("RecordType", "Sheet", "Schema"):
            return details
        is_schema = entry.kind == "Schema"
        migrations = [
            m
            for m in self._migrations_for(
                entry.key, narrowing=VALIDATED_REMOVAL if is_schema else ROWS_REMOVAL
            )
            if not m.relocation
        ]
        for migration in migrations:
            context: RecordTypeRemovalContext | SchemaRemovalContext = (
                SchemaRemovalContext(schema=record)
                if is_schema
                else RecordTypeRemovalContext(record_type=record, live_rows=_live_rows)
            )
            migration.run(context)
            if (summary := context.report.summary()) is not None:
                details.append(f"{migration.name}: {summary}")
        if (
            migrations
            and not is_schema
            and not any(m.keeps_values for m in migrations)
            and (left := _live_rows(record).count())
        ):
            names = ", ".join(repr(m.name) for m in migrations)
            raise MigrationError(
                f"migration(s) {names} for {entry.key!r} left {_plural(left, 'record')} "
                "typed by a record type the plan deletes: the trash would take "
                "them along. Fix the migration to handle every record."
            )
        return details

    def _refuse_deleting_claimed_uids(self, removed: list[LockEntry]) -> None:
        """Refuse to delete a record an object of this very run resolved to.

        A removal is addressed by the uid its lock entry records, so when the plan
        re-declares the same record under another address or kind -- a `Sheet`
        replacing a same-named `RecordType` -- the object it now resolves to and
        the entry it stopped declaring are one record.  Deleting the entry would
        trash the object the new lock is about to record, and lamindb's trash
        cascades to everything under it.
        """
        claimed = {entry.uid: key for key, entry in self.entries.items() if entry.uid}
        conflicts = [
            f"{entry.key!r} (uid {entry.uid!r}) is the record {claimed[entry.uid]!r} "
            "resolved to"
            for entry in removed
            if entry.uid in claimed
        ]
        if conflicts:
            raise DriftError(
                "cannot delete objects the plan no longer declares: the plan "
                "declares their records again under another address, so deleting "
                "them would delete those objects too:\n  "
                + "\n  ".join(conflicts)
                + "\nDeclare the change of address with ctx.moved, or keep "
                "declaring the old object."
            )

    def _vet_feature_deletion(self, entry: LockEntry, record: Any) -> list[str]:
        """Judge deleting a feature against the values still stored for it.

        Trashing detaches nothing, so the values outlive the feature: readable
        only by name, invisible to anything going through a live feature, and not
        inherited by a feature of the same name declared later -- which is how the
        history ends up split across two records.  A deletion that would do that
        is refused unless the plan says what should become of them, declared by
        natural key since there is no plan object left to name::

            ctx.migrate("FeatureType:ft/Feature:retired", drop_values())

        In APPLY the declared migrations run here, before the deletion, so they
        still see a live feature; the values are then re-counted unless the
        migration is one that keeps them deliberately.  PLAN only reports the
        intent, as everywhere else.

        Returns the change details; raises :class:`DriftError` when the deletion
        is refused and :class:`MigrationError` when a migration leaves values
        behind.
        """
        from ..authoring import FeatureRemovalContext
        from ._drift_data import feature_has_any_stored_value

        if entry.kind != "Feature":
            return []
        migrations = self._value_migrations_for(entry.key)
        if not migrations:
            if not feature_has_any_stored_value(record):
                return []
            raise DriftError(
                f"cannot delete {entry.kind} {entry.name!r} (uid {entry.uid!r}): "
                "values are still stored for it, and deleting it would leave them "
                "keyed to a trashed feature -- readable only by name, and not "
                "picked up by a feature of the same name declared later. Declare "
                f"ctx.migrate({entry.key!r}, drop_values()) to delete them, "
                f"ctx.migrate({entry.key!r}, keep_values()) to keep them, or go "
                "on declaring the feature."
            )
        if self.mode is not ApplyMode.APPLY:
            return [
                "stored values handled by "
                + ", ".join(f"{m.name} ({m.reason})" for m in migrations)
            ]
        details: list[str] = []
        keeps = False
        for migration in migrations:
            context = FeatureRemovalContext(feature=record)
            migration.run(context)
            keeps = keeps or migration.keeps_values
            if (summary := context.report.summary()) is not None:
                details.append(f"{migration.name}: {summary}")
        if not keeps and feature_has_any_stored_value(record):
            names = ", ".join(repr(m.name) for m in migrations)
            raise MigrationError(
                f"migration(s) {names} for {entry.key!r} left values stored for a "
                "feature the plan deletes: they would outlive it on a trashed "
                "record. Fix the migration to clear every value, or declare "
                "keep_values() to keep them deliberately."
            )
        return details

    def _settle_member_holders(
        self, removed: list[LockEntry], records: dict[str, Any]
    ) -> tuple[InferredMemberRemoval, list[_Detachment]]:
        """Judge every live schema still listing a feature the pass deletes.

        Deleting saturates at the trash, and trashing detaches nothing: the
        member links carry no branch filter, so a schema still listing the
        feature is left declaring a soft-deleted member.  That is invisible to
        the diff (:func:`~lamindb_setup.evolve.engine._trash.is_live` drops it
        from the member list), silently changes what the schema validates, and is
        what `repair dangling` then reports the schema itself as dangling for.

        Three kinds of holder are not in the way:

        * the plan's own schemas.  A plan schema cannot declare a feature the
          plan no longer does, so the reconcile unlinks it -- already done in
          APPLY mode, and what a PLAN-mode preview has to assume;
        * a schema this same pass deletes, which goes to the trash with it;
        * an inferred artifact schema: it belongs to no plan and validates
          nothing, so the member is removed from it (see
          :mod:`._inferred_schemas`).  That is returned, settled, to be reported
          on each feature's change and applied after the deletions.

        Anything else is genuinely someone else's, and the operator has to say
        which of the two it should be: the deletion is refused, naming every
        such schema -- with the reasons for an unnamed one, whose uid is all the
        operator would otherwise have to go on -- unless the feature declares
        :func:`~lamindb_setup.evolve.detach_from_schemas` listing it, which
        removes the member from it first (see :meth:`_detach_blocker`).  Refused
        before anything is deleted, so a migration never runs for a deletion
        that does not happen.

        A schema losing several features to detachments loses them in a single
        save, judged on the content it ends up with (see
        :meth:`_settle_detachments`).

        Returns the inferred removals and the detachments to make.
        """
        from ._inferred_schemas import (
            InferredMemberRemoval,
            live_member_holders,
            removal_blockers,
        )

        inferred = InferredMemberRemoval()
        detached: dict[Any, _Detachment] = {}
        features = [
            (entry, records[entry.key])
            for entry in removed
            if entry.kind == "Feature" and records.get(entry.key) is not None
        ]
        if not features:
            return inferred, []
        own = {
            str(entry.uid)
            for entry in [*self.entries.values(), *removed]
            if entry.kind == "Schema" and entry.uid is not None
        }
        managed_pks, managed_uids = self._managed_schemas()
        refusals: list[str] = []
        for entry, record in features:
            refused: list[tuple[Any, list[str]]] = []
            holders = [
                schema
                for schema in live_member_holders(record)
                if str(schema.uid) not in own
            ]
            targets = self._detach_targets(entry, holders, refusals)
            for schema in holders:
                managed = schema.pk in managed_pks or str(schema.uid) in managed_uids
                detach = targets.get(schema.pk)
                if detach is not None:
                    blocker = self._detach_blocker(entry, schema, record, managed)
                    if blocker is None:
                        detachment = detached.setdefault(
                            schema.pk,
                            _Detachment(
                                schema=schema,
                                key=_schema_key(schema) or str(schema.uid),
                                obj_id=id(entry),
                            ),
                        )
                        detachment.features.append((record, detach))
                    else:
                        label = getattr(schema, "name", None) or str(schema.uid)
                        refusals.append(
                            f"cannot detach {entry.kind} {entry.name!r} from schema "
                            f"{label!r} with {detach.name}: {blocker}"
                        )
                    continue
                blockers, artifacts = removal_blockers(schema, record, managed=managed)
                if blockers:
                    refused.append((schema, blockers))
                else:
                    inferred.add(record, schema, artifacts)
            if refused:
                refusals.append(_member_holder_refusal(entry, refused))
        if refusals:
            raise DriftError("\n".join(refusals))
        doomed = {
            record.pk
            for entry in removed
            if entry.kind == "Schema" and (record := records.get(entry.key)) is not None
        }
        planned = {check.content_hash: check.schema for check in self._hash_checks}
        excluded = doomed | {check.schema.pk for check in self._hash_checks}
        self._settle_detachments(list(detached.values()))
        for detachment in detached.values():
            # Rehashed before the inferred schemas are, which then find it
            # holding its new content rather than its old one.
            if detachment.content_hash is not None:
                planned.setdefault(detachment.content_hash, detachment.schema)
            excluded.add(detachment.schema.pk)
        inferred.settle(planned=planned, excluded=excluded)
        return inferred, list(detached.values())

    def _detach_targets(
        self, entry: LockEntry, holders: list[Any], refusals: list[str]
    ) -> dict[Any, DataMigration]:
        """The `holders` that `entry`'s detachments name, by primary key.

        A target names a schema by uid or by its full natural key.  A bare
        `Schema:<name>` names one under a schema type as well, but only while
        it names a single holder: a detachment edits schemas no plan owns, so it
        has to say which -- a name shared across schema types does not, and is
        added to `refusals`.
        """
        named: dict[Any, DataMigration] = {}
        for migration in self._migrations_for(entry.key, narrowing=DETACH_MEMBER):
            for target in migration.detaches:
                exact = [s for s in holders if _names_schema(target, s, exactly=True)]
                matches = exact or [s for s in holders if _names_schema(target, s)]
                if len(matches) > 1:
                    refusals.append(
                        f"cannot detach {entry.kind} {entry.name!r} with "
                        f"{migration.name}: {target!r} names {len(matches)} schemas "
                        "listing it ("
                        + ", ".join(
                            f"{_schema_key(s) or s.uid!r} (uid {s.uid})"
                            for s in matches
                        )
                        + "); name each by uid or full natural key"
                    )
                    continue
                for schema in matches:
                    named.setdefault(schema.pk, migration)
        return named

    def _settle_detachments(self, detached: list[_Detachment]) -> None:
        """Predict what each detachment leaves, for the sweeps run at the end.

        The content hash the schema takes is what the PLAN-mode collision sweep
        judges (see :meth:`check_schema_collisions`) and what APPLY looks the
        holder up by when the save is refused (see :meth:`_save_schema`).  Its
        shape before and after goes to the export sweep: a `maximal_set` sheet
        schema no longer allows the column its exports still carry (see
        :meth:`check_sheet_exports`).  Both are read before anything is written,
        so PLAN and APPLY judge the same state.
        """
        from ._inferred_schemas import _content_hash, _members

        for detachment in detached:
            schema = detachment.schema
            dropped = {feature.pk for feature, _ in detachment.features}
            members = _members(schema)
            remaining = [m for m in members if m.pk not in dropped]
            detachment.content_hash = _content_hash(
                schema, remaining, dropped, slots=_live_slots(schema)
            )
            rehashed: dict[Any, tuple[Any, str]] = {}
            if detachment.content_hash not in (None, schema.hash):
                rehashed[schema.pk] = (schema, detachment.content_hash)
                detachment.composites = _predicted_composite_hashes(
                    schema, detachment.content_hash
                )
                rehashed.update(detachment.composites)
            if self.mode is not ApplyMode.APPLY:
                checked = {check.schema.pk for check in self._hash_checks}
                self._hash_checks.extend(
                    _HashCheck(
                        obj_id=detachment.obj_id,
                        key=_schema_key(record) or str(record.uid),
                        schema=record,
                        content_hash=value,
                    )
                    for pk, (record, value) in rehashed.items()
                    if pk not in checked
                )
            live = [m for m in members if is_live(m)]
            optional = set(schema.optionals.get_uids())
            self._schema_shapes.append(
                _SchemaShape(
                    obj_id=detachment.obj_id,
                    key=detachment.key,
                    schema=schema,
                    member_ids={m.pk for m in live if m.pk not in dropped},
                    previous_ids={m.pk for m in live},
                    required_ids={
                        m.pk
                        for m in live
                        if m.pk not in dropped and str(m.uid) not in optional
                    },
                    added_required=[],
                    newly_required=[],
                    maximal=bool(getattr(schema, "maximal_set", False)),
                    structural=True,
                )
            )

    def _detach_blocker(
        self, entry: LockEntry, schema: Any, feature: Any, managed: bool
    ) -> str | None:
        """Why a declared detachment of `feature` from `schema` cannot be made.

        A plan-managed schema is that plan's to change.  An index cannot be
        removed like a member.  And a `maximal_set` schema is judged on its
        configuration, as a plan schema dropping a member is (see
        :meth:`_vet_member_removals`): it forbids a member it does not declare,
        and a dataset's column outlives the feature values mirroring it, so the
        detachment needs a declaration deleting the values -- whatever the
        stored values say.
        """
        if managed:
            return "a plan manages it; remove the member in that plan instead"
        if getattr(getattr(schema, "index", None), "pk", None) == feature.pk:
            return "it is the schema's index"
        if getattr(schema, "maximal_set", False):
            declared = self._value_migrations_for(entry.key)
            if not any(not m.keeps_values for m in declared):
                return (
                    "it is maximal_set, so datasets that still carry the feature "
                    "would have a member the schema no longer allows; "
                    + (
                        "keep_values cannot apply here, declare drop_values to "
                        "delete them"
                        if declared
                        else f"declare ctx.migrate({entry.key!r}, drop_values()) "
                        "to delete its values"
                    )
                )
        return None

    def _detach_members(self, detached: list[_Detachment]) -> None:
        """Take the detached features out of each schema in one save (APPLY).

        Saved through :meth:`_save_schema`, so a schema given the content of
        another live one absorbs it when it is an inferred schema, merges it
        when a :func:`~lamindb_setup.evolve.merge_schema` declared on the
        schema's natural key names it, and is refused naming both otherwise --
        and so are the composites slotting it, rehashed after it (see
        :meth:`_rehash_composites`), exactly as the PLAN-mode sweep previews
        them.  A refusal lamindb raises that the predicted content does not
        explain is reported as drift naming the schema.
        """
        from lamindb.errors import ValidationError

        for detachment in detached:
            schema = type(detachment.schema).objects.get(pk=detachment.schema.pk)
            features = [feature for feature, _ in detachment.features]
            schema.features.remove(*features)
            self._forget_optional_members(schema, features)
            previous_hash = schema.hash
            try:
                detachment.saved.extend(
                    self._save_schema(
                        schema, detachment.key, {}, detachment.content_hash
                    )
                )
                if schema.hash != previous_hash:
                    detachment.saved.extend(
                        self._rehash_composites(
                            schema,
                            {
                                pk: value
                                for pk, (_, value) in detachment.composites.items()
                            },
                        )
                    )
            except ValidationError as exc:
                raise PlanChangeRefusedError(
                    "cannot detach "
                    + ", ".join(repr(f.name) for f in features)
                    + f" from {detachment.key!r}: lamindb refused the save: {exc}"
                ) from exc

    def _warn_unmoved_renames(self, removed: list[LockEntry]) -> None:
        """Flag deletions that look like an undeclared rename.

        A dropped object whose configuration (ignoring its name) is identical to
        a to-be-created object of the same kind is almost certainly a rename the
        author forgot to declare -- applying it would destroy the old object and
        create a new one, losing its uid.  A warning is attached to the create
        suggesting :meth:`~lamindb_setup.evolve.PlanContext.moved`, and the
        deleted object's name is recorded as its `suspected_rename_of`, which
        a strict `apply` (and the `plan`/`ci plan` predicting it) refuses;
        `--no-strict-renames` keeps it advisory, because two genuinely
        distinct objects can share a configuration.  Each create pairs with one
        deletion at most.
        """

        def _without_name(cfg: dict[str, Any]) -> dict[str, Any]:
            return {k: v for k, v in cfg.items() if k != "name"}

        creates = [
            (change, _without_name(self.entries[change.key].config))
            for change in self.changes
            if change.action is Action.CREATE
            and change.name
            and change.key in self.entries
        ]
        if not creates:
            return
        for entry in removed:
            if entry.name is None:
                continue
            target = _without_name(entry.config)
            for change, create_config in creates:
                if (
                    change.suspected_rename_of is None
                    and change.kind == entry.kind
                    and create_config == target
                ):
                    change.warnings.append(
                        ChangeWarning(
                            f"looks like a rename of deleted {entry.kind} "
                            f"{entry.name!r} (identical configuration); declare "
                            f"ctx.moved(<object>, {entry.name!r}) to rename it "
                            "in place instead of delete + create.",
                            WarningCategory.RENAME,
                        )
                    )
                    change.suspected_rename_of = entry.name
                    break

    def build_lock(self, plan_name: str) -> LockFile:
        """Build a :class:`LockFile` from the recorded entries."""
        return LockFile(
            plan=plan_name,
            version=LOCK_VERSION,
            objects=dict(self.entries),
            seeded=dict(self.seeded),
        )

    @property
    def uid_misalignments(self) -> list[ChangeItem]:
        """The changes whose uid disagrees with the plan's other instances."""
        return [c for c in self.changes if c.uid_misalignment is not None]

    @property
    def suspected_renames(self) -> list[tuple[str, str, str]]:
        """The creates that look like an undeclared rename of a deleted object.

        Each is `(kind, old_name, new_name)`, flagged by
        :meth:`_warn_unmoved_renames` (see
        :attr:`~lamindb_setup.evolve.core.ChangeItem.suspected_rename_of`), so a
        strict `apply` -- and the `plan` and `ci plan` predicting it -- can
        refuse them.
        """
        return [
            (c.kind, c.suspected_rename_of, c.name or "")
            for c in self.changes
            if c.suspected_rename_of is not None
        ]


def plan_natural_keys(plan: Plan[E]) -> set[str]:
    """Return the natural keys of every object `plan` declares.

    The plan is finalized first so its dependency DAG is available.  Used to
    derive the keys a plan "claims" so sibling plans managed against the same
    instance do not delete objects this plan now owns.
    """
    plan.finalize()
    return {natural_key(dependency.obj) for dependency in plan.dependencies}


def lock_with_moves(plan: Plan[E], lock: LockFile | None) -> LockFile | None:
    """Return a copy of `lock` re-keyed for the plan's declared `ctx.moved`.

    An object the plan renamed is still recorded under its previous address
    until the next apply rewrites the lock, so reading a lock by the plan's
    *current* keys has to apply the moves first -- as :func:`compute_changes`
    does.  `lock` itself is never modified.
    """
    import copy

    plan.finalize()
    if lock is None or not plan.ctx.moves:
        return lock
    lock = copy.deepcopy(lock)
    _apply_moves_to_lock(
        lock,
        [
            (_old_key_for(natural_key(mv.obj), mv.from_), natural_key(mv.obj))
            for mv in plan.ctx.moves
        ],
    )
    return lock


def resolve_existing(plan: Plan[E], lock: LockFile | None) -> dict[int, Any]:
    """Look every object `plan` declares up in the connected instance, read-only.

    The lookup an apply makes, without the apply: a uid `lock` pins wins,
    anything else is matched by name, kind and parent.  Nothing is pinned by
    another instance's lock -- the point is to see which record *this*
    instance's own state resolves each object to.  Returns the live record (or
    `None`) keyed by `id(obj)`.
    """
    plan.finalize()
    lock = lock_with_moves(plan, lock)
    own_keys = {natural_key(dep.obj) for dep in plan.own_dependencies}
    borrowed = list(plan.inherited_dependencies)
    ctx = ApplyContext(
        mode=ApplyMode.PLAN,
        lock=lock,
        borrowed_ids={id(dep.obj) for dep in borrowed},
        borrowed_keys={natural_key(dep.obj) for dep in borrowed} - own_keys,
    )
    objs = [dependency.obj for dependency in plan.dependencies]
    ctx.prefetch(objs)
    return {id(obj): ctx._existing_cache.get(id(obj)) for obj in objs}


def prefetch_plans(
    entries: Sequence[tuple[Plan[E], LockFile | None]],
    *,
    existing_cache: ExistenceCache,
    members_cache: dict[int, list[Any]],
    sibling_uids: Sequence[SiblingUids | None] | None = None,
) -> None:
    """Bulk-load the instance state several plans will be diffed against.

    Each plan is otherwise diffed with its own :class:`ApplyContext`, so the
    existence prefetch and the schema-member read run once *per plan* even
    though every plan is asking the same instance.  Doing them together turns
    that into a handful of statements for the whole configuration: the reads are
    already batched per registry, and merging the batches is all that is needed.

    `entries` pairs each plan with the lock recorded for it *on this instance*
    -- the lock is what pins an object to a uid, and each plan has its own -- so
    the uid index is built from every plan's lock at once.  Plans are finalized
    and their declared moves applied to a copy of the lock exactly as
    :func:`compute_changes` does, so a renamed object is still found under the
    uid its old address recorded.  The two caches are filled in place for the
    caller to hand to :func:`compute_changes`, which then finds its work already
    done.

    The read itself is a snapshot, so a caller that *writes* between plans must
    say so: applying one plan can create an object a later sibling declares, and
    the entry recording it as missing would still say missing.  Call
    :func:`forget_missing` after each write to drop exactly those entries; the
    records that *were* found need no such care, because every plan resolves the
    same row to the same instance and so already sees the edits.

    `sibling_uids` optionally pairs each entry with the uids its plan's locks
    for other instances record, so an object this instance's lock does not pin
    is looked up by the uid they agree on as :func:`compute_changes` would.
    """
    import copy

    ctx = ApplyContext(
        mode=ApplyMode.PLAN,
        lock=None,
        existing_cache=existing_cache,
        members_cache=members_cache,
    )
    objs: list[Materializable] = []
    locked_uids: dict[int, str] = {}
    pinned_uids: dict[int, str] = {}
    siblings_per_entry = (
        list(sibling_uids) if sibling_uids is not None else [None] * len(entries)
    )
    for (plan, lock), siblings in zip(entries, siblings_per_entry, strict=True):
        plan.finalize()
        # A borrowed object is pinned by the uid in the lock of the plan that
        # owns it -- which, plans being visited prerequisites-first, has already
        # been recorded here.  Reading this plan's lock for it would overwrite
        # that with an address it no longer maintains (see
        # :meth:`ApplyContext.locked_uid`).
        borrowed_ids = {id(dep.obj) for dep in plan.inherited_dependencies}
        if lock is not None and plan.ctx.moves:
            lock = copy.deepcopy(lock)
            _apply_moves_to_lock(
                lock,
                [
                    (_old_key_for(natural_key(mv.obj), mv.from_), natural_key(mv.obj))
                    for mv in plan.ctx.moves
                ],
            )
        pinner = ApplyContext(mode=ApplyMode.PLAN, lock=lock, sibling_uids=siblings)
        for dependency in plan.dependencies:
            obj = dependency.obj
            objs.append(obj)
            if id(obj) in borrowed_ids:
                continue
            key = natural_key(obj)
            uid = lock.uid_for(key) if lock is not None else None
            if uid is not None:
                locked_uids[id(obj)] = uid
                continue
            pinned = pinner.pinned_uid(
                key, seeded=type(obj).__name__ in SEED_ONLY_KINDS
            )
            if pinned is not None:
                pinned_uids[id(obj)] = pinned
    ctx.prefetch(objs, locked_uids=locked_uids, pinned_uids=pinned_uids)
    ctx.prefetch_schema_members(
        [
            record
            for obj in objs
            if type(obj).__name__ == "Schema"
            and (record := existing_cache.get(id(obj))) is not None
        ]
    )


def forget_missing(existing_cache: ExistenceCache) -> None:
    """Drop the "not found" entries from a cache shared across successive applies.

    See :meth:`ExistenceCache.forget_missing`; kept as a function because that
    is how the `ci` apply loop reads.
    """
    existing_cache.forget_missing()


def _old_key_for(new_key: str, from_: str) -> str:
    """Resolve a `ctx.moved` `from_` spec to a full previous natural key.

    A `from_` that already contains a kind (`:`) or a parent chain (`/`)
    is a full previous natural key and is used verbatim.  A bare token is the
    object's previous *leaf name*: it replaces the name of `new_key`'s last
    segment, keeping the same kind and parent chain.
    """
    if ":" in from_ or "/" in from_:
        return from_
    head, sep, leaf = new_key.rpartition("/")
    kind = leaf.partition(":")[0]
    return f"{head}{sep}{kind}:{from_}"


def _rewrite_key_refs(value: Any, old: str, new: str) -> Any:
    """Return `value` with every natural-key reference to `old` rebased on `new`.

    Walks the JSON-like config recursively and rewrites strings that *are*
    `old` or are addressed *under* `old` (`old` + `"/"` prefix), covering
    the `type` parent, schema `features`/`index`/`slots`, `Sheet`
    `schema`, and `Record` feature-map keys.  Plain string values (names,
    descriptions) are left untouched because they never equal a natural key.
    """
    if isinstance(value, str):
        if value == old:
            return new
        if value.startswith(old + "/"):
            return new + value[len(old) :]
        return value
    if isinstance(value, list):
        return [_rewrite_key_refs(item, old, new) for item in value]
    if isinstance(value, dict):
        return {
            (_rewrite_key_refs(k, old, new) if isinstance(k, str) else k): (
                _rewrite_key_refs(v, old, new)
            )
            for k, v in value.items()
        }
    return value


def _rewrite_borrowed_move_refs(
    lock: LockFile, specs: Iterable[tuple[str, str]]
) -> None:
    """Rebase natural-key *references* to objects a prerequisite plan renamed.

    A move declared by a plan in `depends_on` changes the natural key of an
    object this plan borrows, and every entry here that references it -- a
    schema's member list, a sheet's schema -- records it by that key.  Without
    rebasing, the plan's rebuilt configuration reads as "the plan removed a
    schema member", which :meth:`ApplyContext._check_plan_edit` refuses.

    Only the references are rewritten.  The entry for the moved object itself
    belongs to the owning plan's lock, so there is nothing to re-key here and
    nothing to report -- unlike :func:`_rekey_prefix`, which does both for a move
    the plan owns.
    """
    for entry in lock.objects.values():
        for old, new in specs:
            entry.config = _rewrite_key_refs(entry.config, old, new)


def _rekey_prefix(lock: LockFile, old: str, new: str) -> None:
    """Re-key `lock` entries from `old` to `new` in place (with cascade).

    The entry at `old` and every descendant addressed under it (`old` +
    `"/"`) are moved so a renamed `*Type` carries its children along, each
    keeping its `uid`.  Then *every* entry's config has its natural-key
    references rebased via :func:`_rewrite_key_refs` -- not just the moved
    subtree -- so an object that merely *references* the moved one (a schema's
    member list, a sheet's schema, a record's feature map) is updated too.  The
    remote snapshot is left as-is: it addresses references by `uid` (not
    natural keys) and still describes the live, not-yet-renamed record.
    """
    for existing_key in [
        k for k in lock.objects if k == old or k.startswith(old + "/")
    ]:
        if existing_key == old:
            new_key = new
        else:
            new_key = new + existing_key[len(old) :]
        entry = lock.objects.pop(existing_key)
        entry.key = new_key
        entry.name = new_key.rpartition("/")[2].partition(":")[2]
        lock.objects[new_key] = entry
    # A seeded record under a renamed type keeps its uid the same way, so the
    # identity the lock pins for it survives the move.
    for existing_key in [k for k in lock.seeded if k == old or k.startswith(old + "/")]:
        new_key = new if existing_key == old else new + existing_key[len(old) :]
        lock.seeded[new_key] = lock.seeded.pop(existing_key)
    for entry in lock.objects.values():
        entry.config = _rewrite_key_refs(entry.config, old, new)


def _member_holder_refusal(
    entry: LockEntry, refused: list[tuple[Any, list[str]]]
) -> str:
    """Why `entry`'s feature cannot be deleted while `refused` still list it.

    `refused` pairs each holding schema with the reasons the member is not
    removed from it automatically; those are spelled out for an unnamed one
    only, since a name is enough to act on.
    """
    holders = sorted(
        str(getattr(schema, "name", None) or schema.uid) for schema, _ in refused
    )
    lines = [
        f"cannot delete {entry.kind} {entry.name!r} (uid {entry.uid!r}): "
        + ", ".join(repr(name) for name in holders)
        + " still declares it as a member. Deleting it would leave that "
        "schema with a soft-deleted member it goes on validating against. "
        "Remove it from that schema first, or keep declaring the feature."
    ]
    lines.extend(
        sorted(
            f"  the member is not removed from unnamed schema {schema.uid!r} "
            f"automatically: {'; '.join(reasons)}"
            for schema, reasons in refused
            if getattr(schema, "name", None) is None
        )
    )
    return "\n".join(lines)


def _migrations_by_key(declared: Iterable[Migrated]) -> dict[str, list[DataMigration]]:
    """Group `ctx.migrate` declarations by the natural key of their object.

    A list per key rather than a single migration: a plan may declare more than
    one for the same object -- a schema making two members required at once reads
    best as one `ctx.migrate` per member -- and collapsing them into a mapping
    would silently drop all but the last.  Declaration order is preserved, which
    is the order they run in.
    """
    grouped: dict[str, list[DataMigration]] = {}
    for migrated in declared:
        key = (
            migrated.obj if isinstance(migrated.obj, str) else natural_key(migrated.obj)
        )
        grouped.setdefault(key, []).append(migrated.migration)
    return grouped


def _apply_moves_to_lock(
    lock: LockFile, specs: Iterable[tuple[str, str]]
) -> tuple[set[str], MoveOutcome]:
    """Apply declared `(old_key, new_key)` re-keys to `lock` in memory.

    Returns the set of new keys that were re-keyed (so their resolution may
    change the live object's name and be reported as an `Action.MOVE`) and a
    :class:`MoveOutcome` describing what happened for reporting.  A move whose
    new key is already recorded is a no-op (idempotent, so it is safe to leave
    the `ctx.moved` declaration in place across instances).  Chains
    (`A -> B`, `B -> C`) are resolved by iterating to a fixpoint.
    """
    outcome = MoveOutcome()
    remaining = list(specs)
    progressed = True
    while progressed:
        progressed = False
        pending: list[tuple[str, str]] = []
        for old, new in remaining:
            if new in lock.objects:
                outcome.already.append((old, new))
                continue
            if old not in lock.objects:
                pending.append((old, new))
                continue
            _rekey_prefix(lock, old, new)
            outcome.applied.append((old, new))
            progressed = True
        remaining = pending
    outcome.unmatched = remaining
    moved_keys = {new for _old, new in outcome.applied}
    return moved_keys, outcome


def _prerequisite_owners(plan: Plan[Any]) -> dict[int, PrerequisitePlan]:
    """Map every object `plan`'s prerequisites declare to the plan declaring it.

    Keyed by `id(obj)`, like :attr:`ApplyContext.borrowed_ids`.  Walks
    `depends_on` transitively, since a plan borrows what its prerequisites
    borrow too (see :attr:`~lamindb_setup.evolve.Plan.dependencies`).
    """
    owners: dict[int, PrerequisitePlan] = {}
    visited: set[int] = set()
    pending = list(plan.depends_on)
    while pending:
        prerequisite = pending.pop()
        if id(prerequisite) in visited:
            continue
        visited.add(id(prerequisite))
        owner = PrerequisitePlan(prerequisite.name, plan_target(prerequisite))
        for dependency in prerequisite.own_dependencies:
            owners.setdefault(id(dependency.obj), owner)
        pending.extend(prerequisite.depends_on)
    return owners


def _created_in_run_detail(owner: PrerequisitePlan | None) -> str:
    """The detail on a borrowed object a prerequisite plan of the same run creates."""
    plan = (
        "a prerequisite plan" if owner is None else f"prerequisite plan {owner.name!r}"
    )
    return f"managed by {plan} (created by it in this run)"


def compute_changes(
    plan: Plan[E],
    *,
    mode: ApplyMode,
    lock: LockFile | None = None,
    existing_cache: ExistenceCache | None = None,
    members_cache: dict[int, list[Any]] | None = None,
    exports_cache: dict[str, Any] | None = None,
    claimed_keys: set[str] | None = None,
    claimed_by: dict[str, str] | None = None,
    co_applied_keys: set[str] | None = None,
    prefetch: bool = True,
    report: Callable[[str], None] | None = None,
    track: Callable[[Sequence[Any], str], Iterable[Any]] | None = None,
    allow_data_validated_changes: bool = False,
    preview_migrations: bool = False,
    sibling_uids: SiblingUids | None = None,
    strict_uids: bool = True,
) -> ApplyContext:
    """Finalize `plan` and resolve every object in dependency order.

    `existing_cache` lets two passes over the same finalized plan share their
    existence lookups.  `members_cache` shares the live schemas' member lists
    the same way; pass both (filled by :func:`prefetch_plans`) to reuse one bulk
    read across several *read-only* plans diffed against the same instance.
    `exports_cache` shares the instance-wide sheet-export index across the
    plans of one run, so the stale-export sweep reads it once rather than per
    plan (see :meth:`ApplyContext.check_sheet_exports`).
    `claimed_keys` are natural keys owned by sibling plans
    managed against the same instance; removals of those keys are reported as a
    transfer rather than a deletion so an object handed off to a sibling plan is
    not deleted (see :meth:`ApplyContext.apply_removals`).  `claimed_by`
    optionally maps such a key to the sibling plan that now owns it, to name the
    destination of the transfer.  `co_applied_keys` are the natural keys the
    prerequisite plans applied in the same run declare (see
    :func:`~lamindb_setup.evolve.engine._loader.co_applied_prerequisite_keys`):
    applied first, they create what the instance lacks, so a preview reports a
    borrowed object under one of them as created by its owner in this run --
    not as a missing prerequisite the apply refuses (see
    :meth:`ApplyContext._borrow`).  `prefetch` bulk-loads those
    existence lookups up front (one batched query per registry instead of one
    per object); set it to `False` for a second pass that reuses a cache an
    earlier pass already filled.  `report` and `track` are optional progress
    callbacks (see :func:`lamindb_setup.evolve.console.progress_reporter`):
    `report` prints milestones, `track` wraps the per-object loop in a
    progress bar.

    Address changes declared with :meth:`~lamindb_setup.evolve.PlanContext.moved`
    are applied to `lock` first: the matching entry is re-keyed to the new
    address (keeping its `uid`) so resolution renames the live object in place
    (`Action.MOVE`) instead of deleting the old address and creating a new one.

    `allow_data_validated_changes` opts in to lifting a blocked
    validation-affecting change when the live data proves it safe -- the feature
    has no stored values, or every value validates against the new dtype (see
    :meth:`ApplyContext._data_override_reason`).

    `preview_migrations` opts a PLAN-mode diff into *running* each declared
    :meth:`~lamindb_setup.evolve.PlanContext.migrate` migration inside a
    rolled-back savepoint, so the preview proves the migration relocates every
    value instead of only describing intent.  Ignored in APPLY mode (which runs
    migrations for real) and a no-op without a declared migration.

    `sibling_uids` are the uids the plan's lock files for *other* instances
    record (see :func:`._alignment.read_sibling_uids`): objects are created with
    them and checked against them, so the plan keeps one identity per object
    across instances.  `strict_uids` refuses to create an object whose uid
    cannot be pinned (the default); off, it is created with a fresh uid.
    Either way every disagreement is reported on
    :attr:`ChangeItem.uid_misalignment`.
    """
    if report is not None:
        report("Finalizing plan")
    plan.finalize()
    # Objects a plan in `depends_on` declares and this plan merely references.
    # They are resolved like any other, but adopted rather than owned: the plan
    # that declares them is the one whose lock tracks them (see
    # :meth:`ApplyContext._borrow`).
    borrowed = list(plan.inherited_dependencies)
    own_keys = {natural_key(dep.obj) for dep in plan.own_dependencies}
    borrowed_ids = {id(dep.obj) for dep in borrowed}
    # A key this plan also declares itself is not borrowed: the plan owns its own
    # declaration of it, and the key -- unlike the identity -- cannot tell the two
    # objects apart.
    borrowed_keys = {natural_key(dep.obj) for dep in borrowed} - own_keys
    borrowed_owners = _prerequisite_owners(plan) if borrowed else {}
    moved_keys: set[str] = set()
    move_outcome = MoveOutcome()
    borrowed_moves = plan.inherited_moves
    if lock is not None and (plan.ctx.moves or borrowed_moves):
        # Re-key a copy so declared moves never mutate the caller's lock: the
        # preview and apply passes reuse the same on-disk lock, and each must
        # start from the un-migrated baseline.
        import copy

        lock = copy.deepcopy(lock)
        own_specs = [
            (_old_key_for(natural_key(mv.obj), mv.from_), natural_key(mv.obj))
            for mv in plan.ctx.moves
        ]
        if borrowed_moves:
            _rewrite_borrowed_move_refs(
                lock,
                [
                    (_old_key_for(natural_key(mv.obj), mv.from_), natural_key(mv.obj))
                    for mv in borrowed_moves
                ],
            )
        if own_specs:
            moved_keys, move_outcome = _apply_moves_to_lock(lock, own_specs)
    ctx = ApplyContext(
        mode=mode,
        lock=lock,
        default_space_uid=plan.ctx.default_space_uid,
        existing_cache=existing_cache,
        members_cache=members_cache,
        exports_cache=exports_cache,
        plan_name=plan.name,
        claimed_keys=claimed_keys,
        claimed_by=claimed_by,
        moved_keys=moved_keys,
        borrowed_ids=borrowed_ids,
        borrowed_keys=borrowed_keys,
        borrowed_owners=borrowed_owners,
        co_applied_keys=co_applied_keys,
        allow_data_validated_changes=allow_data_validated_changes,
        migrations=_migrations_by_key(plan.ctx.migrations),
        preview_migrations=preview_migrations,
        sibling_uids=sibling_uids,
        strict_uids=strict_uids,
    )
    ctx.move_outcome = move_outcome
    dependencies = list(plan.dependencies)
    if prefetch:
        if report is not None:
            report("Prefetching instance state")
        ctx.prefetch([dependency.obj for dependency in dependencies])
    # Bulk-load the live schemas' member lists.  Done for *both* passes, not only
    # the prefetching one: the members cache is per-context, so an apply pass
    # reusing an earlier pass's existence cache would otherwise fall back to a
    # pair of statements per schema.  Re-reading them here also keeps the apply
    # pass reading the members as they are when it starts, rather than as the
    # preview saw them.
    ctx.prefetch_schema_members(
        [
            record
            for dependency in dependencies
            if type(dependency.obj).__name__ == "Schema"
            and (record := ctx._existing_cache.get(id(dependency.obj))) is not None
        ]
    )
    # Register the plan's existing Feature records so the data-safety checks can
    # bulk-prefetch their stored values on first use (one batched query per value
    # table) and their categorical-link presence (one union across every link
    # table) instead of querying each feature individually in `_override_hint`
    # and `_dtype_change_needs_relocation`.
    ctx._feature_records = [
        record
        for dependency in dependencies
        if type(dependency.obj).__name__ == "Feature"
        and (record := ctx._existing_cache.get(id(dependency.obj))) is not None
    ]

    description = "Applying changes" if mode is ApplyMode.APPLY else "Resolving objects"
    items: Iterable[Any] = (
        track(dependencies, description) if track is not None else dependencies
    )
    for dependency in items:
        ctx.materialize_object(dependency.obj)
    # Before anything acts on the resolved plan: an object that could not be
    # reconciled has no lock entry, so the removal pass would read it as dropped.
    ctx.raise_collected_drift()
    if (
        ctx._deferred_migrations
        or ctx._deferred_member_migrations
        or ctx._deferred_member_removals
        or ctx._deferred_index_changes
        or ctx._deferred_flexible_changes
        or ctx._deferred_record_updates
    ):
        if report is not None:
            report("Running data migrations")
        # Deliberately after the whole plan is materialized: a migration maps
        # values onto records it never creates, so every target it might look up
        # has to exist first -- and expressing that as a creation-order edge on
        # the *feature* is both wrong and cyclic for a self-referential ontology.
        ctx.run_deferred_migrations()
    if report is not None:
        report("Reconciling removals")
    ctx.apply_removals()
    ctx.drop_records()
    # A PLAN-mode preview of the content collisions APPLY settles at save time.
    ctx.check_schema_collisions()
    # Last, because it is judged against the shape the plan leaves behind and has
    # to see every rename the run made, whichever object carried it.  Raising
    # here still rolls an APPLY back: the whole pass runs inside the caller's
    # `transaction.atomic()`.  The re-exports it schedules do not -- they are
    # returned on `ctx.pending_sheet_exports` for the caller to run after the
    # commit (see :func:`run_sheet_export_migrations`).
    ctx.check_sheet_exports()
    return ctx


# === Sheet exports (post-commit) ==============================================


@dataclass
class SheetExportOutcome:
    """What :func:`run_sheet_export_migrations` did, for the caller to report.

    `branch` is the branch the new versions were written onto (`None` when
    nothing had to be written), `merged` whether it made it into the instance,
    and `notes` the per-migration summaries for the diff.  `error` carries the
    failure that stopped it, which the caller turns into a non-zero exit: the
    schema change is already committed by then, so it cannot be rolled back --
    only reported, with the unmerged branch named so the exports can be inspected
    and the apply re-run.
    """

    branch: Any = None
    merged: bool = False
    written: int = 0
    closed: list[str] = field(default_factory=list)
    promotions: list[tuple[Any, Any]] = field(default_factory=list)
    promoted: int = 0
    notes: list[str] = field(default_factory=list)
    error: BaseException | None = None

    @property
    def ok(self) -> bool:
        """Whether every declared re-export ran and the branch was merged."""
        return self.error is None

    @property
    def partial_promotions(self) -> bool:
        """Whether the merge landed but not every head was re-pointed.

        The one failure this function cannot undo: the new versions are already
        visible, and some families still point at the version they superseded.
        Worth saying out loud, because it is not fixed by re-running -- the next
        apply sees a schema that already matches the plan and owes nothing.
        """
        return self.merged and self.promoted < len(self.promotions)

    @property
    def pending_promotions(self) -> list[tuple[Any, Any]]:
        """The `(previous, artifact)` pairs whose head was never re-pointed.

        Promotions run in order and `promoted` counts the ones that committed,
        so everything from there on is outstanding -- the first of them being
        the pair that raised.
        """
        return self.promotions[self.promoted :]

    def describe_pending_promotions(self) -> list[str]:
        """Name each family left pointing at the wrong version.

        This is the only record of what is outstanding.  The failure happens
        after the commit *and* after the merge, so no branch is left holding the
        work and the next apply sees a schema that already matches the plan --
        nothing will re-derive this list.  It therefore carries what an operator
        needs to finish the job by hand: the artifact `key` naming the export,
        the uid that should be the head, and the uid that still is.
        """
        described: list[str] = []
        for previous, artifact in self.pending_promotions:
            key = _artifact_label(artifact) or _artifact_label(previous) or "?"
            described.append(
                f"{key}: promote uid {_uid_of(artifact)} over the current head "
                f"{_uid_of(previous)}"
            )
        return described

    @property
    def branch_name(self) -> str | None:
        """The branch's name, or `None` when none was opened."""
        return None if self.branch is None else str(getattr(self.branch, "name", ""))


# === Rehearsals ================================================================


class _Rehearsed(Exception):
    """Unwinds a rehearsal's transaction once its block has run."""


@dataclass
class _Rehearsal:
    """What the plans of the active rehearsal created so far.

    `drawn` are the uids they created objects with, which the rollback
    discards.  `created` maps the natural key of each object they created to
    the plan that created it, so a later plan borrowing one reports it as its
    prerequisite's create in this run (see :func:`_credit_prerequisites`).
    """

    drawn: set[str] = field(default_factory=set)
    created: dict[str, str] = field(default_factory=dict)


# The active rehearsal; `None` outside one.
_active_rehearsal: ContextVar[_Rehearsal | None] = ContextVar(
    "_active_rehearsal", default=None
)


@contextmanager
def rehearsal() -> Iterator[None]:
    """Roll back everything :func:`preview_changes` applies inside the block.

    The plans rehearsed in one block share its transaction, so each sees what
    the plans before it created -- as each plan of one `apply` sees what the
    earlier ones committed, which a plan adopting a prerequisite's objects
    relies on -- and all of it is rolled back however the block exits.
    """
    with _rehearsing():
        yield


@contextmanager
def _rehearsing() -> Iterator[_Rehearsal]:
    """:func:`rehearsal`, yielding what its plans create.

    The uids the plans' promises resolved while applying are forgotten again
    once the transaction rolled back: they name records the rollback
    discarded (see :func:`~lamindb_setup.evolve.core.provisional_promises`).
    """
    from django.db import transaction

    state = _Rehearsal()
    token = _active_rehearsal.set(state)
    try:
        with provisional_promises(), transaction.atomic():
            yield state
            raise _Rehearsed
    except _Rehearsed:
        pass
    finally:
        _active_rehearsal.reset(token)


def preview_changes(
    plan: Plan[E],
    *,
    lock: LockFile | None = None,
    claimed_keys: set[str] | None = None,
    claimed_by: dict[str, str] | None = None,
    report: Callable[[str], None] | None = None,
    track: Callable[[Sequence[Any], str], Iterable[Any]] | None = None,
    allow_data_validated_changes: bool = False,
    sibling_uids: SiblingUids | None = None,
    strict_uids: bool = True,
) -> ApplyContext:
    """Rehearse applying `plan`: apply it for real, then roll it back.

    A PLAN-mode diff cannot run a migration whose target the same plan creates
    -- a `str -> cat[Record[T]]` change where `T` and its records are new
    -- since it creates nothing.  This applies the plan instead, materializing
    every object and running every migration with its post-check, inside a
    transaction that is always rolled back, so the diff reports what the apply
    would do or raises what it would raise.  Side effects outside the database
    are not reached: sheet re-exports only ever run after a committed apply,
    and no lock is written.

    Within a :func:`rehearsal` the plan joins it, seeing what the plans
    rehearsed before it created, and is rolled back with them; otherwise it is
    rehearsed and rolled back on its own.  A uid the rehearsal drew for an
    object it created is reported as known only after the apply, as a
    PLAN-mode diff reports it.  A borrowed object a plan rehearsed earlier in
    the block created is reported as that prerequisite's create, as a PLAN-mode
    diff given the run's `co_applied_keys` reports it.  The arguments are
    those of :func:`compute_changes`.
    """
    active = _active_rehearsal.get()
    with _rehearsing() if active is None else nullcontext(active) as rehearsed:
        ctx = compute_changes(
            plan,
            mode=ApplyMode.APPLY,
            lock=lock,
            claimed_keys=claimed_keys,
            claimed_by=claimed_by,
            report=report,
            track=track,
            allow_data_validated_changes=allow_data_validated_changes,
            sibling_uids=sibling_uids,
            strict_uids=strict_uids,
        )
        _credit_prerequisites(ctx, rehearsed.created)
        pinned = _pinned_uids(sibling_uids)
        for change in ctx.changes:
            if change.action is not Action.CREATE:
                continue
            # A borrowed create is its prerequisite's, which drew its uid.
            if not change.borrowed:
                rehearsed.created[change.key] = plan.name
                if change.uid not in pinned:
                    rehearsed.drawn.add(change.uid)
            change.uid = PLACEHOLDER_UID
        _forget_drawn_uids(ctx.changes, rehearsed.drawn)
    return ctx


def _credit_prerequisites(ctx: ApplyContext, created: Mapping[str, str]) -> None:
    """Report a borrowed object a prerequisite rehearsed earlier created as its create.

    `created` maps a natural key to the plan that created the object earlier
    in the rehearsal.  Applied after its prerequisite, a plan adopts such an
    object, which resolves as unchanged; against the instance it is a create
    all the same -- the prerequisite's, never this plan's -- so it is reported
    as the borrowed `CREATE` a PLAN-mode diff reports for it.
    """
    for change in ctx.changes:
        if change.action is not Action.NO_CHANGE:
            continue
        if change.obj_id not in ctx.borrowed_ids:
            continue
        owner = ctx.borrowed_owners.get(change.obj_id)
        creator = created.get(change.key)
        if creator is None or (owner is not None and owner.name != creator):
            continue
        change.action = Action.CREATE
        change.borrowed = True
        change.details = [_created_in_run_detail(owner)]


def _pinned_uids(sibling_uids: SiblingUids | None) -> set[str]:
    """Every uid `sibling_uids` pins an object to, known before any apply."""
    if sibling_uids is None:
        return set()
    return {
        uid
        for recorded in (sibling_uids.tracked, sibling_uids.seeded)
        for per_instance in recorded.values()
        for uid in per_instance.values()
    }


_UID_TOKEN = re.compile(r"[0-9A-Za-z]+")


def _forget_drawn_uids(changes: list[ChangeItem], drawn: set[str]) -> None:
    """Report each uid in `drawn` as :data:`PLACEHOLDER_UID` in `changes`.

    Where a created object's uid shows -- a dtype filter naming it, an object a
    later plan adopts -- the rehearsal's uid would be read as the one the apply
    creates it with, which it is not.  A uid pinned by another instance's lock
    is not drawn: the apply creates the object with it too.
    """
    if not drawn:
        return

    def forget(text: str) -> str:
        return _UID_TOKEN.sub(
            lambda match: PLACEHOLDER_UID if match[0] in drawn else match[0], text
        )

    for change in changes:
        if change.uid in drawn:
            change.uid = PLACEHOLDER_UID
        change.details = [forget(detail) for detail in change.details]
        change.warnings = [
            ChangeWarning(forget(warning), warning.category)
            for warning in change.warnings
        ]


def _uid_of(artifact: Any) -> str:
    """Return `artifact`'s uid rendered for a report, or `'?'` if it has none."""
    uid = getattr(artifact, "uid", None)
    return "?" if uid is None else repr(str(uid))


def _artifact_label(artifact: Any) -> str | None:
    """Return the `key` naming `artifact`, falling back to its description."""
    for attribute in ("key", "description"):
        value = getattr(artifact, attribute, None)
        if value:
            return repr(str(value))
    return None


def run_sheet_export_migrations(
    pending: list[_DeferredSheetExports],
    *,
    plan_name: str,
    run_uid: str,
) -> SheetExportOutcome:
    """Run the declared sheet-export migrations, **after** the apply committed.

    The one phase of an apply that lives outside its transaction, because its
    subject does: an export is a file in object storage, and writing one cannot
    be undone by a rollback.  Two consequences shape this function.

    First, it must run *after* the commit, so the rows it exports are the
    migrated ones and so a rollback can never leave the instance pointing at a
    file that describes a schema change it did not keep.

    Second, since the transaction can no longer protect it, the isolation comes
    from a branch: every new version is written onto one, which is merged into
    the instance's branch only once every declared migration has succeeded.  A
    failure therefore leaves the partial work parked on an unmerged branch rather
    than half-visible in the instance, and the returned
    :class:`SheetExportOutcome` names it.  One branch per run, as lamindb intends
    -- a later run redoes the export from the current rows onto its own branch
    and then *closes* the abandoned one, rather than reopening it and rewriting
    what happened (see
    :func:`~lamindb_setup.evolve.engine._sheet_exports.close_branches`).

    A run in which every declaration merely *keeps* its exports
    (:func:`~lamindb_setup.evolve.keep_stale_exports`) opens no branch
    at all: there is nothing to isolate, and an empty branch would be permanent
    noise -- lamindb never deletes them.
    """
    from ._sheet_exports import (
        branch_name_for,
        branch_prefix_for,
        close_branches,
        current_branch,
        merge_branch,
        open_branch,
        promote_head,
        reexport,
    )

    outcome = SheetExportOutcome()
    if not pending:
        return outcome
    name = branch_name_for(plan_name, run_uid)

    def _branch() -> Any:
        # Opened on first write rather than up front, so a run whose declarations
        # all keep their exports leaves no empty branch behind.
        if outcome.branch is None:
            outcome.branch = open_branch(
                name,
                description=(
                    f"Sheet exports re-written by lamin evolve apply of "
                    f"{plan_name!r} (run {run_uid})."
                ),
            )
        return outcome.branch

    try:
        for deferred in pending:
            for migration in deferred.migrations:
                report = MigrationReport()
                context = SheetExportContext(
                    schema=deferred.schema,
                    exports=deferred.exports,
                    report=report,
                    branch_factory=_branch,
                    reexport=reexport,
                    current_branch=current_branch,
                )
                migration.run(context)
                outcome.written += len(context.written)
                outcome.promotions.extend(context.promotions)
                if (summary := report.summary()) is not None:
                    outcome.notes.append(f"{deferred.key}: {migration.name}: {summary}")
    except Exception as exc:
        outcome.error = exc
        return outcome
    if outcome.branch is None:
        # Every declaration kept its exports, so no branch was ever opened.
        outcome.merged = True
        return outcome
    # Merged even when this run wrote nothing: a migration may have written
    # directly through `context.branch`.  Merging an empty one is a no-op.
    try:
        merge_branch(outcome.branch, target=current_branch())
    except Exception as exc:
        outcome.error = exc
        return outcome
    outcome.merged = True
    # Only now: a promotion re-points the head on the instance's *own* branch, so
    # it must not become visible before the run is known to be good.  The merge
    # already ran its own is_latest reconciliation, which never promotes, so
    # these survive it and every later one.
    #
    # Guarded like every step above it.  This is the phase that cannot be rolled
    # back, so a failure here has to arrive as an outcome the caller can report
    # -- which branch holds the work, how far it got -- rather than as an
    # exception thrown past a caller that only handles `TrackError`.
    try:
        for previous, artifact in outcome.promotions:
            promote_head(previous, artifact)
            outcome.promoted += 1
        # This run superseded whatever an earlier failed one left behind, so its
        # branches are retired rather than left looking like outstanding work.
        outcome.closed = close_branches(branch_prefix_for(plan_name), keep=name)
    except Exception as exc:
        outcome.error = exc
    return outcome


def write_lock(ctx: ApplyContext, plan: Plan[E], path: str | Path) -> None:
    """Write the lock file recorded in `ctx` to `path`."""
    ctx.build_lock(plan.name).write(path)
