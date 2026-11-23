# Source behavior retained; host-repository lint exceptions.
# ruff: noqa: TC003
"""Tear down the objects a plan created, after checking for external references.

`destroy` is the inverse of `apply`: it resolves every object a plan
manages against the connected LaminDB instance and permanently deletes them.

Before deleting anything it performs a read-only safety check: for each managed
object it counts how many *other* records (records, record types, or any other
registry) reference it.  References that originate from objects the same plan
manages are ignored, since those are torn down together.  If any external
reference remains, :func:`compute_destroy` reports the count and nothing is
deleted.

When references do remain there are two escape hatches: `--force` permanently
deletes the referencing rows (irreversible), while `--trash-references` moves
the referencing objects that support it to the trash (recoverable) and leaves
un-trashable link rows in place, so the whole teardown stays reversible.

A reference may be held by an object another plan manages -- a plan built on
this one, applied to the same instance.  :func:`plan_held_references` tells
those apart, using the other plans' lock files (:func:`managing_plans`), so the
command can ask for the plans to be destroyed together instead of trashing or
deleting another plan's objects under it.
"""

from __future__ import annotations

from typing import Any

from typing_extensions import TypeVar

E = TypeVar("E", bound="PlanExports")

import contextlib
import io
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from ..core import ApplyMode
from ._apply import ApplyContext, ExistenceCache
from ._lock import (
    LockFile,
    instance_lock_files,
    lock_path,
    natural_key,
    registry_for_kind,
)
from ._trash import delete_object

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping, Sequence

    from ..authoring import Plan, PlanExports
    from ..core import Materializable

__all__ = [
    "DestroyPlan",
    "DestroyTarget",
    "ManagingPlan",
    "MissingTarget",
    "PlanReference",
    "Reference",
    "ReferenceDetail",
    "compute_destroy",
    "compute_destroy_group",
    "delete_references",
    "execute_destroy",
    "managing_plans",
    "merge_destroy_plans",
    "plan_held_references",
    "print_destroy",
    "render_destroy",
    "trash_references",
]


@dataclass
class ReferenceDetail:
    """A single inbound reference relation that blocks deletion.

    The blocking `count` is determined up front with a cheap aggregate query.
    The referencing `records` themselves are only fetched on demand (for
    detailed listing or `--force` deletion) via `_loader`, so the common
    safety check never materializes potentially huge numbers of rows.  `_loader`
    returns a queryset (not a list) so callers can fetch a bounded slice with
    :meth:`head` instead of pulling every referencing row into memory.
    """

    referencing_kind: str
    relation: str
    count: int
    _loader: Callable[[], Any] | None = field(default=None, repr=False)
    _records: list[Any] | None = field(default=None, repr=False)

    @property
    def records(self) -> list[Any]:
        """The referencing records, fetched lazily on first access."""
        if self._records is None:
            self._records = list(self._loader()) if self._loader is not None else []
        return self._records

    def head(self, limit: int) -> list[Any]:
        """Return up to `limit` referencing records with a bounded query.

        Unlike :attr:`records`, this never materializes the full relation: it
        slices the queryset in SQL, so listing a handful of blocking references
        stays cheap even when millions of rows reference a target.
        """
        if limit <= 0:
            return []
        if self._records is not None:
            return self._records[:limit]
        if self._loader is None:
            return []
        return list(self._loader()[:limit])


@dataclass
class Reference:
    """One individual record that references a target and blocks its deletion."""

    target_key: str
    target_kind: str
    target_name: str | None
    target_uid: str
    referencing_kind: str
    relation: str
    attributes: dict[str, Any]

    def describe(self) -> str:
        """Return a single-line description of this reference and its attributes."""
        attrs = ", ".join(f"{key}={value!r}" for key, value in self.attributes.items())
        target = self.target_name if self.target_name is not None else self.target_uid
        return (
            f"{self.referencing_kind}({attrs}) "
            f"--[{self.relation}]--> {self.target_kind} {target!r}"
        )


@dataclass
class DestroyTarget:
    """A plan object that exists in the instance and is slated for deletion."""

    key: str
    kind: str
    name: str | None
    uid: str
    record: Any
    references: list[ReferenceDetail] = field(default_factory=list)

    @property
    def reference_count(self) -> int:
        """Total number of external records referencing this object."""
        return sum(detail.count for detail in self.references)


@dataclass
class MissingTarget:
    """A plan object that does not exist in the instance (nothing to delete)."""

    key: str
    kind: str
    name: str | None


@dataclass
class DestroyPlan:
    """The resolved teardown for a plan: existing targets and missing objects."""

    plan_name: str
    targets: list[DestroyTarget] = field(default_factory=list)
    missing: list[MissingTarget] = field(default_factory=list)

    @property
    def total_references(self) -> int:
        """Total external references across all targets."""
        return sum(target.reference_count for target in self.targets)

    @property
    def is_safe(self) -> bool:
        """Whether the targets can be deleted without breaking external references."""
        return self.total_references == 0

    def references(self, *, limit: int | None = None) -> list[Reference]:
        """Return a flat list of the individual records that block deletion.

        Pass `limit` to stop after collecting that many references.  The
        underlying records are then fetched with a bounded query per relation
        (see :meth:`ReferenceDetail.head`), so listing a preview of the blocking
        references never materializes the full relation nor triggers a per-record
        foreign-key lookup for every blocking row.
        """
        flat: list[Reference] = []
        for target in self.targets:
            for detail in target.references:
                if limit is not None and len(flat) >= limit:
                    return flat
                if limit is None:
                    records = detail.records
                else:
                    records = detail.head(limit - len(flat))
                for record in records:
                    flat.append(_reference(target, detail, record))
                    if limit is not None and len(flat) >= limit:
                        return flat
        return flat


def _reference(
    target: DestroyTarget, detail: ReferenceDetail, record: Any
) -> Reference:
    """Describe `record`, one of `detail`'s referencing rows, as a reference."""
    return Reference(
        target_key=target.key,
        target_kind=target.kind,
        target_name=target.name,
        target_uid=target.uid,
        referencing_kind=detail.referencing_kind,
        relation=detail.relation,
        attributes=_record_attributes(record),
    )


# A small, ordered set of attributes that make a referencing record
# identifiable when listed.  Only those present and set are shown.
_REFERENCE_ATTRS: tuple[str, ...] = (
    "uid",
    "name",
    "is_type",
    "description",
    "dtype",
    "reference",
    "reference_type",
)

# Fields used to describe a link/through record (an `IsLink` row such as
# `RecordJson`) that has no identifying attributes of its own; we surface the
# record it attaches to and the value it stores instead.
_LINK_ATTRS: tuple[str, ...] = ("record", "value")


def _label(value: Any) -> Any:
    """Return a concise label for a related object (its name or uid, else itself)."""
    name = getattr(value, "name", None)
    if name is not None:
        return name
    uid = getattr(value, "uid", None)
    if uid is not None:
        return uid
    return value


def _record_attributes(record: Any) -> dict[str, Any]:
    """Return the notable, set attributes of `record` for one-line display.

    Regular records are described by their identifying attributes
    (:data:`_REFERENCE_ATTRS`).  Link/through records (lamindb `IsLink` rows
    like `RecordJson`) have none of those, so the record they attach to and
    the value they store are surfaced instead (:data:`_LINK_ATTRS`).
    """
    attrs: dict[str, Any] = {}
    for name in _REFERENCE_ATTRS:
        if not hasattr(record, name):
            continue
        value = getattr(record, name)
        if value is None:
            continue
        attrs[name] = value
    if attrs:
        return attrs
    for name in _LINK_ATTRS:
        if not hasattr(record, name):
            continue
        try:
            value = getattr(record, name)
        except Exception:  # pragma: no cover - defensive against lazy FK errors
            continue
        if value is None:
            continue
        attrs[name] = _label(value)
    return attrs


def _through_models(model: Any) -> set[Any]:
    """Return the many-to-many through models of `model`.

    Reverse foreign keys from a many-to-many through model duplicate the
    information already captured by the corresponding many-to-many relation, so
    they are skipped when counting references to avoid double counting.
    """
    throughs: set[Any] = set()
    for model_field in model._meta.get_fields():
        if not getattr(model_field, "many_to_many", False):
            continue
        through = getattr(model_field, "through", None)
        if through is None:
            remote = getattr(model_field, "remote_field", None)
            through = getattr(remote, "through", None)
        if through is not None:
            throughs.add(through)
    return throughs


@dataclass
class _RelationScan:
    """One inbound relation of a model to scan across a group of its targets.

    `bulk` relations (reverse foreign keys and one-to-ones) are counted with a
    single grouped-aggregate query for the whole same-model group, attributing
    each referencing record to its target via the foreign-key column
    (`field_attname`).  Many-to-many relations are counted with a single
    grouped-aggregate query too, joining from the referencing model back to the
    targets through `m2m_query_name` (the lookup from the referencing model to
    the target model).  When that reverse lookup is hidden (`related_name='+'`)
    it is `None` and the relation falls back to a per-target accessor count.
    """

    referencing_kind: str
    relation: str
    related_model: Any
    bulk: bool
    field_name: str | None = None
    field_attname: str | None = None
    m2m_query_name: str | None = None


@dataclass
class _ScanUnit:
    """One relation scan bound to the same-model targets it applies to."""

    relation: _RelationScan
    group: list[DestroyTarget]
    target_pks: list[Any]


def _m2m_reverse_query_name(field: Any) -> str | None:
    """Return the lookup from an m2m field's related model back to its own model.

    Used to count references to every target in a single grouped query.  Returns
    `None` when the reverse relation is hidden (`related_name='+'`), so the
    caller falls back to a per-target accessor count.
    """
    remote = getattr(field, "remote_field", None)
    if remote is not None and getattr(remote, "is_hidden", lambda: False)():
        return None
    try:
        return field.related_query_name()
    except Exception:  # pragma: no cover - defensive against exotic fields
        return None


def _model_relations(model: Any) -> list[_RelationScan]:
    """Return the inbound relations to scan for `model`, in a stable order.

    Reverse foreign keys and one-to-ones become `bulk` scans; many-to-many
    relations (reverse and forward) carry the `m2m_query_name` needed to count
    them for the whole group in one grouped query.  Each relation is scanned with
    a single query regardless of how many targets share the model.  Reverse
    foreign keys originating from a many-to-many through model are skipped, since
    they duplicate the corresponding many-to-many relation.
    """
    throughs = _through_models(model)
    relations: list[_RelationScan] = []
    for rel in model._meta.related_objects:
        related_model = rel.related_model
        if related_model in throughs:
            continue
        accessor = rel.get_accessor_name()
        if accessor is None:
            continue
        field = getattr(rel, "field", None)
        if (
            not rel.many_to_many
            and field is not None
            and hasattr(field, "name")
            and hasattr(field, "attname")
        ):
            relations.append(
                _RelationScan(
                    referencing_kind=related_model.__name__,
                    relation=accessor,
                    related_model=related_model,
                    bulk=True,
                    field_name=field.name,
                    field_attname=field.attname,
                )
            )
        else:
            # Reverse many-to-many: the referencing model defines the m2m field,
            # so its own name is the lookup back to our targets.
            m2m_query_name = (
                field.name
                if rel.many_to_many and field is not None and hasattr(field, "name")
                else None
            )
            relations.append(
                _RelationScan(
                    referencing_kind=related_model.__name__,
                    relation=accessor,
                    related_model=related_model,
                    bulk=False,
                    m2m_query_name=m2m_query_name,
                )
            )

    # Forward many-to-many fields (e.g. a feature's `schemas`) record other
    # objects that include this one; deleting it would alter them, so they count
    # as references too.
    for m2m_field in model._meta.many_to_many:
        relations.append(
            _RelationScan(
                referencing_kind=m2m_field.related_model.__name__,
                relation=m2m_field.name,
                related_model=m2m_field.related_model,
                bulk=False,
                m2m_query_name=_m2m_reverse_query_name(m2m_field),
            )
        )
    return relations


def _bulk_record_loader(
    rel: _RelationScan, target_pk: Any, excluded: set[Any]
) -> Callable[[], Any]:
    """Return a loader that yields the referencing queryset of one bulk target."""

    def load() -> Any:
        assert rel.field_name is not None
        queryset = rel.related_model._default_manager.filter(
            **{rel.field_name: target_pk}
        )
        if excluded:
            queryset = queryset.exclude(pk__in=excluded)
        return queryset

    return load


def _accessor_record_loader(
    record: Any, relation: str, excluded: set[Any]
) -> Callable[[], Any]:
    """Return a loader that yields the referencing queryset via an accessor."""

    def load() -> Any:
        queryset = getattr(record, relation).all()
        if excluded:
            queryset = queryset.exclude(pk__in=excluded)
        return queryset

    return load


def _run_scan_unit(unit: _ScanUnit, excluded_pks_by_model: dict[Any, set[Any]]) -> None:
    """Scan one relation and append a :class:`ReferenceDetail` to blocked targets.

    Only the blocking *count* is fetched here, and with a single query per
    relation regardless of how many targets share the model: bulk relations group
    the referencing rows by their foreign-key column, and many-to-many relations
    group the referencing model by its reverse lookup to the targets.  A m2m
    relation whose reverse lookup is hidden falls back to a per-target
    `count()`.  The referencing records themselves are loaded lazily (see
    :class:`ReferenceDetail`) only if they are later listed or force deleted, so
    the safety check never materializes potentially huge tables.  Records the
    plan manages (`excluded`) are filtered out in SQL.
    """
    from django.db.models import Count

    rel = unit.relation
    excluded = excluded_pks_by_model.get(rel.related_model, set())

    def block(target: DestroyTarget, count: int, loader: Callable[[], Any]) -> None:
        if not count:
            return
        target.references.append(
            ReferenceDetail(
                referencing_kind=rel.referencing_kind,
                relation=rel.relation,
                count=count,
                _loader=loader,
            )
        )

    if rel.bulk:
        assert rel.field_name is not None and rel.field_attname is not None
        queryset = rel.related_model._default_manager.filter(
            **{f"{rel.field_name}__in": unit.target_pks}
        )
        if excluded:
            queryset = queryset.exclude(pk__in=excluded)
        counts = queryset.order_by().values(rel.field_attname).annotate(_n=Count("pk"))
        count_by_target = {row[rel.field_attname]: row["_n"] for row in counts}
        for target in unit.group:
            block(
                target,
                count_by_target.get(target.record.pk, 0),
                _bulk_record_loader(rel, target.record.pk, excluded),
            )
        return

    if rel.m2m_query_name is not None:
        # One grouped query joins the referencing model back to every target
        # through its reverse lookup, so the m2m scan no longer scales with the
        # number of targets.
        queryset = rel.related_model._default_manager.filter(
            **{f"{rel.m2m_query_name}__in": unit.target_pks}
        )
        if excluded:
            queryset = queryset.exclude(pk__in=excluded)
        counts = queryset.order_by().values(rel.m2m_query_name).annotate(_n=Count("pk"))
        count_by_target = {row[rel.m2m_query_name]: row["_n"] for row in counts}
        for target in unit.group:
            block(
                target,
                count_by_target.get(target.record.pk, 0),
                _accessor_record_loader(target.record, rel.relation, excluded),
            )
        return

    # Hidden reverse relation: fall back to a per-target accessor count.
    for target in unit.group:
        queryset = getattr(target.record, rel.relation).all()
        if excluded:
            queryset = queryset.exclude(pk__in=excluded)
        block(
            target,
            queryset.count(),
            _accessor_record_loader(target.record, rel.relation, excluded),
        )


def _resolve_destroy_targets(
    plan: Plan[E],
    *,
    lock: LockFile | None = None,
    report: Callable[[str], None] | None = None,
    existing_cache: ExistenceCache | None = None,
) -> tuple[list[DestroyTarget], list[MissingTarget]]:
    """Resolve a plan's own objects against the instance (no reference scanning).

    Returns the existing targets (in plan creation order) and the objects that
    do not exist.  Existence lookups are bulk-loaded up front (one batched query
    per backing registry instead of one per object), mirroring `apply`'s
    prefetch, so the cost no longer scales with the number of managed objects.
    """
    if report is not None:
        report("Finalizing plan")
    plan.finalize()
    ctx = ApplyContext(
        mode=ApplyMode.PLAN,
        lock=lock,
        default_space_uid=plan.ctx.default_space_uid,
        existing_cache=existing_cache,
    )

    # Dedupe the managed objects while preserving dependency order so a typed
    # object's parent is resolved before the object itself during prefetch.
    #
    # The full dependency set (including objects inherited from the plans in
    # `depends_on`) is prefetched so a child object's natural key, which
    # references its upstream parent type, can be resolved.  Only the plan's own
    # objects become deletion targets, though: objects managed by upstream plans
    # must not be torn down here.
    own_ids = {id(dependency.obj) for dependency in plan.own_dependencies}
    objs: list[Materializable] = []
    own_objs: list[Materializable] = []
    seen: set[int] = set()
    for dependency in plan.dependencies:
        obj: Materializable = dependency.obj
        if id(obj) in seen:
            continue
        seen.add(id(obj))
        objs.append(obj)
        if id(obj) in own_ids:
            own_objs.append(obj)

    if report is not None:
        report("Prefetching instance state")
    ctx.prefetch(objs)

    targets: list[DestroyTarget] = []
    missing: list[MissingTarget] = []
    for obj in own_objs:
        key = natural_key(obj)
        kind = type(obj).__name__
        name = getattr(obj, "name", None)
        existing = ctx._existing(obj)
        if existing is None:
            missing.append(MissingTarget(key=key, kind=kind, name=name))
            continue
        targets.append(
            DestroyTarget(
                key=key,
                kind=kind,
                name=name,
                uid=str(existing.uid),
                record=existing,
            )
        )
    return targets, missing


def _target_pks_by_model(targets: Iterable[DestroyTarget]) -> dict[Any, set[Any]]:
    """Group the primary keys of resolved targets by their backing model."""
    pks_by_model: dict[Any, set[Any]] = {}
    for target in targets:
        pks_by_model.setdefault(type(target.record), set()).add(target.record.pk)
    return pks_by_model


def _scan_references(
    targets: list[DestroyTarget],
    excluded_pks_by_model: dict[Any, set[Any]],
    *,
    report: Callable[[str], None] | None = None,
    track: Callable[[Sequence[Any], str], Iterable[Any]] | None = None,
) -> None:
    """Populate each target's inbound references, excluding managed records.

    Targets are grouped by backing model and each model's inbound relations are
    scanned once for the whole group.  Reverse foreign keys and one-to-ones use a
    single batched query per relation instead of one per target, collapsing the
    previous `targets x relations` query fan-out that made the reference check
    slow.  Many-to-many relations, where a referencing record may point at
    several targets, stay per target.  The progress `track` ticks once per
    relation scanned.
    """
    if report is not None:
        report("Scanning for external references")

    groups: dict[Any, list[DestroyTarget]] = {}
    for target in targets:
        groups.setdefault(type(target.record), []).append(target)

    units: list[_ScanUnit] = []
    for model, group in groups.items():
        target_pks = [target.record.pk for target in group]
        for relation in _model_relations(model):
            units.append(
                _ScanUnit(relation=relation, group=group, target_pks=target_pks)
            )

    scanned: Iterable[_ScanUnit] = (
        track(units, "Checking references") if track is not None else units
    )
    for unit in scanned:
        _run_scan_unit(unit, excluded_pks_by_model)


def compute_destroy(
    plan: Plan[E],
    *,
    lock: LockFile | None = None,
    report: Callable[[str], None] | None = None,
    track: Callable[[Sequence[Any], str], Iterable[Any]] | None = None,
    existing_cache: ExistenceCache | None = None,
) -> DestroyPlan:
    """Resolve `plan`'s objects against the instance and count references.

    Read-only: nothing is deleted.  Returns a :class:`DestroyPlan` describing the
    existing targets (in plan creation order), the objects that do not exist, and
    the external references that would block deletion.

    `report` and `track` are optional progress callbacks (see
    :func:`lamindb_setup.evolve.console.progress_reporter`).
    `existing_cache` lets a caller supply -- and afterwards inspect -- the
    existence lookups this resolution makes (see
    :class:`~lamindb_setup.evolve.engine._apply.ExistenceCache`).
    """
    targets, missing = _resolve_destroy_targets(
        plan, lock=lock, report=report, existing_cache=existing_cache
    )
    _scan_references(targets, _target_pks_by_model(targets), report=report, track=track)
    return DestroyPlan(plan_name=plan.name, targets=targets, missing=missing)


def compute_destroy_group(
    plans: Sequence[Plan[Any]],
    *,
    locks: Sequence[LockFile | None] | None = None,
    report: Callable[[str], None] | None = None,
    track: Callable[[Sequence[Any], str], Iterable[Any]] | None = None,
) -> list[DestroyPlan]:
    """Resolve several plans together, scoping references to the listed plans.

    Returns one :class:`DestroyPlan` per input plan (in the same order).  Every
    plan's objects are resolved first, then each plan's references are scanned
    excluding the objects managed by *any* of the listed plans: an object one
    plan references but another listed plan manages is torn down together and so
    does not count as an external reference that blocks deletion.  This mirrors
    `plan`/`apply`'s multi-plan scoping.

    With a single plan the excluded set is exactly that plan's own objects, so
    the result matches :func:`compute_destroy`.
    """
    if locks is None:
        locks = [None] * len(plans)
    elif len(locks) != len(plans):
        raise ValueError("locks must have the same length as plans")

    resolved: list[tuple[Plan[Any], list[DestroyTarget], list[MissingTarget]]] = []
    for plan, lock in zip(plans, locks, strict=False):
        targets, missing = _resolve_destroy_targets(plan, lock=lock, report=report)
        resolved.append((plan, targets, missing))

    # Every target across all listed plans is excluded from the reference scan,
    # so references between the plans being destroyed together do not block.
    excluded_pks_by_model: dict[Any, set[Any]] = {}
    for _plan, targets, _missing in resolved:
        for model, pks in _target_pks_by_model(targets).items():
            excluded_pks_by_model.setdefault(model, set()).update(pks)

    destroy_plans: list[DestroyPlan] = []
    for plan, targets, missing in resolved:
        _scan_references(targets, excluded_pks_by_model, report=report, track=track)
        destroy_plans.append(
            DestroyPlan(plan_name=plan.name, targets=targets, missing=missing)
        )
    return destroy_plans


def merge_destroy_plans(
    destroy_plans: Sequence[DestroyPlan], *, plan_name: str
) -> DestroyPlan:
    """Merge several destroy plans into one for a single, atomic teardown.

    Targets are concatenated in the given order and deduplicated by backing
    record, so a plan listed earlier keeps its position.  Callers should order
    the plans dependencies-first (a plan before the plans it depends on) so the
    merged plan's reversed deletion removes dependents before the types they
    rely on.  Missing objects are carried over for reporting.
    """
    seen: set[tuple[str, Any]] = set()
    targets: list[DestroyTarget] = []
    missing: list[MissingTarget] = []
    for destroy_plan in destroy_plans:
        for target in destroy_plan.targets:
            marker = (type(target.record).__name__, target.record.pk)
            if marker in seen:
                continue
            seen.add(marker)
            targets.append(target)
        missing.extend(destroy_plan.missing)
    return DestroyPlan(plan_name=plan_name, targets=targets, missing=missing)


# === References held by other plans ===========================================


@dataclass(frozen=True)
class ManagingPlan:
    """A plan applied to the instance, found by its lock file.

    `name` is the plan name the lock records, `target` the plan file as a
    `destroy` takes it -- relative to the working directory when it lies
    beneath it -- and `lock_file` the lock recording the plan's objects.
    """

    name: str
    target: str
    lock_file: Path

    def describe(self) -> str:
        """The plan as a message names it: `'child' (plans/plan_child.py)`."""
        return f"{self.name!r} ({self.target})"

    @property
    def lock_display(self) -> str:
        """The lock file as a message names it, relative like :attr:`target`."""
        return _display_path(self.lock_file)


@dataclass(frozen=True)
class PlanReference:
    """A blocking reference held by an object another plan manages.

    `uids` are the plan-managed objects the referencing row belongs to: the
    row itself, or for a link row (such as a record's feature value) the object
    whose deletion takes the row along.  `plans` are the plans whose lock
    files record those objects.
    """

    reference: Reference
    uids: frozenset[str]
    plans: tuple[ManagingPlan, ...]


# How many uids one `uid__in` filter carries, well below SQLite's limit on
# the variables of a statement.
_UID_CHUNK = 500


def _display_path(path: Path) -> str:
    """`path` relative to the working directory when it lies beneath it."""
    with contextlib.suppress(OSError, ValueError):
        return str(path.relative_to(Path.cwd().resolve()))
    return str(path)


def managing_plans(
    plan_files: Iterable[Path], instance_slug: str
) -> dict[str, tuple[ManagingPlan, ...]]:
    """Map the uid of every object another plan manages on the instance to it.

    The plans are found by their lock files against `instance_slug` under the
    project roots of `plan_files` -- the plans being destroyed, whose own
    locks are skipped -- the way `repair align-uids` finds them (see
    :func:`~._lock.instance_lock_files`).  A lock whose plan file
    (`plan_x.<slug>.lock` -> `plan_x.py`) is gone is skipped too: no plan
    declares its objects any more.  Seeded objects count with the tracked ones,
    since the plan declares both.  A uid several locks record maps to each of
    their plans.
    """
    from ._loader import project_root

    files = [Path(path).resolve() for path in plan_files]
    own = {lock_path(path, instance_slug).resolve() for path in files}
    suffix = f".{instance_slug}.lock"
    owners: dict[str, list[ManagingPlan]] = {}
    roots = {project_root(path) for path in files}
    for lock_file in instance_lock_files(roots, instance_slug):
        if lock_file in own:
            continue
        plan_file = lock_file.with_name(f"{lock_file.name[: -len(suffix)]}.py")
        lock = LockFile.read(lock_file) if plan_file.is_file() else None
        if lock is None:
            continue
        plan = ManagingPlan(
            name=lock.plan or plan_file.stem,
            target=_display_path(plan_file),
            lock_file=lock_file,
        )
        tracked = [entry.uid for entry in lock.objects.values() if entry.uid]
        for uid in dict.fromkeys([*tracked, *lock.seeded.values()]):
            owners.setdefault(uid, []).append(plan)
    return {uid: tuple(plans) for uid, plans in owners.items()}


def _owner_paths(model: Any) -> list[str | None]:
    """The lookups from a `model` row to the plan-managed object it belongs to.

    A row of a registry a lock records (feature, schema, record) is an object
    in its own right, its own uid (`None`).  A link row has no uid: it
    belongs to the object whose deletion takes it along -- the record a feature
    value is stored for -- i.e. its cascading foreign keys into those
    registries.  Any other row (an artifact, a run) is no plan's.
    """
    from django.db.models import CASCADE

    lockable = {registry_for_kind(kind) for kind in ("Feature", "Schema", "Record")}
    if model in lockable:
        return [None]
    fields = model._meta.concrete_fields
    if any(field.name == "uid" for field in fields):
        return []
    return [
        field.name
        for field in fields
        if field.many_to_one
        and field.related_model in lockable
        and field.remote_field.on_delete is CASCADE
    ]


def _rows_owned(queryset: Any, uids: Sequence[str]) -> dict[Any, tuple[Any, set[str]]]:
    """Return `queryset`'s rows that are, or belong to, an object in `uids`.

    Keyed by the row's pk, each with the uids of the objects it belongs to (see
    :func:`_owner_paths`).  The filter runs in SQL, `uids` in chunks.
    """
    owned: dict[Any, tuple[Any, set[str]]] = {}
    for path in _owner_paths(queryset.model):
        lookup = "uid" if path is None else f"{path}__uid"
        for start in range(0, len(uids), _UID_CHUNK):
            rows = queryset.filter(
                **{f"{lookup}__in": uids[start : start + _UID_CHUNK]}
            )
            if path is not None:
                rows = rows.select_related(path)
            for record in rows:
                owner = record if path is None else getattr(record, path)
                owned.setdefault(record.pk, (record, set()))[1].add(owner.uid)
    return owned


def plan_held_references(
    destroy_plan: DestroyPlan, managing: Mapping[str, Sequence[ManagingPlan]]
) -> list[PlanReference]:
    """Return the blocking references held by objects other plans manage.

    `managing` maps a uid to the plans managing that object (see
    :func:`managing_plans`).  Each blocking relation's referencing rows are
    filtered in SQL to those that are, or belong to, such an object (see
    :func:`_owner_paths`), so only the plan-managed rows are fetched, never the
    whole relation.  The blocking references not returned are held by objects
    no plan manages.  The targets' own uids are ignored: a legacy lock may
    still record its prerequisites' objects.
    """
    target_uids = {target.uid for target in destroy_plan.targets}
    uids = [uid for uid in managing if uid not in target_uids]
    if not uids:
        return []
    held: list[PlanReference] = []
    for target in destroy_plan.targets:
        for detail in target.references:
            if detail._loader is None:
                continue
            for record, owners in _rows_owned(detail._loader(), uids).values():
                plans = dict.fromkeys(
                    plan for uid in sorted(owners) for plan in managing[uid]
                )
                held.append(
                    PlanReference(
                        reference=_reference(target, detail, record),
                        uids=frozenset(owners),
                        plans=tuple(plans),
                    )
                )
    return held


def execute_destroy(
    destroy_plan: DestroyPlan,
    *,
    permanent: bool = False,
    force: bool = False,
    trash_references: bool = False,
    track: Callable[[Sequence[Any], str], Iterable[Any]] | None = None,
) -> list[DestroyTarget]:
    """Delete every target, in reverse creation order.

    Deleting leaf objects before the types they depend on avoids deleting an
    object whose parent type already cascaded its removal.

    By default the targets are soft-deleted (moved to the instance's trash so
    the removal can be undone), saturating there: a target already in the trash
    is left alone rather than escalated to a permanent deletion (see
    :func:`~lamindb_setup.evolve.engine._trash.delete_object`).  Pass `permanent=True`
    to delete them irreversibly from the database.

    If external references remain, deletion is refused unless one of two escape
    hatches is chosen:

    * `force=True` *permanently* deletes every blocking referencing record
      (data records, their feature values, annotations, and any other registry
      rows) first so the targets can be removed.  This destroys data that cannot
      be recovered.
    * `trash_references=True` instead moves the blocking references that
      support trashing to the trash (recoverable) and leaves un-trashable link
      rows in place, then soft-deletes the targets.  The whole teardown stays
      reversible, so it cannot be combined with `permanent` or `force`.

    The whole teardown (the forced reference deletion plus every target
    deletion) runs inside a single database transaction, so a mid-destroy
    failure rolls every deletion back and leaves the instance untouched (atomic
    destroy), mirroring `apply`.  `track` is an optional progress callback.
    """
    from django.db import transaction

    if force and trash_references:
        raise ValueError(
            "force=True and trash_references=True are mutually exclusive: choose "
            "either permanently deleting the references or moving them to the trash."
        )
    if permanent and trash_references:
        raise ValueError(
            "permanent=True and trash_references=True are mutually exclusive: "
            "trashing references keeps the teardown recoverable, which is "
            "incompatible with a permanent deletion."
        )
    if not destroy_plan.is_safe and not force and not trash_references:
        raise RuntimeError(
            f"refusing to destroy: {destroy_plan.total_references} external "
            "reference(s) remain."
        )
    deleted: list[DestroyTarget] = []
    with transaction.atomic():
        if not destroy_plan.is_safe:
            if force:
                delete_references(destroy_plan)
            elif trash_references:
                _trash_blocking_records(destroy_plan)
        targets = list(reversed(destroy_plan.targets))
        items: Iterable[DestroyTarget] = (
            track(targets, "Destroying objects") if track is not None else targets
        )
        for target in items:
            delete_object(target.record, permanent=permanent)
            deleted.append(target)
    return deleted


def _iter_blocking_records(destroy_plan: DestroyPlan) -> Iterator[Any]:
    """Yield each unique record that blocks deletion of a target."""
    seen: set[tuple[str, Any]] = set()
    for target in destroy_plan.targets:
        for detail in target.references:
            for record in detail.records:
                marker = (type(record).__name__, record.pk)
                if record.pk is None or marker in seen:
                    continue
                seen.add(marker)
                yield record


def delete_references(destroy_plan: DestroyPlan) -> int:
    """Permanently delete every record that blocks deletion of a target.

    This is irreversible: the referencing records (data records, their stored
    feature values, ontology annotations, and any other referencing registry
    rows) are removed from the database and cannot be recovered.

    Records are deleted with a retry pass so that `PROTECT` dependencies
    between referencing records are resolved (e.g. a value link is removed
    before, or by cascade from, the record that owns it).  Returns the number
    of records that were deleted.
    """
    from django.db.models import ProtectedError

    records = list(_iter_blocking_records(destroy_plan))
    deleted = 0
    pending = records
    while pending:
        remaining: list[Any] = []
        for record in pending:
            if record.pk is None:
                # already removed, e.g. cascaded from a previous deletion.
                continue
            try:
                record.delete(permanent=True)
                deleted += 1
            except ProtectedError:
                remaining.append(record)
        if len(remaining) == len(pending):
            # No progress: a genuine PROTECT cycle remains. Surface it.
            remaining[0].delete(permanent=True)
        pending = remaining
    return deleted


def _trash_blocking_records(destroy_plan: DestroyPlan) -> tuple[int, int]:
    """Move the blocking references that support trashing to the trash.

    Full registry objects (`Record`, `Schema`, `Feature` ...) carry a
    `branch_id` and are soft-deleted (moved to `branch_id == -1`), so they
    are hidden but recoverable.  Link/through rows (lamindb `IsLink` rows such
    as `RecordJson`) have no trash state and cannot be soft-deleted; they are
    left in place, since once both of their endpoints are trashed they are hidden
    anyway.  Returns `(trashed, left)`: how many records were moved to the
    trash and how many un-trashable rows were left untouched.
    """
    trashed = 0
    left = 0
    for record in _iter_blocking_records(destroy_plan):
        if not hasattr(record, "branch_id"):
            # Link/through row without a trash state; leave it in place.
            left += 1
            continue
        try:
            delete_object(record)
        except ValueError:
            # Defensive: any record that refuses a soft delete is left in place.
            left += 1
            continue
        trashed += 1
    return trashed, left


def trash_references(destroy_plan: DestroyPlan) -> tuple[int, int]:
    """Move every trashable record that blocks a target to the trash.

    Recoverable counterpart to :func:`delete_references`: instead of permanently
    removing the blocking references, the ones that support trashing are moved to
    the instance's trash and the un-trashable link rows are left in place.
    Returns `(trashed, left)` (see :func:`_trash_blocking_records`).
    """
    return _trash_blocking_records(destroy_plan)


# === Rendering ================================================================


def _build_renderable(destroy_plan: DestroyPlan, title: str) -> Any:
    from rich.console import Group
    from rich.text import Text

    lines: list[Any] = []
    for target in destroy_plan.targets:
        blocked = target.reference_count > 0
        symbol = Text("  - ", style="bold red" if not blocked else "bold yellow")
        line = Text()
        line.append_text(symbol)
        line.append(target.kind, style="bold cyan")
        if target.name is not None:
            line.append(f" {target.name!r}", style="green")
        line.append("  ")
        line.append(target.uid, style="red" if not blocked else "yellow")
        lines.append(line)
        for detail in target.references:
            detail_line = Text("        ")
            detail_line.append(
                f"referenced by {detail.count} {detail.referencing_kind} "
                f"via {detail.relation}",
                style="yellow",
            )
            lines.append(detail_line)

    for missing in destroy_plan.missing:
        line = Text("    ", style="dim")
        line.append(missing.kind, style="bold cyan")
        if missing.name is not None:
            line.append(f" {missing.name!r}", style="green")
        line.append("  (not in instance)", style="dim")
        lines.append(line)

    summary = Text()
    summary.append("Destroy: ", style="bold")
    summary.append(f"{len(destroy_plan.targets)} to delete", style="red")
    summary.append(", ")
    summary.append(f"{len(destroy_plan.missing)} not present", style="dim")
    summary.append(", ")
    references = destroy_plan.total_references
    summary.append(
        f"{references} blocking reference(s)",
        style="bold yellow" if references else "dim",
    )

    header = Text(title, style="bold white")
    return Group(header, Text(), *lines, Text(), summary)


def render_destroy(
    destroy_plan: DestroyPlan, *, title: str, color: bool = False
) -> str:
    """Render the destroy plan as a terraform-like string."""
    from rich.console import Console

    buffer = io.StringIO()
    console = Console(
        width=None if color else 200,
        file=buffer,
        force_terminal=color or None,
        color_system="auto" if color else None,
    )
    console.print(_build_renderable(destroy_plan, title))
    return buffer.getvalue()


def print_destroy(destroy_plan: DestroyPlan, *, title: str) -> None:
    """Print the destroy plan to stdout with color."""
    from rich.console import Console

    Console().print(_build_renderable(destroy_plan, title))
