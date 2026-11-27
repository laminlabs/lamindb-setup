"""Full export-closure analysis for resolving missing exports (agent-focused).

:func:`run_missing_exports` computes, per instance and in a *single* introspection
pass, the full transitive closure of tracked objects that a plan must export so
every reconstructable untracked object can reference it -- the fixpoint that
`ci drift`'s (capped, one-layer) export suggestions only approach iteratively.

The trick: reconstruct the instance with *every* tracked object treated as an
available external reference (see :func:`..introspect._reverse.build_plan`), so no
object fails reconstruction for an out-of-scope tracked reference.  The tracked
uids that the surviving objects then depend on are exactly the closure;
subtracting the objects the plans already export yields the set still to export,
grouped by the plan that owns (constructs) each object.

The command is read-only: unlike `ci drift` it writes no plan or lock files.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Sequence
    from types import ModuleType

    from ..authoring import Plan
    from ..engine import LockFile
    from ..introspect import ExternalRef, ReconstructedPlan
    from ._config import CIConfig, InstanceConfig

__all__ = [
    "BlockedObject",
    "InstanceMissingExports",
    "MissingExport",
    "MissingExportsResult",
    "PlanExportGroup",
    "run_missing_exports",
]

# Kind ordering for stable, readable output within a plan group.
_KIND_RANK: dict[str, int] = {
    "FeatureType": 0,
    "Feature": 1,
    "SchemaType": 2,
    "Schema": 3,
    "RecordType": 4,
    "Sheet": 5,
}

# Marker in a cascade reconstruction error (an object dropped only because a
# dependency could not be reconstructed, not for its own reason).
_CASCADE_MARKER = "which could not be reconstructed"

# The reconstruction hook: `(instance, exclude_uids, external_refs) -> plan`.
Reconstruct = Callable[["InstanceConfig", "set[str]", "dict[str, Any]"], Any]


@dataclass
class MissingExport:
    """A tracked object a plan must export, with the info needed to locate it."""

    uid: str
    kind: str | None
    name: str | None
    natural_key: str
    """The lock's natural key (`Kind:Name/Kind:Name/...`) -- the type-path that
    disambiguates same-named objects when grepping for the source variable."""
    references: int
    """How many untracked objects reference it (a priority hint)."""


@dataclass
class PlanExportGroup:
    """The objects one plan must export, in stable render order."""

    plan_target: str
    plan_var: str
    objects: list[MissingExport] = field(default_factory=list)


@dataclass
class BlockedObject:
    """An untracked object exporting cannot make reconstructable."""

    message: str
    root_cause: bool
    """False when it is only a cascade of another object's failure."""


@dataclass
class InstanceMissingExports:
    """The missing-exports outcome for one instance."""

    slug: str
    groups: list[PlanExportGroup] = field(default_factory=list)
    blocked: list[BlockedObject] = field(default_factory=list)
    reconstructable: int = 0
    skipped_plans: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def to_export(self) -> int:
        """Total tracked objects still to export across all plans."""
        return sum(len(group.objects) for group in self.groups)

    @property
    def has_missing(self) -> bool:
        """Whether any object still needs exporting."""
        return self.to_export > 0

    @property
    def has_errors(self) -> bool:
        """Whether anything stopped this instance from being reconstructed.

        A failed reconstruction leaves `groups` empty, which reads exactly
        like an instance with nothing left to export -- the opposite of what
        happened.  `--fail-on-missing` cannot cover this, since the report
        found no missing exports precisely because it could not look.
        """
        return bool(self.errors)


@dataclass
class MissingExportsResult:
    """The missing-exports outcome across all selected instances."""

    instances: list[InstanceMissingExports] = field(default_factory=list)

    @property
    def has_missing(self) -> bool:
        """Whether any instance has objects still to export."""
        return any(instance.has_missing for instance in self.instances)

    @property
    def has_errors(self) -> bool:
        """Whether any instance could not be reconstructed."""
        return any(instance.has_errors for instance in self.instances)


def _default_reconstruct(
    instance: InstanceConfig,
    exclude_uids: set[str],
    external_refs: dict[str, Any],
) -> ReconstructedPlan:
    """Read the connected instance and reconstruct it with all tracked in scope."""
    from ..introspect import build_plan, read_instance

    contents = read_instance(
        space_uids=instance.spaces or None,
        exclude_space_uids=instance.exclude_spaces or None,
        exclude_uids=exclude_uids,
        exclude_child_record_types=True,
    )
    return build_plan(contents, external_refs)


def _placeholder_ref(uid: str, owner: tuple[str, str, Any, str] | None) -> ExternalRef:
    """Build an :class:`ExternalRef` that only needs to put `uid` in scope.

    `build_plan` uses external-ref *values* only when rendering imports (which
    this read-only command never does), so the module / field are best-effort and
    unused; the uid being a key is what makes the object in scope.
    """
    from ..introspect import ExternalRef

    module, plan_var = "", ""
    if owner is not None:
        target, plan_var, _entry, _key = owner
        module = Path(target.split(":", 1)[0]).stem
    return ExternalRef(
        uid=uid, module=module, plan_var=plan_var, exports_cls="", field=""
    )


def run_missing_exports(
    config: CIConfig,
    *,
    changed_files: list[str] | None = None,
    instances: Sequence[str] | None = None,
    load_plan: Callable[[str], tuple[Plan, Path, ModuleType]] | None = None,
    connect: Callable[[str], str] | None = None,
    read_lock: Callable[[Path, str], LockFile | None] | None = None,
    reconstruct: Reconstruct | None = None,
) -> MissingExportsResult:
    """Compute, per instance, the full closure of tracked objects still to export.

    `changed_files` restricts the run to the instances that declare one of those
    plan files, and `instances` to the configured instances with those slugs;
    given both, they intersect.  The `load_plan` / `connect` / `read_lock` /
    `reconstruct` hooks default to the real implementations and exist so tests
    can inject fakes.
    """
    from ..engine import LockFile, lock_path, safe_connect
    from ..engine import load_plan as default_load_plan
    from ._apply import _select_instances
    from ._config import CIConfig
    from ._drift import _construct_plans

    load_plan = load_plan or default_load_plan
    connect = connect or safe_connect
    reconstruct = reconstruct or _default_reconstruct
    if read_lock is None:

        def read_lock(plan_file: Path, slug: str) -> LockFile | None:
            return LockFile.read(lock_path(plan_file, slug))

    # `_select_instances` raises ConfigError for a changed plan not configured,
    # and InvalidArgumentError for an instance not configured.
    selected = _select_instances(config, changed_files, instances)

    # Construct every selected plan once, before connecting to any instance: the
    # plan-construction guard rejects building a plan after lamindb is imported
    # (which connecting does).  Reuses the drift helper over a sub-config.
    sub = CIConfig(
        instances=selected,
        config_path=config.config_path,
        ignored_plans=config.ignored_plans,
    )
    constructed, load_errors = _construct_plans(sub, load_plan)

    result = MissingExportsResult()
    for instance in selected:
        result.instances.append(
            _missing_for_instance(
                instance, constructed, load_errors, connect, read_lock, reconstruct
            )
        )
    return result


def _missing_for_instance(
    instance: InstanceConfig,
    constructed: dict[str, tuple[Plan, Path, ModuleType]],
    load_errors: dict[str, str],
    connect: Callable[[str], str],
    read_lock: Callable[[Path, str], LockFile | None],
    reconstruct: Reconstruct,
) -> InstanceMissingExports:
    from ._drift import _external_refs_for_plan

    inst = InstanceMissingExports(slug=instance.slug)
    try:
        slug = connect(instance.slug)
    except Exception as exc:
        inst.errors.append(f"connection failed: {exc}")
        return inst

    tracked_uids: set[str] = set()
    already_exported: set[str] = set()
    # uid -> (plan_target, plan_var, lock entry, natural key)
    owners: dict[str, tuple[str, str, Any, str]] = {}
    for ref in instance.plans:
        if ref.target in load_errors:
            inst.errors.append(f"{ref.target}: {load_errors[ref.target]}")
            continue
        plan_obj, plan_file, module = constructed[ref.target]
        lock = read_lock(plan_file, slug)
        if lock is None:
            inst.skipped_plans.append(ref.target)
            continue
        plan_var = ref.var or plan_obj.name
        plan_tracked, plan_refs = _external_refs_for_plan(
            plan_obj, module, plan_var, lock
        )
        tracked_uids |= plan_tracked
        already_exported |= set(plan_refs)
        for key, entry in lock.objects.items():
            uid = getattr(entry, "uid", None)
            if uid is not None and uid not in owners:
                owners[uid] = (ref.target, plan_var, entry, key)

    # Reconstruct with every tracked object treated as in scope, so nothing fails
    # for an out-of-scope tracked reference; the tracked deps of the survivors are
    # the full closure (the fixpoint drift only reaches by iterating).
    external_refs = {
        uid: _placeholder_ref(uid, owners.get(uid)) for uid in tracked_uids
    }
    try:
        plan = reconstruct(instance, tracked_uids, external_refs)
    except Exception as exc:
        inst.errors.append(f"reconstruction failed: {exc}")
        return inst

    inst.reconstructable = len(plan.specs)

    # Closure + reference counts: how many surviving untracked objects depend on
    # each tracked object.
    counts: Counter[str] = Counter()
    for spec in plan.specs:
        for dep in spec.deps:
            if dep in tracked_uids:
                counts[dep] += 1
    to_export = set(counts) - already_exported

    groups: dict[tuple[str, str], PlanExportGroup] = {}
    unowned = 0
    for uid in to_export:
        owner = owners.get(uid)
        if owner is None:
            unowned += 1
            continue
        target, plan_var, entry, key = owner
        group = groups.get((target, plan_var))
        if group is None:
            group = PlanExportGroup(plan_target=target, plan_var=plan_var)
            groups[(target, plan_var)] = group
        group.objects.append(
            MissingExport(
                uid=uid,
                kind=getattr(entry, "kind", None),
                name=getattr(entry, "name", None),
                natural_key=key,
                references=counts[uid],
            )
        )
    for group in groups.values():
        group.objects.sort(
            key=lambda obj: (
                _KIND_RANK.get(obj.kind or "", 99),
                obj.name or "",
                obj.uid,
            )
        )
    inst.groups = [groups[key] for key in sorted(groups)]
    if unowned:
        inst.errors.append(
            f"{unowned} referenced tracked object(s) are owned by no configured "
            "plan lock and cannot be attributed"
        )

    inst.blocked = [
        BlockedObject(message=message, root_cause=_CASCADE_MARKER not in message)
        for message in plan.errors
    ]
    return inst
