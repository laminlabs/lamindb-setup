"""Scheduled drift detection across the configured instances.

For each instance, :func:`run_drift_detection`:

1. collects the uids every configured plan already manages from their per-instance
   lock files (warning about and skipping any plan with no lock for the instance);
2. writes `plan_drift_<slug>.py` — a plan of the objects in the instance that no
   configured plan tracks, referencing already-tracked objects through the owning
   plan's exports where possible; and
3. diffs each configured plan against the instance to surface tracked objects that
   were modified or deleted directly in the instance.

It is meant to run on a schedule so changes made outside the plans are noticed.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from types import ModuleType

    from ..authoring import Plan
    from ..engine import LockFile
    from ..introspect import ExternalRef, IntrospectionResult
    from ..render import SummaryModel
    from ._config import CIConfig, InstanceConfig
    from ._pr import DeploymentDiff

__all__ = [
    "DriftResult",
    "ExportSuggestion",
    "ExportSuggestionObject",
    "InstanceDriftResult",
    "run_drift_detection",
]


@dataclass
class ExportSuggestionObject:
    """A tracked object an untracked object references but cannot reach.

    The object is already managed by a plan's lock but is not among that plan's
    exports, so a drift plan cannot reference it.  Exporting it from the owning
    plan brings it into scope.
    """

    uid: str
    kind: str | None
    name: str | None
    references: int
    """How many untracked objects directly referenced this object."""


@dataclass
class ExportSuggestion:
    """A suggestion to export some objects from one plan to resolve references."""

    plan_target: str
    plan_var: str
    objects: list[ExportSuggestionObject] = field(default_factory=list)


@dataclass
class InstanceDriftResult:
    """The drift outcome for one instance."""

    slug: str
    output: Path | None = None
    untracked_objects: int = 0
    summary: SummaryModel | None = None
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    skipped_plans: list[str] = field(default_factory=list)
    diffs: list[DeploymentDiff] = field(default_factory=list)
    export_suggestions: list[ExportSuggestion] = field(default_factory=list)

    @property
    def modified_or_deleted(self) -> int:
        """Count tracked objects that drifted (update/delete) in the instance."""
        return sum(
            1
            for diff in self.diffs
            for change in diff.changes
            if change.action in ("update", "delete")
        )

    @property
    def has_drift(self) -> bool:
        """Whether the instance has untracked objects or drifted tracked objects."""
        return self.untracked_objects > 0 or self.modified_or_deleted > 0

    @property
    def has_errors(self) -> bool:
        """Whether anything stopped this instance from being checked at all.

        Distinct from :attr:`has_drift`, and the reason the two cannot share an
        exit code: a failed introspection or diff leaves `untracked_objects`
        and `diffs` at their zero defaults, which reads exactly like a clean
        instance.  A drift check that cannot see an instance has not found "no
        drift", it has found nothing, and saying so is the whole point of
        running it on a schedule.
        """
        return bool(self.errors) or any(not diff.ok for diff in self.diffs)

    @property
    def has_export_suggestions(self) -> bool:
        """Whether any tracked-but-unexported object should be exported."""
        return bool(self.export_suggestions)


@dataclass
class DriftResult:
    """The drift outcome across all configured instances."""

    instances: list[InstanceDriftResult] = field(default_factory=list)

    @property
    def has_drift(self) -> bool:
        """Whether any instance shows drift."""
        return any(i.has_drift for i in self.instances)

    @property
    def has_errors(self) -> bool:
        """Whether any instance could not be checked."""
        return any(i.has_errors for i in self.instances)

    @property
    def has_export_suggestions(self) -> bool:
        """Whether any instance has tracked objects that should be exported."""
        return any(i.has_export_suggestions for i in self.instances)


def _external_refs_for_plan(
    plan_obj: Plan, plan_file: Path, plan_var: str, lock: LockFile
) -> tuple[set[str], dict[str, ExternalRef]]:
    """Return `(tracked_uids, external_refs)` for one applied plan.

    `tracked_uids` is every uid recorded in the lock; `external_refs` maps the
    uids of *exported* objects to an :class:`ExternalRef` so a drift plan can
    reference them through this plan's exports.
    """
    from typing import get_type_hints

    from ..engine import natural_key
    from ..introspect import ExternalRef

    tracked_uids = {e.uid for e in lock.objects.values() if e.uid is not None}

    refs: dict[str, ExternalRef] = {}
    plan_obj.finalize()
    exports = plan_obj._exports  # populated by finalize()
    if exports is None:
        return tracked_uids, refs
    exports_cls = type(exports)
    if exports_cls.__name__ == "PlanExports":
        return tracked_uids, refs
    module = plan_file.stem
    for field_name in get_type_hints(exports_cls):
        obj = getattr(exports, field_name, None)
        if obj is None:
            continue
        uid = lock.uid_for(natural_key(obj))
        if uid is None:
            continue
        refs[uid] = ExternalRef(
            uid=uid,
            module=module,
            plan_var=plan_var,
            exports_cls=exports_cls.__name__,
            field=field_name,
        )
    return tracked_uids, refs


def run_drift_detection(
    config: CIConfig,
    output_dir: str | Path = ".",
    *,
    instances: Sequence[str] | None = None,
    report: Callable[[str], None] | None = None,
    load_plan: Callable[[str], tuple[Plan, Path, ModuleType]] | None = None,
    connect: Callable[[str], str] | None = None,
    introspect: Callable[..., IntrospectionResult] | None = None,
    compute: Callable[[Plan, Path, str], list] | None = None,
    read_lock: Callable[[Path, str], LockFile | None] | None = None,
) -> DriftResult:
    """Detect drift in every configured instance, writing an untracked-plan file.

    `instances` optionally restricts the run to the configured instances with
    those slugs: only their plans are loaded and only they are connected to.  A
    slug the configuration does not list raises
    :class:`~lamindb_setup.evolve.core.InvalidArgumentError` before anything is
    loaded or written.  `output_dir` is created when missing.

    The `load_plan`/`connect`/`introspect`/`compute`/`read_lock` hooks
    default to the real implementations and exist so tests can inject fakes.

    `report` optionally receives a milestone line per stage, so a scheduled run
    that spends minutes introspecting an instance says which instance and which
    stage it is on.
    """
    from ..engine import LockFile, lock_path, safe_connect
    from ..engine import load_plan as default_load_plan
    from ..introspect import introspect_instance
    from ._pr import _default_compute

    def _report(message: str) -> None:
        if report is not None:
            report(message)

    load_plan = load_plan or default_load_plan
    connect = connect or safe_connect
    introspect = introspect or introspect_instance
    compute = compute or _default_compute
    if read_lock is None:

        def read_lock(plan_file: Path, slug: str) -> LockFile | None:
            return LockFile.read(lock_path(plan_file, slug))

    config = config.restricted_to(instances)
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    result = DriftResult()

    # Construct every configured plan across all instances *before* connecting to
    # any instance.  Connecting imports lamindb, after which the plan-construction
    # guard refuses to build any further plan ("lamindb has already been
    # imported"); constructing all plans up front keeps each construction
    # lamindb-free.  Plans shared by several instances are constructed once.
    constructed, load_errors = _construct_plans(config, load_plan, _report)

    for instance in config.instances:
        result.instances.append(
            _drift_for_instance(
                instance,
                out_dir,
                constructed,
                load_errors,
                connect,
                introspect,
                compute,
                read_lock,
                _report,
            )
        )
    return result


def _construct_plans(
    config: CIConfig,
    load_plan: Callable[[str], tuple[Plan, Path, ModuleType]],
    report: Callable[[str], None] | None = None,
) -> tuple[dict[str, tuple[Plan, Path]], dict[str, str]]:
    """Construct every configured plan once, before any instance is connected.

    Returns `(constructed, errors)` keyed by plan target: `constructed` maps a
    target to its `(plan, path)` and `errors` maps a target to the message of
    a load failure.  Constructing all plans up front -- while lamindb is not yet
    imported -- is required because connecting to an instance imports lamindb,
    after which the plan-construction guard rejects any further plan.
    """
    constructed: dict[str, tuple[Plan, Path]] = {}
    errors: dict[str, str] = {}
    for instance in config.instances:
        for ref in instance.plans:
            if ref.target in constructed or ref.target in errors:
                continue
            if report is not None:
                report(f"Loading {ref.target}")
            try:
                plan_obj, plan_file, _module = load_plan(ref.load_target)
            except Exception as exc:
                errors[ref.target] = str(exc)
                continue
            constructed[ref.target] = (plan_obj, plan_file)
    return constructed, errors


def _drift_for_instance(
    instance: InstanceConfig,
    out_dir: Path,
    constructed: dict[str, tuple[Plan, Path]],
    load_errors: dict[str, str],
    connect: Callable[[str], str],
    introspect: Callable[..., IntrospectionResult],
    compute: Callable[[Plan, Path, str], list],
    read_lock: Callable[[Path, str], LockFile | None],
    report: Callable[[str], None],
) -> InstanceDriftResult:
    drift = InstanceDriftResult(slug=instance.slug)
    report(f"Connecting to {instance.slug}")
    try:
        slug = connect(instance.slug)
    except Exception as exc:
        # Recorded like every other per-instance failure rather than raised: a
        # scheduled run covers many instances, and one that is down or whose
        # token expired must not take the report for the others with it.
        drift.errors.append(f"connection failed: {exc}")
        return drift

    tracked_uids: set[str] = set()
    external_refs: dict[str, ExternalRef] = {}
    # Every tracked uid mapped to the plan that owns it (target, plan_var, lock
    # entry), so a reference to a tracked-but-unexported object can be turned
    # into a suggestion to export it from that plan.
    tracked_owners: dict[str, tuple[str, str, Any]] = {}
    loaded: list[tuple[str, Plan, Path]] = []
    for ref in instance.plans:
        if ref.target in load_errors:
            drift.errors.append(f"{ref.target}: {load_errors[ref.target]}")
            continue
        plan_obj, plan_file = constructed[ref.target]
        lock = read_lock(plan_file, slug)
        if lock is None:
            drift.skipped_plans.append(ref.target)
            continue
        loaded.append((ref.target, plan_obj, plan_file))
        plan_var = ref.var or plan_obj.name
        plan_tracked, plan_refs = _external_refs_for_plan(
            plan_obj, plan_file, plan_var, lock
        )
        tracked_uids |= plan_tracked
        external_refs.update(plan_refs)
        for entry in lock.objects.values():
            uid = getattr(entry, "uid", None)
            if uid is not None and uid not in tracked_owners:
                tracked_owners[uid] = (ref.target, plan_var, entry)

    # 1. Write the plan of objects no configured plan tracks.
    output = out_dir / f"plan_drift_{slug}.py"
    report(f"Introspecting {slug} for untracked objects")
    try:
        intro = introspect(
            instance.slug,
            output,
            exclude_uids=tracked_uids,
            external_refs=external_refs,
            space_uids=instance.spaces or None,
            exclude_space_uids=instance.exclude_spaces or None,
            overwrite=True,
            verify=False,
        )
        drift.output = intro.output
        drift.untracked_objects = intro.reconstructed
        drift.summary = getattr(intro, "summary", None)
        drift.errors.extend(intro.errors)
        drift.warnings.extend(getattr(intro, "warnings", None) or [])
        _add_export_suggestions(
            drift, getattr(intro, "out_of_scope_uids", None) or {}, tracked_owners
        )
    except Exception as exc:
        drift.errors.append(f"introspection failed: {exc}")

    # 2. Diff each tracked plan to surface modified / deleted tracked objects.
    from ._pr import DeploymentDiff

    for index, (target, plan_obj, plan_file) in enumerate(loaded, start=1):
        report(f"Diffing {target} against {slug} ({index}/{len(loaded)})")
        try:
            changes = compute(plan_obj, plan_file, slug)
            drift.diffs.append(
                DeploymentDiff(
                    instance_slug=instance.slug,
                    plan_target=target,
                    ok=True,
                    changes=changes,
                )
            )
        except Exception as exc:
            drift.diffs.append(
                DeploymentDiff(
                    instance_slug=instance.slug,
                    plan_target=target,
                    ok=False,
                    error=str(exc),
                )
            )
    return drift


def _add_export_suggestions(
    drift: InstanceDriftResult,
    out_of_scope_uids: dict[str, int],
    tracked_owners: dict[str, tuple[str, str, Any]],
) -> None:
    """Turn out-of-scope references at *tracked* objects into export suggestions.

    `out_of_scope_uids` maps each out-of-scope reference target to how many
    untracked objects referenced it.  A target that is already tracked by a plan
    (present in `tracked_owners`) but not exported by it is recorded as a
    suggestion to export it from the owning plan, grouped per plan.
    """
    by_plan: dict[tuple[str, str], list[ExportSuggestionObject]] = {}
    for uid, references in out_of_scope_uids.items():
        owner = tracked_owners.get(uid)
        if owner is None:
            continue
        target, plan_var, entry = owner
        by_plan.setdefault((target, plan_var), []).append(
            ExportSuggestionObject(
                uid=uid,
                kind=getattr(entry, "kind", None),
                name=getattr(entry, "name", None),
                references=references,
            )
        )
    for (target, plan_var), objects in sorted(by_plan.items()):
        objects.sort(key=lambda obj: (obj.kind or "", obj.name or "", obj.uid))
        drift.export_suggestions.append(
            ExportSuggestion(plan_target=target, plan_var=plan_var, objects=objects)
        )
