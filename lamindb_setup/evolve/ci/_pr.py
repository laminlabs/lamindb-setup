"""Run the pull-request check: validate plans, then diff them against instances.

For a pull request that adds or edits a plan, :func:`run_pr_check` finalizes each
selected plan offline (`validate`) and, for every instance the plan is deployed
to in the configuration, computes a read-only diff (`plan`) -- or, with
`preview_migrations`, rehearses the apply in a transaction that is rolled back
(`plan --preview-migrations`).  The result is a :class:`PRReport` that
:mod:`._markdown` renders for posting back onto the PR.
"""

from __future__ import annotations

from contextlib import nullcontext
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ..core import (
    MissingPrerequisite,
    WarningCategory,
    warning_category,
)
from ._apply import _refs_dependencies_first

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence
    from contextlib import AbstractContextManager
    from types import ModuleType

    from ..authoring import Plan
    from ..engine import ExistenceCache
    from ._config import CIConfig, PlanRef

__all__ = [
    "ChangeRecord",
    "DeploymentDiff",
    "PRReport",
    "PlanValidation",
    "run_pr_check",
]


@dataclass
class ChangeRecord:
    """A single computed change, decoupled from rich rendering.

    `warnings` are the change's
    :class:`~lamindb_setup.evolve.core.ChangeWarning` strings, each keeping its
    category.  `uid_misalignment` describes how the object's uid disagrees
    with the plan's lock files for its other instances, when it does (see
    :attr:`~lamindb_setup.evolve.core.ChangeItem.uid_misalignment`).
    `suspected_rename_of` names the deleted object a create looks like an
    undeclared rename of (see
    :attr:`~lamindb_setup.evolve.core.ChangeItem.suspected_rename_of`).
    `borrowed` marks an object borrowed from a prerequisite plan, whose
    creation is that plan's change rather than this one's (see
    :attr:`~lamindb_setup.evolve.core.ChangeItem.borrowed`).
    `missing_prerequisite` is the borrowed object as the diff recorded it
    missing from the instance, naming the prerequisite plan that declares it
    (see :attr:`~lamindb_setup.evolve.engine._apply.ApplyContext.missing_prerequisites`),
    so the refusal `apply` gives can be quoted.
    """

    action: str
    kind: str
    name: str | None
    uid: str
    details: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    uid_misalignment: str | None = None
    suspected_rename_of: str | None = None
    borrowed: bool = False
    missing_prerequisite: MissingPrerequisite | None = None

    @property
    def is_borrowed_create(self) -> bool:
        """Whether a prerequisite plan has yet to create this borrowed object."""
        return self.borrowed and self.action == "create"

    def as_missing_prerequisite(self) -> MissingPrerequisite:
        """This borrowed object as a missing-prerequisite refusal names it.

        A record built without the diff's own (an injected diff) is described
        by its kind and name, under an unknown prerequisite plan.
        """
        if self.missing_prerequisite is not None:
            return self.missing_prerequisite
        return MissingPrerequisite(
            kind=self.kind, name=self.name, key=self.name or self.uid
        )


@dataclass
class PlanValidation:
    """The offline validation outcome for one plan."""

    target: str
    ok: bool
    error: str | None = None


@dataclass
class DeploymentDiff:
    """The diff of one plan against one instance.

    A diff that ran but that `apply` would refuse is not `ok` either: its
    `error` is the refusal, and its `changes` are still the diff refused.
    """

    instance_slug: str
    plan_target: str
    ok: bool
    error: str | None = None
    changes: list[ChangeRecord] = field(default_factory=list)
    lock_missing: bool = False

    @property
    def actionable(self) -> list[ChangeRecord]:
        """Return the plan's own create / update / delete / move / transfer changes.

        Excludes the unchanged objects and the borrowed objects a prerequisite
        plan has yet to create (see :attr:`borrowed`): creating them is that
        plan's change.
        """
        return [c for c in self.changes if c.action != "no-change" and not c.borrowed]

    @property
    def borrowed(self) -> list[ChangeRecord]:
        """Return the borrowed objects a prerequisite plan has yet to create."""
        return [c for c in self.changes if c.is_borrowed_create]

    @property
    def missing_prerequisites(self) -> list[ChangeRecord]:
        """Return the borrowed objects no plan applied with this one creates.

        Their prerequisite plan is neither applied to the instance nor
        configured for it, so `apply` refuses the plan until it is (see
        :class:`~lamindb_setup.evolve.core.WarningCategory`).
        """
        return [
            c
            for c in self.borrowed
            if any(
                warning_category(w) is WarningCategory.PREREQUISITE for w in c.warnings
            )
        ]

    @property
    def misaligned(self) -> list[ChangeRecord]:
        """Return the changes whose uid disagrees with the plan's other instances."""
        return [c for c in self.changes if c.uid_misalignment is not None]

    @property
    def suspected_renames(self) -> list[tuple[str, str, str]]:
        """Return `(kind, old_name, new_name)` for each undeclared rename."""
        return [
            (c.kind, c.suspected_rename_of, c.name or "")
            for c in self.changes
            if c.suspected_rename_of is not None
        ]


@dataclass
class PRReport:
    """The full pull-request check result: validations and per-instance diffs.

    `strict_uids` is whether `apply` would refuse a misaligned uid -- the
    `--strict-uids` flag ANDed with the configuration's `strict_uids` --
    which decides whether a misalignment is pending or only reported.
    `preview_migrations` is whether the diffs were rehearsed, so the
    migration outcomes under them are what `ci apply` would do to the data.
    """

    validations: list[PlanValidation] = field(default_factory=list)
    diffs: list[DeploymentDiff] = field(default_factory=list)
    strict_uids: bool = True
    preview_migrations: bool = False

    @property
    def has_errors(self) -> bool:
        """Whether any plan failed to validate or any diff failed to run."""
        if any(not v.ok for v in self.validations):
            return True
        return any(not d.ok for d in self.diffs)

    @property
    def has_changes(self) -> bool:
        """Whether any instance would see changes, or has uids apply would refuse.

        A misaligned uid counts while `strict_uids` is on: `apply` refuses
        it, so a pull request that leaves one in place would not apply cleanly.
        With it off, `apply` goes ahead, so the misalignment is reported but
        is nothing to apply.  A borrowed object counts only while no plan
        configured for the instance creates it, which `apply` refuses too.
        """
        return any(
            d.actionable
            or d.missing_prerequisites
            or (self.strict_uids and d.misaligned)
            for d in self.diffs
        )


def _resolve_changed(changed_files: list[str] | None) -> set[Path] | None:
    if changed_files is None:
        return None
    resolved: set[Path] = set()
    for raw in changed_files:
        path = Path(raw)
        resolved.add(
            path.resolve() if path.is_absolute() else (Path.cwd() / path).resolve()
        )
    return resolved


def run_pr_check(
    config: CIConfig,
    changed_files: list[str] | None = None,
    *,
    instances: Sequence[str] | None = None,
    report: Callable[[str], None] | None = None,
    track: Callable[[Sequence[Any], str], Iterable[Any]] | None = None,
    load_plan: Callable[[str], tuple[Plan, Path, ModuleType]] | None = None,
    connect: Callable[[str], str] | None = None,
    compute: Callable[..., list[ChangeRecord]] | None = None,
    keys_of: Callable[[Plan], set[str]] | None = None,
    lock_exists: Callable[[Path, str], bool] | None = None,
    rehearsal: Callable[[], AbstractContextManager[Any]] | None = None,
    strict_uids: bool = True,
    strict_renames: bool = True,
    allow_data_validated_changes: bool = False,
    preview_migrations: bool = False,
) -> PRReport:
    """Validate the configured plans and diff them against their instances.

    `changed_files` optionally restricts which plans are *reported* to those
    whose file is among the given paths (provided by the CI workflow); when
    `None` every configured plan is reported.  Regardless of the filter, all
    configured plans are finalized so an unchanged plan still contributes the
    natural keys it "claims": an object a changed plan drops but a sibling still
    declares is treated as moved, not deleted.  The `load_plan`/`connect`/
    `compute`/`keys_of`/`lock_exists`/`rehearsal` hooks default to the
    CLI helpers and exist so tests can inject fakes.

    `instances` optionally restricts the check to the configured instances
    with those slugs, as if the configuration listed only them: only the plans
    deployed to one of them are loaded, validated and diffed, and only they are
    connected to.  Nothing a selected instance's diff depends on is lost, since
    a plan's siblings are the plans configured for the same instance and its
    other instances' uids come from the lock files.  With `changed_files` the
    two intersect: a changed plan deployed to no selected instance is neither
    validated nor diffed.  A slug the configuration does not list raises
    :class:`~lamindb_setup.evolve.core.InvalidArgumentError`.

    `strict_uids` is ANDed with the configuration's `strict_uids`, exactly
    as `ci apply` does, so the check judges a misaligned uid the way the apply
    it previews will.

    `strict_renames` records a diff `ci apply` would refuse -- a create that
    looks like a rename not declared with `ctx.moved` -- as a failed diff
    carrying the refusal, so the check fails exactly when the apply would.  Off,
    such a diff only carries its warning, as `ci apply --no-strict-renames`
    applies it.  It is ANDed with the configuration's `strict_renames` like
    `strict_uids`.

    `allow_data_validated_changes` lifts a blocked validation-affecting change
    the live data proves safe, as `ci apply --allow-data-validated-changes`
    applies it.  It is ORed with the configuration's
    `allow_data_validated_changes`, again as `ci apply` does.

    `preview_migrations`, ORed with the configuration's key of the same name,
    rehearses each instance's diffs as `plan --preview-migrations` does: all
    of the instance's configured plans -- unchanged ones too, as `ci apply`
    applies them all -- are applied dependencies-first in one
    :func:`~lamindb_setup.evolve.engine.rehearsal`, running every declared
    migration and its checks, and rolled back together.  A plan's diff then
    carries its migrations' outcomes, and one that raises is recorded as that
    plan's failed diff -- an unchanged plan's too, since `ci apply` would
    fail on it.  `ci apply` stops an instance at the first plan it fails on
    or refuses (`strict_renames`), so the rehearsal does as well: each
    changed plan after it is recorded as not rehearsed.  The rehearsal attempts
    writes, so the account needs write access; nothing is persisted, no lock
    file is written and no run is tracked.

    `report` and `track` are the optional progress callbacks of
    :func:`~lamindb_setup.evolve.console.progress_reporter`: without them the
    check runs silently for minutes over a large configuration, with no way to
    tell loading from validating from waiting on an instance.  They are off by
    default so the function stays quiet when called as a library.
    """
    from ..engine import (
        LockFile,
        co_applied_prerequisite_keys,
        lock_path,
        plan_natural_keys,
        prefetch_plans,
        read_sibling_uids,
        safe_connect,
    )
    from ..engine import load_plan as default_load_plan
    from ..engine import rehearsal as default_rehearsal
    from ..render import (
        missing_prerequisite_message,
        unmoved_rename_refusal,
    )

    def _report(message: str) -> None:
        if report is not None:
            report(message)

    def _track(items: Sequence[Any], description: str) -> Iterable[Any]:
        return items if track is None else track(items, description)

    load_plan = load_plan or default_load_plan
    connect = connect or safe_connect
    rehearse = rehearsal or default_rehearsal
    # Whether the per-instance bulk prefetch below runs.  It fills the caches
    # `_default_compute` reads, so it is only worth anything when the default
    # is what actually runs; an injected `compute` is left entirely alone.
    share_reads = compute is None
    compute = compute or _default_compute
    keys_of = keys_of or plan_natural_keys
    if lock_exists is None:

        def lock_exists(plan_file: Path, slug: str) -> bool:
            return LockFile.read(lock_path(plan_file, slug)) is not None

    config = config.restricted_to(instances)
    changed = _resolve_changed(changed_files)
    preview_migrations = preview_migrations or config.preview_migrations
    pr_report = PRReport(
        strict_uids=strict_uids and config.strict_uids,
        preview_migrations=preview_migrations,
    )
    strict_renames = strict_renames and config.strict_renames
    allow_data_validated_changes = (
        allow_data_validated_changes or config.allow_data_validated_changes
    )

    # Construct every configured plan *before* finalizing any of them.  Finalizing
    # a plan runs its body, which imports lamindb; the plan-construction guard
    # then refuses to build any further plan ("lamindb has already been
    # imported").  Loading all plans up front keeps each construction
    # lamindb-free; only then is it safe to finalize them one after another.  All
    # plans are loaded -- not only the changed ones -- because an unchanged plan
    # still claims the keys of objects a changed sibling drops.
    constructed: list[tuple[str, bool, tuple[Plan, Path] | str]] = []
    targets = list(config.plan_targets())
    for target in _track(targets, f"Loading {len(targets)} plan(s)"):
        ref = _find_ref(config, target)
        selected = changed is None or ref.path.resolve() in changed
        try:
            plan_obj, plan_file, _module = load_plan(ref.load_target)
        except Exception as exc:
            constructed.append((target, selected, str(exc)))
            continue
        constructed.append((target, selected, (plan_obj, plan_file)))

    # Finalize the constructed plans.  Record a validation only for selected
    # (changed) plans; unchanged siblings are finalized solely so they can
    # contribute their claimed keys to the diffs below.
    loaded: dict[str, tuple[Plan, Path] | None] = {}
    plan_errors: dict[str, str] = {}
    selected_targets: set[str] = set()
    for target, selected, result in _track(
        constructed, f"Validating {len(constructed)} plan(s)"
    ):
        if selected:
            selected_targets.add(target)
        if isinstance(result, str):
            plan_errors[target] = result
            if selected:
                pr_report.validations.append(
                    PlanValidation(target=target, ok=False, error=result)
                )
            loaded[target] = None
            continue
        plan_obj, plan_file = result
        try:
            plan_obj.finalize()
        except Exception as exc:
            plan_errors[target] = str(exc)
            if selected:
                pr_report.validations.append(
                    PlanValidation(target=target, ok=False, error=str(exc))
                )
            loaded[target] = None
            continue
        if selected:
            pr_report.validations.append(PlanValidation(target=target, ok=True))
        loaded[target] = (plan_obj, plan_file)

    # Diff each selected, validated plan against every instance it is deployed
    # to.  The other configured plans on that instance are siblings whose
    # declared keys must not be deleted, so each plan is diffed with the union of
    # its siblings' keys as `claimed_keys`.  `ci apply` applies them all,
    # dependencies-first, so what a configured prerequisite declares is created
    # before the plans borrowing it: their `co_applied_keys`.  A rehearsal
    # applies them all the same way, so the prerequisite really creates it first.
    for instance in config.instances:
        inst_targets = [p.target for p in instance.plans if p.target in loaded]
        diff_targets = [
            t for t in inst_targets if t in selected_targets and loaded[t] is not None
        ]
        if not diff_targets:
            continue
        invalid_siblings = [t for t in inst_targets if t in plan_errors]
        if invalid_siblings:
            failures = "; ".join(f"{t}: {plan_errors[t]}" for t in invalid_siblings)
            for target in diff_targets:
                pr_report.diffs.append(
                    DeploymentDiff(
                        instance_slug=instance.slug,
                        plan_target=target,
                        ok=False,
                        error=(
                            "cannot compute a safe diff because configured sibling "
                            f"plan validation failed: {failures}"
                        ),
                    )
                )
            continue
        keys_by_target: dict[str, set[str]] = {}
        for t in inst_targets:
            entry = loaded[t]
            if entry is not None:
                keys_by_target[t] = keys_of(entry[0])
        inst_plans = {t: entry[0] for t in inst_targets if (entry := loaded[t])}
        co_applied = (
            {}
            if preview_migrations
            else dict(
                zip(
                    inst_plans,
                    co_applied_prerequisite_keys(list(inst_plans.values())),
                    strict=False,
                )
            )
        )
        _report(f"Connecting to {instance.slug}")
        try:
            slug = connect(instance.slug)
        except Exception as exc:
            # Attributed to every plan deployed here, so the report says which
            # diffs did not run rather than silently omitting them -- and the
            # remaining instances are still checked.
            for target in diff_targets:
                pr_report.diffs.append(
                    DeploymentDiff(
                        instance_slug=instance.slug,
                        plan_target=target,
                        ok=False,
                        error=f"connection failed: {exc}",
                    )
                )
            continue
        diff = compute
        order = diff_targets
        stage = f"Diffing plans against {slug}"
        if preview_migrations:
            # Switching instances rebuilds LaminDB's model classes, and a plan
            # finalized before the switch holds the old ones, which applying it
            # -- as a rehearsal does -- trips over (see `ci apply`).
            for plan_obj in inst_plans.values():
                plan_obj.reset()
            order = [
                ref.target
                for ref in _refs_dependencies_first(
                    [ref for ref in instance.plans if ref.target in inst_plans],
                    {t: (plan_obj,) for t, plan_obj in inst_plans.items()},
                )
            ]
            stage = f"Rehearsing plans on {slug}"
            if compute is _default_compute:
                diff = partial(
                    _default_compute,
                    preview_migrations=True,
                    strict_uids=pr_report.strict_uids,
                )
        elif share_reads:
            # Every plan on this instance is about to be diffed against the
            # same live state, but each builds its own apply context and so
            # repeats the existence prefetch and the schema-member read.  Doing
            # them once for the whole instance turns O(plans) batched queries
            # into a fixed handful -- safe here because the diff only reads.
            from ..engine import ExistenceCache

            existing_cache = ExistenceCache()
            members_cache: dict[int, list[Any]] = {}
            # The sheet-export index does not depend on the plan, so every plan
            # on this instance shares one read of it.
            exports_cache: dict[str, Any] = {}
            _report(f"Reading {slug} state for {len(diff_targets)} plan(s)")
            entries = [entry for t in diff_targets if (entry := loaded[t]) is not None]
            prefetch_plans(
                [
                    (plan_obj, LockFile.read(lock_path(plan_file, slug)))
                    for plan_obj, plan_file in entries
                ],
                existing_cache=existing_cache,
                members_cache=members_cache,
                sibling_uids=[
                    read_sibling_uids(plan_file, plan_obj, slug)
                    for plan_obj, plan_file in entries
                ],
            )
            diff = partial(
                _default_compute,
                existing_cache=existing_cache,
                members_cache=members_cache,
                exports_cache=exports_cache,
            )
        results: dict[str, DeploymentDiff] = {}
        # The plan the rehearsal failed on: `ci apply` stops the instance there.
        failed: str | None = None
        with rehearse() if preview_migrations else nullcontext():
            for target in _track(order, stage):
                entry = loaded[target]
                if entry is None:
                    continue
                reported = target in diff_targets
                if failed is not None:
                    if reported:
                        results[target] = DeploymentDiff(
                            instance_slug=instance.slug,
                            plan_target=target,
                            ok=False,
                            error=(
                                f"not rehearsed: {failed} failed before it, and "
                                "ci apply stops the instance there too"
                            ),
                        )
                    continue
                plan_obj, plan_file = entry
                claimed: set[str] = set()
                claimed_by: dict[str, str] = {}
                for other, other_keys in keys_by_target.items():
                    if other != target:
                        claimed |= other_keys
                        for key in other_keys:
                            claimed_by.setdefault(key, other)
                try:
                    changes = diff(
                        plan_obj,
                        plan_file,
                        slug,
                        claimed,
                        claimed_by,
                        allow_data_validated_changes=allow_data_validated_changes,
                        co_applied_keys=co_applied.get(target, set()),
                    )
                    if not reported:
                        # An unchanged plan, rehearsed only because ci apply
                        # applies it too -- and the plans after it see it.
                        continue
                    deployment = DeploymentDiff(
                        instance_slug=instance.slug,
                        plan_target=target,
                        ok=True,
                        changes=changes,
                        lock_missing=not lock_exists(plan_file, slug),
                    )
                    if strict_renames and deployment.suspected_renames:
                        deployment.ok = False
                        deployment.error = unmoved_rename_refusal(
                            plan_obj.name, deployment.suspected_renames
                        )
                        if preview_migrations:
                            # ci apply refuses it, so it stops there as well.
                            failed = target
                    if missing := deployment.missing_prerequisites:
                        # No plan configured here creates what it borrows:
                        # ci apply refuses it before tracking, as apply does.
                        refusal = missing_prerequisite_message(
                            plan_obj.name,
                            [c.as_missing_prerequisite() for c in missing],
                            instance=instance.slug,
                        )
                        deployment.ok = False
                        deployment.error = (
                            refusal
                            if deployment.error is None
                            else f"{deployment.error}\n\n{refusal}"
                        )
                    results[target] = deployment
                except Exception as exc:
                    if preview_migrations:
                        failed = target
                    results[target] = DeploymentDiff(
                        instance_slug=instance.slug,
                        plan_target=target,
                        ok=False,
                        error=(
                            str(exc)
                            if reported
                            else "rehearsing this unchanged plan, which ci apply "
                            f"applies along with the changed ones, failed: {exc}"
                        ),
                    )
        # Reported in configuration order, however they were rehearsed.
        pr_report.diffs.extend(results[t] for t in inst_targets if t in results)
    return pr_report


def _find_ref(config: CIConfig, target: str) -> PlanRef:
    for instance in config.instances:
        for plan in instance.plans:
            if plan.target == target:
                return plan
    raise KeyError(target)


def _default_compute(
    plan_obj: Plan,
    plan_file: Path,
    slug: str,
    claimed_keys: set[str] | None = None,
    claimed_by: dict[str, str] | None = None,
    *,
    allow_data_validated_changes: bool = False,
    co_applied_keys: set[str] | None = None,
    existing_cache: ExistenceCache | None = None,
    members_cache: dict[int, list[Any]] | None = None,
    exports_cache: dict[str, Any] | None = None,
    preview_migrations: bool = False,
    strict_uids: bool = True,
) -> list[ChangeRecord]:
    """Compute the read-only diff of `plan_obj` against the connected instance.

    `claimed_keys` are the natural keys declared by the sibling plans managed
    against the same instance; removals of those keys are reported as a transfer
    (not a deletion) so an object handed off to a sibling plan is shown as
    transferred.  `claimed_by` optionally maps such a key to the sibling plan
    that now owns it, to name the transfer's destination.  Both are optional so
    the drift flow, which diffs each plan on its own, can reuse this helper
    without siblings.  `allow_data_validated_changes` opts in to lifting a
    blocked validation-affecting change when the live data proves it safe (see
    :meth:`~lamindb_setup.evolve.engine._apply.ApplyContext._data_override_reason`); it
    is off for the drift flow, which stays a pure, data-independent check.
    `co_applied_keys` are the natural keys the prerequisite plans configured
    for the same instance declare, so a borrowed object they create first is
    not reported as missing (see
    :func:`~lamindb_setup.evolve.engine.compute_changes`); the drift flow has none.

    `existing_cache` and `members_cache` are the instance state already read
    by :func:`~lamindb_setup.evolve.engine._apply.prefetch_plans` on behalf of every plan
    on this instance; passing them lets this diff skip its own prefetch, and
    `exports_cache` shares the instance-wide sheet-export index the same way.

    `preview_migrations` rehearses the apply instead (see
    :func:`~lamindb_setup.evolve.engine.preview_changes`), joining the
    :func:`~lamindb_setup.evolve.engine.rehearsal` the caller opened, and
    refuses a uid held here that `strict_uids` pins as the apply does.  A
    rehearsal reads the state the plans before it left, so it neither takes the
    caches nor needs `co_applied_keys`: the prerequisites really created
    their objects first.
    """
    from ..core import ApplyMode
    from ..engine import (
        LockFile,
        compute_changes,
        lock_path,
        preview_changes,
        read_sibling_uids,
    )

    lock = LockFile.read(lock_path(plan_file, slug))
    sibling_uids = read_sibling_uids(plan_file, plan_obj, slug)
    if preview_migrations:
        ctx = preview_changes(
            plan_obj,
            lock=lock,
            claimed_keys=claimed_keys,
            claimed_by=claimed_by,
            allow_data_validated_changes=allow_data_validated_changes,
            sibling_uids=sibling_uids,
            strict_uids=strict_uids,
        )
    else:
        ctx = compute_changes(
            plan_obj,
            mode=ApplyMode.PLAN,
            lock=lock,
            existing_cache=existing_cache,
            members_cache=members_cache,
            exports_cache=exports_cache,
            claimed_keys=claimed_keys,
            claimed_by=claimed_by,
            co_applied_keys=co_applied_keys,
            allow_data_validated_changes=allow_data_validated_changes,
            sibling_uids=sibling_uids,
        )
    missing = {item.key: item for item in ctx.missing_prerequisites}
    return [
        ChangeRecord(
            action=item.action.value,
            kind=item.kind,
            name=item.name,
            uid=item.uid,
            details=list(item.details),
            warnings=list(item.warnings),
            uid_misalignment=(
                item.uid_misalignment.describe()
                if item.uid_misalignment is not None
                else None
            ),
            suspected_rename_of=item.suspected_rename_of,
            borrowed=item.borrowed,
            missing_prerequisite=missing.get(item.key) if item.borrowed else None,
        )
        for item in ctx.changes
    ]
