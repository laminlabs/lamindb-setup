"""Apply the configured plans to their instances, reusing the apply machinery.

`ci apply` is the configuration-driven counterpart of the standalone `apply`
command: instead of taking explicit plan targets and an instance, it reads the
instances and their plans from `lamin_schemas.yml` and applies them.  An
optional changed-file filter restricts the run to the instances that have at
least one of the given plan files, mirroring the pull-request flow, and an
optional instance filter restricts it to the named instances (the two
intersect); for every selected instance *all* of its configured plans are
applied together, so an object handed off between sibling plans is treated as
a transfer, not a deletion, and the unchanged siblings apply as no-ops.
"""

from __future__ import annotations

from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ..console import OutputFormat
from ..core import LaminSchemasError

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence
    from types import ModuleType
    from typing import Unpack

    from ..authoring import Plan
    from ._config import CIConfig, InstanceConfig, PlanRef

__all__ = ["CIApplyError", "run_apply"]


def run_apply(
    config: CIConfig,
    changed_files: list[str] | None = None,
    *,
    instances: Sequence[str] | None = None,
    output_format: OutputFormat = OutputFormat.terminal,
    strict_renames: bool = True,
    allow_data_validated_changes: bool = False,
    strict_uids: bool = True,
    report: Callable[[str], None] | None = None,
    load_plan: Callable[[str], tuple[Plan, Path, ModuleType]] | None = None,
    connect: Callable[[str], str] | None = None,
    require_stem_uid: Callable[[ModuleType, Path], str] | None = None,
    ensure_pushed: Callable[[Sequence[Path]], Mapping[Path, Path] | None] | None = None,
    apply_one: Callable[..., str | None] | None = None,
    track: Callable[..., Any] | None = None,
) -> list[str]:
    """Apply the configured plans to their instances and return Markdown sections.

    `changed_files` optionally restricts the run to the instances that have at
    least one of the given plan files; when `None` every configured instance
    is applied.  `instances` optionally restricts it to the configured
    instances with those slugs, intersecting with `changed_files`; a slug the
    configuration does not list raises
    :class:`~lamindb_setup.evolve.core.InvalidArgumentError`.  The selected
    instances are applied in configuration order, and each still reads the lock
    files the plan has for the others -- whichever run wrote them -- so applying
    one instance on its own pins the uids an earlier run gave the rest.

    For each selected instance all of its configured plans are applied together
    (so a moved object is not deleted), sharing one read of the instance's live
    state, and each writes its own per-instance lock file.
    Returns the rendered sections for a text format (the caller writes
    them out); in terminal mode each application prints its own summary and the
    list is empty.  It never asks for confirmation: a CI run is unattended, and
    the reviewed configuration is what approves it.  The `load_plan`/`connect`/`require_stem_uid`/
    `ensure_pushed`/`apply_one` hooks default to the real helpers (the same
    ones the `apply` command drives) and exist so tests can inject fakes;
    `ensure_pushed` returns each plan's repo-relative path so the per-plan
    tracking does not have to ask git again.  `track`
    overrides the run-tracking context manager the default `apply_one` uses,
    so a measurement run can leave the Transform/Run writes out of what it
    counts without giving up the real apply path; it is ignored when
    `apply_one` is injected, since that replaces the code that would use it.

    `report` optionally receives a milestone line per stage, so a long run says
    what it is doing.  It is a plain callback rather than a progress display
    because applying a plan renders one of its own, and only one can be live.

    `allow_data_validated_changes` is ORed with the configuration's key of the
    same name, and `strict_uids`/`strict_renames` are ANDed with theirs, so
    either the flag or the file can relax a check and neither can tighten one
    the other relaxed.
    """
    from ..engine import (
        SheetExportError,
        ensure_all_committed_and_pushed,
        safe_connect,
        verified_session,
    )
    from ..engine import apply_one as default_apply_one
    from ..engine import load_plan as default_load_plan
    from ..engine import require_stem_uid as default_require_stem_uid

    def _report(message: str) -> None:
        if report is not None:
            report(message)

    load_plan = load_plan or default_load_plan
    connect = connect or safe_connect
    require_stem_uid = require_stem_uid or default_require_stem_uid
    ensure_pushed = ensure_pushed or ensure_all_committed_and_pushed
    # Whether the per-instance bulk prefetch below runs.  It fills caches only
    # `apply_one` knows what to do with, so it is only worth anything when
    # the default is what actually runs; an injected `apply_one` -- which need
    # not even touch an instance -- is left entirely alone.
    share_reads = apply_one is None
    apply_one = apply_one or default_apply_one

    # The `--allow-data-validated-changes` flag and the `lamin_schemas.yml`
    # `allow_data_validated_changes` setting are ORed: either opts the run in.
    allow_data_validated_changes = (
        allow_data_validated_changes or config.allow_data_validated_changes
    )
    # `--no-strict-uids` and `strict_uids: false` are ORed the same way:
    # either one lets an instance apply while its uids are misaligned.
    strict_uids = strict_uids and config.strict_uids
    # And `--no-strict-renames` with `strict_renames: false`: either one
    # lets an undeclared rename apply as a delete + create.
    strict_renames = strict_renames and config.strict_renames

    selected = _select_instances(config, changed_files, instances)

    # Construct every unique plan referenced by a selected instance *before*
    # connecting to any instance.  Connecting imports lamindb, after which the
    # plan-construction guard refuses to build any further plan; constructing
    # all plans up front keeps each construction lamindb-free.  A plan shared by
    # several instances is constructed once and reused.
    loaded: dict[str, tuple[Plan, Path, ModuleType]] = {}
    for instance in selected:
        for ref in instance.plans:
            if ref.target not in loaded:
                _report(f"Loading {ref.target}")
                loaded[ref.target] = load_plan(ref.load_target)

    # Every plan must declare a stem uid and be committed + pushed before it is
    # applied; check all of them before connecting so an unconfigured or dirty
    # plan fails fast, without partially applying the run.
    stem_uids: dict[str, str] = {}
    for target, (_obj, plan_file, module) in loaded.items():
        stem_uids[target] = require_stem_uid(module, plan_file)
    _report(f"Checking {len(loaded)} plan(s) are committed and pushed")
    verified = ensure_pushed(
        [plan_file for _obj, plan_file, _module in loaded.values()]
    )

    sections: list[str] = []
    failures: list[tuple[str, str]] = []
    # Hand the gate's findings to the tracking of each plan so it does not spawn
    # git again to re-establish them (see `verified_session`).
    with verified_session(verified or {}):
        for instance in selected:
            try:
                _apply_instance(
                    instance,
                    config=config,
                    loaded=loaded,
                    stem_uids=stem_uids,
                    sections=sections,
                    connect=connect,
                    apply_one=apply_one,
                    share_reads=share_reads,
                    output_format=output_format,
                    strict_renames=strict_renames,
                    allow_data_validated_changes=allow_data_validated_changes,
                    strict_uids=strict_uids,
                    report=_report,
                    track=track,
                )
            except SheetExportError:
                # The plan applied and only its exports did not, which is a state
                # to inspect before doing anything else -- so this one still
                # stops the run, carrying what was rendered (see `apply_one`).
                raise
            except Exception as exc:
                # Every other failure is contained to its instance.  The apply
                # of a plan is transactional, so a failure here changed nothing
                # on *this* instance -- and stopping would leave the remaining
                # instances neither applied nor reported, which is the state
                # that costs a maintainer the most to unpick.
                failures.append((instance.slug, str(exc)))
                _report(f"Failed to apply to {instance.slug}: {exc}")
    if failures:
        raise CIApplyError(failures, sections=sections)
    return sections


class CIApplyError(LaminSchemasError):
    """One or more instances could not be applied to.

    Carries the sections rendered for the instances that *did* apply, so the
    command still writes its report before exiting non-zero: those applies are
    committed and their lock files written, and a run that hid them would leave
    no record of what landed where.
    """

    def __init__(self, failures: list[tuple[str, str]], *, sections: list[str]) -> None:
        detail = "; ".join(f"{slug}: {error}" for slug, error in failures)
        super().__init__(
            f"failed to apply to {len(failures)} instance(s) -- {detail}. The "
            "other configured instances were still applied; each apply is "
            "transactional, so a failed instance is unchanged."
        )
        self.failures = failures
        self.sections = sections


def _apply_instance(
    instance: InstanceConfig,
    *,
    config: CIConfig,
    loaded: dict[str, tuple[Plan, Path, ModuleType]],
    stem_uids: dict[str, str],
    sections: list[str],
    connect: Callable[[str], str],
    apply_one: Any,
    share_reads: bool,
    output_format: OutputFormat,
    strict_renames: bool,
    allow_data_validated_changes: bool,
    report: Callable[[str], None],
    strict_uids: bool = True,
    track: Callable[..., Any] | None = None,
) -> None:
    """Apply every plan configured for one instance, appending to `sections`."""
    from ..engine import (
        ExistenceCache,
        LockFile,
        SheetExportError,
        forget_missing,
        lock_path,
        prefetch_plans,
        read_sibling_uids,
        sibling_claimed_keys,
    )
    from ..engine import apply_one as default_apply_one

    _report = report

    # Connect *before* finalizing this instance's plans.  Switching
    # instances tears Django down and rebuilds LaminDB's model classes, so
    # a plan finalized against a previous instance holds stale `lamindb`
    # class references (e.g. a categorical dtype's registry) that fail
    # `issubclass` once the new instance is live.  Resetting the loaded
    # plans after connecting forces each one to be finalized afresh against
    # the current instance's classes.  Plans were still *constructed* up
    # front (before any connect), so the construction guard is respected.
    _report(f"Connecting to {instance.slug}")
    instance_slug = connect(instance.slug)
    # The configuration's track_prefix/track_space for *this* instance, folded
    # over the environment once and carried into every plan's tracking below.
    track_config = config.track_config_for(instance.slug)
    _report(f"Finalizing {len(instance.plans)} plan(s) for {instance_slug}")
    for plan_obj, _plan_file, _plan_module in loaded.values():
        plan_obj.reset()
    # Dependencies-first, not configuration order: a plan only ever adopts the
    # objects a prerequisite owns, so applying it before its owner is refused
    # (see `apply_one`).  `lamin_schemas.yml` lists the plans
    # deployed to an instance, which is a statement about membership rather than
    # order, so the order is derived here instead of asked of whoever edits it.
    plan_refs = _refs_dependencies_first(instance.plans, loaded)
    plan_objs = [loaded[ref.target][0] for ref in plan_refs]
    claimed = sibling_claimed_keys(plan_objs)

    # Every plan on this instance is about to be diffed against the same
    # live state, and each would otherwise run the whole existence
    # prefetch and member read for itself.  Reading it once for the
    # instance turns that into a fixed handful of statements for the
    # whole configuration.  This must happen *after* the resets above:
    # the caches are keyed by `id(obj)` and a reset rebuilds every plan
    # object, so a cache filled before them indexes objects that no
    # longer exist -- and CPython reuses their ids.
    apply = apply_one
    existing_cache = ExistenceCache()
    # The sheet-export index does not depend on the plan, so the whole
    # instance shares one read of it across its plans.
    exports_cache: dict[str, Any] = {}
    if share_reads:
        members_cache: dict[int, list[Any]] = {}
        _report(f"Reading {instance_slug} state for {len(instance.plans)} plan(s)")
        entries = [loaded[ref.target] for ref in plan_refs]
        prefetch_plans(
            [
                (plan_obj, LockFile.read(lock_path(plan_file, instance_slug)))
                for plan_obj, plan_file, _module in entries
            ],
            existing_cache=existing_cache,
            members_cache=members_cache,
            sibling_uids=[
                read_sibling_uids(plan_file, plan_obj, instance_slug)
                for plan_obj, plan_file, _module in entries
            ],
        )
        apply = partial(
            default_apply_one,
            existing_cache=existing_cache,
            members_cache=members_cache,
            exports_cache=exports_cache,
            # `track` only reaches the default `apply_one`: an injected
            # `apply_one` replaces the code that would have used it.
            **({} if track is None else {"track": track}),  # type: ignore[arg-type]
        )

    for index, (ref, claimed_keys) in enumerate(
        zip(plan_refs, claimed, strict=False), start=1
    ):
        plan_obj, plan_file, _module = loaded[ref.target]
        _report(f"Applying {ref.target} to {instance_slug} ({index}/{len(plan_refs)})")
        try:
            section = apply(
                plan_obj,
                plan_file,
                stem_uids[ref.target],
                instance.slug,
                instance_slug,
                claimed_keys=claimed_keys,
                confirm=None,
                output_format=output_format,
                strict_renames=strict_renames,
                allow_data_validated_changes=allow_data_validated_changes,
                strict_uids=strict_uids,
                track_config=track_config,
            )
        except SheetExportError as exc:
            # The plan applied; only its sheet re-exports did not.  Carry
            # everything rendered so far on the error so the command can
            # still write its report before exiting non-zero.
            if exc.section is not None:
                sections.append(exc.section)
            exc.sections = sections
            raise
        # This plan may have created objects the shared read recorded as
        # missing; drop those entries so the next sibling re-reads them.
        forget_missing(existing_cache)
        if section is not None:
            sections.append(section)


def _refs_dependencies_first(
    refs: Sequence[PlanRef],
    loaded: Mapping[str, tuple[Plan, Unpack[tuple[Any, ...]]]],  # noqa: UP044 - keep install-time parsing on Python 3.10
) -> list[PlanRef]:
    """Order `refs` so each comes after the listed plans its plan depends on.

    The configuration-order counterpart of
    :func:`~lamindb_setup.evolve.engine._loader.order_dependencies_first`, which does the
    same for explicitly listed targets.  Restricted to the refs given, so a
    prerequisite deployed to a different instance -- or not configured at all --
    does not reorder anything; that case is the operator's to fix, and the apply
    refuses it before tracking a run, naming the plan to apply first.  Refs whose
    plan depends on nothing listed keep their configured order.

    `loaded` maps a target to a tuple starting with its plan; whatever the
    caller loaded with it follows.  `ci plan --preview-migrations` rehearses
    an instance's plans in this order too.
    """
    by_plan: dict[int, PlanRef] = {}
    for ref in refs:
        entry = loaded.get(ref.target)
        if entry is not None:
            by_plan.setdefault(id(entry[0]), ref)
    ordered: list[PlanRef] = []
    visited: set[int] = set()

    def visit(plan_obj: Plan) -> None:
        if id(plan_obj) in visited:
            return
        visited.add(id(plan_obj))
        for dependency in plan_obj.depends_on:
            if id(dependency) in by_plan:
                visit(dependency)
        ref = by_plan.get(id(plan_obj))
        if ref is not None:
            ordered.append(ref)

    for ref in refs:
        entry = loaded.get(ref.target)
        if entry is None:
            ordered.append(ref)
            continue
        visit(entry[0])
    return ordered


def _select_instances(
    config: CIConfig,
    changed_files: list[str] | None,
    instances: Sequence[str] | None = None,
) -> list[InstanceConfig]:
    """Return the instances to run on, filtered to changed plan files and slugs.

    With no filter every configured instance is selected.  `changed_files`
    keeps only the instances that declare at least one of the changed plan
    files, and `instances` only the ones with those slugs (see
    :meth:`CIConfig.select`); given both, an instance must pass both.  Either
    way the configuration's order is kept.

    A changed file that is not configured anywhere is only an error when it
    still exists on disk: an unregistered new plan is a real mistake and must
    fail loudly, while a plan *removed* in this change (its file deleted and its
    entry dropped from the config) is expected -- the instance's surviving
    sibling plans still apply and reconcile the objects it used to own, so the
    removed plan is silently ignored instead of aborting the whole apply.
    """
    from ._config import ConfigError

    # Checked first: a mistyped slug is a usage error, and it is the cheaper
    # one to report.
    selected = config.select(instances)
    if not changed_files:
        return selected

    # A changed plan is judged against the whole configuration, not only the
    # selected instances: one deployed to an instance outside the selection is
    # registered all the same, and only that instance's run applies it.
    changed = {_resolve(raw) for raw in changed_files}
    configured = {ref.path for instance in config.instances for ref in instance.plans}
    missing = sorted(str(path) for path in changed - configured if path.exists())
    if missing:
        raise ConfigError(
            f"changed plan(s) not found in {config.config_path}: {', '.join(missing)}"
        )
    return [
        instance
        for instance in selected
        if changed & {ref.path for ref in instance.plans}
    ]


def _resolve(raw: str) -> Path:
    """Resolve `raw` to an absolute path, relative to the current directory."""
    path = Path(raw)
    return path.resolve() if path.is_absolute() else (Path.cwd() / path).resolve()
