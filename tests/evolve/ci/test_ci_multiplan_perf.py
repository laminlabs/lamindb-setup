# Adapted source tests use deliberately invalid inputs and lightweight stand-ins.
# mypy: disable-error-code="return-value"
"""Performance harness for `ci plan` / `ci apply` over *many* plans.

The single-plan harness (`test_ci_migration_perf`) fits cost against the size
of one plan.  This one fits it against the number of plans -- and instances --
in the configuration, which is the shape a real deployment grows into: dozens of
small plans sharing one instance, each pull request touching one of them.

Costs that only appear at that shape:

* every plan is diffed with the union of *every other* plan's natural keys, so
  the sibling-claim bookkeeping is quadratic in the plan count even though the
  database work is linear; and
* `ci plan` finalizes all configured plans even when a single one changed,
  because an unchanged sibling still claims the keys a changed plan drops.

Four shapes are measured against one seeded instance: the whole-config diff, the
pull-request diff (one changed plan among many), the diff of an *edited*
configuration (every schema gains a feature, so there is real create + update
work rather than an all-no-change walk), and the apply of that edit.

Sizing is environment-driven so the same test doubles as a scaling experiment:
`LAMIN_MULTIPLAN_PLANS` / `LAMIN_MULTIPLAN_SCHEMAS` /
`LAMIN_MULTIPLAN_FEATURES` / `LAMIN_MULTIPLAN_INSTANCES` scale the
configuration, `LAMIN_MULTIPLAN_QUERY_STATS` prints the captured SQL grouped by
shape, and `LAMIN_PERF_PROFILE_DIR` writes a pyinstrument report per phase.

Measured behaviour on a local SQLite instance (2026-08), fitting exactly against
the schema count `S`:

* `ci plan` costs 4 statements for the *whole instance*: 2 for the existence
  prefetch (one `uid__in` per backing registry -- objects the lock already
  pins need no name lookup) and 2 for the bulk member read.  Every plan on the
  instance is diffed against that one read (`prefetch_plans`), so the cost
  grows with neither the plan count nor `S`.
* `ci apply` of a one-feature-per-schema edit costs the same 4 statements for
  the whole instance plus `3 + 11*S` per plan.  It shares the instance-wide
  read too, but cannot share *all* of it: applying one plan can create an object
  a later sibling declares, and the shared read still records that object as
  missing.  `forget_missing` drops exactly those entries after each plan, and
  re-reading them is the one remaining per-plan query; the other two are the
  transaction's `BEGIN` and `COMMIT`.  What remains per schema is write work.

The ceilings below are expressed to match: a flat per-instance budget for `ci
plan`, and a per-instance plus per-plan, per-schema one for `ci apply`, so a
regression that shares work *worse* across siblings -- or reintroduces a
per-object query -- fails loudly.  Wall-clock is reported but never asserted on:
on this hardware the SQL execution itself is ~1% of the measured time, so a
timing threshold would only measure the runner.
"""

from __future__ import annotations

import os
import time
import types
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any

from lamindb_setup.evolve import Feature, FeatureType, LaminDType, Schema
from lamindb_setup.evolve.authoring._plan import Plan, PlanContext, create_plan

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from types import SimpleNamespace as TestInstanceInfo
    from typing import Any as LamindbModuleType

    import pytest


def _sized(name: str, default: int) -> int:
    """Read a positive problem-size knob from the environment."""
    return max(1, int(os.environ.get(name, default)))


# Default: 12 plans x (4 schemas x 6 features + 4 feature types + 4 schemas)
# = 12 x 32 = 384 plan objects across the instance.
_N_PLANS = _sized("LAMIN_MULTIPLAN_PLANS", 12)
_N_SCHEMAS = _sized("LAMIN_MULTIPLAN_SCHEMAS", 4)
_N_FEATURES = _sized("LAMIN_MULTIPLAN_FEATURES", 6)
_N_INSTANCES = _sized("LAMIN_MULTIPLAN_INSTANCES", 1)

_OBJECTS_PER_PLAN = _N_SCHEMAS * (1 + 1 + _N_FEATURES)
_N_OBJECTS = _N_PLANS * _OBJECTS_PER_PLAN

_SLUG = "multi-dev"

# Query ceilings at roughly 1.4x the measured cost (see the module docstring).
#
# `ci plan` reads the live state for *every* plan on an instance in one go
# (`prefetch_plans`), so it costs a fixed handful of statements per instance --
# 4 measured, independent of both the plan count and the schema count.  The
# budget is therefore a flat per-instance number rather than a per-plan one: any
# read that creeps back into the per-plan or per-schema path fails here loudly.
_MAX_PLAN_QUERIES_PER_INSTANCE = 10
# `ci apply` shares that same instance-wide read across the plans it applies
# together, then pays `3 + 16*S` per plan for `S` schemas: two for the
# transaction and one to re-read the objects a previous sibling may have
# created.  Five of the per-schema queries are lamindb's own `Schema.save()`
# (since 2.12 it re-reads the members in link order and checks no other schema
# already holds the new hash).  Keeping the schema term separate is what makes
# this a regression test across the size knobs: a fixed per-plan budget would
# silently pass once `LAMIN_MULTIPLAN_SCHEMAS` grew.
_MAX_APPLY_QUERIES_PER_INSTANCE = 10
_MAX_APPLY_QUERIES_FIXED = 5
_MAX_APPLY_QUERIES_PER_SCHEMA = 22


def _budget(fixed: int, per_schema: int) -> int:
    """Return the query budget for one full pass over the configuration."""
    return _N_INSTANCES * (
        _MAX_APPLY_QUERIES_PER_INSTANCE + _N_PLANS * (fixed + per_schema * _N_SCHEMAS)
    )


def _make_plan(index: int, *, changed: bool = False) -> Plan:
    """Build plan `index` with names disjoint from every other plan's.

    `changed` is the edited variant: every schema gains one feature, so the
    diff against the seeded state produces real create + schema-update work
    rather than an all-no-change walk.
    """
    prefix = f"mp{index}"

    @create_plan
    def _plan(ctx: PlanContext, exports: None) -> None:
        for s in range(_N_SCHEMAS):
            ft = FeatureType(f"{prefix}_ft_{s}")
            features = [
                Feature(f"{prefix}_feat_{s}_{i}", dtype=LaminDType(str), type=ft)
                for i in range(_N_FEATURES)
            ]
            if changed:
                features.append(
                    Feature(f"{prefix}_extra_{s}", dtype=LaminDType(str), type=ft)
                )
            Schema(name=f"{prefix}_Schema_{s}", features=features)

    _plan.name = f"multiplan_{index}"
    return _plan


# Plans must be *constructed* before lamindb is imported (the construction guard
# rejects building one afterwards), so they live at module scope.
_PLANS: list[Plan] = [_make_plan(i) for i in range(_N_PLANS)]
_CHANGED_PLANS: list[Plan] = [_make_plan(i, changed=True) for i in range(_N_PLANS)]


# === harness =================================================================


def _plan_files(tmp_path: Path) -> list[Path]:
    files = []
    for i in range(_N_PLANS):
        path = tmp_path / f"plan_mp{i}.py"
        path.write_text("# plan\n", encoding="utf-8")
        files.append(path)
    return files


def _config(files: list[Path]) -> Any:
    from lamindb_setup.evolve.ci._config import CIConfig, InstanceConfig, PlanRef

    refs = [PlanRef(target=f"{path}:p", path=path, var="p") for path in files]
    return CIConfig(
        instances=[
            InstanceConfig(slug=f"{_SLUG}-{i}", plans=refs) for i in range(_N_INSTANCES)
        ],
        config_path=files[0].parent / "lamin_schemas.yml",
    )


def _loader(
    files: list[Path], plans: list[Plan] | None = None
) -> Callable[[str], tuple[Plan, Path, Any]]:
    module = types.ModuleType("_perf_plan_module")
    by_target = {
        f"{path}:p": (plan, path, module)
        for plan, path in zip(plans or _PLANS, files, strict=False)
    }
    return lambda target: by_target[target]


def _connect(slug: str) -> str:
    return slug


@contextmanager
def _count_queries() -> Iterator[Any]:
    """Yield a capture context; read `len(...)` on it *after* the block exits."""
    from django.db import connection
    from django.test.utils import CaptureQueriesContext

    with CaptureQueriesContext(connection) as cap:
        yield cap


def _query_stats(cap: Any, label: str) -> None:
    """Print captured queries grouped by shape: count and total reported time."""
    import re
    from collections import defaultdict

    if not os.environ.get("LAMIN_MULTIPLAN_QUERY_STATS"):
        return
    buckets: dict[str, list[float]] = defaultdict(list)
    for q in cap.captured_queries:
        sql = q["sql"]
        shape = re.sub(r"\(%s(, %s)*\)", "(...)", sql)
        shape = re.sub(r"'[^']*'", "?", shape)
        shape = re.sub(r"\b\d+\b", "N", shape)
        shape = re.sub(r"(WHEN \S+ = N THEN N )+", "WHEN... ", shape)
        buckets[shape[:150]].append(float(q["time"]))
    print(f"\n--- query shapes: {label} ---")
    rows = sorted(buckets.items(), key=lambda kv: -sum(kv[1]))
    for shape, times in rows[:12]:
        print(f"  n={len(times):5d}  t={sum(times):7.3f}s  {shape}")


@contextmanager
def _profiled(name: str) -> Iterator[None]:
    """Profile the block with pyinstrument when `LAMIN_PERF_PROFILE_DIR` is set."""
    directory = os.environ.get("LAMIN_PERF_PROFILE_DIR")
    if not directory:
        yield
        return

    from pyinstrument import Profiler

    profiler = Profiler()
    profiler.start()
    try:
        yield
    finally:
        profiler.stop()
        out = Path(directory)
        out.mkdir(parents=True, exist_ok=True)
        (out / f"{name}.txt").write_text(
            profiler.output_text(unicode=True, color=False),
            encoding="utf-8",
        )
        (out / f"{name}.full.txt").write_text(
            profiler.output_text(unicode=True, color=False, show_all=True),
            encoding="utf-8",
        )
        (out / f"{name}.html").write_text(profiler.output_html(), encoding="utf-8")


def _seed(files: list[Path]) -> None:
    """Apply every plan to the connected instance and write its per-instance lock."""
    from lamindb_setup.evolve.engine._apply import ApplyMode, compute_changes
    from lamindb_setup.evolve.engine._lock import lock_path

    for plan, path in zip(_PLANS, files, strict=False):
        plan.reset()
        ctx = compute_changes(plan, mode=ApplyMode.APPLY, lock=None)
        lock = ctx.build_lock(plan.name)
        for i in range(_N_INSTANCES):
            lock.write(lock_path(path, f"{_SLUG}-{i}"))


def _reset_all() -> None:
    for plan in _PLANS:
        plan.reset()
    for plan in _CHANGED_PLANS:
        plan.reset()


# === the test ================================================================


def test_multiplan_scales(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
    tmp_path: Path,
    record_property: Callable[[str, object], None],
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Drive `ci plan` and `ci apply` over a many-plan configuration."""
    from contextlib import contextmanager as _contextmanager

    from lamindb_setup.evolve.ci._apply import run_apply
    from lamindb_setup.evolve.ci._pr import run_pr_check

    @_contextmanager
    def _fake_track(*_args: object, **_kwargs: object) -> Iterator[tuple[str, None]]:
        yield ("perf-transform-key", None)

    files = _plan_files(tmp_path)
    config = _config(files)

    with _profiled("multi-seed"):
        seed_start = time.perf_counter()
        _seed(files)
        seed_seconds = time.perf_counter() - seed_start

    load_plan = _loader(files)

    # --- ci plan, whole config ---------------------------------------------
    _reset_all()
    with _profiled("multi-ci-plan-all"):
        all_start = time.perf_counter()
        with _count_queries() as all_cap:
            report = run_pr_check(config, load_plan=load_plan, connect=_connect)
        all_seconds = time.perf_counter() - all_start
    all_queries = len(all_cap)
    _query_stats(all_cap, "ci plan (all)")
    assert not report.has_errors, [d.error for d in report.diffs if not d.ok]
    assert len(report.diffs) == _N_PLANS * _N_INSTANCES
    assert not report.has_changes

    # --- ci plan, single changed plan (the pull-request shape) --------------
    _reset_all()
    with _profiled("multi-ci-plan-one"):
        one_start = time.perf_counter()
        with _count_queries() as one_cap:
            report_one = run_pr_check(
                config,
                changed_files=[str(files[0])],
                load_plan=load_plan,
                connect=_connect,
            )
        one_seconds = time.perf_counter() - one_start
    one_queries = len(one_cap)
    assert not report_one.has_errors
    assert len(report_one.diffs) == _N_INSTANCES

    # --- ci plan, every plan edited (real create + update work) -------------
    _reset_all()
    changed_loader = _loader(files, _CHANGED_PLANS)
    with _profiled("multi-ci-plan-changed"):
        chg_start = time.perf_counter()
        with _count_queries() as chg_cap:
            report_chg = run_pr_check(
                config, load_plan=changed_loader, connect=_connect
            )
        chg_seconds = time.perf_counter() - chg_start
    chg_queries = len(chg_cap)
    _query_stats(chg_cap, "ci plan (edited)")
    assert not report_chg.has_errors, [d.error for d in report_chg.diffs if not d.ok]
    assert report_chg.has_changes

    # --- ci apply -----------------------------------------------------------
    _reset_all()
    with _profiled("multi-ci-apply"):
        apply_start = time.perf_counter()
        with _count_queries() as apply_cap:
            run_apply(
                config,
                load_plan=changed_loader,
                connect=_connect,
                require_stem_uid=lambda _module, _pf: "perfUID00001",
                ensure_pushed=lambda _pfs: None,
                track=_fake_track,
            )
        apply_seconds = time.perf_counter() - apply_start
    apply_queries = len(apply_cap)
    _query_stats(apply_cap, "ci apply (edited)")

    # --- report -------------------------------------------------------------
    for key, value in (
        ("perf_plans", _N_PLANS),
        ("perf_instances", _N_INSTANCES),
        ("perf_objects", _N_OBJECTS),
        ("perf_seed_seconds", round(seed_seconds, 3)),
        ("perf_plan_all_seconds", round(all_seconds, 3)),
        ("perf_plan_all_queries", all_queries),
        ("perf_plan_one_seconds", round(one_seconds, 3)),
        ("perf_plan_one_queries", one_queries),
        ("perf_plan_changed_seconds", round(chg_seconds, 3)),
        ("perf_plan_changed_queries", chg_queries),
        ("perf_apply_seconds", round(apply_seconds, 3)),
        ("perf_apply_queries", apply_queries),
    ):
        record_property(key, value)

    with capsys.disabled():
        print(
            f"\n[multi] {_N_PLANS} plans x {_OBJECTS_PER_PLAN} objects "
            f"= {_N_OBJECTS} objects on {_N_INSTANCES} instance(s)\n"
            f"[multi] seed           {seed_seconds:7.2f}s\n"
            f"[multi] plan  (all)    {all_seconds:7.2f}s  {all_queries:6d} queries "
            f"({all_seconds / _N_PLANS:.3f}s/plan)\n"
            f"[multi] plan  (1 chg)  {one_seconds:7.2f}s  {one_queries:6d} queries\n"
            f"[multi] plan  (edited) {chg_seconds:7.2f}s  {chg_queries:6d} queries\n"
            f"[multi] apply (edited) {apply_seconds:7.2f}s  {apply_queries:6d} queries"
        )

    # --- guards -------------------------------------------------------------
    plan_budget = _MAX_PLAN_QUERIES_PER_INSTANCE * _N_INSTANCES
    for label, queries in (
        ("all", all_queries),
        ("1 changed", one_queries),
        ("edited", chg_queries),
    ):
        assert queries <= plan_budget, (
            f"ci plan ({label}) issued {queries} queries for {_N_PLANS} plans of "
            f"{_N_SCHEMAS} schemas on {_N_INSTANCES} instance(s) (budget "
            f"{plan_budget}); the instance-wide read is no longer shared, or a "
            "per-object query returned."
        )
    apply_budget = _budget(_MAX_APPLY_QUERIES_FIXED, _MAX_APPLY_QUERIES_PER_SCHEMA)
    assert apply_queries <= apply_budget, (
        f"ci apply issued {apply_queries} queries for {_N_PLANS} plans on "
        f"{_N_INSTANCES} instance(s) (budget {apply_budget}); the instance-wide "
        "read is no longer shared across siblings, or the per-schema member "
        "reads regressed."
    )
