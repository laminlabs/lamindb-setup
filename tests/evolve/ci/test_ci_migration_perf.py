# Adapted source tests use deliberately invalid inputs and lightweight stand-ins.
# mypy: disable-error-code="arg-type, return-value"
"""End-to-end performance harness for a `str -> cat[Record[...]]` migration.

The expensive real-world change is not a fresh apply -- it is editing an
*already tracked* schema so one of its features stops being a plain `str` and
becomes a categorical pointing at a specific record type.  That single edit
drags in every slow path the engine has: the drift taxonomy refuses the
narrowing, the data-safety layer reads the feature's stored values to explain
why, and (once a `ctx.migrate` unblocks it) apply relocates every stored
annotation from the scalar store into a categorical link table one row at a
time.

This module drives that change through the real `ci plan` and `ci apply`
entry points against a real instance, seeded to a size where per-object and
per-row costs are visible.  Only the non-database seams are faked (`connect`
is a no-op, `load_plan` returns the in-memory plan, transform tracking is
stubbed out), so the measured work is the production path.

Sizing and profiling are environment-driven so the same test doubles as a
profiling harness:

* `LAMIN_PERF_SCHEMAS` / `LAMIN_PERF_FEATURES` / `LAMIN_PERF_TARGETS` /
  `LAMIN_PERF_RECORDS` scale the seeded instance; and
* `LAMIN_PERF_PROFILE_DIR`, when set, writes a pyinstrument report per phase
  into that directory.

The assertions are correctness plus deliberately generous query-count ceilings:
wall-clock is reported (via `record_property` and stdout) but never asserted
on, because a timing threshold on shared CI hardware is a flake generator.  The
query ceilings are what actually fail on a regression.

Source-suite measurements on a local SQLite instance (2026-07), fitting two knobs
independently:

* `ci plan` costs **0.8 SQL statements per plan object** and is *flat* in the
  number of stored annotations -- the batched data-safety value prefetch holds.
* `ci apply` costs **~3.2 SQL statements per plan object** and is likewise
  *flat* in the number of relocated annotations: the migration plans every
  categorical link row up front and writes them in one batch per table.

That flatness is the point of the ceilings below.  Before the batching,
`ci apply` cost ~12 statements *per relocated annotation* (640 annotations:
8152 queries, 5.3s) because the relocation called `FeatureManager.add_values`
once per stored row; the same case is now 520 queries and 0.33s.
"""

from __future__ import annotations

import os
import time
import types
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any

from lamindb_setup.evolve import (
    Feature,
    FeatureType,
    LaminDType,
    Record,
    RecordType,
    Schema,
    map_to_records,
)
from lamindb_setup.evolve.authoring._plan import Plan, PlanContext, create_plan

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from types import SimpleNamespace as TestInstanceInfo
    from typing import Any as LamindbModuleType

    import pytest


def _sized(name: str, default: int) -> int:
    """Read a positive problem-size knob from the environment."""
    return max(1, int(os.environ.get(name, default)))


# Default size: 12 schemas x 8 features = 96 features + 12 feature types + 12
# schemas + 1 record type + 5 target records = 126 plan objects, of which 12
# features migrate, carrying 12 x 25 = 300 stored annotations between them.
# Large enough that a per-object or per-row cost dominates the profile, small
# enough to stay a normal test.
_N_SCHEMAS = _sized("LAMIN_PERF_SCHEMAS", 12)
_FEATURES_PER_SCHEMA = _sized("LAMIN_PERF_FEATURES", 8)
_N_TARGETS = _sized("LAMIN_PERF_TARGETS", 5)
_N_ANNOTATED = _sized("LAMIN_PERF_RECORDS", 25)

# One feature per schema flips dtype; the rest are unchanged ballast so the
# migration cost is measured against a realistically sized surrounding plan.
_N_MIGRATING = _N_SCHEMAS
_N_ROWS = _N_MIGRATING * _N_ANNOTATED
_N_OBJECTS = (
    _N_SCHEMAS * (1 + 1 + _FEATURES_PER_SCHEMA)  # schema + feature type + features
    + 1  # record type
    + _N_TARGETS
)

_TYPE_NAME = "PerfCompound"
_PLAN_NAME = "perf_migration"

# The slug the fake `connect` returns; also the lock-file suffix.
_SLUG = "perf-dev"

# Query-count ceilings, set with roughly 2x headroom over the measured cost (see
# the module docstring) so they catch a return to per-object / per-row N+1
# querying without pinning the exact count -- lamindb's own statement mix shifts
# between releases.  Both are expressed *per plan object* rather than per stored
# annotation, which is what makes them regression tests: any per-row work in the
# value relocation blows the apply ceiling as soon as `LAMIN_PERF_RECORDS`
# grows.  `ci apply` resolves the plan twice (the preview and the apply pass)
# and then writes, hence the larger factor.
_MAX_PLAN_QUERIES_PER_OBJECT = 2
# This native checkout uses 1189 queries for 126 objects (9.4/object),
# unchanged for 300 versus 1200 annotations. Keep headroom over that baseline.
_MAX_APPLY_QUERIES_PER_OBJECT = 12


def _build_plan(*, migrate: bool) -> Plan:
    """Build the perf plan, optionally with the categorical dtype + migration.

    Both variants declare the record type and its target records, so the
    `migrate` variant is a pure *edit* of an already-tracked schema: only the
    feature's dtype changes.  That keeps the categorical target's uid resolvable
    during the `ci plan` preview instead of degrading to a "not previewed"
    placeholder, so the preview measures the real work.
    """

    @create_plan
    def _plan(ctx: PlanContext, exports: None) -> None:
        import lamindb as ln

        record_type = RecordType(_TYPE_NAME)
        for i in range(_N_TARGETS):
            Record(f"perf_target_{i}", type=record_type)

        for s in range(_N_SCHEMAS):
            feature_type = FeatureType(f"perf_ft_{s}")
            features = []
            for f in range(_FEATURES_PER_SCHEMA):
                name = f"perf_feat_{s}_{f}"
                if f == 0 and migrate:
                    feature = Feature(
                        name,
                        dtype=LaminDType(ln.Record, type__uid=record_type.uid),
                        type=feature_type,
                    )
                    ctx.migrate(feature, map_to_records())
                else:
                    feature = Feature(name, dtype=LaminDType(str), type=feature_type)
                features.append(feature)
            Schema(name=f"perf_Schema_{s}", features=features)

    _plan.name = _PLAN_NAME
    return _plan


# Plans must be *constructed* before lamindb is imported (the construction guard
# rejects building one afterwards), so they live at module scope.  Both carry the
# same plan name: the second is the edited version of the first and reuses its
# lock.
_STR_PLAN = _build_plan(migrate=False)
_CAT_PLAN = _build_plan(migrate=True)


# === harness =================================================================


def _lock_file(plan_file: Path) -> Path:
    from lamindb_setup.evolve.engine._lock import lock_path

    return lock_path(plan_file, _SLUG)


def _config(plan_file: Path) -> object:
    from lamindb_setup.evolve.ci._config import CIConfig, InstanceConfig, PlanRef

    ref = PlanRef(target=f"{plan_file.name}:p", path=plan_file, var="p")
    return CIConfig(
        instances=[InstanceConfig(slug=_SLUG, plans=[ref])],
        config_path=plan_file.parent / "lamin_schemas.yml",
    )


def _loader(plan: Plan, plan_file: Path) -> Callable[[str], tuple[Plan, Path, Any]]:
    module = types.ModuleType("_perf_plan_module")
    return lambda _target: (plan, plan_file, module)


def _no_op_connect(_slug: str) -> str:
    return _SLUG


@contextmanager
def _count_queries() -> Iterator[Any]:
    """Yield a capture context; read `len(...)` on it *after* the block exits."""
    from django.db import connection
    from django.test.utils import CaptureQueriesContext

    with CaptureQueriesContext(connection) as cap:
        yield cap


@contextmanager
def _profiled(name: str) -> Iterator[None]:
    """Profile the block with pyinstrument when `LAMIN_PERF_PROFILE_DIR` is set.

    Three renderings, because each answers a different question: the pruned text
    tree shows which *engine* code is hot, the `.full.txt` one keeps the
    library frames the pruned view collapses (the dominant cost here is inside
    lamindb, so the pruned tree alone hides the answer), and the HTML flame view
    is what a human actually navigates.  Without the environment variable this is
    a no-op, so the test stays cheap by default.
    """
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


def _seed(ln: LamindbModuleType, plan_file: Path) -> None:
    """Apply the `str` plan, write its lock, and annotate records with values."""
    from lamindb.models import RecordJson
    from lamindb_setup.evolve.engine._apply import ApplyMode, compute_changes

    _STR_PLAN.reset()
    ctx = compute_changes(_STR_PLAN, mode=ApplyMode.APPLY, lock=None)
    ctx.build_lock(_PLAN_NAME).write(_lock_file(plan_file))

    # Live data the plan does not own: a sheet of records annotated with the
    # soon-to-be-migrated features.  Rows are written straight into the scalar
    # store rather than through `features.add_values` -- seeding is not what is
    # being measured, and the engine reads the same rows either way.
    sheet = ln.Record(name="PerfSheet", is_type=True).save()
    records = [
        ln.Record(name=f"perf_row_{i}", type=sheet).save() for i in range(_N_ANNOTATED)
    ]
    for s in range(_N_SCHEMAS):
        feature = ln.Feature.get(name=f"perf_feat_{s}_0", is_type=False)
        for i, record in enumerate(records):
            RecordJson(
                record=record,
                feature=feature,
                value=f"perf_target_{i % _N_TARGETS}",
            ).save()


def _scalar_rows() -> int:
    """How many scalar annotations the perf features still hold."""
    from lamindb.models import RecordJson

    return RecordJson.objects.filter(feature__name__startswith="perf_feat_").count()


def _categorical_rows() -> int:
    """How many categorical annotations the perf features hold."""
    from lamindb.models import RecordRecord

    return RecordRecord.objects.filter(feature__name__startswith="perf_feat_").count()


# === the test ================================================================


def test_dtype_migration_to_record_scales(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
    tmp_path: Path,
    record_property: Callable[[str, object], None],
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Drive `ci plan` then `ci apply` over a large str -> cat[Record] edit."""
    from contextlib import contextmanager as _contextmanager

    from lamindb_setup.evolve.ci._apply import run_apply
    from lamindb_setup.evolve.ci._pr import run_pr_check

    plan_file = tmp_path / "plan_perf.py"
    plan_file.write_text("# plan\n", encoding="utf-8")

    # Transform tracking writes Transform/Run rows and talks to the hub; it is
    # orthogonal to the resolution + migration cost under measurement.
    @_contextmanager
    def _fake_track(*_args: object, **_kwargs: object) -> Iterator[tuple[str, None]]:
        yield ("perf-transform-key", None)

    with _profiled("seed"):
        seed_start = time.perf_counter()
        _seed(ln, plan_file)
        seed_seconds = time.perf_counter() - seed_start

    assert _scalar_rows() == _N_ROWS, (
        "seeding did not write the expected number of scalar annotations"
    )

    config = _config(plan_file)

    # --- ci plan ------------------------------------------------------------
    _CAT_PLAN.reset()
    with _profiled("ci-plan"):
        plan_start = time.perf_counter()
        with _count_queries() as plan_cap:
            report = run_pr_check(
                config,
                load_plan=_loader(_CAT_PLAN, plan_file),
                connect=_no_op_connect,
            )
        plan_seconds = time.perf_counter() - plan_start
    plan_queries = len(plan_cap)

    assert not report.has_errors, [d.error for d in report.diffs if not d.ok]
    assert report.has_changes
    updates = [c for c in report.diffs[0].changes if c.action == "update"]
    assert len(updates) == _N_MIGRATING, (
        f"expected {_N_MIGRATING} feature updates, got {len(updates)}"
    )
    # The preview must not have touched the data: PLAN mode only describes the
    # migration, it never runs it.
    assert _scalar_rows() == _N_ROWS

    # --- ci apply -----------------------------------------------------------
    _CAT_PLAN.reset()
    with _profiled("ci-apply"):
        apply_start = time.perf_counter()
        with _count_queries() as apply_cap:
            run_apply(
                config,
                load_plan=_loader(_CAT_PLAN, plan_file),
                connect=_no_op_connect,
                require_stem_uid=lambda _module, _pf: "perfUID00001",
                ensure_pushed=lambda _pfs: None,
                track=_fake_track,
            )
        apply_seconds = time.perf_counter() - apply_start
    apply_queries = len(apply_cap)

    # --- outcome ------------------------------------------------------------
    migrated = ln.Feature.filter(name__startswith="perf_feat_", is_type=False).filter(
        name__endswith="_0"
    )
    assert migrated.count() == _N_MIGRATING
    for feature in migrated:
        assert feature.dtype_as_str.startswith("cat[Record["), feature.dtype_as_str
    # Every scalar annotation was relocated into the categorical link table.
    assert _scalar_rows() == 0
    assert _categorical_rows() == _N_ROWS
    assert _lock_file(plan_file).exists()

    # --- report -------------------------------------------------------------
    for key, value in (
        ("perf_objects", _N_OBJECTS),
        ("perf_migrating_features", _N_MIGRATING),
        ("perf_rows", _N_ROWS),
        ("perf_seed_seconds", round(seed_seconds, 3)),
        ("perf_plan_seconds", round(plan_seconds, 3)),
        ("perf_plan_queries", plan_queries),
        ("perf_apply_seconds", round(apply_seconds, 3)),
        ("perf_apply_queries", apply_queries),
    ):
        record_property(key, value)

    with capsys.disabled():
        print(
            f"\n[perf] {_N_OBJECTS} objects, {_N_MIGRATING} migrating features, "
            f"{_N_ROWS} annotations\n"
            f"[perf] seed      {seed_seconds:7.2f}s\n"
            f"[perf] ci plan   {plan_seconds:7.2f}s  {plan_queries:6d} queries "
            f"({plan_queries / _N_OBJECTS:.1f}/object)\n"
            f"[perf] ci apply  {apply_seconds:7.2f}s  {apply_queries:6d} queries "
            f"({apply_queries / _N_OBJECTS:.1f}/object, "
            f"{apply_queries / _N_ROWS:.1f}/annotation)"
        )

    # --- guards -------------------------------------------------------------
    assert plan_queries <= _MAX_PLAN_QUERIES_PER_OBJECT * _N_OBJECTS, (
        f"ci plan issued {plan_queries} queries for {_N_OBJECTS} objects "
        f"({plan_queries / _N_OBJECTS:.1f}/object); a per-object or per-value "
        "N+1 likely regressed."
    )
    apply_budget = _MAX_APPLY_QUERIES_PER_OBJECT * _N_OBJECTS
    assert apply_queries <= apply_budget, (
        f"ci apply issued {apply_queries} queries for {_N_OBJECTS} objects and "
        f"{_N_ROWS} relocated annotations "
        f"({apply_queries / _N_OBJECTS:.1f}/object, budget {apply_budget}); the "
        "value relocation likely regressed to per-row work."
    )
