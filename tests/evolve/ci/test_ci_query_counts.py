# Adapted source tests use deliberately invalid inputs and lightweight stand-ins.
# mypy: disable-error-code="arg-type, return-value"
"""Query-count regression guards for the DB-touching `ci` subcommands.

Each `ci` subcommand that reaches a LaminDB instance issues SQL per resolved
object.  A naive resolver re-queries a schema's members (a computed property
worth two statements per access) once per :func:`serialize_existing` call and
again for the feature diff -- a classic N+1 that only bites at scale and against
a remote Postgres.  These tests seed a real instance at two sizes and assert the
*incremental* query cost per added schema stays small, so a regression that
reintroduces per-object queries fails loudly.

The command orchestration runs through each command's real `run_*` entry point
with only the non-database seams faked: `connect` is a no-op (the `instance`
fixture is already connected) and `load_plan` returns the in-memory plan.  The
database-touching diff core -- `compute_changes` -- runs for real, so the
captured counts reflect production behavior.

Two paths are deliberately not measured end-to-end because they reconnect to the
instance, which resets Django's debug cursor and defeats query capture:

* `ci drift` first introspects the whole instance; that introspection is
  stubbed here so the *per-plan diff* it then runs (the path this optimization
  touches) is measured cleanly.
* `ci missing-exports` reconstructs the whole connected instance in one bulk
  pass whose cost is independent of the plan under test, so it is guarded with an
  absolute per-object ceiling rather than a per-schema slope.
"""

from __future__ import annotations

import types
from contextlib import contextmanager
from typing import TYPE_CHECKING

from lamindb_setup.evolve import Feature, FeatureType, LaminDType, Schema
from lamindb_setup.evolve.authoring._plan import Plan, PlanContext, create_plan

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path
    from types import SimpleNamespace as TestInstanceInfo
    from typing import Any as LamindbModuleType

# Two problem sizes; the difference isolates the *per-schema* query cost, which
# an N+1 in the reconcile path would inflate.
_SMALL = 4
_LARGE = 12
_FEATURES_PER_SCHEMA = 5

# The slug the fake `connect` returns; also the lock-file suffix.
_SLUG = "acme-dev"

# Ceiling on the extra SQL each added schema may cost.  Sharing the serialized
# snapshot and bulk-loading schema members together took this to zero: every read
# a converged run needs is batched across the whole plan, so the query count no
# longer moves between the two problem sizes at all.  Holding the ceiling at 0
# makes any reintroduced per-object read -- the naive path cost ~6/schema -- fail
# here immediately rather than after it has grown into a production problem.
_MAX_QUERIES_PER_SCHEMA = 0


def _make_plan(prefix: str, n_schemas: int) -> Plan:
    """Build a plan of `n_schemas` schemas, each with its own feature type."""

    @create_plan
    def _plan(ctx: PlanContext, exports: None) -> None:
        for s in range(n_schemas):
            ft = FeatureType(f"{prefix}_ft_{s}")
            features = [
                Feature(f"{prefix}_feat_{s}_{i}", dtype=LaminDType(str), type=ft)
                for i in range(_FEATURES_PER_SCHEMA)
            ]
            Schema(name=f"{prefix}_Schema_{s}", features=features)

    return _plan


# Plans must be *constructed* before lamindb is imported (the construction guard
# rejects building one afterwards), so they live at module scope.  Each command
# gets its own disjoint-named small/large pair so seeding both into one instance
# never collides.
_PLANS: dict[str, tuple[Plan, Plan]] = {
    name: (_make_plan(f"{name}s", _SMALL), _make_plan(f"{name}l", _LARGE))
    for name in ("plan", "apply", "drift", "missing")
}


# How many stored scalar values each feature holds in the data-safety guard.
# The batched value prefetch means the query count must not grow with this, so a
# generous per-feature value count makes a per-feature regression obvious.
_DSAFE_VALUES_PER_FEATURE = 20


def _make_dtype_plan(prefix: str, n: int, dtype: type) -> Plan:
    """Build a plan of `n` bare features (no schema) all of `dtype`."""

    @create_plan
    def _plan(ctx: PlanContext, exports: None) -> None:
        for i in range(n):
            Feature(f"{prefix}_feat_{i}", dtype=LaminDType(dtype))

    return _plan


# `str` and `int` variants share feature names within a size (so the second
# is a dtype *edit* of the first) but the small / large sizes are disjoint.
_DSAFE_PLANS: dict[str, tuple[Plan, Plan]] = {
    "small": (
        _make_dtype_plan("dss", _SMALL, str),
        _make_dtype_plan("dss", _SMALL, int),
    ),
    "large": (
        _make_dtype_plan("dsl", _LARGE, str),
        _make_dtype_plan("dsl", _LARGE, int),
    ),
}


def _lock_suffix_path(plan_file: Path) -> Path:
    from lamindb_setup.evolve.engine._lock import lock_path

    return lock_path(plan_file, _SLUG)


def _seed(plan: Plan, plan_file: Path) -> None:
    """Apply `plan` to the connected instance and write its per-instance lock."""
    from lamindb_setup.evolve.engine._apply import ApplyMode, compute_changes

    plan.reset()
    ctx = compute_changes(plan, mode=ApplyMode.APPLY, lock=None)
    ctx.build_lock(plan.name).write(_lock_suffix_path(plan_file))


def _config(plan_file: Path, var: str, slug_raw: str) -> object:
    from lamindb_setup.evolve.ci._config import CIConfig, InstanceConfig, PlanRef

    ref = PlanRef(target=f"{plan_file.name}:{var}", path=plan_file, var=var)
    return CIConfig(
        instances=[InstanceConfig(slug=slug_raw, plans=[ref])],
        config_path=plan_file.parent / "lamin_schemas.yml",
    )


def _loader(plan: Plan, plan_file: Path) -> Callable[[str], tuple[Plan, Path, object]]:
    module = types.ModuleType("_fake_plan_module")
    return lambda _target: (plan, plan_file, module)


def _no_op_connect(_slug: str) -> str:
    return _SLUG


def _seed_pair(
    name: str, tmp_path: Path
) -> tuple[tuple[Plan, Path], tuple[Plan, Path]]:
    """Seed the small and large plans of a command into the connected instance."""
    small_plan, large_plan = _PLANS[name]
    small_file = tmp_path / "plan_small.py"
    large_file = tmp_path / "plan_large.py"
    small_file.write_text("# plan\n", encoding="utf-8")
    large_file.write_text("# plan\n", encoding="utf-8")
    _seed(small_plan, small_file)
    _seed(large_plan, large_file)
    return (small_plan, small_file), (large_plan, large_file)


@contextmanager
def _count_queries() -> Iterator[object]:
    """Yield a capture context; read `len(...)` on it *after* the block exits.

    Transaction control is not counted: lamindb >= 2.11 wraps every
    `Feature.save()` in its own `transaction.atomic()`.
    """
    from django.db import connection
    from django.test.utils import CaptureQueriesContext

    class _DataQueries(CaptureQueriesContext):
        def __len__(self) -> int:
            return sum(
                not q["sql"].startswith(("BEGIN", "COMMIT", "SAVEPOINT", "RELEASE"))
                for q in self.captured_queries
            )

    with _DataQueries(connection) as cap:
        yield cap


def _assert_low_slope(
    small: int, large: int, max_per_schema: int | None = None
) -> None:
    """Assert the per-schema query cost between the two sizes stays bounded.

    `max_per_schema` overrides the default ceiling for a phase that does
    inherent per-object *write* work, which no batching can remove.
    """
    ceiling = _MAX_QUERIES_PER_SCHEMA if max_per_schema is None else max_per_schema
    # A guard that captured nothing would spuriously pass at a zero slope; a real
    # run touches the instance, so require some queries at the small size.
    assert small > 0, "no queries captured; the measurement is not exercising the DB"
    per_schema = (large - small) / (_LARGE - _SMALL)
    assert per_schema <= ceiling, (
        f"query count scales too steeply with schema count: {small} queries at "
        f"{_SMALL} schemas, {large} at {_LARGE} -> {per_schema:.1f}/schema "
        f"(> {ceiling}); a per-object N+1 likely regressed."
    )


# --- ci plan ----------------------------------------------------------------


def test_ci_plan_query_count_stays_low(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    from lamindb_setup.evolve.ci._pr import run_pr_check

    (small_plan, small_file), (large_plan, large_file) = _seed_pair("plan", tmp_path)

    def measure(plan: Plan, plan_file: Path) -> int:
        with _count_queries() as cap:
            report = run_pr_check(
                _config(plan_file, "p", instance.slug),
                load_plan=_loader(plan, plan_file),
                connect=_no_op_connect,
            )
        assert not report.has_errors
        # A re-run against the seeded instance is all no-change.
        assert not report.has_changes
        return len(cap)  # type: ignore[arg-type]

    _assert_low_slope(measure(small_plan, small_file), measure(large_plan, large_file))


# --- ci apply ---------------------------------------------------------------


def test_ci_apply_query_count_stays_low(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
    tmp_path: Path,
) -> None:
    from lamindb_setup.evolve.ci._apply import run_apply

    # Stub the transform-tracking context manager so no Transform/Run rows (and
    # their queries) are written -- we measure only the plan-resolution path.
    @contextmanager
    def _fake_track(*_args: object, **_kwargs: object) -> Iterator[tuple[str, None]]:
        yield ("fake-transform-key", None)

    (small_plan, small_file), (large_plan, large_file) = _seed_pair("apply", tmp_path)

    def measure(plan: Plan, plan_file: Path) -> int:
        with _count_queries() as cap:
            run_apply(
                _config(plan_file, "p", instance.slug),
                load_plan=_loader(plan, plan_file),
                connect=_no_op_connect,
                require_stem_uid=lambda _module, _pf: "stemUID00001",
                ensure_pushed=lambda _pfs: None,
                track=_fake_track,
            )
        return len(cap)  # type: ignore[arg-type]

    _assert_low_slope(measure(small_plan, small_file), measure(large_plan, large_file))


# --- ci drift ---------------------------------------------------------------


class _FakeIntroResult:
    """Minimal stand-in for an `IntrospectionResult` (drift reads a few attrs)."""

    def __init__(self, output: Path) -> None:
        self.output = output
        self.reconstructed = 0
        self.errors: list[str] = []
        self.warnings: list[str] = []
        self.summary = None
        self.out_of_scope_uids: dict[str, int] = {}


def test_ci_drift_query_count_stays_low(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # `ci drift` introspects the whole instance (a reconnect that resets query
    # capture) and *then* diffs each plan through the real `compute` hook -- the
    # path this optimization touches.  Stub only the introspection so the diff's
    # per-schema query cost is measured cleanly.
    from lamindb_setup.evolve.ci._drift import run_drift_detection

    (small_plan, small_file), (large_plan, large_file) = _seed_pair("drift", tmp_path)

    def fake_introspect(instance_slug: str, output: Path, **_kwargs: object) -> object:
        return _FakeIntroResult(output)

    def measure(plan: Plan, plan_file: Path) -> int:
        with _count_queries() as cap:
            result = run_drift_detection(
                _config(plan_file, "p", instance.slug),
                tmp_path,
                load_plan=_loader(plan, plan_file),
                connect=_no_op_connect,
                introspect=fake_introspect,
            )
        # The plan diff ran (its lock exists), so drift touched the instance.
        assert not result.instances[0].skipped_plans
        return len(cap)  # type: ignore[arg-type]

    _assert_low_slope(measure(small_plan, small_file), measure(large_plan, large_file))


# --- ci missing-exports -----------------------------------------------------


def test_ci_missing_exports_query_count_stays_low(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # `ci missing-exports` reconstructs the whole connected instance in one bulk
    # pass; its cost tracks the instance's total object count, not the plan under
    # test.  Guard it with an absolute ceiling a per-object read regression would
    # blow past (the seeded instance holds ~112 objects).
    from lamindb_setup.evolve.ci._missing_exports import run_missing_exports

    (_small_plan, _small_file), (large_plan, large_file) = _seed_pair(
        "missing", tmp_path
    )
    seeded_objects = (_SMALL + _LARGE) * (2 + _FEATURES_PER_SCHEMA)

    with _count_queries() as cap:
        result = run_missing_exports(
            _config(large_file, "p", instance.slug),
            load_plan=_loader(large_plan, large_file),
            connect=_no_op_connect,
        )
    assert not result.instances[0].errors
    n_queries = len(cap)  # type: ignore[arg-type]
    assert n_queries > 0, (
        "no queries captured; the measurement is not exercising the DB"
    )
    # A bulk read is a handful of statements; a per-object N+1 would approach the
    # object count. Half the seeded objects is a comfortable N+1 tripwire.
    assert n_queries <= seeded_objects // 2, (
        f"missing-exports issued {n_queries} queries for ~{seeded_objects} objects; "
        "a per-object read likely regressed."
    )


# --- ci verify-config -------------------------------------------------------


def test_ci_verify_config_issues_no_queries(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # verify-config is a purely offline check; it must never touch the instance.
    from lamindb_setup.evolve.ci._config import CIConfig
    from lamindb_setup.evolve.ci._verify import verify_config

    (tmp_path / "plans").mkdir()
    cfg = CIConfig(instances=[], config_path=tmp_path / "lamin_schemas.yml")
    with _count_queries() as cap:
        verify_config(cfg)
    assert len(cap) == 0  # type: ignore[arg-type]


# --- data-safety hint batching ----------------------------------------------


def test_data_safety_hint_query_count_stays_flat(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # The data-safety checks (`_data_safe_reason` / `_override_hint`) read a
    # feature's stored values and validate them.  Unbatched, that is a value fetch
    # plus `exists` / `count` per feature (and historically one query *per
    # value* for a categorical target); batched, it is a couple of
    # `feature_id__in` reads for the whole plan plus a memoized categorical
    # lookup.  Seed every feature with many values and drive the flag-on lift path
    # (`str -> int` where all values parse), then assert the per-feature query
    # cost stays bounded -- a regression to per-feature (or per-value) queries
    # blows the slope.
    #
    # Both passes of a real `apply` are measured -- the PLAN preview and the
    # APPLY pass (`prefetch=False`, sharing the preview's existence cache, as
    # the CLI runs it) -- so `apply` is guarded to be as fast as `plan`.  The
    # APPLY pass additionally writes one `save` per narrowed feature; that is
    # inherent work, not data-safety overhead, and stays within the same slope.

    from lamindb.models import JsonValue
    from lamindb_setup.evolve.engine._apply import ApplyMode, compute_changes

    def seed_and_measure(
        prefix: str, n: int, plans: tuple[Plan, Plan]
    ) -> tuple[int, int]:
        str_plan, int_plan = plans
        str_plan.reset()
        seeded = compute_changes(str_plan, mode=ApplyMode.APPLY, lock=None)
        lock = seeded.build_lock(str_plan.name)
        for i in range(n):
            feature = ln.Feature.get(name=f"{prefix}_feat_{i}", is_type=False)
            for value in range(_DSAFE_VALUES_PER_FEATURE):
                JsonValue(feature=feature, value=str(value)).save()

        # Mirror the CLI: one finalized plan, a shared existence cache, a PLAN
        # preview then an APPLY pass that reuses that cache (`prefetch=False`).
        int_plan.reset()
        from lamindb_setup.evolve.engine._apply import ExistenceCache

        existing_cache = ExistenceCache()
        with _count_queries() as cap_preview:
            preview = compute_changes(
                int_plan,
                mode=ApplyMode.PLAN,
                lock=lock,
                existing_cache=existing_cache,
                allow_data_validated_changes=True,
            )
        with _count_queries() as cap_apply:
            applied = compute_changes(
                int_plan,
                mode=ApplyMode.APPLY,
                lock=lock,
                existing_cache=existing_cache,
                prefetch=False,
                allow_data_validated_changes=True,
            )
        # Every feature's block was lifted as data-safe (all values parse as int),
        # so both passes resolved without an early raise and the data-safety path
        # ran for all of them; the APPLY pass narrowed the dtype in place.
        for result in (preview, applied):
            lifted = [c for c in result.changes if c.kind == "Feature"]
            assert len(lifted) == n, f"expected {n} feature changes, got {len(lifted)}"
        assert ln.Feature.get(name=f"{prefix}_feat_0", is_type=False).dtype_as_str == (
            "int"
        )
        return len(cap_preview), len(cap_apply)  # type: ignore[arg-type]

    small_preview, small_apply = seed_and_measure("dss", _SMALL, _DSAFE_PLANS["small"])
    large_preview, large_apply = seed_and_measure("dsl", _LARGE, _DSAFE_PLANS["large"])
    # The preview only reads, so it must stay flat.  The APPLY pass additionally
    # saves each narrowed feature -- one write per feature that batching cannot
    # remove -- so it is allowed exactly that one statement of slope.
    _assert_low_slope(small_preview, large_preview)
    _assert_low_slope(small_apply, large_apply, max_per_schema=1)
