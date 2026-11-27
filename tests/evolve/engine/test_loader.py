# Adapted source tests include deliberately invalid declarations and dynamic models.
# mypy: disable-error-code="list-item"
"""Tests for lamindb_setup.evolve.engine._loader."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from lamindb_setup.evolve import Feature, FeatureType, LaminDType
from lamindb_setup.evolve.authoring._plan import (
    Plan,
    PlanContext,
    PlanExports,
    create_plan,
)
from lamindb_setup.evolve.engine._loader import (
    PlanImportError,
    PlanTargetError,
    StemUidError,
    import_plan_module,
    load_all_plans,
    load_plan,
    order_dependencies_first,
    parse_target,
    plan_target,
    project_root,
    project_root_override,
    require_stem_uid,
    sibling_claimed_keys,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

_PLAN_CODE = """
from lamindb_setup.evolve import Feature
from lamindb_setup.evolve import FeatureType
from lamindb_setup.evolve import LaminDType
from lamindb_setup.evolve import PlanContext
from lamindb_setup.evolve import create_plan

PLAN_TRANSFORM_STEM_UID = "testUID00001"


@create_plan
def {name}(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("{name}_ft")
    Feature("{name}_col", dtype=LaminDType(str), type=ft)
"""


def _write_plan(path: Path, name: str, header: str = "") -> Path:
    """Write a plan file declaring the plan `name`, prefixed with `header`."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(header + _PLAN_CODE.format(name=name), encoding="utf-8")
    return path


@pytest.fixture(autouse=True)
def _isolated_imports() -> Iterator[None]:
    """Undo what importing a plan adds to `sys.modules` and `sys.path`."""
    before = set(sys.modules)
    path = list(sys.path)
    try:
        yield
    finally:
        for name in set(sys.modules) - before:
            del sys.modules[name]
        sys.path[:] = path


def test_absolute_cross_directory_import_resolves(tmp_path: Path) -> None:
    # A plan reaches a plan in another directory by its full dotted path, with
    # nothing on PYTHONPATH: the project root -- here the pyproject.toml's
    # directory -- is on sys.path while the plan is imported, and the
    # directories in between are namespace packages.
    (tmp_path / "pyproject.toml").write_text("[project]\n", encoding="utf-8")
    _write_plan(tmp_path / "plans" / "b" / "plan_y.py", "y_plan")
    entry = _write_plan(
        tmp_path / "plans" / "a" / "plan_x.py",
        "x_plan",
        header="from plans.b.plan_y import y_plan  # noqa: F401\n",
    )

    module = import_plan_module(entry)

    assert module.__name__ == "plans.a.plan_x"
    assert sys.modules["plans.b.plan_y"].y_plan is module.y_plan


def test_nested_bare_sibling_import_fails(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text("[project]\n", encoding="utf-8")
    _write_plan(tmp_path / "plans" / "plan_c.py", "c_plan")
    _write_plan(
        tmp_path / "plans" / "plan_b.py",
        "b_plan",
        header="from plan_c import c_plan  # noqa: F401\n",
    )
    entry = _write_plan(
        tmp_path / "plans" / "plan_a.py",
        "a_plan",
        header="from plans.plan_b import b_plan  # noqa: F401\n",
    )

    with pytest.raises(PlanImportError, match="plan_c.*project root"):
        import_plan_module(entry)


def test_entry_target_and_imported_plan_are_one_module(tmp_path: Path) -> None:
    # The canonical dotted name makes the two ways of reaching a file -- as an
    # entry target and through another plan's import -- the same module, so the
    # plan it declares is one object rather than two.
    (tmp_path / "pyproject.toml").write_text("[project]\n", encoding="utf-8")
    shared = _write_plan(tmp_path / "plans" / "plan_a.py", "a_plan")
    other = _write_plan(
        tmp_path / "plans" / "plan_b.py",
        "b_plan",
        header="from plans.plan_a import a_plan\n",
    )

    module_a = import_plan_module(shared)
    module_b = import_plan_module(other)

    assert module_b.a_plan is module_a.a_plan
    assert import_plan_module(shared) is module_a


def test_plan_target_names_the_file_a_plan_was_loaded_from(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "pyproject.toml").write_text("[project]\n", encoding="utf-8")
    single = _write_plan(tmp_path / "plans" / "plan_a.py", "a_plan")
    # Two plans in one file: a target has to say which one.
    double = _write_plan(
        tmp_path / "plans" / "plan_bc.py", "c_plan", header=_PLAN_CODE.format(name="b")
    )
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    a_plan = import_plan_module(single).a_plan
    c_plan = import_plan_module(double).c_plan

    monkeypatch.chdir(tmp_path)
    assert plan_target(a_plan) == str(Path("plans", "plan_a.py"))
    assert plan_target(c_plan) == f"{Path('plans', 'plan_bc.py')}:c_plan"

    # Outside the working directory the path stays absolute.
    monkeypatch.chdir(elsewhere)
    assert plan_target(a_plan) == str(single.resolve())

    # A plan whose module was not loaded from a file names no target.
    monkeypatch.delitem(sys.modules, "plans.plan_a")
    assert plan_target(a_plan) is None


def test_project_root_walks_up_to_the_nearest_marker(tmp_path: Path) -> None:
    plan = tmp_path / "repo" / "plans" / "plan_a.py"
    plan.parent.mkdir(parents=True)
    plan.touch()

    # No marker at all: the plan's own directory, the historical behavior.
    assert project_root(plan) == plan.parent

    (tmp_path / "repo" / ".git").mkdir()
    assert project_root(plan) == tmp_path / "repo"

    # A pyproject.toml outranks a .git, and a lamin_schemas.yml outranks both,
    # however deep each one sits.
    (tmp_path / "repo" / "plans" / "pyproject.toml").touch()
    assert project_root(plan) == tmp_path / "repo" / "plans"

    (tmp_path / "repo" / "lamin_schemas.yml").touch()
    assert project_root(plan) == tmp_path / "repo"


def test_project_root_reads_the_config_key(tmp_path: Path) -> None:
    plan = tmp_path / "repo" / "conf" / "plans" / "plan_a.py"
    plan.parent.mkdir(parents=True)
    plan.touch()
    config = tmp_path / "repo" / "conf" / "lamin_schemas.yml"

    # The walk-up lands on the config, which points the root one level higher.
    config.write_text(
        "project_root: ..\ninstances:\n  - slug: a/b\n    plans: [plans/plan_a.py]\n",
        encoding="utf-8",
    )
    assert project_root(plan) == tmp_path / "repo"

    # No key: the config's own directory, as before.
    config.write_text(
        "instances:\n  - slug: a/b\n    plans: [plans/plan_a.py]\n", encoding="utf-8"
    )
    assert project_root(plan) == config.parent

    # A configuration this walk-up cannot read is the business of the command
    # that was pointed at one, not of an unrelated 'validate'.
    config.write_text("project_root: [not, a, string\n", encoding="utf-8")
    assert project_root(plan) == config.parent

    config.write_text("project_root: nowhere\n", encoding="utf-8")
    assert project_root(plan) == config.parent


def test_project_root_override_wins_and_does_not_nest(tmp_path: Path) -> None:
    plan = tmp_path / "repo" / "plans" / "plan_a.py"
    plan.parent.mkdir(parents=True)
    plan.touch()
    (tmp_path / "repo" / "pyproject.toml").touch()

    with project_root_override(tmp_path / "elsewhere"):
        assert project_root(plan) == tmp_path / "elsewhere"
        # The outer override is what the run was told to use: a configuration
        # file's root, applied inside it, does not displace it.
        with project_root_override(tmp_path / "repo"):
            assert project_root(plan) == tmp_path / "elsewhere"

    assert project_root(plan) == tmp_path / "repo"


def test_falls_back_when_a_path_segment_is_not_an_identifier(tmp_path: Path) -> None:
    # 'my-plans' cannot appear in an import statement, so the file has no
    # module name under the root and is imported by location instead.
    (tmp_path / "pyproject.toml").write_text("[project]\n", encoding="utf-8")
    entry = _write_plan(tmp_path / "my-plans" / "plan_x.py", "x_plan")

    module = import_plan_module(entry)

    assert module.__name__ == "_lamin_schema_plan_plan_x"
    assert module.x_plan.name == "x_plan"


def test_falls_back_when_the_dotted_name_is_another_module(tmp_path: Path) -> None:
    # A plan file whose dotted name is already taken -- here by the stdlib's
    # json -- is imported by location, and the module that owns the name is
    # left alone.
    (tmp_path / "pyproject.toml").write_text("[project]\n", encoding="utf-8")
    entry = _write_plan(tmp_path / "json.py", "json_plan")

    module = import_plan_module(entry)

    assert module.__name__ == "_lamin_schema_plan_json"
    assert module.json_plan.name == "json_plan"
    assert sys.modules["json"] is json


def test_rejects_a_plan_loaded_under_two_names(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A bare sibling import reaches the file through the plan directory rather
    # than the root, so it executes a second copy under a second name.
    (tmp_path / "pyproject.toml").write_text("[project]\n", encoding="utf-8")
    shared = _write_plan(tmp_path / "plans" / "plan_a.py", "a_plan")
    other = _write_plan(
        tmp_path / "plans" / "plan_b.py",
        "b_plan",
        header="from plan_a import a_plan\n",
    )

    monkeypatch.syspath_prepend(str(shared.parent))
    import_plan_module(shared)
    with pytest.raises(PlanImportError, match="plans.plan_a.*instead of 'plan_a'"):
        import_plan_module(other)


# --- from a target to the plans a command acts on ---------------------------


@create_plan
def _sib_plan_x(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("sib_ft")
    Feature("shared", dtype=LaminDType(str), type=ft)
    Feature("only_x", dtype=LaminDType(str), type=ft)


@create_plan
def _sib_plan_y(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("sib_ft")
    Feature("shared", dtype=LaminDType(str), type=ft)
    Feature("only_y", dtype=LaminDType(str), type=ft)


@create_plan
def _lone_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("lone_ft")
    Feature("lone_col", dtype=LaminDType(str), type=ft)


class _OrderUpstreamExports(PlanExports):
    ft: FeatureType


@create_plan
def _order_upstream_plan(ctx: PlanContext, exports: _OrderUpstreamExports) -> None:
    exports.ft = FeatureType("order_upstream_ft")


@create_plan
def _order_child_plan(
    ctx: PlanContext,
    exports: None,
    upstream: Plan[_OrderUpstreamExports] = _order_upstream_plan,
) -> None:
    FeatureType("order_child_ft", type=upstream.exports.ft)


def test_order_dependencies_first_puts_dependency_before_dependent() -> None:
    # The child depends on the upstream plan, so regardless of the listed order
    # the upstream plan must come first (it is created before / deleted after).
    upstream_item = (_order_upstream_plan, Path("plan_upstream.py"), None)
    child_item = (_order_child_plan, Path("plan_child.py"), None)

    ordered = order_dependencies_first([child_item, upstream_item])

    assert [item[0] for item in ordered] == [_order_upstream_plan, _order_child_plan]


def test_order_dependencies_first_preserves_order_of_independent_plans() -> None:
    x_item = (_sib_plan_x, Path("plan_x.py"), None)
    y_item = (_sib_plan_y, Path("plan_y.py"), None)

    ordered = order_dependencies_first([x_item, y_item])

    assert [item[0] for item in ordered] == [_sib_plan_x, _sib_plan_y]


def test_parse_target_valid(tmp_path: Path) -> None:
    plan_file = tmp_path / "plan_x.py"
    plan_file.write_text("x = 1\n")
    path, var = parse_target(f"{plan_file}:my_plan")
    assert path == plan_file
    assert var == "my_plan"


def test_parse_target_allows_missing_variable(tmp_path: Path) -> None:
    plan_file = tmp_path / "plan_x.py"
    plan_file.write_text("x = 1\n")
    path, var = parse_target(str(plan_file))
    assert path == plan_file
    assert var is None


def test_sibling_claimed_keys_unions_other_plans() -> None:
    from lamindb_setup.evolve.engine._apply import plan_natural_keys
    from lamindb_setup.evolve.engine._lock import natural_key

    claimed = sibling_claimed_keys([_sib_plan_x, _sib_plan_y])

    # Each plan is given exactly the keys declared by the other plan.
    assert claimed[0] == plan_natural_keys(_sib_plan_y)
    assert claimed[1] == plan_natural_keys(_sib_plan_x)

    only_y = next(
        d.obj
        for d in _sib_plan_y.dependencies
        if getattr(d.obj, "name", "") == "only_y"
    )
    assert natural_key(only_y) in claimed[0]


def test_sibling_claimed_keys_single_plan_is_empty() -> None:
    # A standalone plan claims nothing, preserving single-plan delete behavior.
    assert sibling_claimed_keys([_lone_plan]) == [set()]


def test_parse_target_rejects_trailing_colon(tmp_path: Path) -> None:
    plan_file = tmp_path / "plan_x.py"
    plan_file.write_text("x = 1\n")
    with pytest.raises(PlanTargetError):
        parse_target(f"{plan_file}:")


def test_parse_target_rejects_missing_file(tmp_path: Path) -> None:
    with pytest.raises(PlanTargetError):
        parse_target(f"{tmp_path / 'nope.py'}:my_plan")


def test_plan_target_errors_are_library_errors_not_cli_ones(tmp_path: Path) -> None:
    # A Python caller gets the package's own error -- which is also a
    # ValueError -- rather than a CLI framework's usage exception.
    from lamindb_setup.evolve.core import InvalidArgumentError, LaminSchemasError

    with pytest.raises(PlanTargetError) as exc:
        load_plan(str(tmp_path / "nope.py"))

    assert isinstance(exc.value, InvalidArgumentError)
    assert isinstance(exc.value, LaminSchemasError)
    assert isinstance(exc.value, ValueError)
    assert not any(
        cls.__module__.startswith(("typer", "click")) for cls in type(exc.value).__mro__
    )


def test_a_plan_variable_not_found_lists_the_ones_the_file_defines(
    tmp_path: Path,
) -> None:
    # Only the plans the file defines are offered, not one it imports.
    _write_plan(tmp_path / "plan_other.py", "other")
    plan_file = _write_plan(
        tmp_path / "plan_two.py",
        "child",
        header="from plan_other import other  # noqa: F401\n"
        + _PLAN_CODE.format(name="base"),
    )

    with pytest.raises(PlanTargetError) as exc:
        load_plan(f"{plan_file}:nope")

    assert str(exc.value) == (
        f"plan variable 'nope' not found in {plan_file}; available: base, child"
    )


def test_a_target_naming_no_plan_lists_the_ones_the_file_defines(
    tmp_path: Path,
) -> None:
    plan_file = _write_plan(tmp_path / "plan_one.py", "only")

    with pytest.raises(PlanTargetError) as exc:
        load_plan(f"{plan_file}:PLAN_TRANSFORM_STEM_UID")

    assert str(exc.value) == (
        f"'PLAN_TRANSFORM_STEM_UID' in {plan_file} is not a Plan (got str); "
        "available: only"
    )


def test_a_plan_variable_not_found_in_a_file_without_plans_says_so(
    tmp_path: Path,
) -> None:
    plan_file = tmp_path / "plan_none.py"
    plan_file.write_text("x = 1\n", encoding="utf-8")

    with pytest.raises(PlanTargetError, match="; it defines no plan$"):
        load_plan(f"{plan_file}:nope")


@pytest.mark.parametrize(
    ("value", "message"),
    [
        (None, "does not define PLAN_TRANSFORM_STEM_UID; add one with"),
        ("not-a-uid", "is not a valid 12-character base62 stem uid"),
    ],
)
def test_require_stem_uid_raises_instead_of_exiting(
    tmp_path: Path, value: str | None, message: str
) -> None:
    from types import ModuleType

    module = ModuleType("plan_without_uid")
    if value is not None:
        module.PLAN_TRANSFORM_STEM_UID = value  # type: ignore[attr-defined]

    with pytest.raises(StemUidError, match=message):
        require_stem_uid(module, tmp_path / "plan_without_uid.py")


def test_load_all_plans_rejects_path_aliases(tmp_path: Path) -> None:
    plan_file = _write_plan(tmp_path / "plan_cli.py", "my_plan")
    targets = [
        f"{plan_file}:my_plan",
        f"{tmp_path}/./plan_cli.py:my_plan",
    ]

    with pytest.raises(PlanTargetError, match="listed more than once"):
        load_all_plans(targets)


def test_load_all_plans_rejects_implicit_and_explicit_aliases(
    tmp_path: Path,
) -> None:
    plan_file = _write_plan(tmp_path / "plan_cli.py", "my_plan")
    targets = [str(plan_file), f"{plan_file}:my_plan"]

    with pytest.raises(PlanTargetError, match="listed more than once"):
        load_all_plans(targets)


def test_list_all_plans_and_select_each_by_name(tmp_path: Path) -> None:
    from lamindb_setup.evolve.engine._loader import load_plans_in_file

    path = _write_plan(tmp_path / "several.py", "first")
    path.write_text(path.read_text() + _PLAN_CODE.format(name="second"))
    with pytest.raises(PlanTargetError, match="several plans .*first, second"):
        load_plan(str(path))
    found = load_plans_in_file(path)
    assert [plan.name for plan, _, _ in found] == ["first", "second"]
    assert load_all_plans([f"{path}:first", f"{path}:second"]) == found
    assert require_stem_uid(found[0][2], path) == "testUID00001"

    empty = tmp_path / "empty.py"
    empty.write_text("value = 1\n")
    with pytest.raises(PlanTargetError, match="no Plan defined"):
        load_plans_in_file(empty)
    with pytest.raises(PlanTargetError, match="no Plan defined"):
        load_plan(str(empty))
    broken = tmp_path / "broken.py"
    broken.write_text("from missing_plan_package import feature\n")
    with pytest.raises(PlanTargetError, match="missing_plan_package"):
        load_plans_in_file(broken)


def test_import_fallbacks_for_files_outside_the_project_root(
    tmp_path: Path, monkeypatch
) -> None:
    from types import ModuleType

    from lamindb_setup.evolve.engine import _loader

    path = tmp_path / "isolated.py"
    assert _loader._dotted_name(path, tmp_path / "project") is None
    assert _loader._import_dotted("absent_schema_plan", path) is None
    monkeypatch.setitem(
        sys.modules, "existing_schema_plan", ModuleType("existing_schema_plan")
    )
    assert _loader._import_dotted("existing_schema_plan", path) is None
    with pytest.raises(PlanImportError, match="could not import"):
        _loader._import_by_location(tmp_path / "plan.txt")
