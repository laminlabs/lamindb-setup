"""Tests for lamindb_setup.evolve.maintenance._sheet_schemas."""

from __future__ import annotations

from typing import TYPE_CHECKING

from lamindb_setup.evolve import Feature, LaminDType, RecordType, Schema, Sheet
from lamindb_setup.evolve.authoring._plan import PlanContext, create_plan
from lamindb_setup.evolve.engine._apply import ApplyMode, compute_changes
from lamindb_setup.evolve.introspect._read import (
    InstanceContents,
    RecordTypeInfo,
    SchemaTypeInfo,
    read_instance,
)
from lamindb_setup.evolve.introspect._reverse import build_plan
from lamindb_setup.evolve.maintenance._sheet_schemas import (
    compute_sheet_schemas,
    execute_sheet_schemas,
    render_sheet_schemas,
)

if TYPE_CHECKING:
    from types import SimpleNamespace as TestInstanceInfo
    from typing import Any as LamindbModuleType


def _contents(*, feature_uids: list[str] | None = None) -> InstanceContents:
    return InstanceContents(
        schema_types=[
            SchemaTypeInfo(
                uid="ust1",
                name="MySchemas",
                description=None,
                type_uid=None,
                feature_uids=feature_uids or [],
            )
        ],
        record_types=[
            RecordTypeInfo(
                uid="urt1",
                name="Experiments",
                description=None,
                type_uid=None,
                schema_uid="ust1",
            ),
        ],
    )


# --- unit tests (no instance) -----------------------------------------------


def test_a_clean_instance_reports_nothing() -> None:
    contents = _contents()
    contents.record_types[0].schema_uid = None

    plan = compute_sheet_schemas(contents)

    assert plan.is_empty
    assert "no record type is pointed at a schema type" in render_sheet_schemas(
        plan, title="t"
    )


def test_an_empty_schema_type_is_detachable() -> None:
    plan = compute_sheet_schemas(_contents())

    assert [issue.uid for issue in plan.fixable] == ["urt1"]
    assert plan.unfixable == []
    text = render_sheet_schemas(plan, title="Sheet schemas")
    assert "Experiments" in text
    assert "'MySchemas' (ust1)" in text


def test_a_schema_type_with_members_needs_a_human() -> None:
    # It is a schema mislabeled as a type: detaching would drop the enforcement
    # its members do provide, so it is reported and never changed.
    plan = compute_sheet_schemas(_contents(feature_uids=["uf1"]))

    assert plan.fixable == []
    assert [issue.uid for issue in plan.unfixable] == ["urt1"]
    assert "carries members" in plan.unfixable[0].reason


def test_a_record_type_with_a_real_schema_is_ignored() -> None:
    contents = _contents()
    contents.record_types[0].schema_uid = "us1"

    assert compute_sheet_schemas(contents).is_empty


# --- integration tests (against an instance) ---------------------------------


@create_plan
def _seed_plan(ctx: PlanContext, exports: None) -> None:
    schema = Schema(name="ShSheet", features=[Feature("sh_col", dtype=LaminDType(str))])
    Sheet("ShExperiments", schema=schema, type=RecordType("ShRecords"))


def _degenerate_sheet(ln: LamindbModuleType) -> None:
    """Point the seeded sheet at a schema type, as lamindb's FK allows."""
    compute_changes(_seed_plan, mode=ApplyMode.APPLY, lock=None)
    schema_type = ln.Schema(name="ShSchemas", is_type=True).save()
    sheet = ln.Record.get(name="ShExperiments", is_type=True)
    sheet.schema = schema_type
    sheet.save()


def test_a_degenerate_sheet_is_found_and_detached(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    _degenerate_sheet(ln)

    plan = compute_sheet_schemas(read_instance())
    assert [issue.name for issue in plan.fixable] == ["ShExperiments"]

    assert execute_sheet_schemas(plan) == 1
    assert ln.Record.get(name="ShExperiments", is_type=True).schema is None
    assert compute_sheet_schemas(read_instance()).is_empty


def test_detaching_makes_the_instance_reconstructable(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    _degenerate_sheet(ln)

    before = build_plan(read_instance())
    assert any("is a schema type" in error for error in before.errors)

    execute_sheet_schemas(compute_sheet_schemas(read_instance()))

    after = build_plan(read_instance())
    assert after.errors == []
    # The sheet is expressible again, now as a plain record type.
    assert "ShExperiments" in {
        spec.name for spec in after.specs if spec.kind == "RecordType"
    }
