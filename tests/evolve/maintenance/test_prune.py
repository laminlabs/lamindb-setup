"""Tests for lamindb_setup.evolve.maintenance._prune."""

from __future__ import annotations

from typing import TYPE_CHECKING

from lamindb_setup.evolve import Feature, FeatureType, LaminDType, Schema
from lamindb_setup.evolve.authoring._plan import PlanContext, create_plan
from lamindb_setup.evolve.engine._apply import ApplyMode, compute_changes
from lamindb_setup.evolve.introspect._read import (
    FeatureInfo,
    FeatureTypeInfo,
    InstanceContents,
    ObjectMeta,
    RecordTypeInfo,
    SchemaInfo,
    read_instance,
)
from lamindb_setup.evolve.maintenance._prune import (
    compute_prune,
    execute_prune,
    render_prune,
)

if TYPE_CHECKING:
    from types import SimpleNamespace as TestInstanceInfo
    from typing import Any as LamindbModuleType


def _schema(uid: str, name: str | None, **kwargs: object) -> SchemaInfo:
    defaults: dict[str, object] = {
        "description": None,
        "type_uid": None,
        "itype": None,
        "otype": "DataFrame",
        "schema_dtype": None,
        "flexible": None,
        "coerce": None,
        "minimal_set": True,
        "ordered_set": False,
        "maximal_set": False,
    }
    defaults.update(kwargs)
    return SchemaInfo(uid=uid, name=name, **defaults)  # type: ignore[arg-type]


# --- pure unit tests on hand-built InstanceContents -------------------------


def test_compute_prune_finds_feature_referencing_deleted_type() -> None:
    contents = InstanceContents(
        feature_types=[FeatureTypeInfo(uid="uft", name="group", type_uid=None)],
        features=[FeatureInfo(uid="uf", name="col", type_uid="uft", dtype_str="str")],
        deleted_uids={"uft"},
    )

    plan = compute_prune(contents)

    assert [o.uid for o in plan.objects] == ["uf"]
    obj = plan.objects[0]
    assert obj.kind == "feature"
    assert obj.registry == "Feature"
    assert obj.refs[0].relation == "type"
    assert obj.refs[0].target_uid == "uft"
    assert obj.refs[0].target_deleted is True


def test_compute_prune_cascades_to_indirect_dependents() -> None:
    # schema -> member feature -> deleted type. Trashing the type makes the
    # feature dangling, which in turn makes the schema dangling.
    contents = InstanceContents(
        features=[FeatureInfo(uid="uf", name="col", type_uid="uft", dtype_str="str")],
        schemas=[_schema("us", "S", feature_uids=["uf"])],
        deleted_uids={"uft"},
    )

    plan = compute_prune(contents)

    by_uid = {o.uid: o for o in plan.objects}
    assert set(by_uid) == {"uf", "us"}
    # feature is discovered first (direct), schema second (cascade)
    assert [o.uid for o in plan.objects] == ["uf", "us"]
    schema_ref = by_uid["us"].refs[0]
    assert schema_ref.relation == "features"
    assert schema_ref.target_uid == "uf"
    # the member it references was not originally deleted, only pruned
    assert schema_ref.target_deleted is False
    # the referenced object is named, not just uid'd
    assert schema_ref.target_name == "col"


def test_compute_prune_reports_nothing_when_no_deletions() -> None:
    contents = InstanceContents(
        feature_types=[FeatureTypeInfo(uid="uft", name="group", type_uid=None)],
        features=[FeatureInfo(uid="uf", name="col", type_uid="uft", dtype_str="str")],
    )

    plan = compute_prune(contents)

    assert plan.objects == []
    assert plan.is_empty


def test_compute_prune_flags_reference_to_permanently_deleted_object() -> None:
    # The referenced type exists nowhere in the instance (neither live nor
    # trashed): it was permanently deleted, so the feature is dangling.
    contents = InstanceContents(
        features=[FeatureInfo(uid="uf", name="col", type_uid="gone", dtype_str="str")],
    )

    plan = compute_prune(contents)

    assert [o.uid for o in plan.objects] == ["uf"]
    ref = plan.objects[0].refs[0]
    assert ref.relation == "type"
    assert ref.target_uid == "gone"
    assert ref.target_missing is True
    assert ref.target_deleted is False
    assert plan.missing_count == 1


def test_compute_prune_ignores_reference_to_out_of_scope_live_object() -> None:
    # The referenced type is still live elsewhere in the instance (captured in
    # object_meta), just outside the prune scope: it must not be pruned.
    contents = InstanceContents(
        features=[FeatureInfo(uid="uf", name="col", type_uid="oos", dtype_str="str")],
        object_meta={
            "oos": ObjectMeta(name="group", created_by="bob", space_uid="otherSpace01")
        },
    )

    plan = compute_prune(contents)

    assert plan.objects == []
    assert plan.is_empty


def test_compute_prune_cascades_through_permanently_deleted_object() -> None:
    # feature -> permanently-deleted type; schema -> that feature. The feature is
    # dangling (missing target) and the schema cascades.
    contents = InstanceContents(
        features=[FeatureInfo(uid="uf", name="col", type_uid="gone", dtype_str="str")],
        schemas=[_schema("us", "S", feature_uids=["uf"])],
    )

    plan = compute_prune(contents)

    by_uid = {o.uid: o for o in plan.objects}
    assert set(by_uid) == {"uf", "us"}
    assert by_uid["uf"].refs[0].target_missing is True
    # the schema references a pruned (not missing, not trashed) member
    schema_ref = by_uid["us"].refs[0]
    assert schema_ref.target_missing is False
    assert schema_ref.target_deleted is False


def test_render_prune_marks_permanently_deleted_reference() -> None:
    contents = InstanceContents(
        features=[FeatureInfo(uid="uf", name="col", type_uid="gone", dtype_str="str")],
    )
    plan = compute_prune(contents)

    text = render_prune(plan, title="Prune dangling")

    assert "references via type -> gone (permanently deleted)" in text
    assert "1 permanently-deleted object(s)" in text


def test_compute_prune_flags_sheet_referencing_deleted_schema() -> None:
    contents = InstanceContents(
        record_types=[
            RecordTypeInfo(
                uid="urt",
                name="Sheet1",
                description=None,
                type_uid=None,
                schema_uid="us",
            )
        ],
        deleted_uids={"us"},
    )

    plan = compute_prune(contents)

    assert [o.kind for o in plan.objects] == ["sheet"]
    assert plan.objects[0].refs[0].relation == "schema"


def test_render_prune_lists_objects_and_summary() -> None:
    contents = InstanceContents(
        features=[
            FeatureInfo(
                uid="uf",
                name="col",
                type_uid="uft",
                dtype_str="str",
                type_name="group",
                created_by="laminbot",
            )
        ],
        deleted_uids={"uft"},
        deleted_names={"uft": "group"},
    )
    plan = compute_prune(contents)

    text = render_prune(plan, title="Prune dangling")

    assert "Prune dangling" in text
    # type shown as "'group' > 'col'", creator in brackets
    assert "feature 'group' > 'col'" in text
    assert "[laminbot]" in text
    # reference names the deleted target rather than only its uid
    assert "references via type -> group (deleted)" in text
    assert "1 dangling object(s)" in text


def test_render_prune_reports_clean_instance() -> None:
    plan = compute_prune(InstanceContents())

    text = render_prune(plan, title="Prune dangling")

    assert "nothing references a deleted object" in text


def test_compute_prune_carries_space_uid() -> None:
    contents = InstanceContents(
        feature_types=[FeatureTypeInfo(uid="uft", name="group", type_uid=None)],
        features=[
            FeatureInfo(
                uid="uf",
                name="col",
                type_uid="uft",
                dtype_str="str",
                space_uid="spaceUID0001",
            )
        ],
        deleted_uids={"uft"},
    )

    plan = compute_prune(contents)

    assert [o.uid for o in plan.objects] == ["uf"]
    assert plan.objects[0].space_uid == "spaceUID0001"


def test_render_prune_shows_space_uid() -> None:
    contents = InstanceContents(
        features=[
            FeatureInfo(
                uid="uf",
                name="col",
                type_uid="uft",
                dtype_str="str",
                space_uid="spaceUID0001",
            )
        ],
        deleted_uids={"uft"},
    )
    plan = compute_prune(contents)

    text = render_prune(plan, title="Prune dangling")

    assert "[space: spaceUID0001]" in text


# --- integration tests against a real instance ------------------------------


@create_plan
def _prune_seed_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("prune_ft")
    f = Feature("prune_col", dtype=LaminDType(str), type=ft)
    Schema(name="PruneSchema", features=[f])


def test_execute_prune_trashes_dangling_objects(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    compute_changes(_prune_seed_plan, mode=ApplyMode.APPLY, lock=None)

    # Trashing the feature type cascades to its feature; the schema that still
    # references that (now trashed) feature as a member is left dangling.
    ln.Feature.filter(name="prune_ft", is_type=True).one().delete()
    assert ln.Feature.filter(name="prune_col").one_or_none() is None
    assert ln.Schema.filter(name="PruneSchema").one_or_none() is not None

    contents = read_instance()
    plan = compute_prune(contents)

    assert [o.name for o in plan.objects] == ["PruneSchema"]
    assert plan.objects[0].refs[0].relation == "features"
    # the creator is captured from the instance
    assert plan.objects[0].created_by is not None
    # the dangling reference names the now-trashed member feature
    assert plan.objects[0].refs[0].target_name == "prune_col"

    deleted = execute_prune(plan)

    assert deleted == 1
    assert ln.Schema.filter(name="PruneSchema").one_or_none() is None
    # soft-deleted, so the record still exists unfiltered
    assert ln.Schema.objects.filter(name="PruneSchema").one_or_none() is not None


def test_execute_prune_permanent_deletes_irreversibly(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    compute_changes(_prune_seed_plan, mode=ApplyMode.APPLY, lock=None)
    ln.Feature.filter(name="prune_ft", is_type=True).one().delete()

    plan = compute_prune(read_instance())
    deleted = execute_prune(plan, permanent=True)

    assert deleted == 1
    assert ln.Schema.objects.filter(name="PruneSchema").one_or_none() is None
