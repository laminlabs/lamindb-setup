"""Tests for lamindb_setup.evolve.maintenance._dedup."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from lamindb_setup.evolve import Feature, FeatureType, LaminDType, Schema
from lamindb_setup.evolve.authoring._plan import PlanContext, create_plan
from lamindb_setup.evolve.engine._apply import ApplyMode, compute_changes
from lamindb_setup.evolve.introspect._read import (
    FeatureInfo,
    FeatureTypeInfo,
    InstanceContents,
    SchemaInfo,
    read_instance,
)
from lamindb_setup.evolve.maintenance._dedup import (
    CONTENT_ABSORB,
    CONTENT_KEEP,
    CONTENT_MANUAL,
    ContentDuplicateGroup,
    ContentMember,
    DuplicatePlan,
    compute_content_duplicates,
    compute_duplicates,
    execute_dedup,
    render_dedup,
)

if TYPE_CHECKING:
    from types import SimpleNamespace as TestInstanceInfo
    from typing import Any as LamindbModuleType


def _feature(uid: str, name: str, **kwargs: object) -> FeatureInfo:
    defaults: dict[str, object] = {"type_uid": None, "dtype_str": "str"}
    defaults.update(kwargs)
    return FeatureInfo(uid=uid, name=name, **defaults)  # type: ignore[arg-type]


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


def test_compute_duplicates_finds_same_name_no_parent() -> None:
    contents = InstanceContents(
        features=[
            _feature("uf_old", "col", created_at="2024-01-01T00:00:00"),
            _feature("uf_new", "col", created_at="2024-06-01T00:00:00"),
        ],
    )

    plan = compute_duplicates(contents)

    assert len(plan.groups) == 1
    group = plan.groups[0]
    assert group.name == "col"
    assert group.kind == "feature"
    assert group.registry == "Feature"
    # newest is kept, oldest removed
    assert group.keep.uid == "uf_new"
    assert [m.uid for m in group.remove] == ["uf_old"]
    assert plan.remove_count == 1


def test_compute_duplicates_requires_same_parent() -> None:
    # same name but different parents -> not duplicates
    contents = InstanceContents(
        features=[
            _feature("uf_a", "col", type_uid="pa"),
            _feature("uf_b", "col", type_uid="pb"),
        ],
    )

    plan = compute_duplicates(contents)

    assert plan.is_empty


def test_compute_duplicates_groups_by_shared_parent() -> None:
    contents = InstanceContents(
        features=[
            _feature("uf_old", "col", type_uid="p", created_at="2024-01-01T00:00:00"),
            _feature("uf_new", "col", type_uid="p", created_at="2024-02-01T00:00:00"),
        ],
    )

    plan = compute_duplicates(contents)

    assert len(plan.groups) == 1
    assert plan.groups[0].type_uid == "p"
    assert plan.groups[0].keep.uid == "uf_new"
    assert [m.uid for m in plan.groups[0].remove] == ["uf_old"]


def test_compute_duplicates_keeps_newest_removes_all_older() -> None:
    contents = InstanceContents(
        features=[
            _feature("uf1", "col", created_at="2024-01-01T00:00:00"),
            _feature("uf2", "col", created_at="2024-03-01T00:00:00"),
            _feature("uf3", "col", created_at="2024-02-01T00:00:00"),
        ],
    )

    plan = compute_duplicates(contents)

    group = plan.groups[0]
    assert group.keep.uid == "uf2"
    # oldest first
    assert [m.uid for m in group.remove] == ["uf1", "uf3"]


def test_compute_duplicates_missing_created_at_sorts_oldest() -> None:
    contents = InstanceContents(
        features=[
            _feature("uf_dated", "col", created_at="2024-01-01T00:00:00"),
            _feature("uf_undated", "col", created_at=None),
        ],
    )

    plan = compute_duplicates(contents)

    group = plan.groups[0]
    # the dated one is newer than the undated one (treated as oldest)
    assert group.keep.uid == "uf_dated"
    assert [m.uid for m in group.remove] == ["uf_undated"]


def test_compute_duplicates_detects_schema_duplicates() -> None:
    contents = InstanceContents(
        schemas=[
            _schema("us_old", "S", created_at="2024-01-01T00:00:00"),
            _schema("us_new", "S", created_at="2024-02-01T00:00:00"),
        ],
    )

    plan = compute_duplicates(contents)

    assert len(plan.groups) == 1
    assert plan.groups[0].registry == "Schema"
    assert plan.groups[0].keep.uid == "us_new"


def test_compute_duplicates_ignores_unnamed_schemas() -> None:
    contents = InstanceContents(
        schemas=[
            _schema("us1", None),
            _schema("us2", None),
        ],
    )

    plan = compute_duplicates(contents)

    assert plan.is_empty


def test_compute_duplicates_separates_types_from_instances() -> None:
    # a feature type and a feature with the same name+parent are different kinds
    contents = InstanceContents(
        feature_types=[FeatureTypeInfo(uid="uft", name="col", type_uid=None)],
        features=[_feature("uf", "col")],
    )

    plan = compute_duplicates(contents)

    assert plan.is_empty


def test_compute_duplicates_handles_feature_types() -> None:
    contents = InstanceContents(
        feature_types=[
            FeatureTypeInfo(
                uid="uft_old",
                name="grp",
                type_uid=None,
                created_at="2024-01-01T00:00:00",
            ),
            FeatureTypeInfo(
                uid="uft_new",
                name="grp",
                type_uid=None,
                created_at="2024-02-01T00:00:00",
            ),
        ],
    )

    plan = compute_duplicates(contents)

    assert len(plan.groups) == 1
    assert plan.groups[0].kind == "feature type"
    assert plan.groups[0].keep.uid == "uft_new"


def test_compute_duplicates_reports_nothing_when_unique() -> None:
    contents = InstanceContents(
        features=[_feature("uf1", "a"), _feature("uf2", "b")],
    )

    plan = compute_duplicates(contents)

    assert plan.is_empty


# --- rendering --------------------------------------------------------------


def test_render_dedup_lists_groups_and_summary() -> None:
    contents = InstanceContents(
        features=[
            _feature(
                "uf_old",
                "col",
                type_name="grp",
                created_by="alice",
                created_at="2024-01-01T00:00:00",
            ),
            _feature(
                "uf_new",
                "col",
                type_name="grp",
                created_by="bob",
                created_at="2024-06-01T00:00:00",
            ),
        ],
    )
    plan = compute_duplicates(contents)

    text = render_dedup(plan, title="Remove duplicates")

    assert "Remove duplicates" in text
    assert "feature 'grp' > 'col'" in text
    assert "keep" in text
    assert "remove" in text
    assert "uf_new" in text
    assert "uf_old" in text
    assert "created 2024-01-01T00:00:00" in text
    assert "1 object(s)" in text


def test_render_dedup_reports_clean_instance() -> None:
    plan = compute_duplicates(InstanceContents())

    text = render_dedup(plan, title="Remove duplicates")

    assert "no duplicate features or schemas found" in text


def test_render_dedup_shows_space_uid() -> None:
    contents = InstanceContents(
        features=[
            _feature("uf_old", "col", space_uid="spaceUID0001"),
            _feature("uf_new", "col", space_uid="spaceUID0001"),
        ],
    )
    plan = compute_duplicates(contents)

    text = render_dedup(plan, title="Remove duplicates")

    assert "[space: spaceUID0001]" in text


def test_render_dedup_lists_schemas_sharing_their_content() -> None:
    plan = DuplicatePlan(
        content_groups=[
            ContentDuplicateGroup(
                content_hash="hashAAAA",
                members=[
                    ContentMember("keepUID", "Kept", CONTENT_KEEP),
                    ContentMember(
                        "goneUID", None, CONTENT_ABSORB, "inferred: recorded columns"
                    ),
                ],
            ),
            ContentDuplicateGroup(
                content_hash="hashBBBB",
                members=[
                    ContentMember("oneUID", "One", CONTENT_MANUAL, "it is named 'One'"),
                    ContentMember("twoUID", "Two", CONTENT_MANUAL, "it is named 'Two'"),
                ],
                reason="decide by hand which to keep",
            ),
        ]
    )

    text = render_dedup(plan, title="Remove duplicates")

    assert "schema content 'hashAAAA'  (2 schemas share it)" in text
    assert "keep   keepUID 'Kept'" in text
    assert "absorb goneUID (inferred: recorded columns)" in text
    assert "manual oneUID 'One' (it is named 'One')" in text
    assert "needs a decision by hand: decide by hand which to keep" in text
    assert "1 inferred schema(s) to absorb" in text
    assert "1 group(s) of schemas sharing a content need a decision by hand" in text
    assert plan.has_changes and plan.absorb_count == 1
    assert not DuplicatePlan(content_groups=plan.manual_groups).has_changes


# --- integration tests against a real instance ------------------------------


@create_plan
def _dedup_seed_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("dedup_ft")
    f = Feature("dedup_col", dtype=LaminDType(str), type=ft)
    Schema(name="DedupSchema", features=[f])


def test_execute_dedup_trashes_older_duplicate(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    compute_changes(_dedup_seed_plan, mode=ApplyMode.APPLY, lock=None)

    # LaminDB's `.save()` is idempotent on the (name, type) natural key, so a
    # genuine duplicate has to be forced: create a distinctly-named feature, then
    # rename it to collide via a direct DB update that bypasses save-time dedup.
    original = ln.Feature.filter(name="dedup_col", is_type=False).first()
    assert original is not None
    duplicate = ln.Feature(name="dedup_col_tmp", dtype="str", type=original.type).save()
    ln.Feature.objects.filter(uid=duplicate.uid).update(name="dedup_col")
    assert duplicate.uid != original.uid

    contents = read_instance()
    plan = compute_duplicates(contents)

    # exactly one duplicate group for the feature name with two members
    feature_groups = [g for g in plan.groups if g.name == "dedup_col"]
    assert len(feature_groups) == 1
    group = feature_groups[0]
    assert group.keep.uid is not None
    assert len(group.remove) == 1

    keep_uid = group.keep.uid
    remove_uids = {m.uid for m in group.remove}

    deleted = execute_dedup(plan)

    assert deleted == plan.remove_count
    # the kept one is still live
    kept = ln.Feature.objects.filter(uid=keep_uid).one()
    assert kept.branch_id != -1
    # the removed one is soft-deleted (moved to trash, branch_id == -1)
    for uid in remove_uids:
        trashed = ln.Feature.objects.filter(uid=uid).one()
        assert trashed.branch_id == -1


def test_execute_dedup_permanent_deletes_irreversibly(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    ln.Feature(name="dup_perm", dtype="str").save()
    tmp = ln.Feature(name="dup_perm_tmp", dtype="str").save()
    ln.Feature.objects.filter(uid=tmp.uid).update(name="dup_perm")

    plan = compute_duplicates(read_instance())
    group = next(g for g in plan.groups if g.name == "dup_perm")
    remove_uids = {m.uid for m in group.remove}

    deleted = execute_dedup(plan, permanent=True)

    assert deleted >= 1
    for uid in remove_uids:
        assert ln.Feature.objects.filter(uid=uid).one_or_none() is None


# --- schemas sharing one content hash ----------------------------------------


@pytest.fixture
def writable_storage(ln: LamindbModuleType) -> None:
    """Point the cached instance's `Storage` row at where the run actually is."""
    import lamindb_setup as ln_setup

    storage = ln_setup.settings.storage
    root = storage.root_as_str
    ln.Storage.objects.filter(uid=Path(root).name).update(root=root)
    storage._record = None


def _curated(ln: LamindbModuleType, key: str, *columns: str) -> tuple[Any, Any]:
    """An artifact curated against a validator, and its inferred columns schema.

    The validator makes every column optional, so lamindb records the columns
    the frame carries -- all required -- as a new, unnamed schema.
    """
    import pandas as pd
    from lamindb.models.schema import ArtifactSchema

    features = [
        ln.Feature.filter(name=name).one_or_none()
        or ln.Feature(name=name, dtype=str).save()
        for name in ("cd_a", "cd_b", "cd_c")
    ]
    validator = ln.Schema(
        name="CdValidator", features=[f.with_config(optional=True) for f in features]
    ).save()
    frame = pd.DataFrame({column: ["x"] for column in columns})
    artifact = ln.Artifact.from_dataframe(frame, key=key, schema=validator).save()
    inferred = ArtifactSchema.objects.get(artifact=artifact, slot="columns").schema
    assert inferred.name is None
    return artifact, inferred


def _share_content(ln: LamindbModuleType, schema: Any, holder: Any) -> None:
    """Give `schema` the content hash of `holder`, as lamindb < 2.12 could."""
    ln.Schema.objects.filter(pk=schema.pk).update(hash=holder.hash)


def _actions(group: ContentDuplicateGroup) -> list[tuple[str, str]]:
    return [(member.uid, member.action) for member in group.members]


def test_content_duplicates_absorb_an_inferred_schema(
    instance: TestInstanceInfo, ln: LamindbModuleType, writable_storage: None
) -> None:
    from lamindb.models.schema import ArtifactSchema

    artifact, inferred = _curated(ln, "cd/one.parquet", "cd_a", "cd_b")
    a, c = ln.Feature.get(name="cd_a"), ln.Feature.get(name="cd_c")
    keeper = ln.Schema(name="CdKeeper", features=[a, c]).save()
    _share_content(ln, inferred, keeper)

    plan = compute_duplicates(read_instance())
    plan.content_groups = compute_content_duplicates()

    [group] = plan.content_groups
    assert not group.manual
    assert _actions(group) == [
        (str(inferred.uid), CONTENT_ABSORB),
        (str(keeper.uid), CONTENT_KEEP),
    ]
    assert group.absorbed[0].detail == "inferred: recorded columns of 1 artifact(s)"
    assert plan.absorb_count == 1
    assert "1 inferred schema(s) to absorb" in render_dedup(plan, title="t")

    assert execute_dedup(plan) == 0
    assert not ln.Schema.objects.filter(pk=inferred.pk).exists()
    link = ArtifactSchema.objects.get(artifact=artifact, slot="columns")
    assert link.schema_id == keeper.pk


def test_content_duplicates_keep_the_oldest_or_the_one_a_plan_manages(
    instance: TestInstanceInfo, ln: LamindbModuleType, writable_storage: None
) -> None:
    _, older = _curated(ln, "cd/one.parquet", "cd_a", "cd_b")
    _, newer = _curated(ln, "cd/two.parquet", "cd_a", "cd_c")
    _share_content(ln, newer, older)

    [group] = compute_content_duplicates()
    assert _actions(group) == [
        (str(older.uid), CONTENT_KEEP),
        (str(newer.uid), CONTENT_ABSORB),
    ]

    [group] = compute_content_duplicates(managed_uids={str(newer.uid)})
    assert _actions(group) == [
        (str(older.uid), CONTENT_ABSORB),
        (str(newer.uid), CONTENT_KEEP),
    ]

    assert compute_content_duplicates(skip_uids={str(newer.uid)}) == []
    space = older.space.uid
    assert len(compute_content_duplicates(space_uids=[space])) == 1
    assert compute_content_duplicates(exclude_space_uids=[space]) == []


def test_content_duplicates_no_schema_can_be_absorbed_into_are_left_alone(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    a = ln.Feature(name="cd_first", dtype=str).save()
    b = ln.Feature(name="cd_second", dtype=str).save()
    first = ln.Schema(name="CdFirst", features=[a]).save()
    second = ln.Schema(name="CdSecond", features=[b]).save()
    _share_content(ln, second, first)

    [group] = compute_content_duplicates()

    assert group.manual
    assert group.reason == (
        "none of them can be absorbed into another: decide by hand which to keep"
    )
    assert {member.action for member in group.members} == {CONTENT_MANUAL}
    assert "it is named 'CdFirst'" in group.members[0].detail
    plan = DuplicatePlan(content_groups=[group])
    assert not plan.has_changes
    execute_dedup(plan)
    assert ln.Schema.filter(name__in=["CdFirst", "CdSecond"]).count() == 2

    [group] = compute_content_duplicates(managed_uids={str(first.uid)})
    assert group.reason.startswith(
        f"a plan manages schema 'CdFirst' (uid {first.uid}): point what references "
        "the others at it"
    )
    [group] = compute_content_duplicates(managed_uids={str(first.uid), str(second.uid)})
    assert group.reason.startswith("plans manage more than one of them")


def test_content_duplicates_leave_an_inferred_schema_of_a_group_left_alone(
    instance: TestInstanceInfo, ln: LamindbModuleType, writable_storage: None
) -> None:
    _, inferred = _curated(ln, "cd/one.parquet", "cd_a", "cd_b")
    for name in ("CdFirst", "CdSecond"):
        feature = ln.Feature(name=f"{name.lower()}_col", dtype=str).save()
        _share_content(ln, ln.Schema(name=name, features=[feature]).save(), inferred)

    [group] = compute_content_duplicates()

    assert group.manual
    assert group.members[0] == ContentMember(
        str(inferred.uid),
        None,
        CONTENT_MANUAL,
        "an inferred schema, absorbed once the others are resolved",
    )
    assert not DuplicatePlan(content_groups=[group]).absorb_count
