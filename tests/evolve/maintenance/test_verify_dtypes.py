"""Tests for lamindb_setup.evolve.maintenance._verify_dtypes."""

from __future__ import annotations

from typing import TYPE_CHECKING

from lamindb_setup.evolve.introspect._read import (
    FeatureInfo,
    InstanceContents,
    RecordTypeInfo,
    read_instance,
)
from lamindb_setup.evolve.maintenance._verify_dtypes import (
    compute_verify_dtypes,
    execute_verify_dtypes,
    render_verify_dtypes,
)

if TYPE_CHECKING:
    from types import SimpleNamespace as TestInstanceInfo
    from typing import Any as LamindbModuleType


def _record_type(uid: str, name: str) -> RecordTypeInfo:
    return RecordTypeInfo(
        uid=uid,
        name=name,
        description=None,
        type_uid=None,
        schema_uid=None,
    )


# --- pure unit tests on hand-built InstanceContents -------------------------


def test_compute_verify_dtypes_rewrites_bare_cat() -> None:
    contents = InstanceContents(
        features=[FeatureInfo(uid="uf", name="col", type_uid=None, dtype_str="cat")]
    )

    plan = compute_verify_dtypes(contents)

    assert [i.uid for i in plan.issues] == ["uf"]
    issue = plan.issues[0]
    assert issue.fixable is True
    assert issue.current_dtype == "cat"
    assert issue.proposed_dtype == "cat[ULabel]"
    assert plan.fixable == [issue]
    assert plan.unfixable == []


def test_compute_verify_dtypes_resolves_record_name_to_uid() -> None:
    contents = InstanceContents(
        features=[
            FeatureInfo(
                uid="uf",
                name="col",
                type_uid=None,
                dtype_str="cat[Record[MyType]]",
            )
        ],
        record_types=[_record_type("rtUID0001", "MyType")],
    )

    plan = compute_verify_dtypes(contents)

    assert len(plan.issues) == 1
    issue = plan.issues[0]
    assert issue.fixable is True
    assert issue.proposed_dtype == "cat[Record[rtUID0001]]"
    assert issue.recordtype_name == "MyType"
    assert issue.recordtype_uid == "rtUID0001"


def test_compute_verify_dtypes_does_not_guess_between_duplicate_record_names() -> None:
    contents = InstanceContents(
        features=[
            FeatureInfo(
                uid="uf",
                name="col",
                type_uid=None,
                dtype_str="cat[Record[Duplicate]]",
            )
        ],
        record_types=[
            _record_type("rtUID0001", "Duplicate"),
            _record_type("rtUID0002", "Duplicate"),
        ],
    )

    plan = compute_verify_dtypes(contents)

    [issue] = plan.issues
    assert issue.fixable is False
    assert issue.requires_manual_resolution is True
    assert issue.recordtype_uid is None
    assert "multiple live record types" in issue.reason
    assert "rtUID0001" in issue.reason
    assert "rtUID0002" in issue.reason
    assert plan.deletable == []
    assert plan.manual == [issue]


def test_compute_verify_dtypes_does_not_guess_between_deleted_record_names() -> None:
    contents = InstanceContents(
        features=[
            FeatureInfo(
                uid="uf",
                name="col",
                type_uid=None,
                dtype_str="cat[Record[Duplicate]]",
            )
        ],
        deleted_record_uids={"rtUID0001", "rtUID0002"},
        deleted_record_names={
            "rtUID0001": "Duplicate",
            "rtUID0002": "Duplicate",
        },
    )

    plan = compute_verify_dtypes(contents)

    [issue] = plan.issues
    assert issue.fixable is False
    assert issue.requires_manual_resolution is True
    assert issue.recordtype_uid is None
    assert "multiple deleted record types" in issue.reason
    assert plan.deletable == []
    assert plan.manual == [issue]


def test_compute_verify_dtypes_skips_record_addressed_by_uid() -> None:
    contents = InstanceContents(
        features=[
            FeatureInfo(
                uid="uf",
                name="col",
                type_uid=None,
                dtype_str="cat[Record[rtUID0001]]",
            )
        ],
        record_types=[_record_type("rtUID0001", "MyType")],
    )

    plan = compute_verify_dtypes(contents)

    assert plan.issues == []
    assert plan.is_empty


def test_compute_verify_dtypes_skips_live_record_with_cat_filters() -> None:
    contents = InstanceContents(
        features=[
            FeatureInfo(
                uid="uf",
                name="col",
                type_uid=None,
                dtype_str=(
                    "cat[Record[rtUID0001, is_type='True', schema__uid='schUID0001']]"
                ),
            )
        ],
        record_types=[_record_type("rtUID0001", "MyType")],
    )

    plan = compute_verify_dtypes(contents)

    assert plan.issues == []
    assert plan.is_empty


def test_compute_verify_dtypes_resolves_name_and_preserves_filters() -> None:
    contents = InstanceContents(
        features=[
            FeatureInfo(
                uid="uf",
                name="col",
                type_uid=None,
                dtype_str="cat[Record[MyType, is_type='True']]",
            )
        ],
        record_types=[_record_type("rtUID0001", "MyType")],
    )

    plan = compute_verify_dtypes(contents)

    assert len(plan.issues) == 1
    issue = plan.issues[0]
    assert issue.fixable is True
    assert issue.proposed_dtype == "cat[Record[rtUID0001, is_type='True']]"
    assert issue.recordtype_name == "MyType"
    assert issue.recordtype_uid == "rtUID0001"


def test_compute_verify_dtypes_flags_unresolvable_record_name() -> None:
    contents = InstanceContents(
        features=[
            FeatureInfo(
                uid="uf",
                name="col",
                type_uid=None,
                dtype_str="cat[Record[Gone]]",
            )
        ],
    )

    plan = compute_verify_dtypes(contents)

    assert len(plan.issues) == 1
    issue = plan.issues[0]
    assert issue.fixable is False
    assert issue.proposed_dtype is None
    assert issue.recordtype_name == "Gone"
    assert issue.recordtype_uid is None
    assert issue.recordtype_deleted is False
    assert "no record type named 'Gone'" in issue.reason
    assert plan.unfixable == [issue]


def test_compute_verify_dtypes_flags_trashed_record_by_uid() -> None:
    contents = InstanceContents(
        features=[
            FeatureInfo(
                uid="uf",
                name="col",
                type_uid=None,
                dtype_str="cat[Record[rtUID0001]]",
            )
        ],
        deleted_record_uids={"rtUID0001"},
        deleted_record_names={"rtUID0001": "TrashedType"},
    )

    plan = compute_verify_dtypes(contents)

    assert len(plan.issues) == 1
    issue = plan.issues[0]
    assert issue.fixable is False
    assert issue.recordtype_deleted is True
    assert issue.recordtype_uid == "rtUID0001"
    assert issue.recordtype_name == "TrashedType"
    assert "exists but is deleted (trashed)" in issue.reason


def test_compute_verify_dtypes_flags_trashed_record_by_name() -> None:
    contents = InstanceContents(
        features=[
            FeatureInfo(
                uid="uf",
                name="col",
                type_uid=None,
                dtype_str="cat[Record[TrashedType]]",
            )
        ],
        deleted_record_uids={"rtUID0001"},
        deleted_record_names={"rtUID0001": "TrashedType"},
    )

    plan = compute_verify_dtypes(contents)

    assert len(plan.issues) == 1
    issue = plan.issues[0]
    assert issue.fixable is False
    assert issue.recordtype_deleted is True
    assert issue.recordtype_uid == "rtUID0001"
    assert issue.recordtype_name == "TrashedType"


def test_compute_verify_dtypes_uses_leading_uid_for_trashed_record() -> None:
    contents = InstanceContents(
        features=[
            FeatureInfo(
                uid="uf",
                name="col",
                type_uid=None,
                dtype_str="cat[Record[rtUID0001, is_type='True']]",
            )
        ],
        deleted_record_uids={"rtUID0001"},
        deleted_record_names={"rtUID0001": "TrashedType"},
    )

    plan = compute_verify_dtypes(contents)

    assert len(plan.issues) == 1
    assert plan.issues[0].recordtype_deleted is True
    assert plan.issues[0].recordtype_uid == "rtUID0001"


def test_compute_verify_dtypes_ignores_healthy_dtypes() -> None:
    contents = InstanceContents(
        features=[
            FeatureInfo(uid="u1", name="a", type_uid=None, dtype_str="str"),
            FeatureInfo(uid="u2", name="b", type_uid=None, dtype_str="cat[ULabel]"),
            FeatureInfo(uid="u3", name="c", type_uid=None, dtype_str=None),
        ],
    )

    plan = compute_verify_dtypes(contents)

    assert plan.issues == []


def test_render_verify_dtypes_lists_issues_and_summary() -> None:
    contents = InstanceContents(
        features=[
            FeatureInfo(
                uid="uf1",
                name="col",
                type_uid=None,
                dtype_str="cat",
                type_name="group",
                created_by="laminbot",
                space_uid="spaceUID0001",
            ),
            FeatureInfo(
                uid="uf2",
                name="bad",
                type_uid=None,
                dtype_str="cat[Record[Gone]]",
            ),
        ],
    )
    plan = compute_verify_dtypes(contents)

    text = render_verify_dtypes(plan, title="Verify dtypes")

    assert "Verify dtypes" in text
    assert "feature 'group' > 'col'" in text
    assert "[laminbot]" in text
    assert "[space: spaceUID0001]" in text
    assert "dtype 'cat' -> 'cat[ULabel]'" in text
    assert "cannot be fixed" in text
    assert "record type 'Gone' does not exist" in text
    assert "1 fixable" in text
    assert "1 to soft delete" in text


def test_render_verify_dtypes_marks_trashed_record_type() -> None:
    contents = InstanceContents(
        features=[
            FeatureInfo(
                uid="uf",
                name="bad",
                type_uid=None,
                dtype_str="cat[Record[rtUID0001]]",
            ),
        ],
        deleted_record_uids={"rtUID0001"},
        deleted_record_names={"rtUID0001": "TrashedType"},
    )
    plan = compute_verify_dtypes(contents)

    text = render_verify_dtypes(plan, title="Verify dtypes")

    assert "record type 'TrashedType' (rtUID0001) exists but is deleted (trashed)" in (
        text
    )


def test_render_verify_dtypes_reports_clean_instance() -> None:
    plan = compute_verify_dtypes(InstanceContents())

    text = render_verify_dtypes(plan, title="Verify dtypes")

    assert "no broken dtype found" in text


# --- integration tests against a real instance ------------------------------


def test_execute_verify_dtypes_fixes_and_deletes(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    record_type = ln.Record(name="VerifyRT", is_type=True).save()

    trashed_type = ln.Record(name="VerifyTrashedRT", is_type=True).save()
    trashed_uid = trashed_type.uid
    trashed_type.delete()

    bare = ln.Feature(name="verify_bare", dtype="cat[ULabel]").save()
    bare._dtype_str = "cat"
    bare.save()

    named = ln.Feature(name="verify_named", dtype="cat[ULabel]").save()
    named._dtype_str = "cat[Record[VerifyRT]]"
    named.save()

    filtered = ln.Feature(name="verify_filtered", dtype="cat[ULabel]").save()
    filtered._dtype_str = f"cat[Record[{record_type.uid}, is_type='True']]"
    filtered.save()

    trashed_ref = ln.Feature(name="verify_trashed", dtype="cat[ULabel]").save()
    trashed_ref._dtype_str = f"cat[Record[{trashed_uid}]]"
    trashed_ref.save()

    gone = ln.Feature(name="verify_gone", dtype="cat[ULabel]").save()
    gone._dtype_str = "cat[Record[NoSuchType]]"
    gone.save()

    plan = compute_verify_dtypes(read_instance())

    by_name = {i.name: i for i in plan.issues}
    # the live record type referenced by uid with cat filters is healthy
    assert "verify_filtered" not in by_name
    assert set(by_name) == {
        "verify_bare",
        "verify_named",
        "verify_trashed",
        "verify_gone",
    }
    assert by_name["verify_named"].proposed_dtype == f"cat[Record[{record_type.uid}]]"
    # the trashed record type is reported as deleted, not as nonexistent
    assert by_name["verify_trashed"].recordtype_deleted is True
    assert by_name["verify_trashed"].recordtype_uid == trashed_uid
    assert by_name["verify_gone"].recordtype_deleted is False

    fixed, deleted = execute_verify_dtypes(plan)

    assert (fixed, deleted) == (2, 2)
    assert ln.Feature.filter(name="verify_bare").one()._dtype_str == "cat[ULabel]"
    assert (
        ln.Feature.filter(name="verify_named").one()._dtype_str
        == f"cat[Record[{record_type.uid}]]"
    )
    # the unfixable features were soft-deleted (hidden from the default queryset)
    for name in ("verify_trashed", "verify_gone"):
        assert ln.Feature.filter(name=name).one_or_none() is None
        assert ln.Feature.objects.filter(name=name).one_or_none() is not None
