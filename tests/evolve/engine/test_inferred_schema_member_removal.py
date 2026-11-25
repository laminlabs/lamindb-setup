"""Deleting a feature an inferred artifact schema still lists as a member.

Curating a `DataFrame` artifact records, next to the schema it was validated
against, an unnamed schema of the columns it carries.  That schema belongs to no
plan and validates nothing, so a feature the plan deletes is removed from it
(and the schema rehashed) instead of the deletion being refused.  Only a
`maximal_set` schema, one whose index the feature is, and any schema that is
not an inferred one still refuse it.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from lamindb_setup.evolve import Feature, FeatureType, LaminDType, Schema
from lamindb_setup.evolve.authoring._plan import PlanContext, create_plan
from lamindb_setup.evolve.engine._apply import (
    Action,
    ApplyMode,
    DriftError,
    compute_changes,
    write_lock,
)
from lamindb_setup.evolve.engine._lock import LockFile
from lamindb_setup.evolve.introspect._read import read_instance
from lamindb_setup.evolve.maintenance._prune import compute_prune
from lamindb_setup.evolve.render import render_changes

if TYPE_CHECKING:
    from types import SimpleNamespace as TestInstanceInfo
    from typing import Any as LamindbModuleType


def _plan(name: str, members: list[str], *, optional: list[str], schema: bool = True):
    """`IMSchema` over `members`, the `optional` ones marked optional.

    Optional members are what make lamindb record a schema of its own for a
    curated frame: the columns it records carry no optional marker.
    """

    @create_plan
    def _p(ctx: PlanContext, exports: None) -> None:
        ft = FeatureType("im_ft")
        features = {n: Feature(n, dtype=LaminDType(str), type=ft) for n in members}
        if schema:
            Schema(
                name="IMSchema",
                features=list(features.values()),
                optionals=[features[n] for n in optional] or None,
            )

    _p.name = name
    return _p


# Built at module scope: constructing a plan is refused once lamindb is imported.
_V1 = _plan(
    "im_v1", ["im_a", "im_b", "im_c", "im_x", "im_y"], optional=["im_c", "im_x", "im_y"]
)
_DROP_X = _plan(
    "im_drop_x", ["im_a", "im_b", "im_c", "im_y"], optional=["im_c", "im_y"]
)
_DROP_XY = _plan("im_drop_xy", ["im_a", "im_b", "im_c"], optional=["im_c"])
_ONLY_AB = _plan("im_only_ab", ["im_a", "im_b"], optional=[])
_NO_SCHEMA = _plan(
    "im_no_schema", ["im_a", "im_b", "im_c", "im_y"], optional=[], schema=False
)


@pytest.fixture
def writable_storage(ln: LamindbModuleType) -> None:
    """Point the cached instance's `Storage` row at where the run actually is."""
    import lamindb_setup as ln_setup

    storage = ln_setup.settings.storage
    root = storage.root_as_str
    ln.Storage.objects.filter(uid=Path(root).name).update(root=root)
    storage._record = None


def _seed(tmp_path: Path) -> LockFile:
    _V1.reset()
    applied = compute_changes(_V1, mode=ApplyMode.APPLY, lock=None)
    path = tmp_path / "im.lock"
    write_lock(applied, _V1, path)
    lock = LockFile.read(path)
    assert lock is not None
    return lock


def _curate(ln: LamindbModuleType, key: str, columns: list[str]) -> tuple[Any, Any]:
    """A frame with `columns` validated by `IMSchema`, and its inferred schema."""
    import pandas as pd
    from lamindb.models.schema import ArtifactSchema

    frame = pd.DataFrame({column: ["v"] for column in columns})
    schema = ln.Schema.get(name="IMSchema")
    artifact = ln.Artifact.from_dataframe(frame, key=key, schema=schema).save()
    inferred = ArtifactSchema.objects.get(artifact=artifact, slot="columns").schema
    assert inferred.name is None
    assert inferred.pk != schema.pk
    return artifact, inferred


def _run(plan: Any, lock: LockFile, mode: ApplyMode) -> Any:
    plan.reset()
    return compute_changes(
        plan, mode=mode, lock=lock, allow_data_validated_changes=True
    )


def _deletion(ctx: Any, name: str) -> Any:
    [change] = [c for c in ctx.changes if c.name == name and c.action is Action.DELETE]
    return change


def _inferred_detail(change: Any) -> str:
    [detail] = [d for d in change.details if "inferred artifact schema" in d]
    return detail


def _member_names(schema: Any) -> list[str]:
    return sorted(m.name for m in schema.members.all())


def _columns_link(ln: LamindbModuleType, artifact: Any) -> Any:
    from lamindb.models.schema import ArtifactSchema

    return ArtifactSchema.objects.get(artifact_id=artifact.pk, slot="columns")


def _fresh(ln: LamindbModuleType, schema: Any) -> Any:
    return ln.Schema.objects.get(pk=schema.pk)


def test_plan_reports_and_apply_removes_the_member(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
    tmp_path: Path,
    writable_storage: None,
) -> None:
    lock = _seed(tmp_path)
    _, inferred = _curate(ln, "im/one.parquet", ["im_a", "im_b", "im_c", "im_x"])
    old_hash = inferred.hash

    preview = _run(_DROP_X, lock, ApplyMode.PLAN)

    detail = _inferred_detail(_deletion(preview, "im_x"))
    assert detail == (
        f"removed from 1 inferred artifact schema: {inferred.uid!r} (1 artifact)"
    )
    assert detail in render_changes(preview, title="Plan")
    assert _member_names(_fresh(ln, inferred)) == ["im_a", "im_b", "im_c", "im_x"]

    applied = _run(_DROP_X, lock, ApplyMode.APPLY)

    assert _inferred_detail(_deletion(applied, "im_x")) == detail
    edited = _fresh(ln, inferred)
    assert _member_names(edited) == ["im_a", "im_b", "im_c"]
    assert edited.n_members == 3
    assert edited.hash != old_hash
    assert edited.branch_id != -1
    assert ln.Feature.filter(name="im_x", is_type=False).one_or_none() is None
    assert compute_prune(read_instance()).is_empty

    # The recomputed hash is the one lamindb gives these columns, so the next
    # artifact carrying them reuses the schema rather than duplicating it.
    _, again = _curate(ln, "im/two.parquet", ["im_a", "im_b", "im_c"])
    assert again.pk == inferred.pk

    path = tmp_path / "im2.lock"
    write_lock(applied, _DROP_X, path)
    rerun = _run(_DROP_X, LockFile.read(path), ApplyMode.APPLY)  # type: ignore[arg-type]
    assert {c.action for c in rerun.changes} == {Action.NO_CHANGE}


def test_a_maximal_set_inferred_schema_still_refuses(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
    tmp_path: Path,
    writable_storage: None,
) -> None:
    from django.db import transaction

    lock = _seed(tmp_path)
    _, inferred = _curate(ln, "im/strict.parquet", ["im_a", "im_b", "im_c", "im_x"])
    ln.Schema.objects.filter(pk=inferred.pk).update(maximal_set=True)

    for mode in (ApplyMode.PLAN, ApplyMode.APPLY):
        with pytest.raises(DriftError) as excinfo:
            with transaction.atomic():
                _run(_DROP_X, lock, mode)
        message = str(excinfo.value)
        assert "still declares it as a member" in message
        assert f"unnamed schema {inferred.uid!r} automatically" in message
        assert "maximal_set=True" in message
        assert "'im/strict.parquet'" in message

    assert _member_names(_fresh(ln, inferred)) == ["im_a", "im_b", "im_c", "im_x"]
    assert ln.Feature.filter(name="im_x", is_type=False).one_or_none() is not None


def _unnamed_link(ln: LamindbModuleType, schema: Any, key: str) -> Any:
    """Record `schema` as the columns of a fresh, uncurated artifact."""
    import pandas as pd
    from lamindb.models.schema import ArtifactSchema

    frame = pd.DataFrame({"anything": ["v"]})
    artifact = ln.Artifact.from_dataframe(frame, key=key).save()
    ArtifactSchema(artifact=artifact, schema=schema, slot="columns").save()
    return artifact


def test_the_inferred_schemas_index_still_refuses(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
    tmp_path: Path,
    writable_storage: None,
) -> None:
    lock = _seed(tmp_path)
    a, x = (ln.Feature.get(name=n) for n in ("im_a", "im_x"))
    keyed = ln.Schema(features=[a], index=x).save()
    _unnamed_link(ln, keyed, "im/keyed.parquet")

    with pytest.raises(DriftError, match="the feature is its index"):
        _run(_DROP_X, lock, ApplyMode.PLAN)


def test_an_unnamed_schema_that_is_not_inferred_still_refuses(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
    tmp_path: Path,
    writable_storage: None,
) -> None:
    lock = _seed(tmp_path)
    a, x = (ln.Feature.get(name=n) for n in ("im_a", "im_x"))
    loose = ln.Schema(features=[a, x]).save()

    with pytest.raises(DriftError) as excinfo:
        _run(_DROP_X, lock, ApplyMode.PLAN)

    message = str(excinfo.value)
    assert f"{loose.uid!r} still declares it as a member" in message
    assert "no artifact records it as an inferred schema" in message


def test_a_collision_moves_the_links_and_trashes_the_redundant_schema(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
    tmp_path: Path,
    writable_storage: None,
) -> None:
    lock = _seed(tmp_path)
    artifact, inferred = _curate(
        ln, "im/merge.parquet", ["im_a", "im_b", "im_c", "im_x"]
    )
    a, b, c = (ln.Feature.get(name=n) for n in ("im_a", "im_b", "im_c"))
    other = ln.Schema(name="IMOther", features=[a, b, c]).save()

    preview = _run(_DROP_X, lock, ApplyMode.PLAN)

    detail = _inferred_detail(_deletion(preview, "im_x"))
    assert f"then the same as schema 'IMOther' ({other.uid!r})" in detail
    assert _columns_link(ln, artifact).schema_id == inferred.pk

    _run(_DROP_X, lock, ApplyMode.APPLY)

    assert _columns_link(ln, artifact).schema_id == other.pk
    redundant = _fresh(ln, inferred)
    assert redundant.branch_id == -1
    # Trashed as it was recorded, not rewritten.
    assert redundant.hash == inferred.hash
    assert _member_names(_fresh(ln, other)) == ["im_a", "im_b", "im_c"]
    assert compute_prune(read_instance()).is_empty


def test_the_plans_own_schema_taking_the_content_takes_the_links(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
    tmp_path: Path,
    writable_storage: None,
) -> None:
    # The usual way into a collision: the export's columns differ from its
    # schema only by the optional members the plan now drops.  The preview has
    # to foresee the plan's schema holding that content once applied.
    lock = _seed(tmp_path)
    artifact, inferred = _curate(ln, "im/own.parquet", ["im_a", "im_b", "im_x"])

    preview = _run(_ONLY_AB, lock, ApplyMode.PLAN)

    assert "then the same as schema 'IMSchema'" in _inferred_detail(
        _deletion(preview, "im_x")
    )

    _run(_ONLY_AB, lock, ApplyMode.APPLY)

    schema = ln.Schema.get(name="IMSchema")
    assert _columns_link(ln, artifact).schema_id == schema.pk
    assert _fresh(ln, inferred).branch_id == -1
    assert compute_prune(read_instance()).is_empty


def test_inferred_schemas_converging_on_one_content_are_merged(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
    tmp_path: Path,
    writable_storage: None,
) -> None:
    lock = _seed(tmp_path)
    first, older = _curate(ln, "im/x.parquet", ["im_a", "im_b", "im_c", "im_x"])
    second, newer = _curate(
        ln, "im/xy.parquet", ["im_a", "im_b", "im_c", "im_x", "im_y"]
    )

    preview = _run(_DROP_XY, lock, ApplyMode.PLAN)

    x_detail = _inferred_detail(_deletion(preview, "im_x"))
    assert x_detail.startswith("removed from 2 inferred artifact schemas: ")
    assert f"then the same as unnamed schema {older.uid!r}" in x_detail
    y_detail = _inferred_detail(_deletion(preview, "im_y"))
    assert y_detail.startswith("removed from 1 inferred artifact schema: ")

    _run(_DROP_XY, lock, ApplyMode.APPLY)

    assert _member_names(_fresh(ln, older)) == ["im_a", "im_b", "im_c"]
    assert _fresh(ln, newer).branch_id == -1
    assert _columns_link(ln, first).schema_id == older.pk
    assert _columns_link(ln, second).schema_id == older.pk
    assert compute_prune(read_instance()).is_empty


def test_an_inferred_schema_left_without_members_is_unlinked_and_trashed(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
    tmp_path: Path,
    writable_storage: None,
) -> None:
    from lamindb.models.schema import ArtifactSchema

    lock = _seed(tmp_path)
    x = ln.Feature.get(name="im_x")
    lone = ln.Schema(features=[x]).save()
    artifact = _unnamed_link(ln, lone, "im/lone.parquet")

    preview = _run(_DROP_X, lock, ApplyMode.PLAN)

    assert "no member left, so its links are dropped" in _inferred_detail(
        _deletion(preview, "im_x")
    )

    _run(_DROP_X, lock, ApplyMode.APPLY)

    assert not ArtifactSchema.objects.filter(artifact_id=artifact.pk).exists()
    assert _fresh(ln, lone).branch_id == -1
    assert compute_prune(read_instance()).is_empty


def test_a_feature_the_inferred_schema_lists_as_optional_is_unmarked_too(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
    tmp_path: Path,
    writable_storage: None,
) -> None:
    # The optional set is a bare uid list that lamindb hashes as it finds it, so
    # a uid left behind would be hashed as a member the schema no longer has.
    lock = _seed(tmp_path)
    a, x = (ln.Feature.get(name=n) for n in ("im_a", "im_x"))
    marked = ln.Schema(features=[a, x.with_config(optional=True)]).save()
    assert marked.optionals.get_uids() == [x.uid]
    _unnamed_link(ln, marked, "im/marked.parquet")

    _run(_DROP_X, lock, ApplyMode.APPLY)

    edited = _fresh(ln, marked)
    assert edited.optionals.get_uids() == []
    assert _member_names(edited) == ["im_a"]
    assert ln.Schema(features=[a]).pk == marked.pk


def test_a_schema_deleted_with_its_feature_does_not_block_it(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # The plan drops the schema and one of its features together.  The feature
    # goes first (it is the deeper key), while the schema is still live -- but
    # the schema is the plan's own and goes to the trash in the same pass.
    lock = _seed(tmp_path)

    for mode in (ApplyMode.PLAN, ApplyMode.APPLY):
        ctx = _run(_NO_SCHEMA, lock, mode)
        deleted = {c.name for c in ctx.changes if c.action is Action.DELETE}
        assert deleted == {"IMSchema", "im_x"}

    assert ln.Feature.filter(name="im_x", is_type=False).one_or_none() is None
    assert ln.Schema.filter(name="IMSchema").one_or_none() is None


def test_inferred_schema_save_failure_identifies_the_schema_and_deleted_feature(
    ln: LamindbModuleType, tmp_path: Path, writable_storage: None, monkeypatch
) -> None:
    from django.db import transaction
    from lamindb.errors import ValidationError
    from lamindb_setup.evolve.engine import _inferred_schemas as inferred_schemas

    lock = _seed(tmp_path)
    _, schema = _curate(ln, "im/failure.parquet", ["im_a", "im_b", "im_c", "im_x"])
    monkeypatch.setattr(inferred_schemas, "_NAMED_ARTIFACTS", 0)
    assert "1 more" in inferred_schemas._named_artifacts(schema)

    original_save = ln.Schema.save

    def refuse_inferred_save(current, *args, **kwargs):
        if current.pk == schema.pk:
            raise ValidationError("invalid member change")
        return original_save(current, *args, **kwargs)

    monkeypatch.setattr(ln.Schema, "save", refuse_inferred_save)
    with (
        pytest.raises(ValidationError, match="invalid member change") as exc,
        transaction.atomic(),
    ):
        _run(_DROP_X, lock, ApplyMode.APPLY)
    assert any(
        schema.uid in note and "1 deleted feature" in note
        for note in exc.value.__notes__
    )
    assert "im_x" in _member_names(_fresh(ln, schema))
